// Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/*
 * Authors:
 *   Jinlong Xuan <15563983051@163.com>
 *   Xu Ji <sov.matrixac@gmail.com>
 *   Yu Wang <wangyu.steph@bytedance.com>
 *   Bo Liu <liubo.2024@bytedance.com>
 *   Zhenwei Pi <pizhenwei@bytedance.com>
 *   Rui Zhang <zhangrui.1203@bytedance.com>
 *   Changqi Lu <luchangqi.123@bytedance.com>
 *   Enhua Zhou <zhouenhua@bytedance.com>
 */

#include <stdlib.h>
#include <sys/mman.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <sys/time.h>
#include <regex.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>

#include "kv.h"
#include "slab.h"
#include "buddy.h"
#include "crc.h"
#include "memory.h"

#include "priskv-threads.h"
#include "priskv-event.h"
#include "priskv-log.h"
#include "list.h"

#define MAX_EVICT_RETRIES 128

/*
 * biggest prime under 2^n:
 *
 * 32749 - 1048573: 15<=n<=20, step 1
 * 1048573 - 16777213: 20<=n<=24, step 2
 * 16777213 - 134217689: 24<=n<=27, step 3
 */
static const uint32_t primes[] = {32749,   65521,   131071,   262139,   524287,
                                  1048573, 4194301, 16777213, 134217689};
typedef struct priskv_hash_head {
    struct list_head head;
    pthread_spinlock_t lock;
} priskv_hash_head;

/**
 * when a request try lock fails, it is added to the pending queue corresponding to its crc hash
 * value. once the lock holder completes, the request can be re-initiated.
 */
typedef struct priskv_tiering_wait_head {
    struct list_head pending_reqs;
    bool has_inflight_req;
    pthread_spinlock_t lock;
} priskv_tiering_wait_head;

/* statics for expire routine */
typedef struct priskv_expire_routine_statics {
    uint64_t expire_routine_times; /* expire routine executed times */
    uint64_t expire_kv_count;      /* expired kv count in total */
    uint64_t expire_kv_bytes;      /* expired kv bytes in total */
} priskv_expire_routine_statics;

typedef struct priskv_kv {
    priskv_hash_head *hash_heads;
    priskv_tiering_wait_head *tiering_wait_heads;

    // lru head
    struct list_head lru_head;
    pthread_spinlock_t lru_lock;

    uint32_t bucket_count;
    uint32_t max_keys;
    uint16_t max_key_length;
    void *key_slab;    /* key handle of slab */
    uint8_t *key_base; /* key memory base address */

    void *value_buddy;                /* buddy handle of value */
    uint8_t *value_base;              /* buddy memory base address */
    int shm_fd;
    uint64_t shm_len;
    uint32_t expire_routine_interval; /* interval to run expire routine */
    priskv_expire_routine_statics expire_routine_statics;
    void *mf_ctx;
    struct {
        uint64_t pin_ops;
        uint64_t unpin_ops;
        uint64_t unpin_not_closed;
    } pin_stats;
} priskv_kv;

/*
 * TODO(wangyi): Implement PinTTL cleanup mechanism
 *
 * Context:
 * - Pin operations (PIN_ON_ACQUIRE, PIN_ON_SEAL) increase pin_count on the latest visible
 *   version to protect keys from eviction. If a consumer crashes or a request fails, some
 *   pin operations may never be closed by UNPIN (e.g., RELEASE with UNPIN_ON_RELEASE), leaving
 *   keys indefinitely pinned.
 *
 * Goals:
 * - Introduce a best-effort TTL-based cleanup for orphaned pins so that keys are eventually
 *   unpinned when their associated requests are gone.
 * - Maintain multi-version correctness: UNPIN targets the latest version even if the pin was
 *   created before a SEAL publish that replaced the visible version.
 *
 * Proposed design:
 * - PinOperator: a lightweight record created on each effective pin, containing:
 *     - key (bytes + length)
 *     - creation timestamp (monotonic clock)
 *     - ttl_ms (configurable per pin or global default)
 *     - optional origin (ACQUIRE or SEAL) and a debug request_id for observability
 * - PinManager: per-bucket or global manager storing PinOperator entries in an expiry-ordered
 *   min-heap or timing-wheel to enable O(logN) insert and efficient batch expiry checks.
 * - On pin:
 *     - After pin_count++ on the targeted keynode, create and register a PinOperator.
 * - On unpin:
 *     - Remove the corresponding PinOperator (match by key); then decrement pin_count on latest
 *       version using priskv_key_unpin_latest semantics. Multiple pins on the same key will
 *       have multiple PinOperator entries.
 * - On seal (version migration):
 *     - pin_count is already inherited to the new version; PinOperator records keep referencing
 *       the key (not the keynode pointer), so no migration is required.
 * - Scheduling:
 *     - Reuse expire routine infrastructure (timerfd) to periodically check PinManager and
 *       perform cleanup for expired entries. Each expired PinOperator triggers
 *       priskv_key_unpin_latest(kv, old_keynode_of_record) on the latest version by key.
 *     - Consider sharding the PinManager by hash-bucket index to minimize global contention.
 * - Concurrency & locking:
 *     - PinOperator insert/remove should use per-bucket spinlocks consistent with hash-head
 *       protection, avoiding deadlocks by keeping lock order (manager lock -> keynode lock).
 * - Configuration:
 *     - Provide a global default TTL (e.g., kv->pin_ttl_ms) and allow per-request override via
 *       request flags or auxiliary fields in the protocol header (future extension).
 * - Observability & safeguards:
 *     - Export counters: pin_ttl_active, pin_ttl_expired, pin_ttl_cleanup_ops, pin_ttl_orphaned.
 *     - Cap the maximum number of active PinOperator entries to prevent memory blow-up; when
 *       exceeding the cap, log warnings and fallback to immediate unpin or refuse new pins.
 * - Recovery:
 *     - PinTTL metadata is best-effort and may be non-persistent. After restart, keys might
 *       remain pinned by pin_count; scheduled cleanup resumes with new PinOperator records for
 *       future pins. Persistent logging can be considered if stronger guarantees are needed.
 *
 * Next steps:
 * - Add PinManager data structures and lifecycle APIs.
 * - Integrate with pin/unpin paths and expire routine scheduling.
 * - Extend info endpoints to expose PinTTL metrics.
 */

static void priskv_lru_access(priskv_key *keynode, bool is_in_list)
{
    priskv_kv *kv = keynode->kv;

    pthread_spin_lock(&kv->lru_lock);
    if (is_in_list) {
        list_del(&keynode->lru_entry);
    }
    list_add(&kv->lru_head, &keynode->lru_entry);
    pthread_spin_unlock(&kv->lru_lock);
}

static priskv_key *priskv_lru_evict(priskv_kv *kv)
{
    priskv_key *candidate = NULL, *node;

    pthread_spin_lock(&kv->lru_lock);
    /* iterate from tail backwards to find an evictable node */
    list_for_each_rev(&kv->lru_head, node, lru_entry)
    {
        /* Check eviction eligibility: not pinned and no extra references */
        pthread_spin_lock(&node->lock);
        bool evictable = (node->pin_count == 0 && node->refcnt == 1);
        pthread_spin_unlock(&node->lock);
        if (evictable) {
            candidate = node;
            break;
        }
    }
    pthread_spin_unlock(&kv->lru_lock);

    return candidate;
}

static void priskv_lru_del_key(priskv_key *keynode)
{
    priskv_kv *kv = keynode->kv;

    pthread_spin_lock(&kv->lru_lock);
    list_del(&keynode->lru_entry);
    pthread_spin_unlock(&kv->lru_lock);
}

// binary search in primes[]
static inline uint32_t calculate_hash_bucket_count(uint32_t max_keys)
{
    uint32_t result;
    int mid;
    int left = 0, right = sizeof(primes) / sizeof(uint32_t) - 1;

    if (max_keys < primes[0]) {
        return max_keys;
    }

    result = primes[0];
    while (left <= right) {
        mid = left + (right - left) / 2;
        if (primes[mid] <= max_keys) {
            result = primes[mid];
            left = mid + 1;
        } else {
            right = mid - 1;
        }
    }

    return result;
}

void *priskv_new_kv(uint8_t *key_base, uint8_t *value_base, int shm_fd, uint64_t shm_len,
                    uint32_t max_keys, uint16_t max_key_length, uint32_t value_block_size,
                    uint64_t value_blocks, void *mf_ctx)
{
    priskv_kv *kv;
    uint32_t bucket_count;
    assert(key_base);
    assert(value_base);

    /* assert in startup step, it's ok */
    kv = calloc(1, sizeof(priskv_kv));
    assert(kv);

    /* step 1: allocate memory for hash tables */
    bucket_count = calculate_hash_bucket_count(max_keys);
    kv->hash_heads = priskv_mem_malloc(bucket_count * sizeof(priskv_hash_head),
                                       MAP_PRIVATE | MAP_ANONYMOUS, -1, true);
    assert(kv->hash_heads);
    for (uint32_t i = 0; i < bucket_count; i++) {
        priskv_hash_head *hash_head = &kv->hash_heads[i];
        list_head_init(&hash_head->head);
        pthread_spin_init(&hash_head->lock, 0);
    }

    /* step 2: allocate memory for tiering wait queue */
    kv->tiering_wait_heads = priskv_mem_malloc(bucket_count * sizeof(priskv_tiering_wait_head),
                                               MAP_PRIVATE | MAP_ANONYMOUS, -1, true);
    assert(kv->tiering_wait_heads);
    for (uint32_t i = 0; i < bucket_count; i++) {
        priskv_tiering_wait_head *tiering_wait_head = &kv->tiering_wait_heads[i];
        list_head_init(&tiering_wait_head->pending_reqs);
        tiering_wait_head->has_inflight_req = false;
        pthread_spin_init(&tiering_wait_head->lock, 0);
    }

    /* step 3: init lru head */
    list_head_init(&kv->lru_head);
    pthread_spin_init(&kv->lru_lock, 0);

    /* step 4: create slab for keys */
    kv->expire_routine_interval = PRISKV_KV_DEFAULT_EXPIRE_ROUTINE_INTERVAL;
    kv->bucket_count = bucket_count;
    kv->max_keys = max_keys;
    kv->max_key_length = max_key_length;
    kv->key_base = key_base;
    kv->key_slab =
        priskv_slab_create("Keys", kv->key_base, priskv_mem_key_size(max_key_length), max_keys);
    assert(kv->key_slab);

    /* step 5: create buddy for values */
    kv->value_base = value_base;
    kv->shm_fd = shm_fd;
    kv->shm_len = shm_len;
    kv->value_buddy = priskv_buddy_create(value_base, value_blocks, value_block_size);
    assert(kv->value_base == priskv_buddy_base(kv->value_buddy));

    kv->mf_ctx = mf_ctx;
    kv->pin_stats.pin_ops = 0;
    kv->pin_stats.unpin_ops = 0;
    kv->pin_stats.unpin_not_closed = 0;

    priskv_log_notice("KV: max_key %d, max_key_length %d, value_block_size %d, value_blocks %ld\n",
                    max_keys, max_key_length, value_block_size, value_blocks);

    return kv;
}

void priskv_destroy_kv(void *_kv)
{
    priskv_kv *kv = _kv;

    priskv_buddy_destroy(kv->value_buddy);
    priskv_slab_destroy(kv->key_slab);
    priskv_mem_free(kv->hash_heads, kv->bucket_count * sizeof(priskv_hash_head), true);
    // TODO: free pending requests
    priskv_mem_free(kv->tiering_wait_heads, kv->bucket_count * sizeof(priskv_tiering_wait_head), true);
    free(kv);
}

void *priskv_get_value_base(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->value_base;
}

int priskv_get_shm_fd(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->shm_fd;
}

uint64_t priskv_get_shm_length(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->shm_len;
}

uint64_t priskv_get_value_blocks(void *_kv)
{
    priskv_kv *kv = _kv;

    return priskv_buddy_nmemb(kv->value_buddy);
}

uint32_t priskv_get_value_block_size(void *_kv)
{
    priskv_kv *kv = _kv;

    return priskv_buddy_size(kv->value_buddy);
}

uint64_t priskv_get_value_blocks_inuse(void *_kv)
{
    priskv_kv *kv = _kv;

    return priskv_buddy_inuse(kv->value_buddy);
}

static uint8_t *priskv_value_to_pointer(priskv_kv *kv, priskv_key *keynode)
{
    return kv->value_base + keynode->value_off;
}

static uint64_t priskv_pointer_to_value(priskv_kv *kv, uint8_t *val)
{
    return val - kv->value_base;
}

/*
 * Life cycle of keynode:
 * 1. [SET] refcnt++  ->  [DELETE] refcnt--
 * 2. [GET start] refcnt++ -> [GET end] refcnt--
 */
static void priskv_keynode_ref(priskv_key *keynode)
{
    pthread_spin_lock(&keynode->lock);
    keynode->refcnt++;
    pthread_spin_unlock(&keynode->lock);
}

static void priskv_keynode_deref(priskv_key *keynode)
{
    priskv_kv *kv = keynode->kv;
    bool need_free = false;

    pthread_spin_lock(&keynode->lock);
    need_free = --keynode->refcnt == 0;
    pthread_spin_unlock(&keynode->lock);

    if (need_free) {
        priskv_buddy_free(kv->value_buddy, priskv_value_to_pointer(kv, keynode));
        memset(keynode, 0x00, priskv_slab_size(kv->key_slab));
        priskv_slab_free(kv->key_slab, keynode);
    }
}

void priskv_update_valuelen(void *arg, uint32_t valuelen)
{
    priskv_key *keynode = arg;
    
    pthread_spin_lock(&keynode->lock);
    keynode->valuelen = valuelen;
    pthread_spin_unlock(&keynode->lock);
}

static inline bool priskv_key_timeout(priskv_key *keynode, struct timeval now)
{
    if (keynode->expire_time.tv_sec < 0 || keynode->expire_time.tv_usec < 0) {
        return false;
    }

    return priskv_time_elapsed_ms(keynode->expire_time, now) > 0;
}

static priskv_key *priskv_find_key(priskv_kv *kv, uint8_t *key, uint16_t keylen, uint64_t timeout,
                               bool pop, bool *expired)
{
    uint32_t crc = priskv_crc32(key, keylen);
    priskv_hash_head *hash_head;
    priskv_key *keynode;
    struct timeval now;

    gettimeofday(&now, NULL);
    hash_head = &kv->hash_heads[crc % kv->bucket_count];
    pthread_spin_lock(&hash_head->lock);
    list_for_each (&hash_head->head, keynode, entry) {
        if (keynode->keylen != keylen) {
            continue;
        }

        if (!memcmp(keynode->key, key, keylen)) {
            if (pop) {
                /* pop anyway, don't check expired time */
                list_del(&keynode->entry);
            } else if (priskv_key_timeout(keynode, now)) {
                /* key expired */
                *expired = true;
                list_del(&keynode->entry);
            } else {
                /* update expire_time, only for EXPIRE syntax */
                if (timeout < PRISKV_KEY_MAX_TIMEOUT) {
                    priskv_time_add_ms(&now, timeout);
                    keynode->expire_time = now;
                }
                priskv_keynode_ref(keynode);
            }

            pthread_spin_unlock(&hash_head->lock);
            return keynode;
        }
    }
    pthread_spin_unlock(&hash_head->lock);

    return NULL;
}

/*
 * Apply a delta to pin_count on the latest visible version of a key.
 * - Look up the latest version under the hash-bucket lock.
 * - If the key is expired, remove it and return NO_SUCH_KEY.
 * - For delta > 0: increment pin_count by delta.
 * - For delta < 0: decrement pin_count by |delta| if possible; otherwise return
 *   PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED without modifying pin_count.
 * - Does not update kv->pin_stats; the caller is responsible for stats accounting.
 */
static priskv_resp_status priskv_pin_count_delta_latest(priskv_kv *kv, uint8_t *key,
                                                        uint16_t keylen, int32_t delta);

static void __priskv_del_key(priskv_kv *kv, priskv_key *keynode)
{
    priskv_keynode_deref(keynode);
}

static priskv_resp_status priskv_pin_count_delta_latest(priskv_kv *kv, uint8_t *key,
                                                        uint16_t keylen, int32_t delta)
{
    if (!kv || !key || !keylen) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    /* Locate bucket and the latest visible node for the key */
    uint32_t crc = priskv_crc32(key, keylen);
    priskv_hash_head *hash_head = &kv->hash_heads[crc % kv->bucket_count];

    struct timeval now;
    gettimeofday(&now, NULL);

    pthread_spin_lock(&hash_head->lock);
    priskv_key *cur; priskv_key *latest = NULL;
    list_for_each (&hash_head->head, cur, entry) {
        if (cur->keylen == keylen && memcmp(cur->key, key, keylen) == 0) {
            latest = cur;
            break;
        }
    }

    if (latest == NULL) {
        pthread_spin_unlock(&hash_head->lock);
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    /* If expired, remove from hash while holding the bucket lock and cleanup outside */
    if (priskv_key_timeout(latest, now)) {
        list_del(&latest->entry);
        pthread_spin_unlock(&hash_head->lock);
        priskv_lru_del_key(latest);
        __priskv_del_key(kv, latest);
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    priskv_resp_status resp = PRISKV_RESP_STATUS_OK;
    pthread_spin_lock(&latest->lock);
    if (delta >= 0) {
        latest->pin_count += (uint32_t)delta;
    } else {
        uint32_t need = (uint32_t)(-delta);
        if (latest->pin_count >= need) {
            latest->pin_count -= need;
        } else {
            /* Not enough pins closed by the caller */
            resp = PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED;
        }
    }
    pthread_spin_unlock(&latest->lock);
    pthread_spin_unlock(&hash_head->lock);

    return resp;
}

int priskv_get_key_for_seal(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val,
                            uint32_t *valuelen, void **_keynode)
{
    return priskv_get_key_base(_kv, key, keylen, val, valuelen, _keynode, true /* for_seal */);
}

int priskv_get_key(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val, uint32_t *valuelen,
                   void **_keynode)
{
    return priskv_get_key_base(_kv, key, keylen, val, valuelen, _keynode, false /* for_seal */);
}

int priskv_get_key_base(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val, uint32_t *valuelen,
                        void **_keynode, bool for_seal)
{
    priskv_kv *kv = _kv;
    bool expired = false;
    priskv_key *keynode = priskv_find_key(kv, key, keylen, PRISKV_KEY_MAX_TIMEOUT, false, &expired);

    *_keynode = NULL;

    if (!keynode) {
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    if (expired) {
        priskv_lru_del_key(keynode);
        __priskv_del_key(kv, keynode);
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    *_keynode = keynode;

    if (keynode->inprocess) {
        if (!for_seal) {
            return PRISKV_RESP_STATUS_KEY_UPDATING;
        }
    }

    *val = priskv_value_to_pointer(kv, keynode);
    *valuelen = keynode->valuelen;

    // move key-value to head of lru list before rdma WRITE,
    // so reference count of key-value in the tail of lru list
    // probably be 0
    priskv_lru_access(keynode, true);

    return PRISKV_RESP_STATUS_OK;
}

void priskv_get_key_end(void *arg)
{
    priskv_key *keynode = arg;

    if (!keynode) {
        return;
    }

    priskv_keynode_deref(keynode);
}

static inline void priskv_insert_keynode(priskv_kv *kv, priskv_key *keynode)
{
    /* inster key into hash list */
    uint32_t crc = priskv_crc32(keynode->key, keynode->keylen);
    priskv_hash_head *hash_head = &kv->hash_heads[crc % kv->bucket_count];

    pthread_spin_lock(&hash_head->lock);
    list_add_tail(&hash_head->head, &keynode->entry);
    pthread_spin_unlock(&hash_head->lock);
}

// TODO: fix race condition
int priskv_set_key(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val, uint32_t valuelen,
                 uint64_t timeout, void **_keynode)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = NULL, *old_keynode;
    uint8_t *vaddr = NULL;
    int retries = 0;

    /* check parameters */
    old_keynode = priskv_find_key(kv, key, keylen, PRISKV_KEY_MAX_TIMEOUT, true, NULL);
    if (old_keynode) {
        /* free the old one */
        priskv_lru_del_key(old_keynode);
        __priskv_del_key(kv, old_keynode);
    }

    keynode = (priskv_key *)priskv_slab_alloc(kv->key_slab);
    vaddr = priskv_buddy_alloc(kv->value_buddy, valuelen);
    while (!vaddr || !keynode) {
        if (retries++ > MAX_EVICT_RETRIES) {
            priskv_log_warn("KV: failed to allocate key-value after %d evict retries\n", retries);
            goto out;
        }

        old_keynode = priskv_lru_evict(kv);
        if (!old_keynode) {
            priskv_log_warn("KV: failed to allocate key-value due to no key-values to evict\n",
                          retries);
            goto out;
        }

        old_keynode = priskv_find_key(kv, (uint8_t *)old_keynode->key, old_keynode->keylen,
                                    PRISKV_KEY_MAX_TIMEOUT, true, NULL);

        if (!old_keynode) {
            continue;
        }

        // probably delete immediately.
        priskv_lru_del_key(old_keynode);
        __priskv_del_key(kv, old_keynode);

        if (!keynode) {
            keynode = (priskv_key *)priskv_slab_alloc(kv->key_slab);
        }
        if (!vaddr) {
            vaddr = priskv_buddy_alloc(kv->value_buddy, valuelen);
        }
    }

    /* mark this keynode as inprocess state. */
    keynode->inprocess = true;
    if (timeout < PRISKV_KEY_MAX_TIMEOUT) {
        gettimeofday(&keynode->expire_time, NULL);
        priskv_time_add_ms(&keynode->expire_time, timeout);
    } else {
        keynode->expire_time.tv_sec = -1;
        keynode->expire_time.tv_usec = -1;
    }

    list_node_init(&keynode->entry);
    list_node_init(&keynode->lru_entry);
    keynode->kv = kv;
    keynode->keylen = keylen;
    keynode->value_off = priskv_pointer_to_value(kv, vaddr);
    keynode->valuelen = valuelen;
    memcpy(keynode->key, key, keylen);
    keynode->refcnt = 0;
    keynode->pin_count = 0;
    pthread_spin_init(&keynode->lock, 0);
    priskv_keynode_ref(keynode);

    priskv_lru_access(keynode, false);
    priskv_insert_keynode(kv, keynode);

    *val = vaddr;
    *_keynode = keynode;
    return PRISKV_RESP_STATUS_OK;
out:
    if (vaddr) {
        priskv_buddy_free(kv->value_buddy, vaddr);
    }
    if (keynode) {
        priskv_slab_free(kv->key_slab, keynode);
    }
    *_keynode = NULL;
    return PRISKV_RESP_STATUS_NO_MEM;
}

void priskv_set_key_end(void *arg)
{
    priskv_key *keynode = arg;

    if (!keynode) {
        return;
    }

    keynode->inprocess = false;
}

/*
 * Allocate a private keynode for zero-copy write (ALLOC), without publishing to the hash table.
 * - Do not delete the same-named published key to avoid overwriting during the unpublished phase;
 *   replacement happens during publish (SEAL).
 * - Set inprocess=true, record expiry and length, and return the value address for client write.
 * - Initial refcnt=1 (held by the token), and do not join LRU to avoid eviction.
 */
int priskv_alloc_node_private(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val,
                              uint32_t alloc_length, uint64_t timeout, void **_keynode)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = NULL, *old_keynode;
    uint8_t *vaddr = NULL;
    int retries = 0;

    /* Parameter check: zero alloc_length is invalid */
    if (!alloc_length) {
        *_keynode = NULL;
        return PRISKV_RESP_STATUS_VALUE_EMPTY;
    }

    /* Allocate new keynode and value space; try LRU eviction if necessary */
    keynode = (priskv_key *)priskv_slab_alloc(kv->key_slab);
    vaddr = priskv_buddy_alloc(kv->value_buddy, alloc_length);
    while (!vaddr || !keynode) {
        if (retries++ > MAX_EVICT_RETRIES) {
            priskv_log_warn("KV: private alloc failed after %d evict retries\n", retries);
            goto out_nomem;
        }

        old_keynode = priskv_lru_evict(kv);
        if (!old_keynode) {
            priskv_log_warn("KV: private alloc failed, no key-values to evict\n");
            goto out_nomem;
        }

        old_keynode = priskv_find_key(kv, (uint8_t *)old_keynode->key, old_keynode->keylen,
                                      PRISKV_KEY_MAX_TIMEOUT, true, NULL);
        if (old_keynode) {
            priskv_lru_del_key(old_keynode);
            __priskv_del_key(kv, old_keynode);
        }

        if (!keynode) {
            keynode = (priskv_key *)priskv_slab_alloc(kv->key_slab);
        }
        if (!vaddr) {
            vaddr = priskv_buddy_alloc(kv->value_buddy, alloc_length);
        }
    }

    /* Initialize new keynode, but do NOT insert into hash/LRU */
    list_node_init(&keynode->entry);
    list_node_init(&keynode->lru_entry);
    keynode->kv = kv;
    keynode->keylen = keylen;
    keynode->value_off = priskv_pointer_to_value(kv, vaddr);
    keynode->valuelen = alloc_length;
    memcpy(keynode->key, key, keylen);
    keynode->inprocess = true;
    if (timeout < PRISKV_KEY_MAX_TIMEOUT) {
        gettimeofday(&keynode->expire_time, NULL);
        priskv_time_add_ms(&keynode->expire_time, timeout);
    } else {
        keynode->expire_time.tv_sec = -1;
        keynode->expire_time.tv_usec = -1;
    }
    keynode->refcnt = 0;
    keynode->pin_count = 0;
    pthread_spin_init(&keynode->lock, 0);
    priskv_keynode_ref(keynode); /* Initial reference held by the token */

    *val = vaddr;
    *_keynode = keynode;
    return PRISKV_RESP_STATUS_OK;

out_nomem:
    if (vaddr) {
        priskv_buddy_free(kv->value_buddy, vaddr);
    }
    if (keynode) {
        priskv_slab_free(kv->key_slab, keynode);
    }
    *_keynode = NULL;
    return PRISKV_RESP_STATUS_NO_MEM;
}

/*
 * Publish a private keynode (SEAL):
 * - If a same-named published key exists, delete the old node (pop+free) first, then insert the
 *   new node into the hash table.
 * - Join LRU and clear inprocess so it becomes readable (ACQUIRE/GET).
 */
int priskv_publish_node_with_pin(void *_kv, void *_keynode, bool pin_on_publish)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = (priskv_key *)_keynode;
    priskv_key *old_keynode = NULL;

    if (!keynode || keynode->kv != kv) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    /* Atomically replace the visible version under the hash-bucket lock to avoid
     * a window where multiple versions can be inserted concurrently. */
    uint32_t crc = priskv_crc32(keynode->key, keynode->keylen);
    priskv_hash_head *hash_head = &kv->hash_heads[crc % kv->bucket_count];

    pthread_spin_lock(&hash_head->lock);
    /* Find existing visible key (if any) */
    priskv_key *iter;
    list_for_each (&hash_head->head, iter, entry) {
        if (iter->keylen == keynode->keylen &&
            memcmp(iter->key, keynode->key, keynode->keylen) == 0) {
            old_keynode = iter;
            break;
        }
    }

    /* Inherit pin_count from old version before making the new one visible. */
    if (old_keynode) {
        pthread_spin_lock(&old_keynode->lock);
        uint32_t old_pins = old_keynode->pin_count;
        pthread_spin_unlock(&old_keynode->lock);

        pthread_spin_lock(&keynode->lock);
        keynode->pin_count = old_pins;
        pthread_spin_unlock(&keynode->lock);

        /* Remove old from hash list (visibility) while holding the bucket lock. */
        list_del(&old_keynode->entry);
    } else {
        /* No old version: initialize pin_count to 0 for safety. */
        pthread_spin_lock(&keynode->lock);
        keynode->pin_count = 0;
        pthread_spin_unlock(&keynode->lock);
    }

    /* Optional: pin on publish within the same critical section to ensure atomicity
     * relative to visibility. */
    if (pin_on_publish) {
        pthread_spin_lock(&keynode->lock);
        keynode->pin_count++;
        pthread_spin_unlock(&keynode->lock);
    }

    /* Insert new node into hash list under the same lock to ensure single visible version. */
    list_add_tail(&hash_head->head, &keynode->entry);
    pthread_spin_unlock(&hash_head->lock);

    /* Add new node to LRU and finalize publish state. */
    priskv_lru_access(keynode, false);
    keynode->inprocess = false;

    /* Cleanup the old version outside the bucket lock. */
    if (old_keynode) {
        priskv_lru_del_key(old_keynode);
        __priskv_del_key(kv, old_keynode);
    }

    return PRISKV_RESP_STATUS_OK;
}

int priskv_publish_node(void *_kv, void *_keynode)
{
    return priskv_publish_node_with_pin(_kv, _keynode, false);
}

/*
 * Drop for ALLOC-private nodes (unpublished):
 * - This function expects a private keynode allocated via priskv_alloc_node_private.
 * - Private nodes NEVER appear in the hash table or LRU prior to SEAL, so there is no lookup.
 * - Actual reclamation is triggered by releasing the token reference (e.g., priskv_get_key_end),
 *   which decrements refcnt and frees when it reaches zero.
 */
int priskv_drop_node(void *_kv, void *_keynode)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = (priskv_key *)_keynode;

    if (!keynode || keynode->kv != kv) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    /* Sanity: DROP is intended for unpublished private nodes (inprocess==true before SEAL). */
    if (!keynode->inprocess) {
        /* If ever called on a published node, do not touch hash/LRU here. Use DELETE instead. */
        return PRISKV_RESP_STATUS_PERMISSION_DENIED;
    }

    /* No hash/LRU operations for private nodes. Reclamation happens when the caller releases
     * the token reference via priskv_get_key_end(keynode). */
    return PRISKV_RESP_STATUS_OK;
}


/* Decrement pin_count on the latest version of the key corresponding to keynode. */
int priskv_key_unpin_latest(void *_kv, void *_keynode)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    priskv_key *node = (priskv_key *)_keynode;
    if (!kv || !node) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    priskv_resp_status resp = priskv_pin_count_delta_latest(kv, node->key, node->keylen, -1);

    /* Stats: count every UNPIN attempt; record not-closed cases. */
    kv->pin_stats.unpin_ops++;
    if (resp == PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED) {
        kv->pin_stats.unpin_not_closed++;
    }
    return resp;
}

/* Increment pin_count on the latest visible version of the key corresponding to keynode. */
int priskv_key_pin_latest(void *_kv, void *_keynode)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    priskv_key *node = (priskv_key *)_keynode;
    if (!kv || !node) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    priskv_resp_status resp = priskv_pin_count_delta_latest(kv, node->key, node->keylen, +1);
    if (resp == PRISKV_RESP_STATUS_OK) {
        kv->pin_stats.pin_ops++;
    }
    return resp;
}

uint64_t priskv_get_pin_ops(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    return kv ? kv->pin_stats.pin_ops : 0;
}

uint64_t priskv_get_unpin_ops(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    return kv ? kv->pin_stats.unpin_ops : 0;
}

uint64_t priskv_get_unpin_not_closed(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    return kv ? kv->pin_stats.unpin_not_closed : 0;
}

int priskv_value_addr_offset(void *_kv, uint8_t *val, uint64_t *addr_offset)
{
    priskv_kv *kv = _kv;
    void *mf_ctx = kv->mf_ctx;
    if (mf_ctx) {
        *addr_offset = priskv_mem_value_offset(kv->mf_ctx, val);
        return PRISKV_RESP_STATUS_OK;
    } else {
        /* Fallback: compute offset relative to in-memory value base when mf_ctx is NULL.
         * This supports unit tests and non-memfile deployments where values reside in-process. */
        *addr_offset = (uint64_t)(val - kv->value_base);
        return PRISKV_RESP_STATUS_OK;
    }
}

int priskv_delete_key(void *_kv, uint8_t *key, uint16_t keylen)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = priskv_find_key(kv, key, keylen, PRISKV_KEY_MAX_TIMEOUT, true, NULL);

    if (!keynode) {
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    priskv_lru_del_key(keynode);
    __priskv_del_key(kv, keynode);

    return PRISKV_RESP_STATUS_OK;
}

int priskv_expire_key(void *_kv, uint8_t *key, uint16_t keylen, uint64_t timeout)
{
    priskv_kv *kv = _kv;
    bool expired = false;
    priskv_key *keynode = priskv_find_key(kv, key, keylen, timeout, false, &expired);

    if (!keynode) {
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    if (expired) {
        priskv_lru_del_key(keynode);
        __priskv_del_key(kv, keynode);
        return PRISKV_RESP_STATUS_NO_SUCH_KEY;
    }

    priskv_keynode_deref(keynode);

    return PRISKV_RESP_STATUS_OK;
}

void priskv_resume_tiering_req(priskv_tiering_req *treq)
{
    priskv_thread_submit_function(treq->thread, priskv_backend_req_resubmit, treq);
}

bool priskv_key_serialize_enter(struct priskv_tiering_req *treq)
{
    priskv_kv *kv = (priskv_kv *)treq->kv;
    priskv_tiering_wait_head *wait_head = &kv->tiering_wait_heads[treq->hash_head_index];

    pthread_spin_lock(&wait_head->lock);

    if (wait_head->has_inflight_req) {
        list_add_tail(&wait_head->pending_reqs, &treq->node);
        pthread_spin_unlock(&wait_head->lock);
        return false;
    }

    wait_head->has_inflight_req = true;
    pthread_spin_unlock(&wait_head->lock);

    return true;
}

void priskv_key_serialize_exit(struct priskv_tiering_req *completed_req)
{
    priskv_kv *kv = (priskv_kv *)completed_req->kv;
    priskv_tiering_wait_head *wait_head = &kv->tiering_wait_heads[completed_req->hash_head_index];
    struct priskv_tiering_req *next_req = NULL;

    pthread_spin_lock(&wait_head->lock);

    if (list_empty(&wait_head->pending_reqs)) {
        wait_head->has_inflight_req = false;
        pthread_spin_unlock(&wait_head->lock);
        return;
    }

    next_req = list_pop(&wait_head->pending_reqs, struct priskv_tiering_req, node);

    // keep has_inflight_req = true
    pthread_spin_unlock(&wait_head->lock);

    next_req->execute = true;
    priskv_resume_tiering_req(next_req);
}

int priskv_get_keys(void *_kv, uint8_t *regex, uint16_t regexlen, uint8_t *keysbuf, uint32_t keyslen,
                  uint32_t *reallen, uint32_t *nkey)
{
    priskv_kv *kv = _kv;
    regex_t _regex;
    char *regex_str = malloc(regexlen + 1);
    uint8_t *safekey = malloc(1024); /* FIXME */

    memcpy(regex_str, regex, regexlen);
    regex_str[regexlen] = '\0';

    if (regcomp(&_regex, regex_str, REG_NEWLINE)) {
        free(safekey);
        free(regex_str);
        return PRISKV_RESP_STATUS_INVALID_REGEX;
    }

    *nkey = 0;
    *reallen = 0;
    for (uint32_t i = 0; i < kv->bucket_count; i++) {
        priskv_hash_head *hash_head = &kv->hash_heads[i];
        priskv_key *keynode;

        pthread_spin_lock(&hash_head->lock);
        list_for_each (&hash_head->head, keynode, entry) {
            memcpy(safekey, keynode->key, keynode->keylen);
            safekey[keynode->keylen] = '\0';
            if (regexec(&_regex, (const char *)safekey, 0, NULL, 0)) {
                continue;
            }

            if (*reallen + sizeof(priskv_keys_resp) + keynode->keylen <= keyslen) {
                priskv_keys_resp *keys_resp = (priskv_keys_resp *)keysbuf;
                keys_resp->keylen = htobe16(keynode->keylen);
                keys_resp->valuelen = htobe32(keynode->valuelen);
                keys_resp->reserved = htobe16(0);
                keysbuf += sizeof(priskv_keys_resp);

                memcpy(keysbuf, keynode->key, keynode->keylen);
                keysbuf += keynode->keylen;
            }

            *reallen += sizeof(priskv_keys_resp) + keynode->keylen;
            (*nkey)++;
        }
        pthread_spin_unlock(&hash_head->lock);
    }

    regfree(&_regex);
    free(safekey);
    free(regex_str);

    if (*reallen > keyslen) {
        return PRISKV_RESP_STATUS_VALUE_TOO_BIG;
    }

    return PRISKV_RESP_STATUS_OK;
}

int priskv_flush_keys(void *_kv, uint8_t *regex, uint16_t regexlen, uint32_t *nkey)
{
    priskv_kv *kv = _kv;
    regex_t _regex;
    char *regex_str = malloc(regexlen + 1);
    uint8_t *safekey = malloc(1024); /* FIXME */

    memcpy(regex_str, regex, regexlen);
    regex_str[regexlen] = '\0';

    if (regcomp(&_regex, regex_str, REG_NEWLINE)) {
        free(safekey);
        free(regex_str);
        return PRISKV_RESP_STATUS_INVALID_REGEX;
    }

    *nkey = 0;
    for (uint32_t i = 0; i < kv->bucket_count; i++) {
        priskv_hash_head *hash_head = &kv->hash_heads[i];
        priskv_key *keynode, *tmp;

        pthread_spin_lock(&hash_head->lock);
        list_for_each_safe (&hash_head->head, keynode, tmp, entry) {
            memcpy(safekey, keynode->key, keynode->keylen);
            safekey[keynode->keylen] = '\0';
            if (regexec(&_regex, (const char *)safekey, 0, NULL, 0)) {
                continue;
            }

            /* hash_head->lock is already held, we can't use priskv_delete_key here */
            priskv_lru_del_key(keynode);
            list_del(&keynode->entry);
            __priskv_del_key(kv, keynode);

            (*nkey)++;
        }
        pthread_spin_unlock(&hash_head->lock);
    }

    regfree(&_regex);
    free(safekey);
    free(regex_str);

    return PRISKV_RESP_STATUS_OK;
}

void priskv_clear_expired_kv(int fd, void *opaque, uint32_t events)
{
    struct timeval now;
    struct list_head expired_kv;
    uint64_t n;
    priskv_key *keynode, *tmp;
    priskv_kv *kv = opaque;

    read(fd, &n, sizeof(n));
    list_head_init(&expired_kv);
    gettimeofday(&now, NULL);

    for (uint32_t i = 0; i < kv->bucket_count; i++) {
        priskv_hash_head *hash_head = &kv->hash_heads[i];

        pthread_spin_lock(&hash_head->lock);
        list_for_each_safe (&hash_head->head, keynode, tmp, entry) {
            if (priskv_key_timeout(keynode, now)) {
                list_del(&keynode->entry);
                list_add_tail(&expired_kv, &keynode->entry);
                kv->expire_routine_statics.expire_kv_count++;
                kv->expire_routine_statics.expire_kv_bytes += keynode->valuelen;
            }
        }

        pthread_spin_unlock(&hash_head->lock);

        list_for_each_safe (&expired_kv, keynode, tmp, entry) {
            priskv_lru_del_key(keynode);
            list_del(&keynode->entry);
            __priskv_del_key(kv, keynode);
        }

        assert(list_empty(&expired_kv));
    }

    kv->expire_routine_statics.expire_routine_times++;
}

void priskv_expire_routine(priskv_thread *bgthread, void *_kv)
{
    struct itimerspec timerspec;
    int interval, timerfd;
    priskv_kv *kv = _kv;

    interval = priskv_get_expire_routine_interval(kv);
    timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    assert(timerfd >= 0);

    memset(&timerspec, 0, sizeof(struct itimerspec));
    timerspec.it_value.tv_sec = interval;
    timerspec.it_interval.tv_sec = interval;
    timerfd_settime(timerfd, 0, &timerspec, NULL);
    priskv_set_fd_handler(timerfd, priskv_clear_expired_kv, NULL, kv);

    priskv_thread_add_event_handler(bgthread, timerfd);
}

uint32_t priskv_get_keys_inuse(void *_kv)
{
    priskv_kv *kv = _kv;

    return priskv_slab_inuse(kv->key_slab);
}

uint32_t priskv_get_bucket_count(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->bucket_count;
}

uint32_t priskv_get_max_keys(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->max_keys;
}

uint16_t priskv_get_max_key_length(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->max_key_length;
}

uint32_t priskv_get_expire_routine_interval(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->expire_routine_interval;
}

void priskv_set_expire_routine_interval(void *_kv, uint32_t expire_routine_interval)
{
    priskv_kv *kv = _kv;
    kv->expire_routine_interval = expire_routine_interval;
}

uint64_t priskv_get_expire_kv_count(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->expire_routine_statics.expire_kv_count;
}

uint64_t priskv_get_expire_kv_bytes(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->expire_routine_statics.expire_kv_bytes;
}

uint64_t priskv_get_expire_routine_times(void *_kv)
{
    priskv_kv *kv = _kv;

    return kv->expire_routine_statics.expire_routine_times;
}

int priskv_recover(void *_kv)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode;
    uint16_t keysize = priskv_slab_size(kv->key_slab);
    uint8_t *safekey = malloc(keysize + 1);
    int corrupted = 0;

    for (uint32_t i = 0; i < kv->max_keys; i++) {
        keynode = (priskv_key *)(kv->key_base + keysize * i);

        if (!keynode->keylen) {
            continue;
        }

        if (priskv_mem_key_size(keynode->keylen) > keysize) {
            priskv_log_error("KV: failed to recover. corrupted keylen %d, exceed %d\n",
                           keynode->keylen, keysize);
            return -EIO;
        }

        memcpy(safekey, keynode->key, keynode->keylen);
        safekey[keynode->keylen] = '\0';
        if (keynode->inprocess) {
            priskv_log_notice("KV: key [%s] in process, discard it\n", safekey);
            if (keynode->valuelen) {
                priskv_buddy_free(kv->value_buddy, priskv_value_to_pointer(kv, keynode));
                priskv_log_notice("KV: key [%s] in process with value %ld bytes\n", safekey,
                                keynode->valuelen);
            }
            memset(keynode, 0x00, priskv_slab_size(kv->key_slab));
            corrupted++;
            priskv_slab_free(kv->key_slab, keynode);
            continue;
        }

        assert(keynode->valuelen);
        keynode->kv = _kv;
        list_node_init(&keynode->entry);
        assert(priskv_slab_reserve(kv->key_slab, i) == keynode);
        priskv_insert_keynode(kv, keynode);
        priskv_lru_access(keynode, false);
        priskv_log_info("KV: recover key [%s] (%d bytes) with value %ld bytes\n", safekey,
                      keynode->keylen, keynode->valuelen);
    }

    priskv_log_notice("KV: corrupted %d, %d keys loaded successfully.\n", corrupted,
                    priskv_slab_inuse(kv->key_slab));

    free(safekey);
    return 0;
}
