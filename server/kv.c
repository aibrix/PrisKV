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

/* PinTTL operator record type: placed early so list macros (which need
 * concrete type for offsetof) can be used everywhere in this file. */
typedef struct pinop {
    struct list_node node;     /* intrusive list node */
    uint16_t keylen;
    struct timeval expire_at;  /* absolute time to expire */
    /* key bytes follow this struct (flexible array) */
    uint8_t key[0];
} pinop;

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
        uint64_t pin_failed_ops;   /* number of failed PIN attempts */
        uint64_t unpin_ops;
        uint64_t unpin_not_closed;
    } pin_stats;
    /* PinTTL manager */
    struct {
        struct {
            struct list_head head;      /* bucketed list of pin operators */
            pthread_spinlock_t lock;    /* per-bucket lock */
        } buckets[PRISKV_PIN_TTL_BUCKETS];
        uint64_t default_ttl_ms;    /* default TTL for pins */
        /* metrics */
        uint64_t active;            /* current active PinOperator count */
        uint64_t expired_total;     /* total expired entries observed */
        uint64_t cleanup_ops;       /* total successful cleanup unpin ops */
        uint64_t orphaned;          /* expiries with NO_SUCH_KEY or unpin-not-closed */
        uint64_t register_failed;   /* failed PinTTL registrations (e.g., OOM) */
    } pin_ttl;
} priskv_kv;


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
    kv->pin_stats.pin_failed_ops = 0;
    kv->pin_stats.unpin_ops = 0;
    kv->pin_stats.unpin_not_closed = 0;

    /* PinTTL manager init */
    for (int i = 0; i < PRISKV_PIN_TTL_BUCKETS; ++i) {
        list_head_init(&kv->pin_ttl.buckets[i].head);
        pthread_spin_init(&kv->pin_ttl.buckets[i].lock, 0);
    }
    kv->pin_ttl.default_ttl_ms = PRISKV_DEFAULT_PIN_TTL_MS;
    kv->pin_ttl.active = 0;
    kv->pin_ttl.expired_total = 0;
    kv->pin_ttl.cleanup_ops = 0;
    kv->pin_ttl.orphaned = 0;
    kv->pin_ttl.register_failed = 0;

    priskv_log_notice("KV: max_key %d, max_key_length %d, value_block_size %d, value_blocks %ld\n",
                    max_keys, max_key_length, value_block_size, value_blocks);

    return kv;
}

void priskv_destroy_kv(void *_kv)
{
    priskv_kv *kv = _kv;

    /* cleanup PinTTL lists and destroy per-bucket locks */
    for (int i = 0; i < PRISKV_PIN_TTL_BUCKETS; ++i) {
        pthread_spin_lock(&kv->pin_ttl.buckets[i].lock);
        struct pinop *p, *ptmp;
        list_for_each_safe(&kv->pin_ttl.buckets[i].head, p, ptmp, node) {
            list_del(&p->node);
            free(p);
        }
        pthread_spin_unlock(&kv->pin_ttl.buckets[i].lock);
        pthread_spin_destroy(&kv->pin_ttl.buckets[i].lock);
    }

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
/* Atomically apply a delta to pin_count on the latest visible version.
 * - delta > 0: increment and register one TTL entry (ttl_ms == 0 uses default).
 * - delta < 0: decrement and unregister one TTL entry.
 */
static priskv_resp_status priskv_pin_count_delta_latest(priskv_kv *kv, uint8_t *key,
                                                        uint16_t keylen, int32_t delta,
                                                        uint64_t ttl_ms, bool require_expired, bool consumed);

static void __priskv_del_key(priskv_kv *kv, priskv_key *keynode)
{
    priskv_keynode_deref(keynode);
}

static priskv_resp_status priskv_pin_count_delta_latest(priskv_kv *kv, uint8_t *key,
                                                        uint16_t keylen, int32_t delta,
                                                        uint64_t ttl_ms, bool require_expired, bool consumed)
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
    if (delta >= 0) {
        /* PIN: increment count first, then register TTL */
        pthread_spin_lock(&latest->lock);
        latest->pin_count += (uint32_t)delta;
        pthread_spin_unlock(&latest->lock);
        /* Register TTL for this key while still holding bucket lock */
        priskv_pin_ttl_register(kv, key, keylen, ttl_ms);
    } else {
        /* UNPIN: consume TTL first; only on success decrement pin_count */
			  if(!consumed) {
          consumed = priskv_pin_ttl_unregister_one(kv, key, keylen, require_expired);
			  }
        if (!consumed) {
            resp = PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED;
        } else {
            uint32_t need = (uint32_t)(-delta);
            pthread_spin_lock(&latest->lock);
            if (latest->pin_count >= need) {
                latest->pin_count -= need;
            } else {
                /* TTL entry consumed but no matching pin_count; treat as not-closed */
                resp = PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED;
            }
            pthread_spin_unlock(&latest->lock);
        }
    }

    pthread_spin_unlock(&hash_head->lock);

    return resp;
}

/* ---------------- PinTTL Manager ---------------- */

/* TODO(wangyi): Double-decrement risk between explicit UNPIN and PinTTL cleanup
 * Problem:
 *  - PinTTL cleanup thread removes one TTL entry and then decrements pin_count on the latest
 *    version. Meanwhile, an explicit RELEASE+UNPIN can arrive and also decrement pin_count,
 *    because priskv_pin_ttl_unregister_one() may not find the already-removed TTL entry, but
 *    priskv_pin_count_delta_latest() still performs the decrement.
 *  - This can lead to two decrements for a single logical pin (race window), causing premature
 *    eviction.
 * Suggested solution:
 *  - Introduce a 'consumed' or 'in_use' flag into pinop and enforce a strict consume-before-decrement rule:
 *      1) Explicit UNPIN path: under pin_ttl.lock locate an unconsumed pinop for the key and mark it
 *         consumed (or remove it) atomically; only if consumption succeeds, proceed to decrement
 *         pin_count under the bucket lock. If no pinop is available, return UNPIN_NOT_CLOSED.
 *      2) TTL cleanup: perform the same consume step (mark/remove) under pin_ttl.lock; only on success
 *         attempt the decrement on the latest version.
 *  - This ensures a single pinop maps to exactly one pin_count decrement, eliminating the double-decrement race.
 */

static inline void pinop_compute_expire(struct timeval *now, uint64_t ttl_ms, struct timeval *out)
{
    *out = *now;
    priskv_time_add_ms(out, ttl_ms);
}

void priskv_pin_ttl_register(void *_kv, const uint8_t *key, uint16_t keylen, uint64_t ttl_ms)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    if (!kv || !key || !keylen) return;

    if (ttl_ms == 0) {
        ttl_ms = kv->pin_ttl.default_ttl_ms;
    }

    size_t alloc = sizeof(pinop) + keylen;
    pinop *op = (pinop *)malloc(alloc);
    if (!op) {
        /* Track registration failures for observability */
        kv->pin_ttl.register_failed++;
        return; /* best-effort */
    }
    op->keylen = keylen;
    memcpy(op->key, key, keylen);

    struct timeval now;
    gettimeofday(&now, NULL);
    pinop_compute_expire(&now, ttl_ms, &op->expire_at);

    uint32_t crc = priskv_crc32((uint8_t *)key, keylen);
    uint32_t b = crc % PRISKV_PIN_TTL_BUCKETS;
    pthread_spin_lock(&kv->pin_ttl.buckets[b].lock);
    list_add_tail(&kv->pin_ttl.buckets[b].head, &op->node);
    kv->pin_ttl.active++;
    pthread_spin_unlock(&kv->pin_ttl.buckets[b].lock);
}

bool priskv_pin_ttl_unregister_one(void *_kv, const uint8_t *key, uint16_t keylen,
                                   bool require_expired)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    if (!kv || !key || !keylen) return false;

    uint32_t crc = priskv_crc32((uint8_t *)key, keylen);
    uint32_t b = crc % PRISKV_PIN_TTL_BUCKETS;
    pthread_spin_lock(&kv->pin_ttl.buckets[b].lock);
    pinop *op, *tmp;
    list_for_each_safe(&kv->pin_ttl.buckets[b].head, op, tmp, node) {
        if (op->keylen == keylen && memcmp(op->key, key, keylen) == 0) {
            if (require_expired) {
                struct timeval now;
                gettimeofday(&now, NULL);
                if (priskv_time_elapsed_ms(op->expire_at, now) <= 0) {
                    continue; /* not expired, skip */
                }
            }
            list_del(&op->node);
            if (kv->pin_ttl.active > 0) kv->pin_ttl.active--;
            if (require_expired) {
                kv->pin_ttl.expired_total++;
            }
            pthread_spin_unlock(&kv->pin_ttl.buckets[b].lock);
            free(op);
            return true; /* removed one */
        }
    }
    pthread_spin_unlock(&kv->pin_ttl.buckets[b].lock);
    return false;
}

void priskv_set_default_pin_ttl_ms(void *_kv, uint64_t ttl_ms)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    if (!kv) return;
    kv->pin_ttl.default_ttl_ms = ttl_ms ? ttl_ms : PRISKV_DEFAULT_PIN_TTL_MS;
}

uint64_t priskv_get_default_pin_ttl_ms(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    if (!kv) return PRISKV_DEFAULT_PIN_TTL_MS;
    return kv->pin_ttl.default_ttl_ms;
}

uint64_t priskv_get_pin_ttl_active(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv; return kv ? kv->pin_ttl.active : 0;
}
uint64_t priskv_get_pin_ttl_expired(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv; return kv ? kv->pin_ttl.expired_total : 0;
}
uint64_t priskv_get_pin_ttl_cleanup_ops(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv; return kv ? kv->pin_ttl.cleanup_ops : 0;
}
uint64_t priskv_get_pin_ttl_orphaned(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv; return kv ? kv->pin_ttl.orphaned : 0;
}

uint64_t priskv_get_pin_ttl_register_failed(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv; return kv ? kv->pin_ttl.register_failed : 0;
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
static int __priskv_publish_node_with_pin(priskv_kv *kv, priskv_key *keynode,
                                          bool pin_on_publish, uint64_t ttl_ms)
{
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
     * relative to visibility; also register TTL atomically. */
    if (pin_on_publish) {
        pthread_spin_lock(&keynode->lock);
        keynode->pin_count++;
        pthread_spin_unlock(&keynode->lock);
        /* Register TTL while holding the bucket lock to ensure atomic visibility. */
        priskv_pin_ttl_register(kv, keynode->key, keynode->keylen, ttl_ms);
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

int priskv_publish_node_with_pin(void *_kv, void *_keynode, bool pin_on_publish,
                                 uint64_t ttl_ms)
{
    priskv_kv *kv = _kv;
    priskv_key *keynode = (priskv_key *)_keynode;
    return __priskv_publish_node_with_pin(kv, keynode, pin_on_publish, ttl_ms);
}

int priskv_publish_node(void *_kv, void *_keynode)
{
    /* No pin on publish; ttl_ms is irrelevant, pass 0. */
    return priskv_publish_node_with_pin(_kv, _keynode, false, 0);
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

    /* TODO(wangyi): Explicit UNPIN may race with TTL cleanup. A robust approach is to first
     * consume (mark/remove) one PinTTL entry under pin_ttl.lock, and only on successful
     * consumption perform the pin_count decrement under the bucket lock. Otherwise return
     * UNPIN_NOT_CLOSED when no matching TTL entry exists (already consumed by cleanup). */
    priskv_resp_status resp =
        priskv_pin_count_delta_latest(kv, node->key, node->keylen, -1, 0, false, false);

    /* Stats: count every UNPIN attempt; record not-closed cases. */
    kv->pin_stats.unpin_ops++;
    if (resp == PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED) {
        kv->pin_stats.unpin_not_closed++;
    }
    return resp;
}

/* Increment pin_count on the latest visible version of the key corresponding to keynode. */
int priskv_key_pin_latest(void *_kv, void *_keynode, uint64_t ttl_ms)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    priskv_key *node = (priskv_key *)_keynode;
    if (!kv || !node) {
        return PRISKV_RESP_STATUS_SERVER_ERROR;
    }

    priskv_resp_status resp =
        priskv_pin_count_delta_latest(kv, node->key, node->keylen, +1, ttl_ms, false, false);
    if (resp == PRISKV_RESP_STATUS_OK) {
        kv->pin_stats.pin_ops++;
    } else {
        /* Count failed PIN attempts (e.g., NO_SUCH_KEY) */
        kv->pin_stats.pin_failed_ops++;
    }
    return resp;
}

uint64_t priskv_get_pin_ops(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    return kv ? kv->pin_stats.pin_ops : 0;
}

uint64_t priskv_get_pin_failed_ops(void *_kv)
{
    priskv_kv *kv = (priskv_kv *)_kv;
    return kv ? kv->pin_stats.pin_failed_ops : 0;
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

    /* PinTTL cleanup
     *
     * Lock ordering and traversal strategy:
     * - Explicit UNPIN may acquire the hash-bucket lock first and then try to grab the
     *   PinTTL bucket lock from within priskv_pin_ttl_unregister_one(). If the cleanup
     *   thread holds the PinTTL bucket lock and then attempts to acquire the hash-bucket
     *   lock (via priskv_pin_count_delta_latest), the two paths create an inverted lock
     *   order and can deadlock.
     * - To avoid this, we use a "one-entry-at-a-time" pattern: detach a single expired
     *   item under the PinTTL bucket lock, release the bucket lock, then perform the
     *   pin_count decrement on the latest visible key version.
     * - Also avoid crossing the unlock boundary with a list iterator. We re-lock and
     *   rescan the list after processing each detached entry so the iterator never spans
     *   an unlock/lock window.
     */
    for (int b = 0; b < PRISKV_PIN_TTL_BUCKETS; ++b) {
        while (1) {
            /* Find one expired pinop and detach it under the bucket lock */
            pthread_spin_lock(&kv->pin_ttl.buckets[b].lock);
            pinop *op, *found = NULL;
            list_for_each(&kv->pin_ttl.buckets[b].head, op, node) {
                if (priskv_time_elapsed_ms(op->expire_at, now) > 0) {
                    found = op;
                    break;
                }
            }

            if (!found) {
                pthread_spin_unlock(&kv->pin_ttl.buckets[b].lock);
                break; /* No more expired items in this bucket */
            }

            /* Detach from the TTL bucket, update counters, then release the lock */
            list_del(&found->node);
            if (kv->pin_ttl.active > 0) kv->pin_ttl.active--;
            kv->pin_ttl.expired_total++;
            uint16_t klen = found->keylen;
            const uint8_t *kptr = found->key; /* safe to use after list_del; we own 'found' */
            pthread_spin_unlock(&kv->pin_ttl.buckets[b].lock);

            /* Without holding the TTL bucket lock, decrement pin_count on the latest version */
            priskv_resp_status r = priskv_pin_count_delta_latest(kv, (uint8_t *)kptr, klen, -1, 0, false, true);
            if (r == PRISKV_RESP_STATUS_OK) {
                kv->pin_ttl.cleanup_ops++;
            } else {
                /* Could be NO_SUCH_KEY or UNPIN_NOT_CLOSED; count as orphaned for observability */
                kv->pin_ttl.orphaned++;
            }
            free(found);
            /* Continue with the next expired item in this bucket */
        }
    }
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
