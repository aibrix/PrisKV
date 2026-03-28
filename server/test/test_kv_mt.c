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

#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include "memory.h"
#include "kv.h"
#include "buddy.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-utils.h"
#include "../transport/transport.h"

#define NUM_THREADS 4
#define MAX_KEYS_PER_THREAD (64 * 1024)
#define MAX_KEYS (MAX_KEYS_PER_THREAD * NUM_THREADS)
#define MAX_KEY_LENGTH 64
#define VALUE_BLOCK_SIZE 1024
#define VALUE_BLOCKS (MAX_KEYS * 8)
#define MAX_VALUE_LENGTH (VALUE_BLOCK_SIZE * 8)

typedef struct test_kv {
    uint16_t keylen;
    uint8_t *key;
    uint32_t valuelen;
    uint8_t *value;
    uint8_t *value_in_kv;
    uint32_t valuelen_in_kv;

    /* for acquire/release (snapshot isolation) test */
    void *acquired_keynode;
    uint8_t *acquired_value;
    uint32_t acquired_valuelen;
    uint8_t *old_value_copy;

    /* for alloc/drop (unpublished private node) test */
    void *alloc_keynode; /* Unpublished private token returned by ALLOC */
} test_kv;

typedef struct test_kv_thread_arg {
    uint32_t thdid;
    uint64_t timeout;
} test_kv_thread_arg;

typedef void *(*test_kv_func)(void *arg);
static test_kv *test_kvs;
static void *kv;

/* --- Internal Helpers for Test Case Simplification --- */

static void __test_kv_save_old_value(test_kv *tkv, uint8_t *current_val, uint32_t len)
{
    if (tkv->old_value_copy) {
        free(tkv->old_value_copy);
    }
    tkv->acquired_value = current_val;
    tkv->acquired_valuelen = len;
    tkv->old_value_copy = malloc(len);
    memcpy(tkv->old_value_copy, tkv->value, len);
}

static void __test_kv_release_token(test_kv *tkv)
{
    if (tkv->acquired_keynode) {
        priskv_get_key_end(tkv->acquired_keynode);
        tkv->acquired_keynode = NULL;
    }
    if (tkv->old_value_copy) {
        free(tkv->old_value_copy);
        tkv->old_value_copy = NULL;
    }
    tkv->acquired_value = NULL;
}

/* --- Thread Functions --- */

static void *test_kv_gen(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        tkv->keylen = priskv_rdtsc() % (MAX_KEY_LENGTH / 2) + MAX_KEY_LENGTH / 2;
        tkv->key = calloc(1, tkv->keylen);
        priskv_random_string(tkv->key, tkv->keylen);

        tkv->valuelen = priskv_rdtsc() % (MAX_VALUE_LENGTH / 2) + MAX_VALUE_LENGTH / 2;
        tkv->value = calloc(1, tkv->valuelen);
        priskv_random_string(tkv->value, tkv->valuelen);
    }

    return NULL;
}

static void *test_kv_get_empty(void *arg)
{
    uint32_t thdid = *(int *)arg;
    void *keynode;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        int ret = priskv_get_key(kv, tkv->key, tkv->keylen, &tkv->value_in_kv,
                                 &tkv->valuelen_in_kv, &keynode);
        if (ret != PRISKV_RESP_STATUS_NO_SUCH_KEY || keynode != NULL) {
            printf("TEST KV: get keys from empty KV [FAILED]\n");
            assert(0);
        }
    }

    return NULL;
}

static void *test_kv_set(void *arg)
{
    test_kv_thread_arg *args = (test_kv_thread_arg *)arg;
    uint32_t thdid = args->thdid;
    uint64_t timeout = args->timeout;
    void *keynode;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        int ret = priskv_set_key(kv, tkv->key, tkv->keylen, &tkv->value_in_kv, tkv->valuelen,
                                 timeout, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: set keys to empty KV [FAILED]\n");
            assert(0);
        }
        memcpy(tkv->value_in_kv, tkv->value, tkv->valuelen);
        priskv_set_key_end(keynode);
    }

    return NULL;
}

static void *test_kv_expire(void *arg)
{
    test_kv_thread_arg *args = (test_kv_thread_arg *)arg;
    uint32_t thdid = args->thdid;
    uint64_t timeout = args->timeout;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        int ret = priskv_expire_key(kv, tkv->key, tkv->keylen, timeout);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: set kv expire time [FAILED]\n");
            assert(0);
        }
    }

    return NULL;
}

static void *test_kv_delete(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        int ret = priskv_delete_key(kv, tkv->key, tkv->keylen);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: delete keys from filled KV [FAILED]\n");
            assert(0);
        }
    }
    return NULL;
}

static void *test_kv_verify(void *arg)
{
    uint32_t thdid = *(int *)arg;
    void *keynode;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        uint8_t *value_in_kv;
        int ret = priskv_get_key(kv, tkv->key, tkv->keylen, &value_in_kv,
                                 &tkv->valuelen_in_kv, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: verify status FAILED at line %d [thd=%u idx=%u] ret=%s keylen=%u keynode=%p\n",
                   __LINE__, thdid, i, priskv_resp_status_str(ret), tkv->keylen, keynode);
            assert(0);
        }

        if (value_in_kv != tkv->value_in_kv) {
            printf("TEST KV: verify pointer FAILED at line %d [thd=%u idx=%u] value_in_kv=%p expected=%p valuelen_in_kv=%u saved_valuelen=%u\n",
                   __LINE__, thdid, i, value_in_kv, tkv->value_in_kv,
                   tkv->valuelen_in_kv, tkv->valuelen);
            assert(0);
        }

        if (tkv->valuelen != tkv->valuelen_in_kv) {
            printf("TEST KV: verify length FAILED at line %d [thd=%u idx=%u] valuelen=%u valuelen_in_kv=%u\n",
                   __LINE__, thdid, i, tkv->valuelen, tkv->valuelen_in_kv);
            assert(0);
        }

        if (memcmp(value_in_kv, tkv->value, tkv->valuelen)) {
            uint32_t dump_len = tkv->valuelen < 16 ? tkv->valuelen : 16;
            printf("TEST KV: verify memcmp FAILED at line %d [thd=%u idx=%u] len=%u dump_len=%u\n",
                   __LINE__, thdid, i, tkv->valuelen, dump_len);
            printf("TEST KV:   first bytes (expected/actual):");
            for (uint32_t b = 0; b < dump_len; b++) {
                printf(" %02x/%02x", tkv->value[b], value_in_kv[b]);
            }
            printf("\n");
            assert(0);
        }

        priskv_get_key_end(keynode);
    }

    return NULL;
}

static void *test_kv_alloc_publish(void *arg)
{
    test_kv_thread_arg *args = (test_kv_thread_arg *)arg;
    uint32_t thdid = args->thdid;
    uint64_t timeout = args->timeout;
    void *keynode;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        uint8_t *val_ptr;
        int ret = priskv_alloc_node_private(kv, tkv->key, tkv->keylen, &val_ptr, tkv->valuelen,
                                            timeout, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: alloc node private [FAILED] ret %d\n", ret);
            assert(0);
        }
        tkv->value_in_kv = val_ptr;
        memcpy(tkv->value_in_kv, tkv->value, tkv->valuelen);

        ret = priskv_publish_node(kv, keynode);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: publish node [FAILED] ret %d\n", ret);
            assert(0);
        }
    }

return NULL;
}

static void *test_kv_drop_unpublished(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        /* Drop on an unpublished private node (ALLOC): should be OK */
        int ret = priskv_drop_node(kv, tkv->alloc_keynode);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: DROP unpublished node should be OK [FAILED], ret %d\n", ret);
            assert(0);
        }
        /* Release the private ALLOC-held reference to complete reclamation (not ACQUIRE release) */
        priskv_get_key_end(tkv->alloc_keynode);

        /* Clear local pointers */
        tkv->alloc_keynode = NULL;
        tkv->acquired_value = NULL;
        tkv->acquired_valuelen = 0;
        if (tkv->old_value_copy) {
            free(tkv->old_value_copy);
            tkv->old_value_copy = NULL;
        }
        /* Do not call ACQUIRE release here; this is a private ALLOC reference release */
    }

    return NULL;
}

static void *test_kv_acquire_all(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        uint8_t *val_ptr;
        uint32_t val_len;
        int ret = priskv_get_key(kv, tkv->key, tkv->keylen, &val_ptr, &val_len,
                                 &tkv->acquired_keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !tkv->acquired_keynode) {
            printf("TEST KV: acquire (get_key) [FAILED] ret %d\n", ret);
            assert(0);
        }
        __test_kv_save_old_value(tkv, val_ptr, val_len);
    }

return NULL;
}

static void *test_kv_verify_acquired(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        if (memcmp(tkv->acquired_value, tkv->old_value_copy, tkv->acquired_valuelen)) {
            printf("TEST KV: verify acquired snapshot [FAILED]\n");
            assert(0);
        }
    }

return NULL;
}

static void *test_kv_release_all(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        __test_kv_release_token(tkv);
    }

return NULL;
}

static void *test_kv_update_data(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        /* regenerate value with new random data */
        priskv_random_string(tkv->value, tkv->valuelen);
    }

return NULL;
}

static void *test_kv_alloc_only(void *arg)
{
    test_kv_thread_arg *args = (test_kv_thread_arg *)arg;
    uint32_t thdid = args->thdid;
    uint64_t timeout = args->timeout;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        uint8_t *val_ptr;
        int ret = priskv_alloc_node_private(kv, tkv->key, tkv->keylen, &val_ptr, tkv->valuelen,
                                            timeout, &tkv->alloc_keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !tkv->alloc_keynode) {
            printf("TEST KV: alloc only [FAILED] ret %d\n", ret);
            assert(0);
        }
        tkv->value_in_kv = val_ptr;
        memcpy(tkv->value_in_kv, tkv->value, tkv->valuelen);
    }

return NULL;
}

static void *test_kv_drop_published(void *arg)
{
    uint32_t thdid = *(int *)arg;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        /* 1. Try DROP on a published (acquired) node: should be denied */
        int ret = priskv_drop_node(kv, tkv->acquired_keynode);
        if (ret != PRISKV_RESP_STATUS_PERMISSION_DENIED) {
            printf("TEST KV: DROP published node should be denied [FAILED], ret %d\n", ret);
            assert(0);
        }

        /* Do not release here; release is verified separately in acquire path */
    }

return NULL;
}

static void *test_kv_alloc_alloc_seal_order(void *arg)
{
    test_kv_thread_arg *args = (test_kv_thread_arg *)arg;
    uint32_t thdid = args->thdid;
    uint64_t timeout = args->timeout;

    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];

        /* ALLOC1: write version-1 data */
        uint8_t *val1_ptr;
        void *node1;
        int ret = priskv_alloc_node_private(kv, tkv->key, tkv->keylen, &val1_ptr, tkv->valuelen,
                                            timeout, &node1);
        if (ret != PRISKV_RESP_STATUS_OK || !node1) {
            printf("TEST KV: double alloc (alloc1) [FAILED] ret %d\n", ret);
            assert(0);
        }
        memcpy(val1_ptr, tkv->value, tkv->valuelen);

        /* Prepare version-2 data */
        uint8_t *expected2 = malloc(tkv->valuelen);
        priskv_random_string(expected2, tkv->valuelen);

        /* ALLOC2: write version-2 data */
        uint8_t *val2_ptr;
        void *node2;
        ret = priskv_alloc_node_private(kv, tkv->key, tkv->keylen, &val2_ptr, tkv->valuelen,
                                        timeout, &node2);
        if (ret != PRISKV_RESP_STATUS_OK || !node2) {
            printf("TEST KV: double alloc (alloc2) [FAILED] ret %d\n", ret);
            assert(0);
        }
        memcpy(val2_ptr, expected2, tkv->valuelen);

        /* SEAL2: publish version-2 first; expect visible = version-2 */
        ret = priskv_publish_node(kv, node2);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: double alloc (seal2 publish) [FAILED] ret %d\n", ret);
            assert(0);
        }

        uint8_t *read_ptr;
        uint32_t read_len;
        void *keynode;
        ret = priskv_get_key(kv, tkv->key, tkv->keylen, &read_ptr, &read_len, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: double alloc (verify after seal2) [FAILED] ret %d\n", ret);
            assert(0);
        }
        if (read_len != tkv->valuelen || memcmp(read_ptr, expected2, read_len)) {
            printf("TEST KV: double alloc (verify content equals v2) [FAILED]\n");
            assert(0);
        }
        priskv_get_key_end(keynode);

        /* SEAL1: then publish version-1; final visible should be version-1 (last sealed) */
        ret = priskv_publish_node(kv, node1);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: double alloc (seal1 publish) [FAILED] ret %d\n", ret);
            assert(0);
        }

        ret = priskv_get_key(kv, tkv->key, tkv->keylen, &read_ptr, &read_len, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: double alloc (verify after seal1) [FAILED] ret %d\n", ret);
            assert(0);
        }
        if (read_len != tkv->valuelen || memcmp(read_ptr, tkv->value, read_len)) {
            printf("TEST KV: double alloc (verify content equals v1 final) [FAILED]\n");
            assert(0);
        }
        /* Update final visible pointer to align with verification */
        tkv->value_in_kv = read_ptr;
        priskv_get_key_end(keynode);

        free(expected2);
    }

    return NULL;
}

static void test_kv_free()
{
    test_kv *tkv;

    for (uint32_t i = 0; i < MAX_KEYS; i++) {
        tkv = &test_kvs[i];
        free(tkv->key);
        free(tkv->value);
        if (tkv->old_value_copy)
            free(tkv->old_value_copy);
    }

    free(test_kvs);
}

static int do_test_mt_kv(test_kv_func func, pthread_t *threads, int *thdid, uint64_t timeout)
{
    void *arg;
    struct test_kv_thread_arg thread_args[NUM_THREADS];

    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        if (func == test_kv_set || func == test_kv_expire || func == test_kv_alloc_publish ||
            func == test_kv_alloc_alloc_seal_order) {
            thread_args[i] = (test_kv_thread_arg) {.thdid = thdid[i], .timeout = timeout};
            arg = (void *)&thread_args[i];
        } else {
            arg = (void *)&thdid[i];
        }

        pthread_create(&threads[i], NULL, func, arg);
    }

    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        if (pthread_join(threads[i], NULL) != 0) {
            printf("pthread_join failed on preparing KV: %m\n");
            return 1;
        }
    }

    return 0;
}

int main()
{
    uint8_t *key_base, *value_base;
    pthread_t threads[NUM_THREADS];
    int thdid[NUM_THREADS];
    int ret;

    /* step 0, prepare test env */
    srandom(getpid());

    test_kvs = calloc(MAX_KEYS, sizeof(test_kv));
    assert(test_kvs);

    /* Double the capacity to support snapshot isolation test (hold old version while writing new
     * version) */
    key_base = calloc(MAX_KEYS * 2, priskv_mem_key_size(MAX_KEY_LENGTH));
    value_base = calloc(1, priskv_buddy_mem_size(VALUE_BLOCKS * 2, VALUE_BLOCK_SIZE));
    kv = priskv_new_kv(key_base, value_base, -1, 0, MAX_KEYS * 2, MAX_KEY_LENGTH, VALUE_BLOCK_SIZE,
                       VALUE_BLOCKS * 2, NULL /* mf_ctx */);
    assert(kv);

    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        thdid[i] = i;
    }

    ret = do_test_mt_kv(test_kv_gen, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: generate keys[OK]\n");

    /* --- SECTION 1: Basic KV Operations (SET/GET/DELETE) --- */

    /* step 1, get keys from empty KV */
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 2, set keys to empty KV */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        goto out;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 3, get keys from KV and compare values */
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: verify keys from filled KV [OK]\n");

    /* step 4, delete keys from KV */
    ret = do_test_mt_kv(test_kv_delete, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: delete keys from filled KV [OK]\n");

    /* step 5, get keys from empty KV */
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* --- SECTION 2: Expiration Logic (TTL/EXPIRE) --- */

    /* step 6, set keys to empty KV with timeout 5s */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, 5 * 1000);
    if (ret) {
        goto out;
    }

    printf("TEST KV: set keys to empty KV with timeout 5s [OK]\n");

    /* step 7, get keys from KV and compare values before expired */
    sleep(3);
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: verify keys from filled KV before expired [OK]\n");

    /* step 8, get keys from KV after expired */
    sleep(6);
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: get keys after expired [OK]\n");

    /* step 9, set keys to empty KV without timeout */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        goto out;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 10, get keys from KV and compare values after a while */
    sleep(5);
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: verify keys from filled KV after a while with no expire time [OK]\n");

    /* step 11, set expire time 5s */
    ret = do_test_mt_kv(test_kv_expire, threads, thdid, 5 * 1000);
    if (ret) {
        goto out;
    }

    printf("TEST KV: set expire time 5s [OK]\n");

    /* step 12, get keys from empty KV */
    sleep(6);
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* --- SECTION 3: Zero-Copy Write (ALLOC/PUBLISH) --- */

    /* step 13, set keys to empty KV via alloc and publish */
    ret = do_test_mt_kv(test_kv_alloc_publish, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        goto out;
    }

    printf("TEST KV: set keys to empty KV via alloc and publish [OK]\n");

    /* step 14, get keys from KV and compare values */
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        goto out;
    }

    printf("TEST KV: verify keys from filled KV after alloc and publish [OK]\n");

    /* --- SECTION 4: Snapshot Isolation (ACQUIRE/UPDATE/RELEASE) --- */

    /* step 15, acquire all keys for snapshot isolation test */
    ret = do_test_mt_kv(test_kv_acquire_all, threads, thdid, 0);
    if (ret) {
        goto out;
    }
    printf("TEST KV: acquire all keys (Snapshot Start) [OK]\n");

    /* step 16, update all keys to new version while readers hold old version */
    ret = do_test_mt_kv(test_kv_update_data, threads, thdid, 0);
    if (ret)
        goto out;
    ret = do_test_mt_kv(test_kv_alloc_publish, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret)
        goto out;
    printf("TEST KV: update all keys to new version [OK]\n");

    /* step 17, verify readers still see old data */
    ret = do_test_mt_kv(test_kv_verify_acquired, threads, thdid, 0);
    if (ret) {
        goto out;
    }
    printf("TEST KV: verify readers see old snapshot [OK]\n");

    /* step 18, release old version */
    ret = do_test_mt_kv(test_kv_release_all, threads, thdid, 0);
    if (ret) {
        goto out;
    }
    printf("TEST KV: release all keys (Snapshot End) [OK]\n");

    /* step 19, verify new version is now the latest */
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        goto out;
    }
    printf("TEST KV: verify new version is latest [OK]\n");

    /* --- SECTION 5: Token DROP Semantics (DROP) --- */

    /* step 20, test DROP: acquire and then drop (published should be denied) */
    ret = do_test_mt_kv(test_kv_acquire_all, threads, thdid, 0);
    if (ret)
        goto out;
    printf("TEST KV: acquire all keys (before DROP) [OK]\n");

    ret = do_test_mt_kv(test_kv_drop_published, threads, thdid, 0);
    if (ret)
        goto out;
    printf("TEST KV: DROP denied on published [OK]\n");

    /* Release ACQUIRE tokens and then delete keys */
    ret = do_test_mt_kv(test_kv_release_all, threads, thdid, 0);
    if (ret) {
        printf("TEST KV: release all after DROP denied [FAILED]\n");
        goto out;
    }
    printf("TEST KV: release all after DROP denied [OK]\n");

    ret = do_test_mt_kv(test_kv_delete, threads, thdid, 0);
    if (ret) {
        printf("TEST KV: delete after release [FAILED]\n");
        goto out;
    }
    printf("TEST KV: delete after release [OK]\n");

    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        printf("TEST KV: verify empty after delete [FAILED]\n");
        goto out;
    }
    printf("TEST KV: verify empty after delete [OK]\n");

    /* step 21, test DROP after ALLOC (without SEAL) */
    ret = do_test_mt_kv(test_kv_alloc_only, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret)
        goto out;
    printf("TEST KV: alloc all keys (without SEAL) [OK]\n");

    ret = do_test_mt_kv(test_kv_drop_unpublished, threads, thdid, 0);
    if (ret)
        goto out;
    printf("TEST KV: DROP all unpublished allocated keys [OK]\n");

    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        printf("TEST KV: verify empty after DROP (ALLOC) [FAILED]\n");
        goto out;
    }
    printf("TEST KV: verify empty after DROP (ALLOC) [OK]\n");

    /* --- SECTION 6: Double ALLOC with out-of-order SEAL final visibility --- */

    /* step 22, For each key perform Alloc1/Alloc2/Seal2/Seal1. Final visible data must equal
     * the last SEAL'ed version. */
    ret = do_test_mt_kv(test_kv_alloc_alloc_seal_order, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret)
        goto out;
    printf("TEST KV: Alloc1/Alloc2/Seal2/Seal1 final visible equals last sealed [OK]\n");

    /* step 23, delete all keys */
    ret = do_test_mt_kv(test_kv_delete, threads, thdid, 0);
    if (ret)
        goto out;
    printf("TEST KV: delete keys after double seal order [OK]\n");

    /* step 24, verify empty again */
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret)
        goto out;
    printf("TEST KV: verify empty after double seal order cleanup [OK]\n");

out:
    if (kv)
        priskv_destroy_kv(kv);
    if (key_base)
        free(key_base);
    if (value_base)
        free(value_base);
    test_kv_free();

    return ret;
}
