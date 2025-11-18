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

#define NUM_THREADS 8
#define MAX_KEYS_PER_THREAD (128 * 1024)
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
} test_kv;

typedef struct test_kv_thread_arg {
    uint32_t thdid;
    uint64_t timeout;
} test_kv_thread_arg;

typedef void *(*test_kv_func)(void *arg);
static test_kv *test_kvs;
static void *kv;

static void *test_kv_gen(void *arg)
{
    uint32_t thdid = *(int *)arg;

    // printf("GEN KV: thread %d\n", thdid);
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

    // printf("GEN KV: thread %d\n", thdid);
    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        int ret = priskv_get_key(kv, tkv->key, tkv->keylen, &tkv->value_in_kv, &tkv->valuelen_in_kv,
                               &keynode);
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
        int ret = priskv_set_key(kv, tkv->key, tkv->keylen, &tkv->value_in_kv, tkv->valuelen, timeout,
                               &keynode);
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

    // printf("DELETE KV: thread %d\n", thdid);
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

    // printf("VERIFY KV: thread %d\n", thdid);
    for (uint32_t i = 0; i < MAX_KEYS_PER_THREAD; i++) {
        test_kv *tkv = &test_kvs[i + MAX_KEYS_PER_THREAD * thdid];
        uint8_t *value_in_kv;
        int ret =
            priskv_get_key(kv, tkv->key, tkv->keylen, &value_in_kv, &tkv->valuelen_in_kv, &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: verify KV [%d] status [%s] [FAILED]\n", i, priskv_resp_status_str(ret));
            assert(0);
        }

        if (value_in_kv != tkv->value_in_kv) {
            printf("TEST KV: verify KV [%d] value pointer [%p] against [%p] [FAILED]\n", i,
                   value_in_kv, tkv->value_in_kv);
            assert(0);
        }

        if (tkv->valuelen != tkv->valuelen_in_kv) {
            printf("TEST KV: verify KV [%d] value length [%d] against [%d] [FAILED]\n", i,
                   tkv->valuelen, tkv->valuelen_in_kv);
            assert(0);
        }

        if (memcmp(value_in_kv, tkv->value, tkv->valuelen)) {
            printf("TEST KV: verify KV [%d] value compare [FAILED]\n", i);
            printf("\t%s\n", value_in_kv);
            printf("\t%s\n", tkv->value);
            assert(0);
        }

        priskv_get_key_end(keynode);
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
    }

    free(test_kvs);
}

static int do_test_mt_kv(test_kv_func func, pthread_t *threads, int *thdid, uint64_t timeout)
{
    void *arg;
    struct test_kv_thread_arg thread_args[NUM_THREADS];

    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        if (func == test_kv_set || func == test_kv_expire) {
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

    key_base = calloc(MAX_KEYS, priskv_mem_key_size(MAX_KEY_LENGTH));
    value_base = calloc(1, priskv_buddy_mem_size(VALUE_BLOCKS, VALUE_BLOCK_SIZE));
    kv =
        priskv_new_kv(key_base, value_base, MAX_KEYS, MAX_KEY_LENGTH, VALUE_BLOCK_SIZE, VALUE_BLOCKS);
    assert(kv);

    for (uint32_t i = 0; i < NUM_THREADS; i++) {
        thdid[i] = i;
    }

    ret = do_test_mt_kv(test_kv_gen, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: generate keys[OK]\n");

    /* step 1, get keys from empty KV */
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 2, set keys to empty KV */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        return 1;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 3, get keys from KV and compare values */
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: verify keys from filled KV [OK]\n");

    /* step 4, delete keys from KV */
    ret = do_test_mt_kv(test_kv_delete, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: delete keys from filled KV [OK]\n");

    /* step 5, get keys from empty KV */
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 6, set keys to empty KV with timeout 5s */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, 5 * 1000);
    if (ret) {
        return 1;
    }

    printf("TEST KV: set keys to empty KV with timeout 5s [OK]\n");

    /* step 7, get keys from KV and compare values before expired */
    sleep(3);
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: verify keys from filled KV before expired [OK]\n");

    /* step 8, get keys from KV after expired */
    sleep(6);
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: get keys after expired [OK]\n");

    /* step 9, set keys to empty KV without timeout */
    ret = do_test_mt_kv(test_kv_set, threads, thdid, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        return 1;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 10, get keys from KV and compare values after a while */
    sleep(5);
    ret = do_test_mt_kv(test_kv_verify, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: verify keys from filled KV after a while with no expire time [OK]\n");

    /* step 11, set expire time 5s */
    ret = do_test_mt_kv(test_kv_expire, threads, thdid, 5 * 1000);
    if (ret) {
        return 1;
    }

    printf("TEST KV: set expire time 5s [OK]\n");

    /* step 12, get keys from empty KV */
    sleep(6);
    ret = do_test_mt_kv(test_kv_get_empty, threads, thdid, 0);
    if (ret) {
        return 1;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    test_kv_free(test_kvs, MAX_KEYS);

    return 0;
}
