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
 *   Yu Wang <wangyu.steph@bytedance.com>
 *   Jinlong Xuan <15563983051@163.com>
 *   Xu Ji <sov.matrixac@gmail.com>
 *   Bo Liu <liubo.2024@bytedance.com>
 *   Zhenwei Pi <pizhenwei@bytedance.com>
 *   Rui Zhang <zhangrui.1203@bytedance.com>
 *   Changqi Lu <luchangqi.123@bytedance.com>
 *   Enhua Zhou <zhouenhua@bytedance.com>
 */

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdint.h>

#include "kv.h"
#include "priskv-threads.h"
#include "priskv-protocol.h"
#include "memory.h"
#include "buddy.h"

int main()
{
    void *kv, *keynode;
    uint8_t *key_base, *value_base;
    uint32_t nkey, reallen;
    int ret = 0;
    uint8_t iothreads = 1, bgthreads = 1;
    uint32_t max_keys = 128 * 1024, expire_routine_interval = 5;
    uint16_t max_key_length = 128;
    uint32_t value_block_size = 4096;
    uint64_t value_blocks = max_keys * 16;
    uint8_t *key = (uint8_t *)"key", *value = (uint8_t *)"value";
    priskv_threadpool *threadpool = priskv_threadpool_create("test", iothreads, bgthreads, 0);
    assert(threadpool);
    priskv_thread *bgthread = priskv_threadpool_get_bgthread(threadpool, 0);
    assert(bgthread);

    key_base = calloc(max_keys, priskv_mem_key_size(max_key_length));
    value_base = calloc(1, priskv_buddy_mem_size(value_blocks, value_block_size));
    kv = priskv_new_kv(key_base, value_base, -1, 0, max_keys, max_key_length, value_block_size,
                       value_blocks);
    if (!kv) {
        printf("TEST BG_THREAD: create kv [FAILED]\n");
        return -1;
    }

    priskv_set_expire_routine_interval(kv, expire_routine_interval);
    priskv_expire_routine(bgthread, kv);

    ret = priskv_set_key(kv, key, sizeof(key), &value, sizeof(value), 2000, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST BG_THREAD: set key to KV with timeout 2s [FAILED]\n");
        return 1;
    }

    sleep(4);

    ret = priskv_get_keys(kv, (uint8_t *)".*", 2, NULL, 0, &reallen, &nkey);
    /* PRISKV_RESP_STATUS_VALUE_TOO_BIG is expected */

    if (ret != PRISKV_RESP_STATUS_VALUE_TOO_BIG) {
        printf("TEST BG_THREAD: get nrkeys from filled KV [FAILED], status [%d]\n", ret);
        return 1;
    }

    if (nkey != 1) {
        printf("TEST BG_THREAD: expired keys wiped before executing bg thread [FAILED]\n");
        return 1;
    }

    ret = priskv_set_key(kv, key, sizeof(key), &value, sizeof(value), 2000, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST BG_THREAD: set key to KV with timeout 2s [FAILED]\n");
        return 1;
    }

    sleep(6);

    ret = priskv_get_keys(kv, (uint8_t *)".*", 2, NULL, 0, &reallen, &nkey);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST BG_THREAD: get nrkeys from empty KV [FAILED], status [%d]\n", ret);
        return 1;
    }

    if (nkey) {
        printf("TEST BG_THREAD: expired keys not wiped after executing bg thread [FAILED]\n");
        return 1;
    }

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);

    printf("TEST BG_THREAD: [OK]\n");
    return 0;
}
