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

#include "memory.h"
#include "kv.h"
#include "buddy.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-utils.h"

typedef struct test_kv {
    uint16_t keylen;
    uint8_t *key;
    uint32_t valuelen;
    uint8_t *value;
    uint8_t *value_in_kv;
    uint32_t valuelen_in_kv;
} test_kv;

typedef struct test_keys_bucket {
    uint32_t max_keys;
    uint32_t bucket_count;
} test_keys_bucket;

static test_kv *test_kv_gen(uint32_t max_keys, uint16_t max_key_length, uint32_t max_value_length)
{
    test_kv *test_kvs, *tkv;

    srandom(getpid());
    test_kvs = calloc(max_keys, sizeof(test_kv));

    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        tkv->keylen = random() % (max_key_length / 2) + max_key_length / 2;
        tkv->key = calloc(1, tkv->keylen);
        priskv_random_string(tkv->key, tkv->keylen);

        tkv->valuelen = random() % (max_value_length / 2) + max_value_length / 2;
        tkv->value = calloc(1, tkv->valuelen);
        priskv_random_string(tkv->value, tkv->valuelen);
    }

    return test_kvs;
}

static void test_kv_free(test_kv *test_kvs, uint32_t max_keys)
{
    test_kv *tkv;

    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        free(tkv->key);
        free(tkv->value);
    }

    free(test_kvs);
}

static int set_kv_with_timeout(void *kv, test_kv *test_kvs, uint32_t max_keys, uint64_t timeout)
{
    test_kv *tkv;
    void *keynode;

    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        int ret = priskv_set_key(kv, tkv->key, tkv->keylen, &tkv->value_in_kv, tkv->valuelen, timeout,
                               &keynode);
        if (ret != PRISKV_RESP_STATUS_OK || !keynode) {
            printf("TEST KV: set keys with timeout %ld [FAILED]\n", timeout);
            return 1;
        }

        memcpy(tkv->value_in_kv, tkv->value, tkv->valuelen);
        priskv_set_key_end(keynode);
    }

    return 0;
}

static int get_kv_and_compare(void *kv, test_kv *test_kvs, uint32_t max_keys, bool expect_empty)
{
    test_kv *tkv;
    void *keynode;

    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        uint8_t *value_in_kv;

        // TODO: why not also use value_in_kv when empty?
        int ret =
            priskv_get_key(kv, tkv->key, tkv->keylen, &value_in_kv, &tkv->valuelen_in_kv, &keynode);
        if (expect_empty) {
            if (ret == PRISKV_RESP_STATUS_NO_SUCH_KEY && !keynode) {
                continue;
            }

            printf("TEST KV: get keys from empty KV [FAILED]\n");
            return 1;
        }

        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: verify KV [%d] status [%s] [FAILED]\n", i, priskv_resp_status_str(ret));
            return 1;
        }

        if (value_in_kv != tkv->value_in_kv) {
            printf("TEST KV: verify KV [%d] value pointer [%p] against [%p] [FAILED]\n", i,
                   value_in_kv, tkv->value_in_kv);
            return 1;
        }

        if (tkv->valuelen != tkv->valuelen_in_kv) {
            printf("TEST KV: verify KV [%d] value length [%d] against [%d] [FAILED]\n", i,
                   tkv->valuelen, tkv->valuelen_in_kv);
            return 1;
        }

        if (memcmp(value_in_kv, tkv->value, tkv->valuelen)) {
            printf("TEST KV: verify KV [%d] value compare [FAILED]\n", i);
            printf("\t%s\n", value_in_kv);
            printf("\t%s\n", tkv->value);
            return 1;
        }

        priskv_get_key_end(keynode);
    }

    return 0;
}

static bool test_kv_has_key(test_kv *test_kvs, uint32_t max_keys, uint8_t *key, uint16_t keylen,
                            uint32_t valuelen)
{
    test_kv *tkv;
    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        if ((tkv->keylen == keylen) && (tkv->valuelen == valuelen)) {
            if (!memcmp(tkv->key, key, keylen)) {
                return true;
            }
        }
    }

    return false;
}

static int get_keys(void *kv, test_kv *test_kvs, uint32_t max_keys)
{
    test_kv *tkv;
    uint32_t reallen, totalkeylen = 0, nkey;
    void *keysbuf;
    int ret;

    ret = set_kv_with_timeout(kv, test_kvs, max_keys, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        printf("TEST KV: set keys to empty KV during test regex [OK]\n");
        return ret;
    }

    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];

        totalkeylen += tkv->keylen;
        totalkeylen += sizeof(priskv_keys_resp);
    }

    /* use a small buffer, segment fault on bug */
    keysbuf =
        priskv_mem_malloc(totalkeylen / 2, MAP_PRIVATE | MAP_ANONYMOUS, -1, true);
    ret = priskv_get_keys(kv, (uint8_t *)".*", 2, keysbuf, totalkeylen / 2, &reallen, &nkey);
    if (ret != PRISKV_RESP_STATUS_VALUE_TOO_BIG) {
        printf("TEST KV: get regex (.) from empty KV in short buffer [FAILED]\n");
    }

    if (!reallen || (reallen != totalkeylen)) {
        printf("TEST KV: get regex (.) from empty KV verify length [FAILED]\n");
    }

    priskv_mem_free(keysbuf, totalkeylen / 2, true);
    printf("TEST KV: get regex (.) from empty KV in short buffer [OK]\n");

    /* use a large buffer, verify all keys */
    keysbuf = priskv_mem_malloc(totalkeylen, MAP_PRIVATE | MAP_ANONYMOUS, -1, true);
    ret = priskv_get_keys(kv, (uint8_t *)".*", 2, keysbuf, totalkeylen, &reallen, &nkey);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: get regex (.) from empty KV in large buffer [FAILED]\n");
    }

    if (!reallen || (reallen != totalkeylen)) {
        printf("TEST KV: get regex (.) from empty KV verify length [FAILED]\n");
    }

    uint8_t *buf = (uint8_t *)keysbuf;
    for (uint32_t i = 0; i < max_keys; i++) {
        priskv_keys_resp *keys_resp = (priskv_keys_resp *)buf;
        uint16_t keylen = be16toh(keys_resp->keylen);
        uint32_t valuelen = be32toh(keys_resp->valuelen);

        buf += sizeof(priskv_keys_resp);

        assert(test_kv_has_key(test_kvs, max_keys, buf, keylen, valuelen));
        buf += keylen;
    }

    priskv_mem_free(keysbuf, totalkeylen, true);
    printf("TEST KV: get regex (.) from empty KV in large buffer [OK]\n");

    return 0;
}

static int set_key_refcnt_test(void *kv, const char *key, void **_keynode)
{
    uint16_t keylen = strlen(key) + 1;
    const char *value = "value";
    uint32_t valuelen = strlen(value) + 1;
    uint8_t *value_in_kv;
    void *keynode;
    int ret;

    ret = priskv_set_key(kv, (uint8_t *)key, keylen, &value_in_kv, valuelen, PRISKV_KEY_MAX_TIMEOUT,
                       &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] set key to KV, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return ret;
    }

    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after setting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    memcpy(value_in_kv, value, valuelen);
    priskv_set_key_end(keynode);

    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing setting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    *_keynode = keynode;

    return 0;
}

static int get_key_refcnt_test(void *kv, const char *key, void *_keynode)
{
    uint16_t keylen = strlen(key) + 1;
    uint8_t *value_in_kv;
    uint32_t valuelen_in_kv;
    void *keynode;
    int ret;

    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &value_in_kv, &valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] get key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }

    if (keynode != _keynode) {
        printf("TEST KV: [%s] incorrect keynode(%p) [FAILED]\n", __func__, keynode);
        return 1;
    }

    if (((priskv_key *)keynode)->refcnt != 2) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after getting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    priskv_get_key_end(keynode);

    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing getting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    return 0;
}

static int expire_key_refcnt_test(void *kv, const char *key, void *keynode)
{
    uint16_t keylen = strlen(key) + 1;
    int ret;

    ret = priskv_expire_key(kv, (uint8_t *)key, keylen, PRISKV_KEY_MAX_TIMEOUT);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] expire key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }

    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after expiring key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    return 0;
}

static int delete_key_refcnt_test(void *kv, const char *key, void *keynode)
{
    uint16_t keylen = strlen(key) + 1;
    int ret;

    ret = priskv_delete_key(kv, (uint8_t *)key, keylen);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] delete key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }

    if (((priskv_key *)keynode)->refcnt != 0) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after delete key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    return 0;
}

static int get_and_delete_key_refcnt_test(void *kv)
{
    const char *key = __func__;
    uint16_t keylen = strlen(key) + 1;
    const char *value = "value";
    uint32_t valuelen = strlen(value) + 1;
    uint8_t *set_value_in_kv, *get_value_in_kv;
    uint32_t get_valuelen_in_kv;
    void *keynode;
    int ret;

    priskv_key empty_keynode;
    memset(&empty_keynode, 0, sizeof(empty_keynode));

    /* set key, refcnt = 1 */
    ret = priskv_set_key(kv, (uint8_t *)key, keylen, &set_value_in_kv, valuelen, PRISKV_KEY_MAX_TIMEOUT,
                       &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] set key to KV, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return ret;
    }
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after setting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }
    memcpy(set_value_in_kv, value, valuelen);
    priskv_set_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing setting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* get key, refcnt = 2 */
    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &get_value_in_kv, &get_valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] get key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 2) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after getting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* get key again, refcnt = 3 */
    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &get_value_in_kv, &get_valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] get key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 3) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after getting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* delete key, refcnt = 2 */
    ret = priskv_delete_key(kv, (uint8_t *)key, keylen);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] delete key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 2) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after delete key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* finish to get key, refcnt = 1 */
    priskv_get_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing getting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* finish to get key again, refcnt = 0 */
    priskv_get_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 0) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing getting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* check if keynode is empty, it is OK that keynode is recycled */
    if (memcmp(&empty_keynode, keynode, sizeof(empty_keynode))) {
        printf("TEST KV: [%s] keynode is not empty [FAILED]\n", __func__);
        return 1;
    }

    return 0;
}

static int get_updated_key_refcnt_test(void *kv)
{
    const char *key = __func__;
    uint16_t keylen = strlen(key) + 1;
    const char *value = "value";
    uint32_t valuelen = strlen(value) + 1;
    uint8_t *set_value_in_kv, *get_value_in_kv;
    uint32_t get_valuelen_in_kv;
    void *keynode, *new_keynode;
    int ret;

    priskv_key empty_keynode;
    memset(&empty_keynode, 0, sizeof(empty_keynode));

    /* set key, keynode->refcnt = 1 */
    ret = priskv_set_key(kv, (uint8_t *)key, keylen, &set_value_in_kv, valuelen, PRISKV_KEY_MAX_TIMEOUT,
                       &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] set key to KV, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return ret;
    }
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after setting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }
    memcpy(set_value_in_kv, value, valuelen);
    priskv_set_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing setting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* get key, keynode->refcnt = 2 */
    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &get_value_in_kv, &get_valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] get key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 2) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after getting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* set the same key, new_keynode->refcnt = 1, keynode->refcnt = 1 */
    ret = priskv_set_key(kv, (uint8_t *)key, keylen, &set_value_in_kv, valuelen, PRISKV_KEY_MAX_TIMEOUT,
                       &new_keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] set the same key to KV, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return ret;
    }
    if (((priskv_key *)new_keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after setting new key [FAILED]\n", __func__,
               ((priskv_key *)new_keynode)->refcnt);
        return 1;
    }
    memcpy(set_value_in_kv, value, valuelen);
    priskv_set_key_end(new_keynode);
    if (((priskv_key *)new_keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing setting new key [FAILED]\n",
               __func__, ((priskv_key *)new_keynode)->refcnt);
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after getting key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* finish to get key, keynode->refcnt = 0 */
    priskv_get_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 0) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after finishing getting key [FAILED]\n",
               __func__, ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    /* check if keynode is empty, it is OK that keynode is recycled */
    if (memcmp(&empty_keynode, keynode, sizeof(empty_keynode))) {
        printf("TEST KV: [%s] keynode is not empty [FAILED]\n", __func__);
        return 1;
    }

    /* delete key, new_keynode->refcnt = 0 */
    ret = priskv_delete_key(kv, (uint8_t *)key, keylen);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] delete key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)new_keynode)->refcnt != 0) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after delete key [FAILED]\n", __func__,
               ((priskv_key *)new_keynode)->refcnt);
        return 1;
    }

    return 0;
}

static int key_refcnt_test(void *kv)
{
    void *keynode;
    const char *key = __func__;

    if (set_key_refcnt_test(kv, key, &keynode)) {
        return 1;
    }

    if (get_key_refcnt_test(kv, key, keynode)) {
        return 1;
    }

    if (expire_key_refcnt_test(kv, key, keynode)) {
        return 1;
    }

    if (delete_key_refcnt_test(kv, key, keynode)) {
        return 1;
    }

    if (get_and_delete_key_refcnt_test(kv)) {
        return 1;
    }

    if (get_updated_key_refcnt_test(kv)) {
        return 1;
    }

    return 0;
}

static int get_key_in_updating(void *kv)
{
    const char *key = __func__;
    uint16_t keylen = strlen(key) + 1;
    const char *value = "value";
    uint32_t valuelen = strlen(value) + 1;
    uint8_t *set_value_in_kv, *get_value_in_kv;
    uint32_t get_valuelen_in_kv;
    void *keynode;
    int ret;

    /* set key, keynode->refcnt = 1, status = PRISKV_RESP_STATUS_KEY_UPDATING */
    ret = priskv_set_key(kv, (uint8_t *)key, keylen, &set_value_in_kv, valuelen, PRISKV_KEY_MAX_TIMEOUT,
                       &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [get UPDATING key] set key to empty KV [FAILED]\n");
        return ret;
    }
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [get UPDATING key] incorrect refcnt after setting key [FAILED]\n");
        return 1;
    }

    /* get key, keynode->refcnt = 2, status = PRISKV_RESP_STATUS_KEY_UPDATING */
    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &get_value_in_kv, &get_valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_KEY_UPDATING) {
        printf("TEST KV: [get UPDATING key] status [%s] is incorrect [FAILED]\n",
               priskv_resp_status_str(ret));
        return 1;
    }

    /* finish to get key, keynode->refcnt = 1 */
    priskv_get_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [get UPDATING key] incorrect refcnt after first getting key [FAILED]\n");
        return 1;
    }

    /* finish to set key, keynode->refcnt = 1, status = PRISKV_RESP_STATUS_OK */
    memcpy(set_value_in_kv, value, valuelen);
    priskv_set_key_end(keynode);

    /* get key, keynode->refcnt = 2, status = PRISKV_RESP_STATUS_OK */
    ret = priskv_get_key(kv, (uint8_t *)key, keylen, &get_value_in_kv, &get_valuelen_in_kv, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [get UPDATING key] status [%s] is incorrect [FAILED]\n",
               priskv_resp_status_str(ret));
        return 1;
    }

    if (memcmp(get_value_in_kv, value, valuelen)) {
        printf("TEST KV: [get UPDATING key] get value '%s', expected '%s' [FAILED]\n",
               get_value_in_kv, value);
        return 1;
    }

    /* finish to get key, keynode->refcnt = 1 */
    priskv_get_key_end(keynode);
    if (((priskv_key *)keynode)->refcnt != 1) {
        printf("TEST KV: [get UPDATING key] incorrect refcnt after second getting key [FAILED]\n");
        return 1;
    }

    /* delete key, new_keynode->refcnt = 0 */
    ret = priskv_delete_key(kv, (uint8_t *)key, keylen);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV: [%s] delete key from kv, status [%s] [FAILED]\n", __func__,
               priskv_resp_status_str(ret));
        return 1;
    }
    if (((priskv_key *)keynode)->refcnt != 0) {
        printf("TEST KV: [%s] incorrect refcnt(%u) after delete key [FAILED]\n", __func__,
               ((priskv_key *)keynode)->refcnt);
        return 1;
    }

    return 0;
}

static int test_hash_bucket_count()
{
    void *kv;
    uint8_t *key_base, *value_base;
    test_keys_bucket keys_bucket_pair[] = {
        {2, 2},
        {32, 32},
        {512, 512},
        {8192, 8192},
        {131072, 131071},
        {262144, 262139},
        {524288, 524287},
        {1048576, 1048573},
        {2097152, 1048573},
        {8388608, 4194301},
        {16777216, 16777213},
        {67108864, 16777213},
        {1073741824, 134217689},
    };

    int ret = 0, len = sizeof(keys_bucket_pair) / sizeof(test_keys_bucket);
    /*
     * The following variables only for creating shared key_base and value_base.
     *
     * Since we just test bucket_count, it is reasonable creating smaller key_base and value_base.
     */
    uint32_t max_keys = 128 * 1024;
    uint16_t max_key_length = 128;
    uint32_t value_block_size = 4096;
    uint64_t value_blocks = max_keys * 16;

    key_base = calloc(max_keys, priskv_mem_key_size(max_key_length));
    value_base = calloc(1, priskv_buddy_mem_size(value_blocks, value_block_size));

    for (int i = 0; i < len; i++) {
        kv = priskv_new_kv(key_base, value_base, -1, 0, keys_bucket_pair[i].max_keys, max_key_length,
                           value_block_size, value_blocks);
        assert(kv);

        if (priskv_get_bucket_count(kv) != keys_bucket_pair[i].bucket_count) {
            printf("TEST KV: incorrect bucket count(%u) for max_keys(%u), expected %u [FAILED]\n",
                   priskv_get_bucket_count(kv), keys_bucket_pair[i].max_keys,
                   keys_bucket_pair[i].bucket_count);
            ret = 1;
            priskv_destroy_kv(kv);
            goto end;
        }

        priskv_destroy_kv(kv);
    }

end:
    free(key_base);
    free(value_base);
    return ret;
}

int main()
{
    void *kv;
    int ret = 0;
    uint32_t max_keys = 128 * 1024;
    uint16_t max_key_length = 128;
    uint32_t value_block_size = 4096;
    uint64_t value_blocks = max_keys * 16;
    uint32_t max_value_length = value_block_size * 16;
    test_kv *test_kvs, *tkv;
    uint8_t *key_base, *value_base;
    uint32_t nkey;

    ret = test_hash_bucket_count();
    if (ret) {
        return ret;
    }

    printf("TEST KV: test bucket_count [OK]\n");

    /* step 0, prepare test env */
    test_kvs = test_kv_gen(max_keys, max_key_length, max_value_length);
    assert(test_kvs);

    key_base = calloc(max_keys, priskv_mem_key_size(max_key_length));
    value_base = calloc(1, priskv_buddy_mem_size(value_blocks, value_block_size));
    kv = priskv_new_kv(key_base, value_base, -1, 0, max_keys, max_key_length, value_block_size,
                       value_blocks);
    assert(kv);

    /* step 1, get keys from empty KV */
    ret = get_kv_and_compare(kv, test_kvs, max_keys, true);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 2, set keys to empty KV without timeout */
    ret = set_kv_with_timeout(kv, test_kvs, max_keys, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        return ret;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 3, get keys from KV and compare values */
    ret = get_kv_and_compare(kv, test_kvs, max_keys, false);
    if (ret) {
        return ret;
    }

    printf("TEST KV: verify keys from filled KV [OK]\n");

    /* step 4, delete keys from empty KV */
    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        ret = priskv_delete_key(kv, tkv->key, tkv->keylen);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: delete keys from filled KV [FAILED]\n");
            return 1;
        }
    }

    printf("TEST KV: delete all keys in KV [OK]\n");

    /* step 5, get keys from empty KV */
    ret = get_kv_and_compare(kv, test_kvs, max_keys, true);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 6, set keys with timeout 5s*/
    ret = set_kv_with_timeout(kv, test_kvs, max_keys, 5 * 1000);
    if (ret) {
        return ret;
    }

    printf("TEST KV: set keys with timeout 5s [OK]\n");

    /* step 7, get keys from KV before expired and compare values */
    sleep(3);
    ret = get_kv_and_compare(kv, test_kvs, max_keys, false);
    if (ret) {
        return ret;
    }

    printf("TEST KV: verify keys from filled KV before expired [OK]\n");

    /* step 8, get keys from KV after expired */
    sleep(7);
    ret = get_kv_and_compare(kv, test_kvs, max_keys, true);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get keys after expired [OK]\n");

    /* step 9, set keys to empty KV without timeout */
    ret = set_kv_with_timeout(kv, test_kvs, max_keys, PRISKV_KEY_MAX_TIMEOUT);
    if (ret) {
        return ret;
    }

    printf("TEST KV: set keys to empty KV [OK]\n");

    /* step 10, get keys from KV and compare values after a while */
    sleep(7);
    ret = get_kv_and_compare(kv, test_kvs, max_keys, false);
    if (ret) {
        return ret;
    }

    printf("TEST KV: verify keys from filled KV after a while with no expire time [OK]\n");

    /* step 11, set expire time 5s */
    for (uint32_t i = 0; i < max_keys; i++) {
        tkv = &test_kvs[i];
        ret = priskv_expire_key(kv, tkv->key, tkv->keylen, 5 * 1000);
        if (ret != PRISKV_RESP_STATUS_OK) {
            printf("TEST KV: set expire time [FAILED]\n");
            return 1;
        }
    }

    printf("TEST KV: set expire time 5s [OK]\n");

    /* step 12, get keys from empty KV */
    sleep(7);
    ret = get_kv_and_compare(kv, test_kvs, max_keys, true);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get keys from empty KV [OK]\n");

    /* step 13, get keys from empty KV */
    uint32_t reallen;
    ret = priskv_get_keys(kv, (uint8_t *)".", 2, NULL, 0, &reallen, &nkey);
    if (ret || reallen) {
        printf("TEST KV: get regex (.) from empty KV [FAILED]\n");
        return 1;
    }

    printf("TEST KV: get regex (.) from empty KV [OK]\n");

    /* step 14, key refcnt test */
    ret = key_refcnt_test(kv);
    if (ret) {
        return ret;
    }

    printf("TEST KV: key refcnt test [OK]\n");

    /* step 15, get UPDATING key */
    ret = get_key_in_updating(kv);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get UPDATING key [OK]\n");

    /* step 16, set keys to empty KV without timeout */
    ret = get_keys(kv, test_kvs, max_keys);
    if (ret) {
        return ret;
    }

    printf("TEST KV: get regex (.) from filled KV [OK]\n");

    printf("TEST KV: All test [OK]\n");
    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    test_kv_free(test_kvs, max_keys);

    return 0;
}
