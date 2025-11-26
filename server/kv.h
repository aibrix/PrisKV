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

#ifndef __PRISKV_SERVER_KV__
#define __PRISKV_SERVER_KV__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdint.h>

#include "priskv-threads.h"
#include "priskv-protocol.h"
#include "backend/backend.h"

struct priskv_transport_conn;
typedef struct priskv_transport_conn priskv_transport_conn;

#define PRISKV_KV_DEFAULT_EXPIRE_ROUTINE_INTERVAL 600

void *priskv_new_kv(uint8_t *key_base, uint8_t *value_base, uint32_t max_keys,
                  uint16_t max_key_length, uint32_t value_block_size, uint64_t value_blocks);

void priskv_destroy_kv(void *kv);

int priskv_recover(void *kv);

void *priskv_get_value_base(void *kv);

uint64_t priskv_get_value_blocks(void *kv);

uint32_t priskv_get_value_block_size(void *kv);

uint64_t priskv_get_value_blocks_inuse(void *kv);

void priskv_update_valuelen(void *arg, uint32_t valuelen);

int priskv_get_key(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val, uint32_t *valuelen,
                 void **_keynode);
void priskv_get_key_end(void *arg);

int priskv_set_key(void *_kv, uint8_t *key, uint16_t keylen, uint8_t **val, uint32_t valuelen,
                 uint64_t timeout, void **_keynode);
void priskv_set_key_end(void *arg);

int priskv_delete_key(void *kv, uint8_t *key, uint16_t keylen);

int priskv_expire_key(void *kv, uint8_t *key, uint16_t keylen, uint64_t timeout);

int priskv_get_keys(void *kv, uint8_t *regex, uint16_t regexlen, uint8_t *keysbuf, uint32_t keyslen,
                  uint32_t *reallen, uint32_t *nkey);

int priskv_flush_keys(void *_kv, uint8_t *regex, uint16_t regexlen, uint32_t *nkey);

void priskv_clear_expired_kv(int fd, void *opaque, uint32_t events);

uint32_t priskv_get_keys_inuse(void *_kv);

uint32_t priskv_get_max_keys(void *_kv);

uint16_t priskv_get_max_key_length(void *_kv);

uint32_t priskv_get_bucket_count(void *_kv);

uint32_t priskv_get_expire_routine_interval(void *_kv);

void priskv_set_expire_routine_interval(void *_kv, uint32_t interval);

void priskv_expire_routine(priskv_thread *bgthread, void *_kv);

uint64_t priskv_get_expire_kv_count(void *_kv);

uint64_t priskv_get_expire_kv_bytes(void *_kv);

uint64_t priskv_get_expire_routine_times(void *_kv);

// save pending requests context when:
// 1. For the same key, a request has already been sent to the backend;
// 2. The number of current concurrent requests exceeds the backend queue depth.
typedef struct priskv_tiering_req {
    priskv_thread *thread;
    priskv_backend_device *backend;
    void *kv;
    priskv_request *req;
    uint64_t request_id;
    priskv_transport_conn *conn;

    /* kv operation context */
    uint8_t *key;
    uint16_t keylen;
    uint8_t *value;
    uint32_t valuelen;
    uint32_t remote_valuelen;
    void *keynode;
    uint64_t timeout;

    priskv_req_command cmd;

    bool execute;
    struct list_node node;
    uint32_t hash_head_index;
    
    priskv_backend_status backend_status;

    bool recv_reposted;
} priskv_tiering_req;

int priskv_backend_req_resubmit(void *req);
struct priskv_tiering_req *priskv_tiering_req_new(priskv_transport_conn *conn, priskv_request *req,
                                                  uint8_t *key, uint16_t keylen, uint64_t timeout,
                                                  priskv_req_command cmd, uint32_t remote_valuelen,
                                                  priskv_resp_status *resp_status);

// tiering concurrency control
bool priskv_key_serialize_enter(struct priskv_tiering_req *treq);
void priskv_key_serialize_exit(struct priskv_tiering_req *completed_req);

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_SERVER_KV__ */
