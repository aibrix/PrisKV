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

#ifndef __PRISKV_OBJECTS_H__
#define __PRISKV_OBJECTS_H__

#include "priskv-codec.h"

typedef struct priskv_version_response {
    const char *version;
} priskv_version_response;

extern priskv_object priskv_version_response_obj;

typedef struct priskv_memory_info {
    const char *type;
    const char *path;
    uint64_t filesize;
    uint64_t pagesize;
    uint64_t feature0;
} priskv_memory_info;

extern priskv_object priskv_memory_info_obj;

typedef struct priskv_acl_info {
    char **rules;
    int nrules;
} priskv_acl_info;

extern priskv_object priskv_acl_info_obj;

typedef struct priskv_kv_info {
    uint64_t keys_inuse;
    uint64_t bucket_count;
    uint64_t keys_max;
    uint64_t key_max_length;
    uint64_t value_block_size;
    uint64_t value_blocks;
    uint64_t value_blocks_inuse;
    uint64_t expire_routine_times;
    uint64_t expire_kv_count;
    uint64_t expire_kv_bytes;
    /* pin/unpin observability */
    uint64_t pin_ops;
    uint64_t pin_failed_ops;
    uint64_t unpin_ops;
    uint64_t unpin_not_closed;
    /* TODO(wangyi): Extend with PinTTL metrics once available
     * - uint64_t pin_ttl_active;
     * - uint64_t pin_ttl_expired;
     * - uint64_t pin_ttl_cleanup_ops;
     * - uint64_t pin_ttl_orphaned;
     */
} priskv_kv_info;

extern priskv_object priskv_kv_info_obj;

typedef struct priskv_conn_client_stats_info {
    uint64_t get_ops;
    uint64_t get_bytes;
    uint64_t set_ops;
    uint64_t set_bytes;
    uint64_t test_ops;
    uint64_t delete_ops;
    uint64_t expire_ops;
    uint64_t resps;
} priskv_conn_client_stats_info;

typedef struct priskv_conn_client_info {
    char *address;
    bool closing;
    priskv_conn_client_stats_info stats;
} priskv_conn_client_info;

typedef struct priskv_conn_listener_info {
    char *address;
    priskv_conn_client_info *clients;
    int nclients;
} priskv_conn_listener_info;

typedef struct priskv_connection_info {
    priskv_conn_listener_info *listeners;
    int nlisteners;
} priskv_connection_info;

extern priskv_object priskv_connection_info_obj;

typedef struct priskv_kvmanage_info {
    char *addr;
    int port;
    char **keys;
    int nkeys;
} priskv_kvmanage_info;

extern priskv_object priskv_kvmanage_info_obj;

typedef struct priskv_cpu_info {
    uint64_t used_cpu_sys_ticks;
    uint64_t used_cpu_user_ticks;
    uint64_t clock_ticks;
} priskv_cpu_info;

extern priskv_object priskv_cpu_info_obj;

#endif /* __PRISKV_OBJECTS_H__ */
