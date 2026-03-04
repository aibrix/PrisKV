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

#include "jsonobjs.h"

/* define for priskv_version_response_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_version_response)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_version_response, "version", version, priskv_string, required,
                             forced)
PRISKV_DECL_OBJECT_END(priskv_version_response, priskv_version_response)

/* define for priskv_memory_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_memory_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_memory_info, "type", type, priskv_string, required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_memory_info, "path", path, priskv_string, required, ignored)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_memory_info, "filesize", filesize, priskv_uint64, required, ignored)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_memory_info, "pagesize", pagesize, priskv_uint64, required, ignored)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_memory_info, "feature0", feature0, priskv_uint64, required, ignored)
PRISKV_DECL_OBJECT_END(priskv_memory_info, priskv_memory_info)

/* define for priskv_acl_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_acl_info)
PRISKV_DECL_OBJECT_ARRAY_FIELD(priskv_acl_info, "rules", rules, nrules, priskv_string, required, forced)
PRISKV_DECL_OBJECT_END(priskv_acl_info, priskv_acl_info)

/* define for priskv_kv_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_kv_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "bucket_count", bucket_count, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "keys_inuse", keys_inuse, priskv_uint64, required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "keys_max", keys_max, priskv_uint64, required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "key_max_length", key_max_length, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "value_block_size", value_block_size, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "value_blocks", value_blocks, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "value_blocks_inuse", value_blocks_inuse, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "expire_routine_times", expire_routine_times,
                             priskv_uint64, required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "expire_kv_count", expire_kv_count, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "expire_kv_bytes", expire_kv_bytes, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "pin_ops", pin_ops, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "unpin_ops", unpin_ops, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kv_info, "unpin_not_closed", unpin_not_closed, priskv_uint64,
                             required, forced)

PRISKV_DECL_OBJECT_END(priskv_kv_info, priskv_kv_info)

/* define for priskv_conn_client_stats_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_conn_client_stats_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "get_ops", get_ops, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "get_bytes", get_bytes, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "set_ops", set_ops, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "set_bytes", set_bytes, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "test_ops", test_ops, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "delete_ops", delete_ops, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "expire_ops", expire_ops, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_stats_info, "resps", resps, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_END(priskv_conn_client_stats_info, priskv_conn_client_stats_info)

/* define for priskv_conn_client_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_conn_client_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_info, "address", address, priskv_string, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_info, "dying", closing, priskv_boolean, required,
                             forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_client_info, "stats", stats, priskv_conn_client_stats_info,
                             required, forced)
PRISKV_DECL_OBJECT_END(priskv_conn_client_info, priskv_conn_client_info)

/* define for priskv_conn_listener_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_conn_listener_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_conn_listener_info, "address", address, priskv_string, required,
                             forced)
PRISKV_DECL_OBJECT_ARRAY_FIELD(priskv_conn_listener_info, "clients", clients, nclients,
                             priskv_conn_client_info, required, forced)
PRISKV_DECL_OBJECT_END(priskv_conn_listener_info, priskv_conn_listener_info)

/* define for priskv_connection_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_connection_info)
PRISKV_DECL_OBJECT_ARRAY_FIELD(priskv_connection_info, "listeners", listeners, nlisteners,
                             priskv_conn_listener_info, required, forced)
PRISKV_DECL_OBJECT_END(priskv_connection_info, priskv_connection_info)

/* define for priskv_kvmanage_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_kvmanage_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kvmanage_info, "addr", addr, priskv_string, required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_kvmanage_info, "port", port, priskv_int, required, forced)
PRISKV_DECL_OBJECT_ARRAY_FIELD(priskv_kvmanage_info, "keys", keys, nkeys, priskv_string, required, forced)
PRISKV_DECL_OBJECT_END(priskv_kvmanage_info, priskv_kvmanage_info)

/* define for priskv_cpu_info_obj */
PRISKV_DECL_OBJECT_BEGIN(priskv_cpu_info)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_cpu_info, "used_cpu_sys_ticks", used_cpu_sys_ticks, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_cpu_info, "used_cpu_user_ticks", used_cpu_user_ticks, priskv_uint64,
                             required, forced)
PRISKV_DECL_OBJECT_VALUE_FIELD(priskv_cpu_info, "clock_ticks", clock_ticks, priskv_uint64, required,
                             forced)
PRISKV_DECL_OBJECT_END(priskv_cpu_info, priskv_cpu_info)
