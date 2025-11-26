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

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/sysinfo.h>
#include <errno.h>

#include "priskv-log.h"
#include "jsonobjs.h"
#include "info.h"
#include "acl.h"
#include "memory.h"
#include "kv.h"
#include "transport/transport.h"

void priskv_info_get_acl(void *data)
{
    priskv_acl_info *info = (priskv_acl_info *)data;
    info->rules = priskv_acl_get_rules(&info->nrules);
}

void priskv_info_free_acl(void *data)
{
    priskv_acl_info *info = (priskv_acl_info *)data;
    priskv_acl_free_rules(info->rules, info->nrules);
}

void priskv_info_get_memory(void *data)
{
    priskv_memory_info *info = (priskv_memory_info *)data;
    priskv_mem_info *_info = priskv_mem_info_get();

    info->type = _info->type;
    info->path = _info->path;
    info->filesize = _info->filesize;
    info->pagesize = _info->pagesize;
    info->feature0 = _info->feature0;
}

void priskv_info_get_kv(void *data)
{
    priskv_kv_info *info = (priskv_kv_info *)data;
    void *kv = priskv_transport_get_kv();

    info->bucket_count = priskv_get_bucket_count(kv);
    info->keys_inuse = priskv_get_keys_inuse(kv);
    info->keys_max = priskv_get_max_keys(kv);
    info->key_max_length = priskv_get_max_key_length(kv);
    info->value_block_size = priskv_get_value_block_size(kv);
    info->value_blocks = priskv_get_value_blocks(kv);
    info->value_blocks_inuse = priskv_get_value_blocks_inuse(kv);
    info->expire_routine_times = priskv_get_expire_routine_times(kv);
    info->expire_kv_count = priskv_get_expire_kv_count(kv);
    info->expire_kv_bytes = priskv_get_expire_kv_bytes(kv);
}

void priskv_info_get_connection(void *data)
{
    priskv_connection_info *info = (priskv_connection_info *)data;

    priskv_transport_listener *listeners = priskv_transport_get_listeners(&info->nlisteners);
    info->listeners = calloc(info->nlisteners, sizeof(priskv_conn_listener_info));

    for (int i = 0; i < info->nlisteners; i++) {
        priskv_transport_listener *listener = &listeners[i];
        priskv_conn_listener_info *listener_info = &info->listeners[i];

        listener_info->address = strdup(listener->address);
        listener_info->nclients = listener->nclients;
        listener_info->clients = calloc(listener->nclients, sizeof(priskv_conn_client_info));

        for (int j = 0; j < listener->nclients; j++) {
            priskv_transport_client *client = &listener->clients[j];
            priskv_conn_client_info *client_info = &listener_info->clients[j];

            client_info->address = strdup(client->address);
            client_info->closing = client->closing;
            client_info->stats.get_ops = client->stats[PRISKV_COMMAND_GET].ops;
            client_info->stats.get_bytes = client->stats[PRISKV_COMMAND_GET].bytes;
            client_info->stats.set_ops = client->stats[PRISKV_COMMAND_SET].ops;
            client_info->stats.set_bytes = client->stats[PRISKV_COMMAND_SET].bytes;
            client_info->stats.delete_ops = client->stats[PRISKV_COMMAND_DELETE].ops;
            client_info->stats.test_ops = client->stats[PRISKV_COMMAND_TEST].ops;
            client_info->stats.expire_ops = client->stats[PRISKV_COMMAND_EXPIRE].ops;
            client_info->stats.resps = client->resps;
        }
    }

    priskv_transport_free_listeners(listeners, info->nlisteners);
}

void priskv_info_free_connection(void *data)
{
    priskv_connection_info *info = (priskv_connection_info *)data;

    for (int i = 0; i < info->nlisteners; i++) {
        free(info->listeners[i].address);
        for (int j = 0; j < info->listeners[i].nclients; j++) {
            free(info->listeners[i].clients[j].address);
        }
        free(info->listeners[i].clients);
    }
    free(info->listeners);
}

int get_process_cpu_time(pid_t pid, long *used_cpu_user_ticks, long *used_cpu_sys_ticks,
                         long *clock_ticks)
{
    char path[64];
    snprintf(path, sizeof(path), "/proc/%d/stat", pid);

    FILE *fp = fopen(path, "r");
    if (!fp) {
        perror("fopen");
        return -1;
    }

    char line[1024];
    if (fgets(line, sizeof(line), fp) == NULL) {
        fclose(fp);
        return -1;
    }

    fclose(fp);

    // 解析 /proc/[pid]/stat
    // 字段 14: utime（用户态 CPU 时间）
    // 字段 15: stime（内核态 CPU 时间）
    int fields = 0;
    long utime_ticks, stime_ticks;

    char *token = strtok(line, " ");
    while (token != NULL && fields <= 15) {
        if (fields == 13) {
            utime_ticks = atoll(token); // 字段14
        } else if (fields == 14) {
            stime_ticks = atoll(token); // 字段15
            break;
        }
        token = strtok(NULL, " ");
        fields++;
    }

    *clock_ticks = sysconf(_SC_CLK_TCK);
    *used_cpu_user_ticks = utime_ticks;
    *used_cpu_sys_ticks = stime_ticks;

    return 0;
}

void priskv_info_get_cpu(void *data)
{
    priskv_cpu_info *info = (priskv_cpu_info *)data;

    long used_cpu_sys_ticks, used_cpu_user_ticks, clock_ticks;
    pid_t pid = getpid();
    int ret = get_process_cpu_time(pid, &used_cpu_user_ticks, &used_cpu_sys_ticks, &clock_ticks);
    if (!ret) {
        priskv_log_warn("failed to get process cpu time\n");
    }

    info->used_cpu_sys_ticks = (uint64_t)used_cpu_sys_ticks;
    info->used_cpu_user_ticks = (uint64_t)used_cpu_user_ticks;
    info->clock_ticks = (uint64_t)clock_ticks;
}

typedef struct priskv_info_drv {
    const char *name;
    priskv_object *object;
    size_t data_size;
    void (*get)(void *data);
    void (*free)(void *data);
} priskv_info_drv;

static priskv_info_drv g_info_drv[] = {
    {"acl", &priskv_acl_info_obj, sizeof(priskv_acl_info), priskv_info_get_acl, priskv_info_free_acl},
    {"memory", &priskv_memory_info_obj, sizeof(priskv_memory_info), priskv_info_get_memory, NULL},
    {"kv", &priskv_kv_info_obj, sizeof(priskv_kv_info), priskv_info_get_kv, NULL},
    {"connection", &priskv_connection_info_obj, sizeof(priskv_connection_info),
     priskv_info_get_connection, priskv_info_free_connection},
    {"cpu", &priskv_cpu_info_obj, sizeof(priskv_cpu_info), priskv_info_get_cpu, NULL},
};

static void priskv_info_items_get_all(const char **items, int *nitems)
{
    *nitems = 0;
    for (int i = 0; i < sizeof(g_info_drv) / sizeof(priskv_info_drv); i++) {
        items[(*nitems)++] = g_info_drv[i].name;
    }
}

static bool priskv_info_items_need_all(const char **items, int nitems)
{
    for (int i = 0; i < nitems; i++) {
        if (!items[i] || !strcmp(items[i], "all")) {
            return true;
        }
    }
    return false;
}

static void priskv_info_items_deduplicate(const char **items, int nitems)
{
    for (int i = 0; i < nitems; i++) {
        if (!items[i]) {
            continue;
        }
        for (int j = i + 1; j < nitems; j++) {
            if (!items[j]) {
                continue;
            }
            if (!strcmp(items[i], items[j])) {
                items[j] = NULL;
            }
        }
    }
}

static void priskv_info_items_tidy(const char **items, int *nitems)
{
    if (priskv_info_items_need_all(items, *nitems)) {
        memset(items, 0, sizeof(char *) * (*nitems));
        priskv_info_items_get_all(items, nitems);
        return;
    }

    priskv_info_items_deduplicate(items, *nitems);
}

bool priskv_info_item_available(const char *item)
{
    if (!item) {
        return true;
    }

    for (int i = 0; i < sizeof(g_info_drv) / sizeof(priskv_info_drv); i++) {
        if (!strcmp(item, "all") || !strcmp(item, g_info_drv[i].name)) {
            return true;
        }
    }

    return false;
}

bool priskv_info_items_available(const char **items, int nitems)
{
    if (!items) {
        return false;
    }

    for (int i = 0; i < nitems; i++) {
        if (!priskv_info_item_available(items[i])) {
            return false;
        }
    }

    return true;
}

char *priskv_info_json(priskv_codec *codec, const char **items, int nitems)
{
    priskv_object *result_obj = priskv_codec_object_new();
    void *result_data = NULL;
    size_t result_data_size = 0;
    char *json;

    if (!items) {
        return NULL;
    }

    if (!nitems) {
        priskv_info_items_get_all(items, &nitems);
    } else {
        priskv_info_items_tidy(items, &nitems);
    }

    for (int i = 0; i < nitems; i++) {
        if (!items[i]) {
            continue;
        }

        for (int j = 0; j < sizeof(g_info_drv) / sizeof(priskv_info_drv); j++) {
            if (strcmp(g_info_drv[j].name, items[i])) {
                continue;
            }

            void *data = calloc(1, g_info_drv[j].data_size);
            g_info_drv[j].get(data);

            result_data_size += g_info_drv[j].data_size;
            result_data = realloc(result_data, result_data_size);

            memcpy(result_data + result_data_size - g_info_drv[j].data_size, data,
                   g_info_drv[j].data_size);
            priskv_codec_object_append_field(result_obj, g_info_drv[j].name, g_info_drv[j].object,
                                           false, false);

            free(data);
        }
    }

    json = priskv_codec_code(codec, result_data, result_obj);
    if (!json) {
        priskv_log_error("failed to encode info: %s", priskv_codec_get_error(codec));
    }

    size_t offset = 0;
    for (int i = 0; i < nitems; i++) {
        if (!items[i]) {
            continue;
        }

        for (int j = 0; j < sizeof(g_info_drv) / sizeof(priskv_info_drv); j++) {
            if (strcmp(g_info_drv[j].name, items[i])) {
                continue;
            }

            if (g_info_drv[j].free) {
                void *data = result_data + offset;
                g_info_drv[j].free(data);
            }
            offset += g_info_drv[j].data_size;
        }
    }

    free(result_data);
    priskv_codec_object_free(result_obj);

    return json;
}
