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
#include <unistd.h>
#include <string.h>
#include <event.h>
#include <sys/eventfd.h>

#include "priskv.h"
#include "kv.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-log.h"

void *g_kv;

typedef enum priskv_kvmanage_action priskv_kvmanage_action;
typedef struct priskv_kvmanage_context priskv_kvmanage_context;
typedef int (*priskv_kvmanage_task)(priskv_kvmanage_context *ctx);

enum priskv_kvmanage_action {
    PRISKV_KVMANAGE_COPY = 0,
    PRISKV_KVMANAGE_MOVE,
    PRISKV_KVMANAGE_MAX,
};

struct priskv_kvmanage_context {
    void *client;

    char **keys;
    int nkeys;
    int nsuccess;

    void (*cb)(void *context, int status);
    void *context;
    int status;

    priskv_kvmanage_action action;
    priskv_kvmanage_task *flow;
    int flow_len;
    int step;

    struct event_base *evbase;
    struct event *ev;
};

static int priskv_kvmanage_workflow_run(priskv_kvmanage_context *ctx, priskv_status status);

static const char *action_str[] = {
    [PRISKV_KVMANAGE_COPY] = "copy",
    [PRISKV_KVMANAGE_MOVE] = "move",
    [PRISKV_KVMANAGE_MAX] = "unknown",
};
static inline const char *action2str(priskv_kvmanage_action action)
{
    if (action < PRISKV_KVMANAGE_COPY || action > PRISKV_KVMANAGE_MAX) {
        action = PRISKV_KVMANAGE_MAX;
    }
    return action_str[action];
}

static void __priskv_process(evutil_socket_t fd, short events, void *arg)
{
    priskv_client *client = arg;
    priskv_process(client, 0);
}

static void priskv_run_later(struct event_base *evbase, event_callback_fn fn, void *arg)
{
    uint64_t u = 1;

    int fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    event_base_once(evbase, fd, EV_READ, fn, arg, NULL);
    write(fd, &u, sizeof(u));
}

static void priskv_run_later_end(struct event_base *evbase, int fd)
{
    uint64_t u = 1;

    read(fd, &u, sizeof(u));
}

static void __priskv_kvmanage_context_destory(evutil_socket_t fd, short events, void *arg)
{
    priskv_kvmanage_context *ctx = arg;

    priskv_run_later_end(ctx->evbase, fd);

    priskv_close(ctx->client);
    event_del(ctx->ev);
    event_free(ctx->ev);
    ctx->cb(ctx->context, ctx->status);
    free(ctx);
}

static inline void priskv_kvmanage_context_destory(priskv_kvmanage_context *ctx, int status)
{
    if (!ctx) {
        return;
    }

    ctx->status = status;

    priskv_run_later(ctx->evbase, __priskv_kvmanage_context_destory, ctx);
}

static void priskv_set_cb(uint64_t request_id, priskv_status status, void *result)
{
    priskv_kvmanage_context *ctx = (priskv_kvmanage_context *)request_id;
    if (priskv_kvmanage_workflow_run(ctx, status) < 0) {
        priskv_kvmanage_context_destory(ctx, -1);
    }
}

static int priskv_kvmanage_do_copy(priskv_kvmanage_context *ctx)
{
    char *key = ctx->keys[ctx->nsuccess];
    uint16_t keylen = strlen(key);
    uint8_t *val;
    uint32_t valuelen = 0;
    void *keynode = NULL;
    priskv_sgl sgl;
    priskv_resp_status status;
    int ret = 0;

    status = priskv_get_key(g_kv, (uint8_t *)key, keylen, &val, &valuelen, &keynode);
    if (status != PRISKV_RESP_STATUS_OK || !keynode) {
        priskv_log_error("KVmanage(copy): failed to get key %s from KV module, status (%d): %s\n",
                       (const char *)key, status, priskv_resp_status_str(status));
        ret = -1;
        goto exit;
    }

    sgl.iova = (uint64_t)val;
    sgl.length = valuelen;
    sgl.mem = NULL;

    status =
        priskv_set_async(ctx->client, key, &sgl, 1, PRISKV_KEY_MAX_TIMEOUT, (uint64_t)ctx, priskv_set_cb);
    if (status != PRISKV_RESP_STATUS_OK) {
        priskv_log_error(
            "KVmanage(copy): failed to set key %s to remote PrisKV server, status (%d): %s\n",
            (const char *)key, status, priskv_resp_status_str(status));
        ret = -1;
        goto exit;
    }

exit:
    priskv_get_key_end(keynode);
    return ret;
}

static int priskv_kvmanage_do_delete(priskv_kvmanage_context *ctx)
{
    char *key = ctx->keys[ctx->nsuccess];
    uint16_t keylen = strlen(key);
    priskv_resp_status status;

    status = priskv_delete_key(g_kv, (uint8_t *)key, keylen);
    if (status != PRISKV_RESP_STATUS_OK) {
        priskv_log_error(
            "KVmanage(delete): failed to delete key %s from local PrisKV server, status (%d): %s\n",
            (const char *)key, status, priskv_resp_status_str(status));
        return -1;
    }

    return priskv_kvmanage_workflow_run(ctx, status);
}

static int priskv_kvmanage_task_copy(priskv_kvmanage_context *ctx)
{
    /* skip the first time */
    if (ctx->step > 1) {
        priskv_log_debug("KVmanage: successfully %s key %s to remote PrisKV server\n",
                       action2str(ctx->action), (const char *)ctx->keys[ctx->nsuccess]);

        if (++ctx->nsuccess == ctx->nkeys) {
            priskv_kvmanage_context_destory(ctx, 0);
            return 0;
        }
    }

    return priskv_kvmanage_do_copy(ctx);
}

static int priskv_kvmanage_task_delete(priskv_kvmanage_context *ctx)
{
    return priskv_kvmanage_do_delete(ctx);
}

static priskv_kvmanage_task priskv_kvmanage_copy_flow[] = {priskv_kvmanage_task_copy};
static priskv_kvmanage_task priskv_kvmanage_move_flow[] = {priskv_kvmanage_task_copy,
                                                       priskv_kvmanage_task_delete};

static int priskv_kvmanage_workflow_run(priskv_kvmanage_context *ctx, priskv_status status)
{
    if (status != PRISKV_STATUS_OK) {
        return -1;
    }

    int stage = ctx->step % ctx->flow_len;
    ctx->step++;

    return ctx->flow[stage](ctx);
}

int priskv_kvmanage_do_action(priskv_kvmanage_action action, const char *addr, int port, char **keys,
                            int nkeys, struct event_base *evbase,
                            void (*cb)(void *context, int status), void *context)
{
    priskv_kvmanage_context *ctx = calloc(1, sizeof(priskv_kvmanage_context));

    ctx->client = priskv_connect(addr, port, NULL, 0, 0);
    if (!ctx->client) {
        priskv_log_error("KVmanage: cannot connect to remote PrisKV server, addr: %s, port: %d\n", addr,
                       port);
        free(ctx);
        return -1;
    }

    ctx->action = action;
    ctx->evbase = evbase;
    ctx->context = context;
    ctx->cb = cb;
    ctx->nkeys = nkeys;
    ctx->keys = keys;
    ctx->nsuccess = 0;
    ctx->step = 0;

    switch (action) {
    case PRISKV_KVMANAGE_COPY:
        ctx->flow = priskv_kvmanage_copy_flow;
        ctx->flow_len = sizeof(priskv_kvmanage_copy_flow) / sizeof(priskv_kvmanage_task);
        break;
    case PRISKV_KVMANAGE_MOVE:
        ctx->flow = priskv_kvmanage_move_flow;
        ctx->flow_len = sizeof(priskv_kvmanage_move_flow) / sizeof(priskv_kvmanage_task);
        break;
    default:
        priskv_log_error("KVmanage: unknown action %d\n", action);
        priskv_close(ctx->client);
        free(ctx);
        return -1;
    }

    ctx->ev = event_new(evbase, priskv_get_fd(ctx->client), EV_READ | EV_PERSIST, __priskv_process,
                        ctx->client);
    event_add(ctx->ev, NULL);

    if (priskv_kvmanage_workflow_run(ctx, 0)) {
        priskv_close(ctx->client);
        event_del(ctx->ev);
        event_free(ctx->ev);
        free(ctx);
        return -1;
    }

    return 0;
}

int priskv_kvmanage_copy_to(const char *addr, int port, char **keys, int nkeys,
                          struct event_base *evbase, void (*cb)(void *context, int status),
                          void *context)
{
    return priskv_kvmanage_do_action(PRISKV_KVMANAGE_COPY, addr, port, keys, nkeys, evbase, cb,
                                   context);
}

int priskv_kvmanage_move_to(const char *addr, int port, char **keys, int nkeys,
                          struct event_base *evbase, void (*cb)(void *context, int status),
                          void *context)
{
    return priskv_kvmanage_do_action(PRISKV_KVMANAGE_MOVE, addr, port, keys, nkeys, evbase, cb,
                                   context);
}
