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
 *   Jinlong Xuan <xuanjinlong@bytedance.com>
 */

#define _DEFAULT_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>

#include "backend.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "list.h"

/* hiredis async */
#include <hiredis/async.h>
#include <hiredis/hiredis.h>
#include <hiredis/adapters/libevent.h>
#include <event2/event.h>

#define REDIS_DEFAULT_HOST "127.0.0.1"
#define REDIS_DEFAULT_PORT 6379
#define REDIS_DEFAULT_DB 0
#define REDIS_DEFAULT_TIMEOUT_MS 5000
#define REDIS_DEFAULT_MAX_DEPTH 1024
#define MAX_REASONABLE_TIMEOUT_MS (365 * 24 * 60 * 60 * 1000ULL) // 1 year in milliseconds

typedef enum {
    REDIS_OP_GET,
    REDIS_OP_SET,
    REDIS_OP_DEL,
    REDIS_OP_TEST,
} redis_op_type;

typedef struct redis_shared_context {
    char host[256];
    int port;
    int db;
    char *password;
    char *prefix;
    int timeout_ms;

    // queue depth control
    pthread_spinlock_t queue_lock;
    uint32_t inflight_count;
    uint32_t max_depth;
    struct list_head pending_queue;

    // libevent + hiredis async context
    struct event_base *evbase;
    redisAsyncContext *async_ctx;
    pthread_t event_thread;

    // connection and authentication state
    pthread_mutex_t state_lock;
    bool connected;
    bool authenticated;
    bool ready;
    bool shutting_down;

    int ref_count;
    pthread_spinlock_t lock;
} redis_shared_context;

typedef struct redis_thread_context {
    priskv_backend_device *bdev;
    redis_shared_context *shared_ctx;
} redis_thread_context;

typedef struct redis_request {
    redis_thread_context *ctx;
    struct list_node node;

    char *key;
    uint8_t *val;
    uint64_t valuelen;
    uint64_t timeout_ms;
    priskv_backend_driver_cb cb;
    void *cbarg;
    redis_op_type op_type;
    priskv_backend_status status;
    bool queued_once;
} redis_request;

static redis_shared_context *redis_shared_ctx = NULL;
static pthread_mutex_t redis_shared_ctx_lock = PTHREAD_MUTEX_INITIALIZER;

static void redis_complete_request(redis_request *req);
static bool submit_redis_request(redis_request *req);

static redis_shared_context *redis_shared_context_create(const char *address);
static void redis_shared_context_destroy(redis_shared_context *ctx);

static redis_shared_context *redis_shared_context_get(void)
{
    redis_shared_context *ctx = NULL;

    pthread_mutex_lock(&redis_shared_ctx_lock);
    if (redis_shared_ctx) {
        pthread_spin_lock(&redis_shared_ctx->lock);
        redis_shared_ctx->ref_count++;
        pthread_spin_unlock(&redis_shared_ctx->lock);
        ctx = redis_shared_ctx;
    }
    pthread_mutex_unlock(&redis_shared_ctx_lock);

    return ctx;
}

static void redis_shared_context_put(redis_shared_context *ctx)
{
    if (!ctx) {
        return;
    }

    bool should_free = false;

    pthread_mutex_lock(&redis_shared_ctx_lock);
    pthread_spin_lock(&ctx->lock);
    ctx->ref_count--;
    if (ctx->ref_count == 0) {
        should_free = true;
        redis_shared_ctx = NULL;
    }
    pthread_spin_unlock(&ctx->lock);
    pthread_mutex_unlock(&redis_shared_ctx_lock);

    if (should_free) {
        redis_shared_context_destroy(ctx);
    }
}

/* --- small helpers --- */
static const char *getenv_default(const char *name, const char *def)
{
    const char *v = getenv(name);
    return (v && *v) ? v : def;
}

static int getenv_int_default(const char *name, int def)
{
    const char *v = getenv(name);
    if (!v || !*v)
        return def;
    char *end = NULL;
    long n = strtol(v, &end, 10);
    if (end == v || errno == ERANGE)
        return def;
    return (int)n;
}

/* address format:
 * host=127.0.0.1&port=6379&db=0&password=xxx&timeout_ms=5000&prefix=abc:&max_depth=1024 */
static void redis_parse_address(redis_shared_context *ctx, const char *address)
{
    priskv_log_debug("redis_parse_address: starting with address: %s\n",
                     address ? address : "NULL");

    /* defaults possibly overridden by env */
    snprintf(ctx->host, sizeof(ctx->host), "%s",
             getenv_default("PRISKV_REDIS_HOST", REDIS_DEFAULT_HOST));
    ctx->port = getenv_int_default("PRISKV_REDIS_PORT", REDIS_DEFAULT_PORT);
    ctx->db = getenv_int_default("PRISKV_REDIS_DB", REDIS_DEFAULT_DB);
    ctx->timeout_ms = getenv_int_default("PRISKV_REDIS_TIMEOUT_MS", REDIS_DEFAULT_TIMEOUT_MS);
    ctx->max_depth = getenv_int_default("PRISKV_REDIS_MAX_DEPTH", REDIS_DEFAULT_MAX_DEPTH);

    const char *env_pass = getenv("PRISKV_REDIS_PASSWORD");
    const char *env_prefix = getenv("PRISKV_REDIS_PREFIX");

    priskv_log_debug("redis_parse_address: environment variables:\n");
    priskv_log_debug("  PRISKV_REDIS_HOST: %s\n",
                     getenv("PRISKV_REDIS_HOST") ? getenv("PRISKV_REDIS_HOST") : "NULL");
    priskv_log_debug("  PRISKV_REDIS_PORT: %d\n", ctx->port);
    priskv_log_debug("  PRISKV_REDIS_DB: %d\n", ctx->db);
    priskv_log_debug("  PRISKV_REDIS_PASSWORD: [configured]\n");
    priskv_log_debug("  PRISKV_REDIS_PREFIX: %s\n", env_prefix ? env_prefix : "NULL");
    priskv_log_debug("  PRISKV_REDIS_TIMEOUT_MS: %d\n", ctx->timeout_ms);
    priskv_log_debug("  PRISKV_REDIS_MAX_DEPTH: %d\n", ctx->max_depth);

    if (env_pass && *env_pass) {
        ctx->password = strdup(env_pass);
        priskv_log_debug("redis_parse_address: password set from environment: [%s] (length: %zu)\n",
                         ctx->password, strlen(ctx->password));
    } else {
        priskv_log_debug("redis_parse_address: no password from environment\n");
    }

    if (env_prefix && *env_prefix) {
        ctx->prefix = strdup(env_prefix);
        priskv_log_debug("redis_parse_address: prefix set from environment: [%s]\n", ctx->prefix);
    } else {
        priskv_log_debug("redis_parse_address: no prefix from environment\n");
    }

    if (!address || !*address) {
        priskv_log_debug(
            "redis_parse_address: no inline address provided, using environment/config only\n");
        return; /* no inline address, env-only */
    }

    priskv_log_debug("redis_parse_address: parsing inline address: %s\n", address);
    char *addr = strdup(address);
    char *save = addr;
    char *tok;
    while ((tok = strsep(&addr, "&")) != NULL) {
        char *eq = strchr(tok, '=');
        if (!eq)
            continue;
        *eq = '\0';
        const char *k = tok;
        const char *v = eq + 1;
        if (!strcmp(k, "host")) {
            snprintf(ctx->host, sizeof(ctx->host), "%s", v);
            priskv_log_debug("redis_parse_address: set host from address: %s\n", ctx->host);
        } else if (!strcmp(k, "port")) {
            ctx->port = atoi(v);
            priskv_log_debug("redis_parse_address: set port from address: %d\n", ctx->port);
        } else if (!strcmp(k, "db")) {
            ctx->db = atoi(v);
            priskv_log_debug("redis_parse_address: set db from address: %d\n", ctx->db);
        } else if (!strcmp(k, "password")) {
            if (ctx->password) {
                free(ctx->password);
                ctx->password = NULL;
            }
            if (*v) {
                ctx->password = strdup(v);
                priskv_log_debug(
                    "redis_parse_address: set password from address: [%s] (length: %zu)\n",
                    ctx->password, strlen(ctx->password));
            } else {
                priskv_log_debug("redis_parse_address: cleared password from address\n");
            }
        } else if (!strcmp(k, "timeout_ms")) {
            ctx->timeout_ms = atoi(v);
            priskv_log_debug("redis_parse_address: set timeout_ms from address: %d\n",
                             ctx->timeout_ms);
        } else if (!strcmp(k, "prefix")) {
            if (ctx->prefix) {
                free(ctx->prefix);
                ctx->prefix = NULL;
            }
            if (*v) {
                ctx->prefix = strdup(v);
                priskv_log_debug("redis_parse_address: set prefix from address: [%s]\n",
                                 ctx->prefix);
            } else {
                priskv_log_debug("redis_parse_address: cleared prefix from address\n");
            }
        } else if (!strcmp(k, "max_depth")) {
            ctx->max_depth = atoi(v);
            priskv_log_debug("redis_parse_address: set max_depth from address: %d\n",
                             ctx->max_depth);
        }
    }
    free(save);

    // Final configuration log
    priskv_log_debug("redis_parse_address: final Redis configuration:\n");
    priskv_log_debug("  host: %s\n", ctx->host);
    priskv_log_debug("  port: %d\n", ctx->port);
    priskv_log_debug("  db: %d\n", ctx->db);
    priskv_log_debug("  password: %s\n", ctx->password ? "SET" : "NOT SET");
    if (ctx->password) {
        priskv_log_debug("  password value: [%s] (length: %zu)\n", ctx->password,
                         strlen(ctx->password));
    }
    priskv_log_debug("  prefix: %s\n", ctx->prefix ? ctx->prefix : "NOT SET");
    priskv_log_debug("  timeout_ms: %d\n", ctx->timeout_ms);
    priskv_log_debug("  max_depth: %d\n", ctx->max_depth);
}

/* --- Async callbacks --- */
static void redis_connect_callback(const redisAsyncContext *c, int status);
static void redis_disconnect_callback(const redisAsyncContext *c, int status);
static void redis_auth_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_select_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_get_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_set_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_set_ttl_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_del_callback(redisAsyncContext *ac, void *r, void *privdata);
static void redis_test_callback(redisAsyncContext *ac, void *r, void *privdata);

static void *redis_event_thread(void *arg)
{
    redis_shared_context *ctx = (redis_shared_context *)arg;
    priskv_log_debug("Redis event thread starting\n");

    // Start event loop immediately
    event_base_dispatch(ctx->evbase);

    priskv_log_debug("Redis event thread exiting\n");
    return NULL;
}

static void redis_connect_callback(const redisAsyncContext *c, int status)
{
    redis_shared_context *ctx = (redis_shared_context *)c->data;

    pthread_mutex_lock(&ctx->state_lock);

    if (status != REDIS_OK) {
        priskv_log_error("Redis connection failed: %s\n", c->errstr);
        ctx->connected = false;
        ctx->authenticated = false;
        ctx->ready = false;
    } else {
        priskv_log_debug("Redis connected to %s:%d\n", ctx->host, ctx->port);
        ctx->connected = true;

        // Add detailed authentication decision log
        priskv_log_debug("redis_connect_callback: checking authentication requirements\n");
        priskv_log_debug("  password pointer: %p\n", ctx->password);
        priskv_log_debug("  password value: %s\n", ctx->password ? "[SET]" : "NULL");
        if (ctx->password) {
            priskv_log_debug("  password length: %zu\n", strlen(ctx->password));
        }

        if (ctx->password && *ctx->password) {
            priskv_log_debug("Initiating authentication with password\n");
            if (redisAsyncCommand((redisAsyncContext *)c, redis_auth_callback, ctx, "AUTH %s",
                                  ctx->password) != REDIS_OK) {
                priskv_log_error("Failed to send AUTH command\n");
                ctx->authenticated = false;
                ctx->ready = false;
            } else {
                priskv_log_debug("AUTH command sent successfully\n");
            }
        } else {
            priskv_log_debug("No password required or password is empty, selecting database\n");
            // No password, directly select database
            if (redisAsyncCommand((redisAsyncContext *)c, redis_select_callback, ctx, "SELECT %d",
                                  ctx->db) != REDIS_OK) {
                priskv_log_error("Failed to send SELECT command\n");
                ctx->ready = false;
            } else {
                priskv_log_debug("SELECT command sent successfully\n");
            }
        }
    }

    pthread_mutex_unlock(&ctx->state_lock);
}

static void redis_disconnect_callback(const redisAsyncContext *c, int status)
{
    redis_shared_context *ctx = (redis_shared_context *)c->data;

    pthread_mutex_lock(&ctx->state_lock);

    if (status != REDIS_OK) {
        priskv_log_error("Redis disconnected with error: %s\n", c->errstr);
    } else {
        priskv_log_debug("Redis disconnected normally\n");
    }

    ctx->connected = false;
    ctx->authenticated = false;
    ctx->ready = false;

    pthread_mutex_unlock(&ctx->state_lock);

    // Fail all pending requests on disconnect
    pthread_spin_lock(&ctx->queue_lock);
    priskv_log_error("Redis disconnected, failing all pending requests. inflight_count=%u, "
                     "pending_queue_empty=%d\n",
                     ctx->inflight_count, list_empty(&ctx->pending_queue));

    while (!list_empty(&ctx->pending_queue)) {
        redis_request *req = list_pop(&ctx->pending_queue, redis_request, node);
        pthread_spin_unlock(&ctx->queue_lock);

        priskv_log_error("Failing pending request for key: %s due to disconnect\n", req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
        redis_complete_request(req);

        pthread_spin_lock(&ctx->queue_lock);
    }
    pthread_spin_unlock(&ctx->queue_lock);
}

static void redis_auth_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    redis_shared_context *ctx = (redis_shared_context *)privdata;
    redisReply *reply = (redisReply *)r;

    pthread_mutex_lock(&ctx->state_lock);

    if (!reply) {
        priskv_log_error("AUTH failed: no reply\n");
        ctx->authenticated = false;
        ctx->ready = false;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("AUTH failed: %s\n", reply->str);
        ctx->authenticated = false;
        ctx->ready = false;
    } else {
        priskv_log_debug("AUTH successful\n");
        ctx->authenticated = true;

        // Authentication successful, select database
        if (redisAsyncCommand(ac, redis_select_callback, ctx, "SELECT %d", ctx->db) != REDIS_OK) {
            priskv_log_error("Failed to send SELECT command after AUTH\n");
            ctx->ready = false;
        } else {
            priskv_log_debug("SELECT command sent after AUTH\n");
        }
    }

    pthread_mutex_unlock(&ctx->state_lock);
}

static void redis_select_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    redis_shared_context *ctx = (redis_shared_context *)privdata;
    redisReply *reply = (redisReply *)r;

    pthread_mutex_lock(&ctx->state_lock);

    if (!reply) {
        priskv_log_error("SELECT failed: no reply\n");
        ctx->ready = false;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("SELECT failed: %s\n", reply->str);
        ctx->ready = false;
    } else {
        priskv_log_debug("SELECT successful, Redis backend is ready\n");
        ctx->ready = true;
    }

    pthread_mutex_unlock(&ctx->state_lock);
}

static redis_shared_context *redis_shared_context_create(const char *address)
{
    redis_shared_context *ctx = calloc(1, sizeof(*ctx));
    bool lock_initialized = false;
    bool queue_lock_initialized = false;
    bool state_lock_initialized = false;

    if (!ctx) {
        priskv_log_error("Failed to allocate redis shared context\n");
        return NULL;
    }

    list_head_init(&ctx->pending_queue);
    ctx->inflight_count = 0;
    ctx->connected = false;
    ctx->authenticated = false;
    ctx->ready = false;
    ctx->shutting_down = false;

    if (pthread_spin_init(&ctx->lock, 0)) {
        priskv_log_error("Failed to init lock\n");
        goto err_free_ctx;
    }
    lock_initialized = true;

    if (pthread_spin_init(&ctx->queue_lock, 0)) {
        priskv_log_error("Failed to init queue lock\n");
        goto err_destroy_lock;
    }
    queue_lock_initialized = true;

    if (pthread_mutex_init(&ctx->state_lock, NULL)) {
        priskv_log_error("Failed to init state lock\n");
        goto err_destroy_queue_lock;
    }
    state_lock_initialized = true;

    redis_parse_address(ctx, address);

    // Create event loop
    ctx->evbase = event_base_new();
    if (!ctx->evbase) {
        priskv_log_error("Failed to create libevent base\n");
        goto err_destroy_state_lock;
    }

    // Print final configuration before creating connection
    priskv_log_debug("Creating Redis connection with final config:\n");
    priskv_log_debug("  Host: %s\n", ctx->host);
    priskv_log_debug("  Port: %d\n", ctx->port);
    priskv_log_debug("  DB: %d\n", ctx->db);
    priskv_log_debug("  Password set: %s\n", ctx->password ? "YES" : "NO");
    priskv_log_debug("  Password value: [configured]\n");
    priskv_log_debug("  Prefix: [%s]\n", ctx->prefix ? ctx->prefix : "NULL");
    priskv_log_debug("  Timeout: %d ms\n", ctx->timeout_ms);
    priskv_log_debug("  Max depth: %d\n", ctx->max_depth);

    // Create async connection
    priskv_log_debug("Creating async Redis connection to %s:%d\n", ctx->host, ctx->port);
    ctx->async_ctx = redisAsyncConnect(ctx->host, ctx->port);
    if (!ctx->async_ctx || ctx->async_ctx->err) {
        priskv_log_error("Failed to create async Redis connection: %s\n",
                         ctx->async_ctx ? ctx->async_ctx->errstr : "no context");
        goto err_free_evbase;
    }

    // Set callbacks
    ctx->async_ctx->data = ctx;
    redisAsyncSetConnectCallback(ctx->async_ctx, redis_connect_callback);
    redisAsyncSetDisconnectCallback(ctx->async_ctx, redis_disconnect_callback);

    // Bind to event loop
    if (redisLibeventAttach(ctx->async_ctx, ctx->evbase) != REDIS_OK) {
        priskv_log_error("Failed to attach redis async to libevent\n");
        goto err_free_async_ctx;
    }

    // Start event thread
    if (pthread_create(&ctx->event_thread, NULL, redis_event_thread, ctx)) {
        priskv_log_error("Failed to create redis event thread\n");
        goto err_free_async_ctx;
    }

    // Wait for connection to be established (brief wait)
    priskv_log_debug("Waiting for Redis connection to be ready...\n");
    int wait_count = 0;
    const int max_wait_count = 50; // 5 seconds total
    while (wait_count < max_wait_count) {
        pthread_mutex_lock(&ctx->state_lock);
        bool is_ready = ctx->ready;
        pthread_mutex_unlock(&ctx->state_lock);

        if (is_ready) {
            priskv_log_debug("Redis connection ready after %d attempts\n", wait_count);
            break;
        }

        usleep(100000); // 100ms
        wait_count++;
    }

    pthread_mutex_lock(&ctx->state_lock);
    if (!ctx->ready) {
        pthread_mutex_unlock(&ctx->state_lock);
        priskv_log_error("Redis connection not ready after timeout (%d attempts)\n",
                         max_wait_count);
        priskv_log_error("Connection state: connected=%d, authenticated=%d, ready=%d\n",
                         ctx->connected, ctx->authenticated, ctx->ready);
        goto err_join_thread;
    }
    pthread_mutex_unlock(&ctx->state_lock);

    ctx->ref_count = 1;
    priskv_log_debug("Redis shared context created successfully\n");
    return ctx;

err_join_thread:
    ctx->shutting_down = true;
    if (ctx->evbase) {
        event_base_loopbreak(ctx->evbase);
    }
    if (ctx->event_thread) {
        pthread_join(ctx->event_thread, NULL);
    }
err_free_async_ctx:
    if (ctx->async_ctx) {
        redisAsyncFree(ctx->async_ctx);
        ctx->async_ctx = NULL;
    }
err_free_evbase:
    if (ctx->evbase) {
        event_base_free(ctx->evbase);
        ctx->evbase = NULL;
    }
err_destroy_state_lock:
    if (state_lock_initialized) {
        pthread_mutex_destroy(&ctx->state_lock);
    }
err_destroy_queue_lock:
    if (queue_lock_initialized) {
        pthread_spin_destroy(&ctx->queue_lock);
    }
err_destroy_lock:
    if (lock_initialized) {
        pthread_spin_destroy(&ctx->lock);
    }
err_free_ctx:
    if (ctx->password)
        free(ctx->password);
    if (ctx->prefix)
        free(ctx->prefix);
    free(ctx);
    return NULL;
}

static void redis_shared_context_destroy(redis_shared_context *ctx)
{
    if (!ctx) {
        return;
    }

    priskv_log_debug("Destroying Redis shared context\n");
    ctx->shutting_down = true;

    // Stop event loop
    if (ctx->evbase) {
        event_base_loopbreak(ctx->evbase);
    }
    if (ctx->event_thread) {
        pthread_join(ctx->event_thread, NULL);
    }

    // Free async connection
    if (ctx->async_ctx) {
        redisAsyncFree(ctx->async_ctx);
        ctx->async_ctx = NULL;
    }

    // Free event base
    if (ctx->evbase) {
        event_base_free(ctx->evbase);
        ctx->evbase = NULL;
    }

    // Free requests in pending queue
    while (1) {
        redis_request *req;

        pthread_spin_lock(&ctx->queue_lock);
        req = list_pop(&ctx->pending_queue, redis_request, node);
        if (!req) {
            pthread_spin_unlock(&ctx->queue_lock);
            break;
        }
        pthread_spin_unlock(&ctx->queue_lock);

        req->status = PRISKV_BACKEND_STATUS_ERROR;
        redis_complete_request(req);
    }

    pthread_mutex_destroy(&ctx->state_lock);
    pthread_spin_destroy(&ctx->queue_lock);
    pthread_spin_destroy(&ctx->lock);

    if (ctx->password)
        free(ctx->password);
    if (ctx->prefix)
        free(ctx->prefix);

    free(ctx);
    priskv_log_debug("Redis shared context destroyed\n");
}

static void redis_complete_request(redis_request *req)
{
    priskv_log_debug("redis_complete_request: starting for key %s, status=%d, valuelen=%lu\n",
                     req->key, req->status, req->valuelen);

    if (req->cb) {
        priskv_log_debug("redis_complete_request: calling callback with status %d, valuelen %lu\n",
                         req->status, req->valuelen);
        req->cb(req->status, req->valuelen, req->cbarg);
        priskv_log_debug("redis_complete_request: callback completed\n");
    } else {
        priskv_log_debug("redis_complete_request: no callback to call\n");
    }

    priskv_log_debug("redis_complete_request: freeing key and request\n");
    free(req->key);
    free(req);
    priskv_log_debug("redis_complete_request: completed\n");
}

// 好好看看这里的代码
static void pump_pending_queue(redis_shared_context *shared_ctx)
{
    pthread_spin_lock(&shared_ctx->queue_lock);

    priskv_log_debug(
        "pump_pending_queue: starting, inflight_count=%u, max_depth=%u, pending_queue_empty=%d\n",
        shared_ctx->inflight_count, shared_ctx->max_depth, list_empty(&shared_ctx->pending_queue));

    // Process waiting queue
    int processed = 0;
    while (shared_ctx->inflight_count < shared_ctx->max_depth) {
        redis_request *pending = list_pop(&shared_ctx->pending_queue, redis_request, node);
        if (!pending) {
            break;
        }

        processed++;
        priskv_log_debug("pump_pending_queue: processing pending request %d for key %s\n",
                         processed, pending->key);

        // Submit request outside lock
        pthread_spin_unlock(&shared_ctx->queue_lock);
        bool submitted = submit_redis_request(pending);
        pthread_spin_lock(&shared_ctx->queue_lock);

        if (!submitted) {
            priskv_log_debug("pump_pending_queue: pending request requeued\n");
            // If submission failed, put back to head of queue
            list_add(&shared_ctx->pending_queue, &pending->node);
            break;
        }
    }

    priskv_log_debug("pump_pending_queue: completed, processed %d requests, inflight_count=%u\n",
                     processed, shared_ctx->inflight_count);
    pthread_spin_unlock(&shared_ctx->queue_lock);
}

/* --- Command callbacks --- */
static void redis_get_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    (void)ac;
    redis_request *req = (redis_request *)privdata;
    redis_shared_context *shared_ctx = req->ctx->shared_ctx;
    redisReply *reply = (redisReply *)r;

    priskv_log_debug("redis_get_callback: called for key %s\n", req->key);
    priskv_log_debug("  reply type: %d\n", reply ? reply->type : -1);

    if (!reply) {
        priskv_log_error("redis_get_callback: no reply for key %s\n", req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_STRING) {
        priskv_log_debug("redis_get_callback: got string reply, len=%d, expected=%lu\n", reply->len,
                         req->valuelen);
        if ((uint64_t)reply->len > req->valuelen) {
            priskv_log_error("redis GET len too big: got=%d expect=%lu\n", reply->len,
                             req->valuelen);
            req->status = PRISKV_BACKEND_STATUS_VALUE_TOO_BIG;
            req->valuelen = 0;
        } else {
            memcpy(req->val, reply->str, reply->len);
            req->valuelen = reply->len;
            req->status = PRISKV_BACKEND_STATUS_OK;
            priskv_log_debug("redis_get_callback: GET successful, valuelen=%lu\n", req->valuelen);
        }
    } else if (reply->type == REDIS_REPLY_NIL) {
        priskv_log_debug("redis_get_callback: key not found\n");
        req->status = PRISKV_BACKEND_STATUS_NOT_FOUND;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("redis_get_callback: error reply: %s\n", reply->str);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else {
        priskv_log_error("redis_get_callback: unexpected reply type: %d\n", reply->type);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    }

    priskv_log_debug("redis_get_callback: completing request with status %d, valuelen %lu\n",
                     req->status, req->valuelen);
    redis_complete_request(req);

    // Decrease inflight count after request completion
    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count > 0) {
        shared_ctx->inflight_count--;
        priskv_log_debug("redis_get_callback: decreased inflight_count to %u\n",
                         shared_ctx->inflight_count);
    } else {
        priskv_log_warn("redis_get_callback: inflight underflow\n");
    }
    pthread_spin_unlock(&shared_ctx->queue_lock);

    pump_pending_queue(shared_ctx);
}

static void redis_set_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    (void)ac;
    redis_request *req = (redis_request *)privdata;
    redis_shared_context *shared_ctx = req->ctx->shared_ctx;
    redisReply *reply = (redisReply *)r;

    priskv_log_debug("redis_set_callback: called for key %s\n", req->key);
    priskv_log_debug("  reply type: %d\n", reply ? reply->type : -1);

    if (!reply) {
        priskv_log_error("redis_set_callback: no reply for key %s\n", req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("redis_set_callback: error reply: %s\n", reply->str);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else {
        priskv_log_debug("redis_set_callback: SET successful\n");
        req->status = PRISKV_BACKEND_STATUS_OK;
        // For SET operations, valuelen remains the original value length
    }

    priskv_log_debug("redis_set_callback: completing request with status %d, valuelen %lu\n",
                     req->status, req->valuelen);
    redis_complete_request(req);

    // Decrease inflight count after request completion
    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count > 0) {
        shared_ctx->inflight_count--;
        priskv_log_debug("redis_set_callback: decreased inflight_count to %u\n",
                         shared_ctx->inflight_count);
    } else {
        priskv_log_warn("redis_set_callback: inflight underflow\n");
    }
    pthread_spin_unlock(&shared_ctx->queue_lock);

    pump_pending_queue(shared_ctx);
}

static void redis_set_ttl_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    priskv_log_debug("redis_set_ttl_callback: called\n");
    redis_set_callback(ac, r, privdata);
}

static void redis_del_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    (void)ac;
    redis_request *req = (redis_request *)privdata;
    redis_shared_context *shared_ctx = req->ctx->shared_ctx;
    redisReply *reply = (redisReply *)r;

    priskv_log_debug("redis_del_callback: called for key %s\n", req->key);
    priskv_log_debug("  reply type: %d\n", reply ? reply->type : -1);

    if (!reply) {
        priskv_log_error("redis_del_callback: no reply for key %s\n", req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        priskv_log_debug("redis_del_callback: integer reply: %lld\n", reply->integer);
        req->status = (reply->integer > 0) ? PRISKV_BACKEND_STATUS_OK
                                           : PRISKV_BACKEND_STATUS_NOT_FOUND;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("redis_del_callback: error reply: %s\n", reply->str);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else {
        priskv_log_error("redis_del_callback: unexpected reply type: %d\n", reply->type);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    }

    priskv_log_debug("redis_del_callback: completing request with status %d, valuelen %lu\n",
                     req->status, req->valuelen);
    redis_complete_request(req);

    // Decrease inflight count after request completion
    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count > 0) {
        shared_ctx->inflight_count--;
        priskv_log_debug("redis_del_callback: decreased inflight_count to %u\n",
                         shared_ctx->inflight_count);
    } else {
        priskv_log_warn("redis_del_callback: inflight underflow\n");
    }
    pthread_spin_unlock(&shared_ctx->queue_lock);

    pump_pending_queue(shared_ctx);
}

static void redis_test_callback(redisAsyncContext *ac, void *r, void *privdata)
{
    (void)ac;
    redis_request *req = (redis_request *)privdata;
    redis_shared_context *shared_ctx = req->ctx->shared_ctx;
    redisReply *reply = (redisReply *)r;

    priskv_log_debug("redis_test_callback: called for key %s\n", req->key);
    priskv_log_debug("  reply type: %d\n", reply ? reply->type : -1);

    if (!reply) {
        priskv_log_error("redis_test_callback: no reply for key %s\n", req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_INTEGER) {
        priskv_log_debug("redis_test_callback: integer reply: %lld\n", reply->integer);
        req->status = (reply->integer > 0) ? PRISKV_BACKEND_STATUS_OK
                                           : PRISKV_BACKEND_STATUS_NOT_FOUND;
        req->valuelen = 0;
    } else if (reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("redis_test_callback: error reply: %s\n", reply->str);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    } else {
        priskv_log_error("redis_test_callback: unexpected reply type: %d\n", reply->type);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
    }

    priskv_log_debug("redis_test_callback: completing request with status %d, valuelen %lu\n",
                     req->status, req->valuelen);
    redis_complete_request(req);

    // Decrease inflight count after request completion
    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count > 0) {
        shared_ctx->inflight_count--;
        priskv_log_debug("redis_test_callback: decreased inflight_count to %u\n",
                         shared_ctx->inflight_count);
    } else {
        priskv_log_warn("redis_test_callback: inflight underflow\n");
    }
    pthread_spin_unlock(&shared_ctx->queue_lock);

    pump_pending_queue(shared_ctx);
}

static bool is_redis_ready(redis_shared_context *shared_ctx)
{
    bool ready;
    pthread_mutex_lock(&shared_ctx->state_lock);
    ready = shared_ctx->ready;
    pthread_mutex_unlock(&shared_ctx->state_lock);
    return ready;
}

static bool submit_redis_request(redis_request *req)
{
    redis_thread_context *tctx = req->ctx;
    redis_shared_context *shared_ctx = tctx->shared_ctx;
    bool submitted = false;

    priskv_log_debug("submit_redis_request: starting for key %s, op_type=%d, valuelen=%lu\n",
                     req->key, req->op_type, req->valuelen);

    // Check connection status - if not ready, immediately fail the request
    if (!is_redis_ready(shared_ctx)) {
        priskv_log_error("submit_redis_request: Redis not ready, failing request for key %s\n",
                         req->key);
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
        redis_complete_request(req);
        return true; // Request handled (failed)
    }

    // Submit Redis command
    int redis_ret = REDIS_ERR;
    switch (req->op_type) {
    case REDIS_OP_GET:
        priskv_log_debug("submit_redis_request: sending GET command for key %s\n", req->key);
        redis_ret =
            redisAsyncCommand(shared_ctx->async_ctx, redis_get_callback, req, "GET %s", req->key);
        break;
    case REDIS_OP_SET:
        // Fix: check if timeout_ms is within reasonable range
        if (req->timeout_ms > 0 && req->timeout_ms <= MAX_REASONABLE_TIMEOUT_MS) {
            priskv_log_debug("submit_redis_request: sending SET command with TTL for key %s, "
                             "timeout=%lu, valuelen=%lu\n",
                             req->key, req->timeout_ms, req->valuelen);
            redis_ret = redisAsyncCommand(shared_ctx->async_ctx, redis_set_ttl_callback, req,
                                          "SET %s %b PX %lu", req->key, req->val,
                                          (size_t)req->valuelen, (unsigned long)req->timeout_ms);
        } else {
            priskv_log_debug("submit_redis_request: sending SET command without TTL for key %s "
                             "(timeout=%lu, valuelen=%lu)\n",
                             req->key, req->timeout_ms, req->valuelen);
            redis_ret = redisAsyncCommand(shared_ctx->async_ctx, redis_set_callback, req,
                                          "SET %s %b", req->key, req->val, (size_t)req->valuelen);
        }
        break;
    case REDIS_OP_DEL:
        priskv_log_debug("submit_redis_request: sending DEL command for key %s\n", req->key);
        redis_ret =
            redisAsyncCommand(shared_ctx->async_ctx, redis_del_callback, req, "DEL %s", req->key);
        break;
    case REDIS_OP_TEST:
        priskv_log_debug("submit_redis_request: sending EXISTS command for key %s\n", req->key);
        redis_ret = redisAsyncCommand(shared_ctx->async_ctx, redis_test_callback, req, "EXISTS %s",
                                      req->key);
        break;
    default:
        priskv_log_error("Invalid redis op: %d\n", req->op_type);
        break;
    }

    submitted = (redis_ret == REDIS_OK);

    if (!submitted) {
        priskv_log_error("Failed to submit Redis command for key: %s, error: %s\n", req->key,
                         shared_ctx->async_ctx->errstr);

        req->status = PRISKV_BACKEND_STATUS_ERROR;
        req->valuelen = 0;
        redis_complete_request(req);
    } else {
        priskv_log_debug("Redis command submitted successfully: op=%d, key=%s\n", req->op_type,
                         req->key);
    }

    return submitted;
}

static redis_request *redis_make_request(redis_thread_context *ctx, const char *key, uint8_t *val,
                                         uint64_t valuelen, uint64_t timeout_ms,
                                         redis_op_type op_type, priskv_backend_driver_cb cb,
                                         void *cbarg)
{
    redis_shared_context *shared_ctx = ctx->shared_ctx;
    redis_request *req = malloc(sizeof(redis_request));
    if (req == NULL) {
        priskv_log_error("Failed to allocate redis request\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return NULL;
    }

    req->ctx = ctx;
    req->val = val;
    req->valuelen = valuelen;

    // Fix: if timeout_ms is too large, set it to 0 (no expiration)
    if (timeout_ms > MAX_REASONABLE_TIMEOUT_MS) {
        priskv_log_debug(
            "redis_make_request: timeout_ms %lu is too large, setting to 0 (no expiration)\n",
            timeout_ms);
        req->timeout_ms = 0;
    } else {
        req->timeout_ms = timeout_ms;
    }

    req->cb = cb;
    req->cbarg = cbarg;
    req->op_type = op_type;
    list_node_init(&req->node);
    req->queued_once = false;

    priskv_log_debug("redis_make_request: creating request for key %s, op_type=%d, valuelen=%lu, "
                     "timeout_ms=%lu\n",
                     key, op_type, valuelen, req->timeout_ms);

    /* build full key with prefix */
    if (shared_ctx->prefix && *shared_ctx->prefix) {
        size_t plen = strlen(shared_ctx->prefix);
        size_t klen = strlen(key);
        req->key = malloc(plen + klen + 1);
        if (!req->key) {
            priskv_log_error("Failed to alloc key buffer\n");
            req->status = PRISKV_BACKEND_STATUS_ERROR;
            req->valuelen = 0;
            redis_complete_request(req);
            return NULL;
        }
        memcpy(req->key, shared_ctx->prefix, plen);
        memcpy(req->key + plen, key, klen);
        req->key[plen + klen] = '\0';
        priskv_log_debug("redis_make_request: built prefixed key: %s\n", req->key);
    } else {
        req->key = strdup(key);
        if (!req->key) {
            priskv_log_error("Failed to dup key\n");
            req->status = PRISKV_BACKEND_STATUS_ERROR;
            req->valuelen = 0;
            redis_complete_request(req);
            return NULL;
        }
        priskv_log_debug("redis_make_request: using original key: %s\n", req->key);
    }

    return req;
}

/* --- driver interface implementations --- */
static int redis_open(priskv_backend_device *bdev)
{
    redis_shared_context *shared_ctx = NULL;
    redis_thread_context *ctx = NULL;

    priskv_log_debug("redis_open: opening Redis backend device\n");

    shared_ctx = redis_shared_context_get();

    if (!shared_ctx) {
        pthread_mutex_lock(&redis_shared_ctx_lock);

        if (redis_shared_ctx) {
            pthread_spin_lock(&redis_shared_ctx->lock);
            redis_shared_ctx->ref_count++;
            pthread_spin_unlock(&redis_shared_ctx->lock);
            shared_ctx = redis_shared_ctx;
            pthread_mutex_unlock(&redis_shared_ctx_lock);
        } else {
            priskv_log_debug("redis_open: creating new Redis shared context\n");
            shared_ctx = redis_shared_context_create(bdev->link.address);
            if (!shared_ctx) {
                pthread_mutex_unlock(&redis_shared_ctx_lock);
                goto err;
            }

            redis_shared_ctx = shared_ctx;
            pthread_mutex_unlock(&redis_shared_ctx_lock);
        }
    }

    ctx = calloc(1, sizeof(redis_thread_context));
    if (ctx == NULL) {
        priskv_log_error("Failed to allocate context\n");
        goto err_put_shared_ctx;
    }

    ctx->bdev = bdev;
    ctx->shared_ctx = shared_ctx;

    bdev->private_data = ctx;

    priskv_log_debug("redis_open: Redis backend device opened successfully\n");
    return 0;

err_put_shared_ctx:
    redis_shared_context_put(shared_ctx);
err:
    priskv_log_error("Failed to open redis device\n");
    return -1;
}

static int redis_close(priskv_backend_device *bdev)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        return -1;
    }

    priskv_log_debug("redis_close: closing Redis backend device\n");

    redis_thread_context *ctx = bdev->private_data;
    redis_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    free(ctx);
    bdev->private_data = NULL;

    redis_shared_context_put(shared_ctx);

    priskv_log_debug("redis_close: Redis backend device closed successfully\n");
    return 0;
}

static bool redis_is_cacheable(priskv_backend_device *bdev, uint64_t valuelen)
{
    (void)bdev;
    (void)valuelen;
    return true;
}

static void redis_get(priskv_backend_device *bdev, const char *key, uint8_t *val, uint64_t valuelen,
                      priskv_backend_driver_cb cb, void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_log_debug("redis_get: starting for key %s, valuelen=%lu\n", key, valuelen);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, val, valuelen, 0, REDIS_OP_GET, cb, cbarg);
    if (req == NULL) {
        return;
    }

    // Queue depth control
    pthread_spin_lock(&ctx->shared_ctx->queue_lock);
    priskv_log_debug("redis_get: checking queue depth, inflight=%u, max_depth=%u\n",
                     ctx->shared_ctx->inflight_count, ctx->shared_ctx->max_depth);

    if (ctx->shared_ctx->inflight_count >= ctx->shared_ctx->max_depth) {
        priskv_log_debug("redis_get: queue full, adding to pending queue\n");
        if (req->queued_once) {
            list_add(&ctx->shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&ctx->shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        pthread_spin_unlock(&ctx->shared_ctx->queue_lock);
        return;
    }

    ctx->shared_ctx->inflight_count++;
    priskv_log_debug("redis_get: increased inflight_count to %u\n",
                     ctx->shared_ctx->inflight_count);
    pthread_spin_unlock(&ctx->shared_ctx->queue_lock);

    (void)submit_redis_request(req);
}

static void redis_set(priskv_backend_device *bdev, const char *key, uint8_t *val, uint64_t valuelen,
                      uint64_t timeout, priskv_backend_driver_cb cb, void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_log_debug("redis_set: starting for key %s, valuelen=%lu, timeout=%lu\n", key, valuelen,
                     timeout);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req =
        redis_make_request(ctx, key, val, valuelen, timeout, REDIS_OP_SET, cb, cbarg);
    if (req == NULL) {
        return;
    }

    // Queue depth control
    pthread_spin_lock(&ctx->shared_ctx->queue_lock);
    priskv_log_debug("redis_set: checking queue depth, inflight=%u, max_depth=%u\n",
                     ctx->shared_ctx->inflight_count, ctx->shared_ctx->max_depth);

    if (ctx->shared_ctx->inflight_count >= ctx->shared_ctx->max_depth) {
        priskv_log_debug("redis_set: queue full, adding to pending queue\n");
        if (req->queued_once) {
            list_add(&ctx->shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&ctx->shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        pthread_spin_unlock(&ctx->shared_ctx->queue_lock);
        return;
    }

    ctx->shared_ctx->inflight_count++;
    priskv_log_debug("redis_set: increased inflight_count to %u\n",
                     ctx->shared_ctx->inflight_count);
    pthread_spin_unlock(&ctx->shared_ctx->queue_lock);

    (void)submit_redis_request(req);
}

static void redis_del(priskv_backend_device *bdev, const char *key, priskv_backend_driver_cb cb,
                      void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_log_debug("redis_del: starting for key %s\n", key);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, NULL, 0, 0, REDIS_OP_DEL, cb, cbarg);
    if (req == NULL) {
        return;
    }

    // Queue depth control
    pthread_spin_lock(&ctx->shared_ctx->queue_lock);
    priskv_log_debug("redis_del: checking queue depth, inflight=%u, max_depth=%u\n",
                     ctx->shared_ctx->inflight_count, ctx->shared_ctx->max_depth);

    if (ctx->shared_ctx->inflight_count >= ctx->shared_ctx->max_depth) {
        priskv_log_debug("redis_del: queue full, adding to pending queue\n");
        if (req->queued_once) {
            list_add(&ctx->shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&ctx->shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        pthread_spin_unlock(&ctx->shared_ctx->queue_lock);
        return;
    }

    ctx->shared_ctx->inflight_count++;
    priskv_log_debug("redis_del: increased inflight_count to %u\n",
                     ctx->shared_ctx->inflight_count);
    pthread_spin_unlock(&ctx->shared_ctx->queue_lock);

    (void)submit_redis_request(req);
}

static void redis_test(priskv_backend_device *bdev, const char *key, priskv_backend_driver_cb cb,
                       void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_log_debug("redis_test: starting for key %s\n", key);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, NULL, 0, 0, REDIS_OP_TEST, cb, cbarg);
    if (req == NULL) {
        return;
    }

    // Queue depth control
    pthread_spin_lock(&ctx->shared_ctx->queue_lock);
    priskv_log_debug("redis_test: checking queue depth, inflight=%u, max_depth=%u\n",
                     ctx->shared_ctx->inflight_count, ctx->shared_ctx->max_depth);

    if (ctx->shared_ctx->inflight_count >= ctx->shared_ctx->max_depth) {
        priskv_log_debug("redis_test: queue full, adding to pending queue\n");
        if (req->queued_once) {
            list_add(&ctx->shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&ctx->shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        pthread_spin_unlock(&ctx->shared_ctx->queue_lock);
        return;
    }

    ctx->shared_ctx->inflight_count++;
    priskv_log_debug("redis_test: increased inflight_count to %u\n",
                     ctx->shared_ctx->inflight_count);
    pthread_spin_unlock(&ctx->shared_ctx->queue_lock);

    (void)submit_redis_request(req);
}

static void redis_evict(priskv_backend_device *bdev, priskv_backend_driver_cb cb, void *cbarg)
{
    if (cb) {
        cb(PRISKV_BACKEND_STATUS_OK, 0, cbarg);
    }
}

static int redis_clearup(priskv_backend_device *bdev)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        return -1;
    }

    redis_thread_context *ctx = bdev->private_data;
    redis_shared_context *shared_ctx = ctx->shared_ctx;

    priskv_log_debug("redis_clearup: starting Redis clearup\n");

    // Use synchronous connection for clearup
    redisContext *sync_conn = redisConnect(shared_ctx->host, shared_ctx->port);
    if (!sync_conn || sync_conn->err) {
        priskv_log_error("redis clearup connect err: %s\n",
                         sync_conn ? sync_conn->errstr : "no context");
        if (sync_conn) {
            redisFree(sync_conn);
        }
        return -1;
    }

    if (shared_ctx->password && *shared_ctx->password) {
        priskv_log_debug("redis_clearup: authenticating with password\n");
        redisReply *auth_reply = redisCommand(sync_conn, "AUTH %s", shared_ctx->password);
        if (!auth_reply || auth_reply->type == REDIS_REPLY_ERROR) {
            priskv_log_error("AUTH error during clearup: %s\n",
                             auth_reply ? auth_reply->str : "no reply");
            if (auth_reply) {
                freeReplyObject(auth_reply);
            }
            redisFree(sync_conn);
            return -1;
        }
        freeReplyObject(auth_reply);
    }

    redisReply *select_reply = redisCommand(sync_conn, "SELECT %d", shared_ctx->db);
    if (!select_reply || select_reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("SELECT error during clearup: %s\n",
                         select_reply ? select_reply->str : "no reply");
        if (select_reply) {
            freeReplyObject(select_reply);
        }
        redisFree(sync_conn);
        return -1;
    }
    freeReplyObject(select_reply);

    priskv_log_debug("redis_clearup: executing FLUSHDB\n");
    redisReply *flush_reply = redisCommand(sync_conn, "FLUSHDB");
    int ret = 0;
    if (!flush_reply || flush_reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("FLUSHDB error: %s\n", flush_reply ? flush_reply->str : "no reply");
        ret = -1;
    } else {
        priskv_log_debug("redis_clearup: FLUSHDB successful\n");
    }
    if (flush_reply) {
        freeReplyObject(flush_reply);
    }

    redisFree(sync_conn);
    priskv_log_debug("redis_clearup: completed with result %d\n", ret);
    return ret;
}

static priskv_backend_driver redis_driver = {
    .name = "redis",
    .open = redis_open,
    .close = redis_close,
    .is_cacheable = redis_is_cacheable,
    .get = redis_get,
    .set = redis_set,
    .del = redis_del,
    .test = redis_test,
    .evict = redis_evict,
    .clearup = redis_clearup,
};

static void priskv_backend_init_redis()
{
    priskv_log_debug("Initializing Redis backend driver\n");
    priskv_backend_register(&redis_driver);
    priskv_log_debug("Redis backend driver registered\n");
}

backend_init(priskv_backend_init_redis);