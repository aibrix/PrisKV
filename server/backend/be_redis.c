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
 */

#define _DEFAULT_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <pthread.h>
#include <assert.h>
#include <time.h>
#include <sys/eventfd.h>

#include "backend.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "priskv-event.h"
#include "list.h"

/* hiredis */
#include <hiredis/hiredis.h>

#define REDIS_DEFAULT_HOST "127.0.0.1"
#define REDIS_DEFAULT_PORT 6379
#define REDIS_DEFAULT_DB 0
#define REDIS_POOL_INITIAL_SIZE 10
#define REDIS_POOL_MAX_SIZE 50
#define REDIS_THREAD_POOL_SIZE 8

typedef enum {
    REDIS_OP_GET,
    REDIS_OP_SET,
    REDIS_OP_DEL,
    REDIS_OP_TEST,
} redis_op_type;

/* Single connection in connection pool */
typedef struct redis_connection {
    redisContext *conn;
    bool in_use;
    time_t last_used;
    pthread_mutex_t lock;
} redis_connection;

/* Connection pool */
typedef struct redis_connection_pool {
    redis_connection *connections;
    int pool_size;
    int max_pool_size;

    /* Connection configuration */
    char host[256];
    int port;
    int db;
    char *password;
    char *prefix;

    pthread_mutex_t pool_lock;
    pthread_cond_t connection_available;
} redis_connection_pool;

/* Worker thread in thread pool */
typedef struct redis_worker {
    pthread_t thread;
    bool running;
    struct redis_thread_pool *pool;
} redis_worker;

/* Thread pool */
typedef struct redis_thread_pool {
    redis_worker *workers;
    int num_workers;
    bool shutdown;

    /* Task queue */
    struct list_head task_queue;
    pthread_mutex_t queue_lock;
    pthread_cond_t task_available;

    /* Completion queue */
    struct list_head completion_queue;
    pthread_mutex_t completion_lock;

    /* Connection pool */
    redis_connection_pool *conn_pool;
} redis_thread_pool;

/* Request structure */
typedef struct redis_request {
    struct list_node node;
    redis_op_type op_type;
    char *key;
    uint8_t *val;
    uint64_t valuelen;
    priskv_backend_driver_cb cb;
    void *cbarg;
    /* Used to identify which IO thread the request belongs to */
    void *io_thread_context;
} redis_request;

/* Completion notification structure */
typedef struct redis_completion {
    struct list_node node;
    priskv_backend_driver_cb cb;
    void *cbarg;
    priskv_backend_status status;
    uint64_t valuelen;
} redis_completion;

/* Thread context */
typedef struct redis_thread_context {
    priskv_backend_device *bdev;
    redis_thread_pool *thread_pool;
    /* IO thread specific completion queue */
    struct list_head io_completion_queue;
    pthread_mutex_t io_completion_lock;

    /* Mechanism for notifying main event loop */
    int completion_eventfd;
} redis_thread_context;

/* Global thread pool instance */
static redis_thread_pool *g_thread_pool = NULL;
static pthread_mutex_t g_thread_pool_lock = PTHREAD_MUTEX_INITIALIZER;

/* Helper functions */
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

/* Configuration parsing */
static void redis_parse_config(char *host, size_t hostlen, int *port, int *db, char **password,
                               char **prefix, const char *address)
{
    /* Set default values */
    snprintf(host, hostlen, "%s", getenv_default("PRISKV_REDIS_HOST", REDIS_DEFAULT_HOST));
    *port = getenv_int_default("PRISKV_REDIS_PORT", REDIS_DEFAULT_PORT);
    *db = getenv_int_default("PRISKV_REDIS_DB", REDIS_DEFAULT_DB);

    const char *env_pass = getenv("PRISKV_REDIS_PASSWORD");
    const char *env_prefix = getenv("PRISKV_REDIS_PREFIX");

    if (env_pass && *env_pass) {
        *password = strdup(env_pass);
    }
    if (env_prefix && *env_prefix) {
        *prefix = strdup(env_prefix);
    }

    if (!address || !*address) {
        return;
    }

    /* Parse inline address */
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
            snprintf(host, hostlen, "%s", v);
        } else if (!strcmp(k, "port")) {
            *port = atoi(v);
        } else if (!strcmp(k, "db")) {
            *db = atoi(v);
        } else if (!strcmp(k, "password")) {
            if (*password) {
                free(*password);
                *password = NULL;
            }
            if (*v) {
                *password = strdup(v);
            }
        } else if (!strcmp(k, "prefix")) {
            if (*prefix) {
                free(*prefix);
                *prefix = NULL;
            }
            if (*v) {
                *prefix = strdup(v);
            }
        }
    }
    free(save);
}

/* Connection pool implementation */

/* Initialize single connection */
static int redis_connection_init(redis_connection *conn, redis_connection_pool *pool)
{
    conn->conn = redisConnect(pool->host, pool->port);
    if (!conn->conn || conn->conn->err) {
        priskv_log_error("Failed to create Redis connection: %s\n",
                         conn->conn ? conn->conn->errstr : "no context");
        return -1;
    }

    /* Set timeout */
    struct timeval timeout = {1, 0}; // 1 second timeout
    redisSetTimeout(conn->conn, timeout);

    /* Authentication */
    if (pool->password && *pool->password) {
        redisReply *reply = redisCommand(conn->conn, "AUTH %s", pool->password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            priskv_log_error("AUTH failed: %s\n", reply ? reply->str : "no reply");
            if (reply)
                freeReplyObject(reply);
            redisFree(conn->conn);
            conn->conn = NULL;
            return -1;
        }
        freeReplyObject(reply);
    }

    /* Select database */
    redisReply *reply = redisCommand(conn->conn, "SELECT %d", pool->db);
    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("SELECT failed: %s\n", reply ? reply->str : "no reply");
        if (reply)
            freeReplyObject(reply);
        redisFree(conn->conn);
        conn->conn = NULL;
        return -1;
    }
    freeReplyObject(reply);

    conn->last_used = time(NULL);
    conn->in_use = false;

    return 0;
}

/* Create connection pool */
static redis_connection_pool *redis_connection_pool_create(const char *host, int port, int db,
                                                           const char *password, const char *prefix,
                                                           int initial_pool_size, int max_pool_size)
{
    redis_connection_pool *pool = calloc(1, sizeof(redis_connection_pool));
    if (!pool) {
        priskv_log_error("Failed to allocate connection pool\n");
        return NULL;
    }

    strncpy(pool->host, host, sizeof(pool->host) - 1);
    pool->port = port;
    pool->db = db;
    pool->max_pool_size = max_pool_size;

    if (password && *password) {
        pool->password = strdup(password);
    }
    if (prefix && *prefix) {
        pool->prefix = strdup(prefix);
    }

    if (pthread_mutex_init(&pool->pool_lock, NULL) != 0) {
        priskv_log_error("Failed to init pool mutex\n");
        goto error;
    }

    if (pthread_cond_init(&pool->connection_available, NULL) != 0) {
        priskv_log_error("Failed to init pool condition variable\n");
        goto error_destroy_mutex;
    }

    /* Create initial connections */
    pool->connections = calloc(initial_pool_size, sizeof(redis_connection));
    if (!pool->connections) {
        priskv_log_error("Failed to allocate connections array\n");
        goto error_destroy_cond;
    }

    for (int i = 0; i < initial_pool_size; i++) {
        if (pthread_mutex_init(&pool->connections[i].lock, NULL) != 0) {
            priskv_log_error("Failed to init connection mutex %d\n", i);
            goto error_destroy_connections;
        }

        if (redis_connection_init(&pool->connections[i], pool) != 0) {
            priskv_log_error("Failed to initialize connection %d\n", i);
            pthread_mutex_destroy(&pool->connections[i].lock);
            goto error_destroy_connections;
        }
    }

    pool->pool_size = initial_pool_size;

    priskv_log_debug("Redis connection pool created with %d connections to %s:%d\n",
                     initial_pool_size, host, port);
    return pool;

error_destroy_connections:
    for (int i = 0; i < initial_pool_size; i++) {
        if (pool->connections[i].conn) {
            redisFree(pool->connections[i].conn);
        }
        pthread_mutex_destroy(&pool->connections[i].lock);
    }
    free(pool->connections);
error_destroy_cond:
    pthread_cond_destroy(&pool->connection_available);
error_destroy_mutex:
    pthread_mutex_destroy(&pool->pool_lock);
error:
    if (pool->password)
        free(pool->password);
    if (pool->prefix)
        free(pool->prefix);
    free(pool);
    return NULL;
}

/* Get connection from connection pool */
static redis_connection *redis_connection_pool_get(redis_connection_pool *pool)
{
    pthread_mutex_lock(&pool->pool_lock);

    priskv_log_debug("Redis connection pool: trying to get connection, pool_size=%d\n",
                     pool->pool_size);

    /* First try to find an idle connection */
    for (int i = 0; i < pool->pool_size; i++) {
        pthread_mutex_lock(&pool->connections[i].lock);
        if (!pool->connections[i].in_use && pool->connections[i].conn) {
            pool->connections[i].in_use = true;
            pool->connections[i].last_used = time(NULL);
            pthread_mutex_unlock(&pool->connections[i].lock);
            pthread_mutex_unlock(&pool->pool_lock);
            priskv_log_debug("Redis connection pool: got existing connection %d\n", i);
            return &pool->connections[i];
        }
        pthread_mutex_unlock(&pool->connections[i].lock);
    }

    priskv_log_debug("Redis connection pool: no free connections, pool_size=%d, max_pool_size=%d\n",
                     pool->pool_size, pool->max_pool_size);

    /* If no idle connections and can expand, create new connection */
    if (pool->pool_size < pool->max_pool_size) {
        int new_index = pool->pool_size;
        priskv_log_debug("Redis connection pool: expanding pool, creating new connection\n");

        redis_connection *new_connections =
            realloc(pool->connections, (pool->pool_size + 1) * sizeof(redis_connection));
        if (!new_connections) {
            priskv_log_error("Failed to expand connection pool\n");
            pthread_mutex_unlock(&pool->pool_lock);
            return NULL;
        }

        pool->connections = new_connections;
        memset(&pool->connections[new_index], 0, sizeof(redis_connection));

        if (pthread_mutex_init(&pool->connections[new_index].lock, NULL) != 0) {
            priskv_log_error("Failed to init new connection mutex\n");
            pthread_mutex_unlock(&pool->pool_lock);
            return NULL;
        }

        if (redis_connection_init(&pool->connections[new_index], pool) == 0) {
            pool->connections[new_index].in_use = true;
            pool->connections[new_index].last_used = time(NULL);
            pool->pool_size++;
            pthread_mutex_unlock(&pool->pool_lock);
            priskv_log_debug("Redis connection pool: successfully created new connection %d\n",
                             new_index);
            return &pool->connections[new_index];
        } else {
            priskv_log_error("Failed to create new connection\n");
            pthread_mutex_destroy(&pool->connections[new_index].lock);
        }
    }

    priskv_log_debug("Redis connection pool: waiting for available connection...\n");

    /* Wait for connection to become available */
    while (true) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += 1; /* 1 second timeout */

        int ret = pthread_cond_timedwait(&pool->connection_available, &pool->pool_lock, &ts);
        if (ret == ETIMEDOUT) {
            priskv_log_warn("Timeout waiting for Redis connection\n");
            pthread_mutex_unlock(&pool->pool_lock);
            return NULL;
        }

        priskv_log_debug("Redis connection pool: woke up, checking for available connections\n");

        /* Try to get connection again */
        for (int i = 0; i < pool->pool_size; i++) {
            pthread_mutex_lock(&pool->connections[i].lock);
            if (!pool->connections[i].in_use && pool->connections[i].conn) {
                pool->connections[i].in_use = true;
                pool->connections[i].last_used = time(NULL);
                pthread_mutex_unlock(&pool->connections[i].lock);
                pthread_mutex_unlock(&pool->pool_lock);
                priskv_log_debug("Redis connection pool: got connection after wait %d\n", i);
                return &pool->connections[i];
            }
            pthread_mutex_unlock(&pool->connections[i].lock);
        }
    }
}

/* Release connection back to connection pool */
static void redis_connection_pool_release(redis_connection_pool *pool, redis_connection *conn)
{
    if (!conn)
        return;

    pthread_mutex_lock(&conn->lock);
    conn->in_use = false;
    conn->last_used = time(NULL);
    pthread_mutex_unlock(&conn->lock);

    pthread_mutex_lock(&pool->pool_lock);
    pthread_cond_signal(&pool->connection_available);
    pthread_mutex_unlock(&pool->pool_lock);

    priskv_log_debug("Redis connection released back to pool\n");
}

/* Destroy connection pool */
static void redis_connection_pool_destroy(redis_connection_pool *pool)
{
    if (!pool)
        return;

    priskv_log_debug("Destroying Redis connection pool with %d connections\n", pool->pool_size);

    for (int i = 0; i < pool->pool_size; i++) {
        if (pool->connections[i].conn) {
            redisFree(pool->connections[i].conn);
        }
        pthread_mutex_destroy(&pool->connections[i].lock);
    }

    free(pool->connections);
    if (pool->password)
        free(pool->password);
    if (pool->prefix)
        free(pool->prefix);

    pthread_mutex_destroy(&pool->pool_lock);
    pthread_cond_destroy(&pool->connection_available);

    free(pool);
}

/* Thread pool implementation */

/* Worker thread function */
static void *redis_worker_func(void *arg)
{
    redis_worker *worker = (redis_worker *)arg;
    redis_thread_pool *pool = worker->pool;

    priskv_log_debug("Redis worker thread started\n");

    while (worker->running) {
        pthread_mutex_lock(&pool->queue_lock);

        /* Wait for tasks */
        while (list_empty(&pool->task_queue) && worker->running) {
            pthread_cond_wait(&pool->task_available, &pool->queue_lock);
        }

        if (!worker->running) {
            pthread_mutex_unlock(&pool->queue_lock);
            break;
        }

        /* Get task */
        redis_request *req = list_pop(&pool->task_queue, redis_request, node);
        pthread_mutex_unlock(&pool->queue_lock);

        if (!req) {
            continue;
        }

        priskv_log_debug("Worker processing request: op=%d, key=%s\n", req->op_type, req->key);

        /* Execute Redis operation */
        priskv_log_debug("Worker: getting Redis connection\n");
        redis_connection *conn = redis_connection_pool_get(pool->conn_pool);
        if (!conn) {
            priskv_log_error("Failed to get Redis connection\n");
            /* Create completion notification */
            redis_completion *comp = malloc(sizeof(redis_completion));
            if (comp) {
                comp->cb = req->cb;
                comp->cbarg = req->cbarg;
                comp->status = PRISKV_BACKEND_STATUS_ERROR;
                comp->valuelen = 0;

                pthread_mutex_lock(&pool->completion_lock);
                list_add_tail(&pool->completion_queue, &comp->node);
                pthread_mutex_unlock(&pool->completion_lock);
                priskv_log_debug("Worker: created error completion due to connection failure\n");

                /* Notify main event loop */
                redis_thread_context *ctx = req->io_thread_context;
                if (ctx && ctx->completion_eventfd >= 0) {
                    uint64_t val = 1;
                    if (write(ctx->completion_eventfd, &val, sizeof(val)) != sizeof(val)) {
                        priskv_log_error("Failed to write to completion eventfd: %s\n",
                                         strerror(errno));
                    } else {
                        priskv_log_debug("Worker: notified main event loop\n");
                    }
                }
            }
            goto cleanup_request;
        }

        priskv_log_debug("Worker: got connection, executing Redis command\n");

        priskv_backend_status status = PRISKV_BACKEND_STATUS_ERROR;
        uint64_t result_len = 0;
        redisReply *reply = NULL;

        /* Execute command */
        switch (req->op_type) {
        case REDIS_OP_GET:
            priskv_log_debug("Worker: executing GET command for key %s\n", req->key);
            reply = redisCommand(conn->conn, "GET %s", req->key);
            break;

        case REDIS_OP_SET:
            priskv_log_debug("Worker: executing SET command for key %s, value length %lu\n",
                             req->key, req->valuelen);
            reply =
                redisCommand(conn->conn, "SET %s %b", req->key, req->val, (size_t)req->valuelen);
            break;

        case REDIS_OP_DEL:
            priskv_log_debug("Worker: executing DEL command for key %s\n", req->key);
            reply = redisCommand(conn->conn, "DEL %s", req->key);
            break;

        case REDIS_OP_TEST:
            priskv_log_debug("Worker: executing EXISTS command for key %s\n", req->key);
            reply = redisCommand(conn->conn, "EXISTS %s", req->key);
            break;

        default:
            break;
        }

        priskv_log_debug("Worker: Redis command executed, processing reply\n");

        /* Process response */
        if (!reply) {
            priskv_log_error("Redis command failed: no reply, error: %s\n", conn->conn->errstr);
        } else if (reply->type == REDIS_REPLY_ERROR) {
            priskv_log_error("Redis command error: %s\n", reply->str);
        } else {
            switch (req->op_type) {
            case REDIS_OP_GET:
                if (reply->type == REDIS_REPLY_STRING) {
                    if ((uint64_t)reply->len > req->valuelen) {
                        priskv_log_error("GET value too big: %d > %lu\n", reply->len,
                                         req->valuelen);
                        status = PRISKV_BACKEND_STATUS_VALUE_TOO_BIG;
                    } else {
                        memcpy(req->val, reply->str, reply->len);
                        result_len = reply->len;
                        status = PRISKV_BACKEND_STATUS_OK;
                        priskv_log_debug("Worker: GET successful, value length %lu\n", result_len);
                    }
                } else if (reply->type == REDIS_REPLY_NIL) {
                    status = PRISKV_BACKEND_STATUS_NOT_FOUND;
                    priskv_log_debug("Worker: GET key not found\n");
                }
                break;

            case REDIS_OP_SET:
                status = PRISKV_BACKEND_STATUS_OK;
                result_len = req->valuelen;
                priskv_log_debug("Worker: SET successful\n");
                break;

            case REDIS_OP_DEL:
                if (reply->type == REDIS_REPLY_INTEGER) {
                    status = (reply->integer > 0) ? PRISKV_BACKEND_STATUS_OK
                                                  : PRISKV_BACKEND_STATUS_NOT_FOUND;
                    priskv_log_debug("Worker: DEL %s, deleted %ld keys\n", req->key,
                                     reply->integer);
                }
                break;

            case REDIS_OP_TEST:
                if (reply->type == REDIS_REPLY_INTEGER) {
                    status = (reply->integer > 0) ? PRISKV_BACKEND_STATUS_OK
                                                  : PRISKV_BACKEND_STATUS_NOT_FOUND;
                    priskv_log_debug("Worker: TEST key %s exists: %ld\n", req->key, reply->integer);
                }
                break;

            default:
                break;
            }
        }

        if (reply) {
            freeReplyObject(reply);
        }

        priskv_log_debug("Worker: releasing connection\n");
        /* Release connection */
        redis_connection_pool_release(pool->conn_pool, conn);

        /* Create completion notification */
        redis_completion *comp = malloc(sizeof(redis_completion));
        if (comp) {
            comp->cb = req->cb;
            comp->cbarg = req->cbarg;
            comp->status = status;
            comp->valuelen = result_len;

            priskv_log_debug("Worker: adding completion to global queue\n");

            pthread_mutex_lock(&pool->completion_lock);
            list_add_tail(&pool->completion_queue, &comp->node);
            pthread_mutex_unlock(&pool->completion_lock);

            priskv_log_debug("Worker: created completion with status %d\n", status);

            /* Notify main event loop */
            redis_thread_context *ctx = req->io_thread_context;
            if (ctx && ctx->completion_eventfd >= 0) {
                uint64_t val = 1;
                if (write(ctx->completion_eventfd, &val, sizeof(val)) != sizeof(val)) {
                    priskv_log_error("Failed to write to completion eventfd: %s\n",
                                     strerror(errno));
                } else {
                    priskv_log_debug("Worker: notified main event loop\n");
                }
            }
        }

    cleanup_request:
        /* Cleanup request */
        priskv_log_debug("Worker: cleaning up request\n");
        if (req->key)
            free(req->key);
        free(req);
        priskv_log_debug("Worker: request cleanup completed\n");
    }

    priskv_log_debug("Redis worker thread exiting\n");
    return NULL;
}

/* Create thread pool */
static redis_thread_pool *redis_thread_pool_create(const char *host, int port, int db,
                                                   const char *password, const char *prefix,
                                                   int num_workers)
{
    redis_thread_pool *pool = calloc(1, sizeof(redis_thread_pool));
    if (!pool) {
        priskv_log_error("Failed to allocate thread pool\n");
        return NULL;
    }

    /* Create connection pool */
    pool->conn_pool = redis_connection_pool_create(host, port, db, password, prefix,
                                                   REDIS_POOL_INITIAL_SIZE, REDIS_POOL_MAX_SIZE);
    if (!pool->conn_pool) {
        priskv_log_error("Failed to create connection pool for thread pool\n");
        free(pool);
        return NULL;
    }

    pool->num_workers = num_workers;
    pool->shutdown = false;

    /* Initialize queues */
    list_head_init(&pool->task_queue);
    list_head_init(&pool->completion_queue);

    if (pthread_mutex_init(&pool->queue_lock, NULL) != 0) {
        priskv_log_error("Failed to init queue mutex\n");
        goto error_destroy_conn_pool;
    }

    if (pthread_cond_init(&pool->task_available, NULL) != 0) {
        priskv_log_error("Failed to init task condition variable\n");
        goto error_destroy_queue_mutex;
    }

    if (pthread_mutex_init(&pool->completion_lock, NULL) != 0) {
        priskv_log_error("Failed to init completion mutex\n");
        goto error_destroy_task_cond;
    }

    /* Create worker threads */
    pool->workers = calloc(num_workers, sizeof(redis_worker));
    if (!pool->workers) {
        priskv_log_error("Failed to allocate workers array\n");
        goto error_destroy_completion_mutex;
    }

    for (int i = 0; i < num_workers; i++) {
        pool->workers[i].pool = pool;
        pool->workers[i].running = true;

        if (pthread_create(&pool->workers[i].thread, NULL, redis_worker_func, &pool->workers[i]) !=
            0) {
            priskv_log_error("Failed to create worker thread %d\n", i);
            pool->workers[i].running = false;
            goto error_stop_workers;
        }
    }

    priskv_log_debug("Redis thread pool created with %d workers\n", num_workers);
    return pool;

error_stop_workers:
    for (int i = 0; i < num_workers; i++) {
        if (pool->workers[i].running) {
            pool->workers[i].running = false;
        }
    }
    pthread_cond_broadcast(&pool->task_available);

    for (int i = 0; i < num_workers; i++) {
        if (pool->workers[i].running) {
            pthread_join(pool->workers[i].thread, NULL);
        }
    }
    free(pool->workers);
error_destroy_completion_mutex:
    pthread_mutex_destroy(&pool->completion_lock);
error_destroy_task_cond:
    pthread_cond_destroy(&pool->task_available);
error_destroy_queue_mutex:
    pthread_mutex_destroy(&pool->queue_lock);
error_destroy_conn_pool:
    redis_connection_pool_destroy(pool->conn_pool);
    free(pool);
    return NULL;
}

/* Destroy thread pool */
static void __attribute__((unused)) redis_thread_pool_destroy(redis_thread_pool *pool)
{
    if (!pool)
        return;

    priskv_log_debug("Destroying Redis thread pool\n");

    pool->shutdown = true;

    /* Stop worker threads */
    for (int i = 0; i < pool->num_workers; i++) {
        pool->workers[i].running = false;
    }

    pthread_cond_broadcast(&pool->task_available);

    for (int i = 0; i < pool->num_workers; i++) {
        pthread_join(pool->workers[i].thread, NULL);
    }

    free(pool->workers);

    /* Cleanup remaining tasks */
    pthread_mutex_lock(&pool->queue_lock);
    while (!list_empty(&pool->task_queue)) {
        redis_request *req = list_pop(&pool->task_queue, redis_request, node);
        if (req) {
            if (req->key)
                free(req->key);
            free(req);
        }
    }
    pthread_mutex_unlock(&pool->queue_lock);

    /* Cleanup completion notifications */
    pthread_mutex_lock(&pool->completion_lock);
    while (!list_empty(&pool->completion_queue)) {
        redis_completion *comp = list_pop(&pool->completion_queue, redis_completion, node);
        free(comp);
    }
    pthread_mutex_unlock(&pool->completion_lock);

    pthread_mutex_destroy(&pool->completion_lock);
    pthread_cond_destroy(&pool->task_available);
    pthread_mutex_destroy(&pool->queue_lock);

    redis_connection_pool_destroy(pool->conn_pool);
    free(pool);
}

/* Submit task to thread pool */
static int redis_thread_pool_submit(redis_thread_pool *pool, redis_request *req)
{
    if (!pool || !req)
        return -1;

    pthread_mutex_lock(&pool->queue_lock);
    list_add_tail(&pool->task_queue, &req->node);
    pthread_cond_signal(&pool->task_available);
    pthread_mutex_unlock(&pool->queue_lock);

    return 0;
}

/* Move all nodes from source list to destination list */
static void list_move_all(struct list_head *dest, struct list_head *src)
{
    while (!list_empty(src)) {
        struct list_node *node = src->n.next;
        list_del(node);
        list_add_tail(dest, node);
    }
}

/* Count number of elements in list */
static int list_count(struct list_head *head)
{
    int count = 0;
    struct list_node *node = head->n.next;

    while (node != &head->n) {
        count++;
        node = node->next;
    }

    return count;
}

/* Process completion notifications */
static void redis_process_completions(redis_thread_context *ctx)
{
    redis_thread_pool *pool = ctx->thread_pool;

    priskv_log_debug("redis_process_completions: checking global completion queue\n");

    /* Get completion notifications for this IO thread from global completion queue */
    pthread_mutex_lock(&pool->completion_lock);
    priskv_log_debug("redis_process_completions: global completion queue empty=%d\n",
                     list_empty(&pool->completion_queue));

    if (!list_empty(&pool->completion_queue)) {
        /* Move completion notifications to IO thread's local queue */
        list_move_all(&ctx->io_completion_queue, &pool->completion_queue);
        priskv_log_debug("redis_process_completions: moved %d completions to IO queue\n",
                         list_count(&ctx->io_completion_queue));
    }
    pthread_mutex_unlock(&pool->completion_lock);

    /* Process local completion queue */
    pthread_mutex_lock(&ctx->io_completion_lock);
    priskv_log_debug("redis_process_completions: IO completion queue count=%d\n",
                     list_count(&ctx->io_completion_queue));

    while (!list_empty(&ctx->io_completion_queue)) {
        redis_completion *comp = list_pop(&ctx->io_completion_queue, redis_completion, node);
        pthread_mutex_unlock(&ctx->io_completion_lock);

        priskv_log_debug(
            "redis_process_completions: calling callback with status=%d, valuelen=%lu\n",
            comp->status, comp->valuelen);

        if (comp->cb) {
            comp->cb(comp->status, comp->valuelen, comp->cbarg);
            priskv_log_debug("redis_process_completions: callback executed\n");
        } else {
            priskv_log_warn("redis_process_completions: callback is NULL!\n");
        }

        free(comp);
        pthread_mutex_lock(&ctx->io_completion_lock);
    }
    pthread_mutex_unlock(&ctx->io_completion_lock);

    priskv_log_debug("redis_process_completions: completed\n");
}

/* Handle Redis completion event */
static void handle_redis_completion_event(int fd, void *opaque, uint32_t events)
{
    redis_thread_context *ctx = opaque;

    priskv_log_debug("handle_redis_completion_event: processing completion events\n");

    /* Read eventfd to clear notification */
    uint64_t val;
    if (read(fd, &val, sizeof(val)) < 0) {
        if (errno != EAGAIN) {
            priskv_log_error("Failed to read from completion eventfd: %s\n", strerror(errno));
        }
    }

    /* Process completion notifications */
    redis_process_completions(ctx);
}

/* Backend driver interface implementation */

/* Open backend device */
static int redis_open(priskv_backend_device *bdev)
{
    pthread_mutex_lock(&g_thread_pool_lock);

    /* If global thread pool doesn't exist, create it */
    if (!g_thread_pool) {
        /* Parse configuration */
        char host[256];
        int port, db;
        char *password = NULL, *prefix = NULL;

        redis_parse_config(host, sizeof(host), &port, &db, &password, &prefix, bdev->link.address);

        priskv_log_debug("Creating global Redis thread pool: host=%s, port=%d, db=%d\n", host, port,
                         db);

        g_thread_pool =
            redis_thread_pool_create(host, port, db, password, prefix, REDIS_THREAD_POOL_SIZE);

        if (password)
            free(password);
        if (prefix)
            free(prefix);

        if (!g_thread_pool) {
            priskv_log_error("Failed to create global Redis thread pool\n");
            pthread_mutex_unlock(&g_thread_pool_lock);
            return -1;
        }
    }

    pthread_mutex_unlock(&g_thread_pool_lock);

    /* Create thread context */
    redis_thread_context *ctx = calloc(1, sizeof(redis_thread_context));
    if (!ctx) {
        priskv_log_error("Failed to allocate thread context\n");
        return -1;
    }

    ctx->bdev = bdev;
    ctx->thread_pool = g_thread_pool;
    list_head_init(&ctx->io_completion_queue);

    if (pthread_mutex_init(&ctx->io_completion_lock, NULL) != 0) {
        priskv_log_error("Failed to init IO completion mutex\n");
        free(ctx);
        return -1;
    }

    /* Create eventfd for completion notification */
    ctx->completion_eventfd = eventfd(0, EFD_NONBLOCK);
    if (ctx->completion_eventfd < 0) {
        priskv_log_error("Failed to create completion eventfd: %s\n", strerror(errno));
        pthread_mutex_destroy(&ctx->io_completion_lock);
        free(ctx);
        return -1;
    }

    /* Register event handler */
    priskv_set_fd_handler(ctx->completion_eventfd, handle_redis_completion_event, NULL, ctx);
    priskv_add_event_fd(bdev->epollfd, ctx->completion_eventfd);

    bdev->private_data = ctx;

    priskv_log_debug("Redis backend opened successfully\n");
    return 0;
}

/* Close backend device */
static int redis_close(priskv_backend_device *bdev)
{
    if (!bdev || !bdev->private_data) {
        return -1;
    }

    redis_thread_context *ctx = bdev->private_data;

    /* Process remaining completion notifications */
    redis_process_completions(ctx);

    /* Cleanup event notification mechanism */
    if (ctx->completion_eventfd >= 0) {
        priskv_del_event(bdev->epollfd, ctx->completion_eventfd);
        close(ctx->completion_eventfd);
    }

    /* Cleanup IO thread's completion queue */
    pthread_mutex_lock(&ctx->io_completion_lock);
    while (!list_empty(&ctx->io_completion_queue)) {
        redis_completion *comp = list_pop(&ctx->io_completion_queue, redis_completion, node);
        free(comp);
    }
    pthread_mutex_unlock(&ctx->io_completion_lock);

    pthread_mutex_destroy(&ctx->io_completion_lock);
    free(ctx);
    bdev->private_data = NULL;

    priskv_log_debug("Redis backend closed successfully\n");
    return 0;
}

/* Check if cacheable */
static bool redis_is_cacheable(priskv_backend_device *bdev, uint64_t valuelen)
{
    (void)bdev;
    (void)valuelen;
    return true;
}

/* Create request */
static redis_request *redis_make_request(redis_thread_context *ctx, const char *key, uint8_t *val,
                                         uint64_t valuelen, redis_op_type op_type,
                                         priskv_backend_driver_cb cb, void *cbarg)
{
    redis_request *req = malloc(sizeof(redis_request));
    if (!req) {
        priskv_log_error("Failed to allocate redis request\n");
        return NULL;
    }

    req->op_type = op_type;
    req->val = val;
    req->valuelen = valuelen;
    req->cb = cb;
    req->cbarg = cbarg;
    req->io_thread_context = ctx;
    list_node_init(&req->node);

    priskv_log_debug("redis_make_request: created request with cb=%p, cbarg=%p\n", cb, cbarg);

    /* Build key with prefix */
    if (ctx->thread_pool->conn_pool->prefix && *ctx->thread_pool->conn_pool->prefix) {
        size_t plen = strlen(ctx->thread_pool->conn_pool->prefix);
        size_t klen = strlen(key);
        req->key = malloc(plen + klen + 1);
        if (!req->key) {
            priskv_log_error("Failed to alloc key buffer\n");
            free(req);
            return NULL;
        }
        memcpy(req->key, ctx->thread_pool->conn_pool->prefix, plen);
        memcpy(req->key + plen, key, klen);
        req->key[plen + klen] = '\0';
    } else {
        req->key = strdup(key);
        if (!req->key) {
            priskv_log_error("Failed to dup key\n");
            free(req);
            return NULL;
        }
    }

    return req;
}

/* GET operation */
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

    priskv_log_debug("redis_get: submitting request for key %s, valuelen=%lu\n", key, valuelen);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, val, valuelen, REDIS_OP_GET, cb, cbarg);
    if (!req) {
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        return;
    }

    /* Submit to thread pool */
    if (redis_thread_pool_submit(ctx->thread_pool, req) != 0) {
        priskv_log_error("Failed to submit GET request to thread pool\n");
        if (req->key)
            free(req->key);
        free(req);
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
    }
}

/* SET operation */
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

    priskv_log_debug("redis_set: submitting request for key %s, valuelen=%lu\n", key, valuelen);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, val, valuelen, REDIS_OP_SET, cb, cbarg);
    if (!req) {
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        return;
    }

    /* Submit to thread pool */
    if (redis_thread_pool_submit(ctx->thread_pool, req) != 0) {
        priskv_log_error("Failed to submit SET request to thread pool\n");
        if (req->key)
            free(req->key);
        free(req);
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
    }
}

/* DEL operation */
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

    priskv_log_debug("redis_del: submitting request for key %s\n", key);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, NULL, 0, REDIS_OP_DEL, cb, cbarg);
    if (!req) {
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        return;
    }

    /* Submit to thread pool */
    if (redis_thread_pool_submit(ctx->thread_pool, req) != 0) {
        priskv_log_error("Failed to submit DEL request to thread pool\n");
        if (req->key)
            free(req->key);
        free(req);
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
    }
}

/* TEST operation */
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

    priskv_log_debug("redis_test: submitting request for key %s\n", key);

    redis_thread_context *ctx = bdev->private_data;

    redis_request *req = redis_make_request(ctx, key, NULL, 0, REDIS_OP_TEST, cb, cbarg);
    if (!req) {
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        return;
    }

    /* Submit to thread pool */
    if (redis_thread_pool_submit(ctx->thread_pool, req) != 0) {
        priskv_log_error("Failed to submit TEST request to thread pool\n");
        if (req->key)
            free(req->key);
        free(req);
        if (cb)
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
    }
}

/* Evict operation (empty implementation) */
static void redis_evict(priskv_backend_device *bdev, priskv_backend_driver_cb cb, void *cbarg)
{
    if (cb) {
        cb(PRISKV_BACKEND_STATUS_OK, 0, cbarg);
    }
}

/* Cleanup operation */
static int redis_clearup(priskv_backend_device *bdev)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("Invalid device or context\n");
        return -1;
    }

    redis_thread_context *ctx = bdev->private_data;
    redis_connection_pool *conn_pool = ctx->thread_pool->conn_pool;

    redis_connection *conn = redis_connection_pool_get(conn_pool);
    if (!conn) {
        priskv_log_error("Failed to get connection for clearup\n");
        return -1;
    }

    priskv_log_debug("redis_clearup: executing FLUSHDB\n");
    redisReply *reply = redisCommand(conn->conn, "FLUSHDB");
    int ret = 0;

    if (!reply || reply->type == REDIS_REPLY_ERROR) {
        priskv_log_error("FLUSHDB error: %s\n", reply ? reply->str : "no reply");
        ret = -1;
    } else {
        priskv_log_debug("redis_clearup: FLUSHDB successful\n");
    }

    if (reply)
        freeReplyObject(reply);
    redis_connection_pool_release(conn_pool, conn);

    return ret;
}

/* Process completion notifications (needs to be called periodically in IO thread) */
void redis_backend_process_completions(priskv_backend_device *bdev)
{
    if (!bdev || !bdev->private_data) {
        priskv_log_debug("redis_backend_process_completions: invalid device or context\n");
        return;
    }

    redis_thread_context *ctx = bdev->private_data;
    priskv_log_debug("redis_backend_process_completions: starting to process completions\n");
    redis_process_completions(ctx);
    priskv_log_debug("redis_backend_process_completions: finished processing completions\n");
}

/* Driver registration */
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

/* Initialization function */
static void priskv_backend_init_redis()
{
    priskv_log_debug("Initializing Redis backend driver with thread pool\n");
    priskv_backend_register(&redis_driver);
    priskv_log_debug("Redis backend driver registered successfully\n");
}

backend_init(priskv_backend_init_redis);