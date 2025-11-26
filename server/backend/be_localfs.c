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
 *   Bo Liu <liubo.2024@bytedance.com>
 *   Jinlong Xuan <15563983051@163.com>
 *   Xu Ji <sov.matrixac@gmail.com>
 *   Yu Wang <wangyu.steph@bytedance.com>
 *   Zhenwei Pi <pizhenwei@bytedance.com>
 *   Rui Zhang <zhangrui.1203@bytedance.com>
 *   Changqi Lu <luchangqi.123@bytedance.com>
 *   Enhua Zhou <zhouenhua@bytedance.com>
 */

#define _DEFAULT_SOURCE
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <fcntl.h>
#include <dirent.h>
#include <liburing.h>
#include <sys/epoll.h>
#include <pthread.h>

#include "backend.h"
#include "priskv-log.h"
#include "priskv-event.h"
#include "priskv-utils.h"
#include "policy.h"
#include "list.h"

#define DEFAULT_MAX_DEPTH 1023

typedef struct localfs_shared_context {
    char *path;
    uint64_t total_size;

    // Only effective when SSD is used as cache device, otherwise policy is ineffective.
    priskv_policy *policy;
    uint64_t free_size;
    int ref_count;
    pthread_spinlock_t lock;

    // queue depth control
    pthread_spinlock_t queue_lock;
    uint32_t inflight_count;
    uint32_t max_depth;
    struct list_head pending_queue;
} localfs_shared_context;

typedef struct localfs_thread_context {
    priskv_backend_device *bdev;
    struct io_uring ring;

    struct localfs_shared_context *shared_ctx;
} localfs_thread_context;

static localfs_shared_context *localfs_shared_ctx = NULL;
static pthread_mutex_t localfs_shared_ctx_lock = PTHREAD_MUTEX_INITIALIZER;

static localfs_shared_context *localfs_shared_context_create(const char *address);
static void localfs_shared_context_destroy(localfs_shared_context *ctx);

static localfs_shared_context *localfs_shared_context_get(void)
{
    localfs_shared_context *ctx = NULL;

    pthread_mutex_lock(&localfs_shared_ctx_lock);
    if (localfs_shared_ctx) {
        localfs_shared_ctx->ref_count++;
        ctx = localfs_shared_ctx;
    }
    pthread_mutex_unlock(&localfs_shared_ctx_lock);

    return ctx;
}

static void localfs_shared_context_put(localfs_shared_context *ctx)
{
    if (!ctx) {
        return;
    }

    bool should_free = false;

    pthread_mutex_lock(&localfs_shared_ctx_lock);
    ctx->ref_count--;
    if (ctx->ref_count == 0) {
        should_free = true;
        localfs_shared_ctx = NULL;
    }
    pthread_mutex_unlock(&localfs_shared_ctx_lock);

    if (should_free) {
        localfs_shared_context_destroy(ctx);
    }
}

typedef enum {
    LOCALFS_OP_GET,
    LOCALFS_OP_SET,
    LOCALFS_OP_DEL,
    LOCALFS_OP_TEST,
} localfs_op_type;

typedef struct localfs_request {
    localfs_thread_context *ctx;
    struct list_node node;

    const char *key;
    uint8_t *val;
    uint64_t valuelen; // Will be updated to actual read/write length after SET and GET operations
                       // complete
    uint64_t old_valuelen; // Size of old value being overwritten (for SET)
    uint64_t timeout;
    priskv_backend_driver_cb cb;
    void *cbarg;
    char path[PATH_MAX];
    int fd;
    localfs_op_type op_type;
    priskv_backend_status status;
    bool queued_once;
} localfs_request;

static void localfs_complete_request(localfs_request *req);
static bool submit_io_request(localfs_request *req);

static void handle_io_uring_events(int fd, void *opaque, uint32_t events)
{
    localfs_thread_context *ctx = opaque;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    struct io_uring_cqe *cqe;
    size_t completed_reqs = 0;

    while (io_uring_peek_cqe(&ctx->ring, &cqe) == 0) {
        localfs_request *req = (localfs_request *)io_uring_cqe_get_data(cqe);
        int res = cqe->res;
        io_uring_cqe_seen(&ctx->ring, cqe);
        cqe = NULL;

        if (res < 0) { // res is errno returned by read/write, just log it
            priskv_log_error("BE_LOCALFS: async operation failed: %s\n", strerror(-res));
            req->status = PRISKV_BACKEND_STATUS_ERROR;

            if (req->op_type == LOCALFS_OP_GET && shared_ctx->policy) {
                pthread_spin_lock(&shared_ctx->lock);
                priskv_policy_unref_key(shared_ctx->policy, req->key);
                pthread_spin_unlock(&shared_ctx->lock);
            }

            localfs_complete_request(req);
            continue;
        }

        pthread_spin_lock(&shared_ctx->lock);

        if (req->op_type == LOCALFS_OP_SET) {
            if (res != req->valuelen) {
                priskv_log_error("BE_LOCALFS: incomplete write: %d, expect: %d\n", res,
                                 req->valuelen);
                req->status = PRISKV_BACKEND_STATUS_ERROR;
                pthread_spin_unlock(&shared_ctx->lock);
                localfs_complete_request(req);
                continue;
            }

            int64_t delta = (int64_t)req->valuelen - (int64_t)req->old_valuelen;
            if (delta > 0) {
                shared_ctx->free_size -= delta;
            } else {
                shared_ctx->free_size += (-delta);
            }

            if (shared_ctx->policy) {
                priskv_policy_access(shared_ctx->policy, req->key);
            }
        } else if (req->op_type == LOCALFS_OP_GET) {
            if (shared_ctx->policy) {
                priskv_policy_unref_key(shared_ctx->policy, req->key);
            }
        }

        pthread_spin_unlock(&shared_ctx->lock);

        req->valuelen = res;
        req->status = PRISKV_BACKEND_STATUS_OK;
        localfs_complete_request(req);

        completed_reqs++;
    }

    if (completed_reqs == 0) {
        return;
    }

    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count < completed_reqs) {
        priskv_log_warn("BE_LOCALFS: inflight underflow: inflight=%u completed=%zu\n",
                        shared_ctx->inflight_count, completed_reqs);
        shared_ctx->inflight_count = 0;
    } else {
        shared_ctx->inflight_count -= completed_reqs;
    }

    while (shared_ctx->inflight_count < shared_ctx->max_depth) {
        localfs_request *pending = list_pop(&shared_ctx->pending_queue, localfs_request, node);

        if (!pending) {
            break;
        }

        if (!submit_io_request(pending)) {
            break;
        }
    }
    pthread_spin_unlock(&shared_ctx->queue_lock);
}

static int check_or_create_dir(const char *path)
{
    struct stat st;

    if (stat(path, &st) == 0) {
        if (S_ISDIR(st.st_mode)) {
            return 0;
        } else {
            priskv_log_error("BE_LOCALFS: path exists but is not a directory\n");
            return -1;
        }
    } else {
        if (mkdir(path, 0755) == 0) {
            return 0;
        } else {
            priskv_log_error("BE_LOCALFS: failed to create directory: %s\n", strerror(errno));
            return -1;
        }
    }

    return 0;
}

static int64_t get_dir_used_size(const char *path)
{
    DIR *dir = opendir(path);
    if (!dir) {
        priskv_log_error("BE_LOCALFS: failed to open directory: %s\n", strerror(errno));
        return -1;
    }

    int64_t used_size = 0;
    struct dirent *entry;
    struct stat st;
    char full_path[PATH_MAX];

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) {
            snprintf(full_path, sizeof(full_path), "%s/%s", path, entry->d_name);
            if (stat(full_path, &st) == 0) {
                used_size += st.st_size;
            }
        }
    }

    closedir(dir);
    return used_size;
}

static uint64_t get_file_size(const char *path)
{
    struct stat st;

    if (stat(path, &st) == 0) {
        return (uint64_t)st.st_size;
    }

    return 0;
}

static int64_t get_filesystem_available_size(const char *path)
{
    struct statvfs vfs;

    if (statvfs(path, &vfs) != 0) {
        priskv_log_error("BE_LOCALFS: failed to get filesystem info for %s: %s\n", path,
                         strerror(errno));
        return -1;
    }

    /* f_bavail: available blocks for non-privileged users
     * f_frsize: fragment size (preferred block size)
     */
    uint64_t available_size = (uint64_t)vfs.f_bavail * vfs.f_frsize;

    return (int64_t)available_size;
}

/* ADDRESS: /a/b/c/&size=4GB */
static int localfs_parse_address(const char *address, char **path, uint64_t *size)
{
    char *p = strchr(address, '&');
    int64_t fs_size;

    if (!p) {
        *path = strdup(address);
        goto get_fs_size;
    }

    *path = strndup(address, p - address);

    p = strstr(p, "size=");
    if (!p) {
        goto get_fs_size;
    }

    return priskv_str2num(p + 5, (int64_t *)size);

get_fs_size:
    fs_size = get_filesystem_available_size(*path);
    if (fs_size < 0) {
        free(*path);
        *path = NULL;
        return -1;
    }
    *size = (uint64_t)fs_size;
    return 0;
}

static localfs_shared_context *localfs_shared_context_create(const char *address)
{
    localfs_shared_context *ctx = calloc(1, sizeof(*ctx));
    bool lock_initialized = false;
    bool queue_lock_initialized = false;

    if (!ctx) {
        priskv_log_error("BE_LOCALFS: failed to allocate shared context\n");
        return NULL;
    }

    if (pthread_spin_init(&ctx->lock, 0)) {
        priskv_log_error("BE_LOCALFS: failed to initialize shared context lock\n");
        goto err;
    }
    lock_initialized = true;

    if (pthread_spin_init(&ctx->queue_lock, 0)) {
        priskv_log_error("BE_LOCALFS: failed to initialize queue lock\n");
        goto err;
    }
    queue_lock_initialized = true;

    list_head_init(&ctx->pending_queue);
    ctx->inflight_count = 0;
    ctx->max_depth = DEFAULT_MAX_DEPTH;

    if (localfs_parse_address(address, &ctx->path, &ctx->total_size)) {
        priskv_log_error("BE_LOCALFS: failed to parse address\n");
        goto err;
    }

    if (strlen(ctx->path) + NAME_MAX + 1 > PATH_MAX) {
        priskv_log_error("BE_LOCALFS: path too long\n");
        goto err;
    }

    if (check_or_create_dir(ctx->path) != 0) {
        goto err;
    }

    int64_t used_size = get_dir_used_size(ctx->path);
    if (used_size < 0) {
        priskv_log_error("BE_LOCALFS: failed to get directory used size\n");
        goto err;
    }

    if ((uint64_t)used_size > ctx->total_size) {
        priskv_log_error(
            "BE_LOCALFS: directory used size (%lu) exceeds configured capacity (%lu)\n",
            (uint64_t)used_size, ctx->total_size);
        goto err;
    }

    ctx->free_size = ctx->total_size - (uint64_t)used_size;

    ctx->policy = priskv_policy_create("lru");
    if (ctx->policy == NULL) {
        priskv_log_error("BE_LOCALFS: failed to create LRU policy\n");
        goto err;
    }

    ctx->ref_count = 1;

    return ctx;

err:
    if (ctx->policy) {
        priskv_policy_destroy(ctx->policy);
        ctx->policy = NULL;
    }
    if (ctx->path) {
        free(ctx->path);
        ctx->path = NULL;
    }
    if (queue_lock_initialized) {
        pthread_spin_destroy(&ctx->queue_lock);
    }
    if (lock_initialized) {
        pthread_spin_destroy(&ctx->lock);
    }
    free(ctx);
    return NULL;
}

// TODO: wait for all in-flight requests to complete
static void localfs_shared_context_destroy(localfs_shared_context *ctx)
{
    if (!ctx) {
        return;
    }

    while (1) {
        localfs_request *req;

        pthread_spin_lock(&ctx->queue_lock);
        req = list_pop(&ctx->pending_queue, localfs_request, node);
        if (!req) {
            pthread_spin_unlock(&ctx->queue_lock);
            break;
        }
        pthread_spin_unlock(&ctx->queue_lock);

        req->status = PRISKV_BACKEND_STATUS_ERROR;
        localfs_complete_request(req);
    }

    pthread_spin_destroy(&ctx->queue_lock);
    pthread_spin_destroy(&ctx->lock);

    if (ctx->policy) {
        priskv_policy_destroy(ctx->policy);
    }
    if (ctx->path) {
        free(ctx->path);
    }

    free(ctx);
}

static int localfs_open(priskv_backend_device *bdev)
{
    localfs_shared_context *shared_ctx = NULL;
    localfs_thread_context *ctx = NULL;

    shared_ctx = localfs_shared_context_get();

    if (!shared_ctx) {
        pthread_mutex_lock(&localfs_shared_ctx_lock);

        if (localfs_shared_ctx) {
            localfs_shared_ctx->ref_count++;
            shared_ctx = localfs_shared_ctx;
            pthread_mutex_unlock(&localfs_shared_ctx_lock);
        } else {
            shared_ctx = localfs_shared_context_create(bdev->link.address);
            if (!shared_ctx) {
                pthread_mutex_unlock(&localfs_shared_ctx_lock);
                goto err;
            }

            localfs_shared_ctx = shared_ctx;
            pthread_mutex_unlock(&localfs_shared_ctx_lock);
        }
    }

    ctx = calloc(1, sizeof(localfs_thread_context));
    if (ctx == NULL) {
        priskv_log_error("BE_LOCALFS: failed to allocate context\n");
        goto err_put_shared_ctx;
    }

    ctx->bdev = bdev;
    ctx->shared_ctx = shared_ctx;

    struct io_uring_params ring_params = {0};
    ring_params.cq_entries = DEFAULT_MAX_DEPTH;

    if (io_uring_queue_init_params(128, &ctx->ring, &ring_params) < 0) {
        priskv_log_error("BE_LOCALFS: failed to initialize io_uring\n");
        goto err_free_ctx;
    }

    priskv_set_fd_handler(ctx->ring.ring_fd, handle_io_uring_events, NULL, ctx);
    priskv_add_event_fd(bdev->epollfd, ctx->ring.ring_fd);

    bdev->private_data = ctx;

    return 0;

err_free_ctx:
    free(ctx);
err_put_shared_ctx:
    localfs_shared_context_put(shared_ctx);
    goto err;

err:
    priskv_log_error("BE_LOCALFS: failed to open device\n");
    return -1;
}

static int localfs_close(priskv_backend_device *bdev)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        return -1;
    }

    localfs_thread_context *ctx = bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    priskv_del_event(bdev->epollfd, ctx->ring.ring_fd);
    io_uring_queue_exit(&ctx->ring);
    free(ctx);
    bdev->private_data = NULL;

    localfs_shared_context_put(shared_ctx);

    return 0;
}

static bool localfs_is_cacheable(priskv_backend_device *bdev, uint64_t valuelen)
{
    bool res;

    localfs_thread_context *ctx = (localfs_thread_context *)bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    pthread_spin_lock(&shared_ctx->lock);
    res = valuelen <= shared_ctx->free_size;
    pthread_spin_unlock(&shared_ctx->lock);

    return res;
}

static localfs_request *localfs_make_request(localfs_thread_context *ctx, const char *key,
                                             uint8_t *val, uint64_t valuelen,
                                             localfs_op_type op_type, priskv_backend_driver_cb cb,
                                             void *cbarg)
{
    localfs_request *req = malloc(sizeof(localfs_request));
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    if (req == NULL) {
        priskv_log_error("BE_LOCALFS: failed to allocate localfs request\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return NULL;
    }

    req->fd = -1;
    req->ctx = ctx;
    req->key = strdup(key);
    req->val = val;
    req->valuelen = valuelen;
    req->old_valuelen = 0;
    req->cb = cb;
    req->cbarg = cbarg;
    req->op_type = op_type;
    list_node_init(&req->node);
    req->queued_once = false;

    if (snprintf(req->path, sizeof(req->path), "%s/%s", shared_ctx->path, key) >=
        sizeof(req->path)) {
        priskv_log_error("BE_LOCALFS: path too long\n");
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        localfs_complete_request(req);
        return NULL;
    }

    int flags = O_RDONLY;
    uint32_t file_size = get_file_size(req->path);
    if (op_type == LOCALFS_OP_SET) {
        req->old_valuelen = file_size;
        flags = O_CREAT | O_WRONLY | O_TRUNC | O_SYNC;
    } else if (op_type == LOCALFS_OP_GET) {
        if (file_size > valuelen) {
            priskv_log_error("BE_LOCALFS: file size %lu exceeds buffer size %lu for key %s\n",
                             file_size, valuelen, key);
            req->status = PRISKV_BACKEND_STATUS_VALUE_TOO_BIG;
            localfs_complete_request(req);
            return NULL;
        }
    }

    req->fd = open(req->path, flags, 0644);
    if (req->fd < 0) {
        priskv_log_error("BE_LOCALFS: failed to open file(%s) when %s: %s\n", req->path,
                         op_type == LOCALFS_OP_SET ? "set" : "get", strerror(errno));
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        localfs_complete_request(req);
        return NULL;
    }

    return req;
}

static void localfs_free_request(localfs_request *req)
{
    if (req == NULL) {
        return;
    }

    if (req->fd >= 0) {
        close(req->fd);
    }
    free((char *)req->key);
    free(req);
}

static void localfs_complete_request(localfs_request *req)
{
    if (req->cb) {
        req->cb(req->status, req->valuelen, req->cbarg);
    }

    localfs_free_request(req);
}

static bool submit_io_request(localfs_request *req)
{
    localfs_thread_context *ctx = req->ctx;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    struct io_uring_sqe *sqe = NULL;

    pthread_spin_lock(&shared_ctx->queue_lock);
    if (shared_ctx->inflight_count >= shared_ctx->max_depth) {
        if (req->queued_once) {
            list_add(&shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        goto can_not_submit;
    }

    sqe = io_uring_get_sqe(&ctx->ring);
    if (!sqe) { // rarely
        if (req->queued_once) {
            list_add(&shared_ctx->pending_queue, &req->node);
        } else {
            list_add_tail(&shared_ctx->pending_queue, &req->node);
            req->queued_once = true;
        }
        goto can_not_submit;
    }

    shared_ctx->inflight_count++;
    pthread_spin_unlock(&shared_ctx->queue_lock);

    switch (req->op_type) {
    case LOCALFS_OP_GET:
        if (shared_ctx->policy) {
            bool ref_acquired;
            pthread_spin_lock(&shared_ctx->lock);
            ref_acquired = priskv_policy_try_ref_key(shared_ctx->policy, req->key);
            pthread_spin_unlock(&shared_ctx->lock);

            if (!ref_acquired) {
                priskv_log_debug("BE_LOCALFS: key %s not found in policy during submit\n",
                                 req->key);
                pthread_spin_lock(&shared_ctx->queue_lock);
                if (shared_ctx->inflight_count > 0) {
                    shared_ctx->inflight_count--;
                }
                pthread_spin_unlock(&shared_ctx->queue_lock);
                req->status = PRISKV_BACKEND_STATUS_NOT_FOUND;
                localfs_complete_request(req);
                return false;
            }
        }
        io_uring_prep_read(sqe, req->fd, req->val, req->valuelen, 0);
        break;
    case LOCALFS_OP_SET:
        io_uring_prep_write(sqe, req->fd, req->val, req->valuelen, 0);
        break;
    default:
        pthread_spin_lock(&shared_ctx->queue_lock);
        if (shared_ctx->inflight_count > 0) {
            shared_ctx->inflight_count--;
        }
        pthread_spin_unlock(&shared_ctx->queue_lock);
        priskv_log_error("BE_LOCALFS: invalid op type\n");
        req->status = PRISKV_BACKEND_STATUS_ERROR;
        localfs_complete_request(req);
        return false;
    }

    io_uring_sqe_set_data(sqe, req);
    io_uring_submit(&ctx->ring);

    return true;

can_not_submit:
    pthread_spin_unlock(&shared_ctx->queue_lock);
    return false;
}

static void localfs_get(priskv_backend_device *bdev, const char *key, uint8_t *val,
                        uint64_t valuelen, priskv_backend_driver_cb cb, void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    localfs_thread_context *ctx = bdev->private_data;

    localfs_request *req = localfs_make_request(ctx, key, val, valuelen, LOCALFS_OP_GET, cb, cbarg);
    if (req == NULL) {
        return;
    }

    submit_io_request(req);
}

static void localfs_set(priskv_backend_device *bdev, const char *key, uint8_t *val,
                        uint64_t valuelen, uint64_t timeout, priskv_backend_driver_cb cb,
                        void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    localfs_thread_context *ctx = bdev->private_data;

    localfs_request *req = localfs_make_request(ctx, key, val, valuelen, LOCALFS_OP_SET, cb, cbarg);
    if (req == NULL) {
        return;
    }

    submit_io_request(req);
}

static void localfs_del(priskv_backend_device *bdev, const char *key, priskv_backend_driver_cb cb,
                        void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_backend_status status = PRISKV_BACKEND_STATUS_OK;

    localfs_thread_context *ctx = (localfs_thread_context *)bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    char path[PATH_MAX];

    if (snprintf(path, sizeof(path), "%s/%s", shared_ctx->path, key) >= sizeof(path)) {
        priskv_log_error("BE_LOCALFS: path too long\n");
        status = PRISKV_BACKEND_STATUS_ERROR;
        goto out;
    }

    uint64_t valuelen = get_file_size(path);

    if (unlink(path) && errno != ENOENT) {
        priskv_log_error("BE_LOCALFS: failed to delete file: %s\n", strerror(errno));
        status = PRISKV_BACKEND_STATUS_ERROR;
    } else {
        pthread_spin_lock(&shared_ctx->lock);
        shared_ctx->free_size += valuelen;

        if (shared_ctx->policy) {
            priskv_policy_del_key(shared_ctx->policy, key);
        }
        pthread_spin_unlock(&shared_ctx->lock);
    }

out:
    if (cb) {
        cb(status, valuelen, cbarg);
    }
}

static void localfs_evict(priskv_backend_device *bdev, priskv_backend_driver_cb cb, void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    localfs_thread_context *ctx = bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    pthread_spin_lock(&shared_ctx->lock);
    if (!shared_ctx->policy) {
        pthread_spin_unlock(&shared_ctx->lock);
        priskv_log_error("BE_LOCALFS: no policy configured for eviction\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    const char *key = priskv_policy_evict(shared_ctx->policy);
    if (!key) {
        pthread_spin_unlock(&shared_ctx->lock);
        priskv_log_debug("BE_LOCALFS: no key available for eviction\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_NO_SPACE, 0, cbarg);
        }
        return;
    }
    pthread_spin_unlock(&shared_ctx->lock);

    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/%s", shared_ctx->path, key) >= sizeof(path)) {
        priskv_log_error("BE_LOCALFS: path too long for key: %s\n", key);
        free((char *)key);
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    uint64_t valuelen = get_file_size(path);

    if (unlink(path) && errno != ENOENT) {
        priskv_log_error("BE_LOCALFS: failed to delete file %s: %s\n", path, strerror(errno));
        free((char *)key);
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, valuelen, cbarg);
        }
        return;
    }

    pthread_spin_lock(&shared_ctx->lock);
    shared_ctx->free_size += valuelen;
    pthread_spin_unlock(&shared_ctx->lock);

    priskv_log_debug("BE_LOCALFS: evicted key %s, freed %lu bytes\n", key, valuelen);
    free((char *)key);

    if (cb) {
        cb(PRISKV_BACKEND_STATUS_OK, valuelen, cbarg);
    }
}

static int localfs_clearup(priskv_backend_device *bdev)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        return -1;
    }

    localfs_thread_context *ctx = bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    DIR *dir = opendir(shared_ctx->path);
    if (!dir) {
        priskv_log_error("BE_LOCALFS: failed to open directory: %s\n", strerror(errno));
        return -1;
    }

    struct dirent *entry;
    char full_path[PATH_MAX];

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) {
            snprintf(full_path, sizeof(full_path), "%s/%s", shared_ctx->path, entry->d_name);
            if (unlink(full_path) && errno != ENOENT) {
                priskv_log_error("BE_LOCALFS: failed to delete file: %s\n", strerror(errno));
                closedir(dir);
                return -1;
            }
        }
    }

    pthread_spin_lock(&shared_ctx->lock);
    shared_ctx->free_size = shared_ctx->total_size;
    pthread_spin_unlock(&shared_ctx->lock);

    closedir(dir);

    return 0;
}

static void localfs_test(priskv_backend_device *bdev, const char *key, priskv_backend_driver_cb cb,
                         void *cbarg)
{
    if (bdev == NULL || bdev->private_data == NULL) {
        priskv_log_error("BE_LOCALFS: invalid device or context\n");
        if (cb) {
            cb(PRISKV_BACKEND_STATUS_ERROR, 0, cbarg);
        }
        return;
    }

    priskv_backend_status status = PRISKV_BACKEND_STATUS_NOT_FOUND;
    bool ref_acquired = false;
    uint32_t valuelen = 0;

    localfs_thread_context *ctx = (localfs_thread_context *)bdev->private_data;
    localfs_shared_context *shared_ctx = ctx->shared_ctx;
    assert(shared_ctx);

    if (shared_ctx->policy) {
        pthread_spin_lock(&shared_ctx->lock);
        ref_acquired = priskv_policy_try_ref_key(shared_ctx->policy, key);
        pthread_spin_unlock(&shared_ctx->lock);

        if (!ref_acquired) {
            priskv_log_debug("BE_LOCALFS: key %s not found in policy\n", key);
            goto out;
        }
    }

    char path[PATH_MAX];

    if (snprintf(path, sizeof(path), "%s/%s", shared_ctx->path, key) >= sizeof(path)) {
        priskv_log_error("BE_LOCALFS: path too long\n");
        status = PRISKV_BACKEND_STATUS_ERROR;
        goto out_unref;
    }

    valuelen = get_file_size(path);
    if (valuelen > 0) {
        status = PRISKV_BACKEND_STATUS_OK;
    } else if (errno != ENOENT) {
        priskv_log_error("BE_LOCALFS: failed to get file size: %s\n", strerror(errno));
        status = PRISKV_BACKEND_STATUS_ERROR;
    }

out_unref:
    if (ref_acquired && shared_ctx->policy) {
        pthread_spin_lock(&shared_ctx->lock);
        priskv_policy_unref_key(shared_ctx->policy, key);
        pthread_spin_unlock(&shared_ctx->lock);
    }

out:
    if (cb) {
        cb(status, valuelen, cbarg);
    }
}

static priskv_backend_driver localfs_driver = {
    .name = "localfs",
    .open = localfs_open,
    .close = localfs_close,
    .is_cacheable = localfs_is_cacheable,
    .get = localfs_get,
    .set = localfs_set,
    .del = localfs_del,
    .test = localfs_test,
    .evict = localfs_evict,
    .clearup = localfs_clearup,
};

static void priskv_backend_init_localfs()
{
    priskv_backend_register(&localfs_driver);
}

backend_init(priskv_backend_init_localfs);
