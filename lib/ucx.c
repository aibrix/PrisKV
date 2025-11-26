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

#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <netdb.h>

#include "priskv-log.h"
#include "priskv-ucx.h"

// forward declaration
static priskv_ucx_ep *priskv_ucx_ep_create(priskv_ucx_worker *worker, ucp_ep_params_t *params,
                                           priskv_ucx_ep_close_cb close_cb, void *close_cb_data,
                                           uint8_t paired_worker_ep);

static int token_eq(const char *s, size_t len, const char *t)
{
    size_t tl = strlen(t);
    if (len != tl)
        return 0;
    return strncmp(s, t, tl) == 0;
}

int priskv_ucx_tls_has_cuda_disabled(const char *tls)
{
    if (!tls)
        return 1;
    const char *q = tls;
    while (*q == ' ' || *q == '\t')
        q++;
    if (*q == '\0')
        return 1;
    if (*q == '^') {
        const char *p = q + 1;
        while (*p) {
            const char *start = p;
            const char *comma = strchr(p, ',');
            size_t len = comma ? (size_t)(comma - start) : strlen(start);
            while (len > 0 && (start[0] == ' ' || start[0] == '\t')) {
                start++;
                len--;
            }
            while (len > 0 && (start[len - 1] == ' ' || start[len - 1] == '\t')) {
                len--;
            }
            if (token_eq(start, len, "cuda") || token_eq(start, len, "cuda_copy"))
                return 1;
            if (!comma)
                break;
            p = comma + 1;
        }
        return 0;
    }
    const char *end = q + strlen(q);
    while (end > q && ((end[-1] == ' ') || (end[-1] == '\t')))
        end--;
    if (token_eq(q, (size_t)(end - q), "all"))
        return 0;
    if (strstr(q, "cuda") != NULL)
        return 0;
    return 1;
}

priskv_ucx_context *priskv_ucx_context_init(uint8_t busy_polling)
{
    ucp_config_t *config;
    ucs_status_t status = ucp_config_read(NULL, NULL, &config);
    PRISKV_UCX_RETURN_IF_ERROR(status, "priskv_ucx_init: failed to read config", {}, NULL);

    // enable reuseaddr
    ucp_config_modify(config, "CM_REUSEADDR", "y");

    if (priskv_get_log_level() >= priskv_log_info) {
        ucp_config_print(config, stdout, "UCX Config", UCS_CONFIG_PRINT_CONFIG);
    }

    ucp_params_t params;
    memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES | UCP_PARAM_FIELD_MT_WORKERS_SHARED;
    params.features = UCP_FEATURE_TAG | UCP_FEATURE_RMA;
    params.mt_workers_shared = 1;

    if (!busy_polling) {
        params.features |= UCP_FEATURE_WAKEUP;
    }

    priskv_ucx_context *context = malloc(sizeof(priskv_ucx_context));
    if (ucs_unlikely(!context)) {
        priskv_log_error("priskv_ucx_init: failed to malloc context\n");
        return NULL;
    }
    status = ucp_init(&params, config, &context->handle);
    ucp_config_release(config);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_init: failed to init context", { free(context); }, NULL);
    context->busy_polling = busy_polling;
    if (priskv_get_log_level() >= priskv_log_info) {
        ucp_context_print_info(context->handle, stdout);
    }
    return context;
}

int priskv_ucx_context_has_cuda_support(priskv_ucx_context *context)
{
    ucp_context_attr_t attr = {.field_mask = UCP_ATTR_FIELD_MEMORY_TYPES};
    ucp_context_query(context->handle, &attr);
    uint8_t has_cuda_support = (attr.memory_types & UCS_MEMORY_TYPE_CUDA) == UCS_MEMORY_TYPE_CUDA;
    if (has_cuda_support) {
        priskv_log_info("priskv_ucx_init: UCX supports CUDA memory type\n");
        const char *tls_env = getenv("UCX_TLS");
        if (priskv_ucx_tls_has_cuda_disabled(tls_env)) {
            has_cuda_support = 0;
        }
        if (has_cuda_support) {
            priskv_log_info("priskv_ucx_init: CUDA support enabled by UCX_TLS\n");
        } else {
            priskv_log_info("priskv_ucx_init: CUDA support disabled by UCX_TLS\n");
        }
    }

    return has_cuda_support;
}

priskv_ucx_memh *priskv_ucx_mmap(priskv_ucx_context *context, void *buffer, size_t count,
                                 ucs_memory_type_t type)
{
    priskv_ucx_memh *memh = malloc(sizeof(priskv_ucx_memh));
    if (ucs_unlikely(!memh)) {
        priskv_log_error("priskv_ucx_mem_register: failed to malloc memh\n");
        return NULL;
    }
    memh->context = context;

    ucp_mem_map_params_t params = {.field_mask = UCP_MEM_MAP_PARAM_FIELD_LENGTH |
                                                 UCP_MEM_MAP_PARAM_FIELD_MEMORY_TYPE,
                                   .length = count,
                                   .memory_type = type};
    if (buffer == NULL) {
        priskv_log_debug("priskv_ucx_mmap: allocate memory of size %zu\n", count);
        params.field_mask |= UCP_MEM_MAP_PARAM_FIELD_FLAGS;
        params.flags = UCP_MEM_MAP_ALLOCATE;
    } else {
        priskv_log_debug("priskv_ucx_mmap: map memory %p of size %zu\n", buffer, count);
        params.field_mask |= UCP_MEM_MAP_PARAM_FIELD_ADDRESS;
        params.address = buffer;
    }

    ucs_status_t status = ucp_mem_map(context->handle, &params, &memh->handle);
    PRISKV_UCX_RETURN_IF_ERROR(status, "priskv_ucx_mmap: failed to mmap", { free(memh); }, NULL);

    ucp_mem_attr_t attr = {.field_mask = UCP_MEM_ATTR_FIELD_ADDRESS | UCP_MEM_ATTR_FIELD_LENGTH |
                                         UCP_MEM_ATTR_FIELD_MEM_TYPE};

    status = ucp_mem_query(memh->handle, &attr);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_mmap: failed to query",
        {
            ucp_mem_unmap(context->handle, memh->handle);
            free(memh);
        },
        NULL);

    memh->addr = (uint64_t)attr.address;
    memh->len = attr.length;
    memh->type = attr.mem_type;
    memh->rkey_buffer = NULL;

    priskv_log_debug("priskv_ucx_mmap: memh %p [addr=%lu, len=%zu, type=%s] mapped\n", memh,
                     memh->addr, memh->len, ucs_memory_type_names[memh->type]);

    status = ucp_rkey_pack(context->handle, memh->handle, &memh->rkey_buffer, &memh->rkey_length);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_mmap: failed to pack",
        {
            ucp_mem_unmap(context->handle, memh->handle);
            free(memh);
        },
        NULL);

    return memh;
}

ucs_status_t priskv_ucx_munmap(priskv_ucx_memh *memh)
{
    if (ucs_unlikely(memh == NULL)) {
        priskv_log_error("priskv_ucx_munmap: memh is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    if (ucs_unlikely(memh->handle == NULL)) {
        priskv_log_error("priskv_ucx_munmap: handle is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    if (memh->rkey_buffer) {
        ucp_memh_buffer_release_params_t params = {.field_mask = 0};
        ucp_memh_buffer_release(memh->rkey_buffer, &params);
        memh->rkey_buffer = NULL;
    }
    ucp_mem_unmap(memh->context->handle, memh->handle);
    memh->handle = NULL;
    free(memh);
    return UCS_OK;
}

priskv_ucx_worker *priskv_ucx_worker_create(priskv_ucx_context *context, uint64_t client_id)
{
    if (ucs_unlikely(context == NULL)) {
        priskv_log_error("priskv_ucx_worker_init: context is NULL\n");
        return NULL;
    }

    priskv_ucx_worker *worker = malloc(sizeof(priskv_ucx_worker));
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_worker_init: failed to malloc worker\n");
        return NULL;
    }

    worker->context = context;
    worker->address = NULL;

    ucp_worker_params_t params = {.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE |
                                                UCP_WORKER_PARAM_FIELD_CLIENT_ID,
                                  .thread_mode = UCS_THREAD_MODE_SINGLE,
                                  .client_id = client_id};

    ucs_status_t status = ucp_worker_create(context->handle, &params, &worker->handle);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_worker_init: failed to init worker", { free(worker); }, NULL);
    if (!context->busy_polling) {
        status = ucp_worker_get_efd(worker->handle, &worker->efd);
        PRISKV_UCX_RETURN_IF_ERROR(
            status, "priskv_ucx_worker_init: failed to get efd", { free(worker); }, NULL);
        if (priskv_set_nonblock(worker->efd)) {
            priskv_log_error("priskv_ucx_worker_init: failed to set nonblock\n");
            free(worker);
            return NULL;
        }
        status = ucp_worker_arm(worker->handle);
        PRISKV_UCX_RETURN_IF_ERROR(
            status, "priskv_ucx_worker_init: failed to arm worker", { free(worker); }, NULL);
    } else {
        worker->efd = -1;
    }

    status = ucp_worker_get_address(worker->handle, &worker->address, &worker->address_len);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_worker_init: failed to get address", { free(worker); }, NULL);

    if (priskv_get_log_level() >= priskv_log_info) {
        ucp_worker_print_info(worker->handle, stdout);
    }
    return worker;
}

static inline void priskv_ucx_request_init(priskv_ucx_request *request)
{
    request->status = UCS_INPROGRESS;
    request->worker = NULL;
    request->ep = NULL;
    request->key = NULL;
}

static void priskv_ucx_request_complete(priskv_ucx_request *request, ucs_status_t status,
                                        const ucp_tag_recv_info_t *info)
{
    if (ucs_unlikely(request->handle == NULL)) {
        goto cb;
    }

    if (ucs_unlikely(request->status != UCS_INPROGRESS)) {
        priskv_log_warn("priskv_ucx_request_complete: request %p completed with status %s, "
                        "but status is already set to %s\n",
                        request, ucs_status_string(status), ucs_status_string(request->status));
    }

    request->status = status;

    if (UCS_PTR_IS_PTR(request->handle)) {
        ucp_request_free(request->handle);
        request->handle = NULL;
    }

cb:
    if (request->cb != NULL) {
        priskv_log_debug("priskv_ucx_request_complete: call cb %p\n", request->cb);
        if (info == NULL) {
            request->cb(status, request->cb_data);
        } else {
            request->tag_recv_cb(status, info->sender_tag, info->length, request->cb_data);
        }
        request->cb = NULL;
    }
}

static ucs_status_ptr_t priskv_ucx_request_progress(priskv_ucx_request *request,
                                                    ucp_request_param_t *params)
{
    ucs_status_t status = UCS_INPROGRESS;
    ucs_status_ptr_t handle = request->handle;

    if (UCS_PTR_IS_ERR(handle)) {
        status = UCS_PTR_STATUS(handle);
    } else if (UCS_PTR_IS_PTR(handle)) {
        // still in progress
        status = UCS_INPROGRESS;
    } else {
        // Operation completed immediately
        status = UCS_OK;
    }

    if (status != UCS_INPROGRESS) {
        ucp_tag_recv_info_t *info = NULL;
        if (params->recv_info.tag_info) {
            info = params->recv_info.tag_info;
        }
        priskv_ucx_request_complete(request, status, info);

        request->handle = NULL;
        free(request);
        return UCS_STATUS_PTR(status);
    }

    return request;
}

ucs_status_t priskv_ucx_request_cancel(priskv_ucx_request *request)
{
    if (ucs_unlikely(request == NULL || request->handle == NULL)) {
        priskv_log_error("priskv_ucx_request_cancel: failed to cancel request\n");
        if (request) {
            free(request);
        }
        return UCS_ERR_INVALID_PARAM;
    }

    if (request->status == UCS_INPROGRESS) {
        if (UCS_PTR_IS_ERR(request->handle)) {
            ucs_status_t status = UCS_PTR_STATUS(request->handle);
            priskv_log_debug(
                "priskv_ucx_request_cancel: unprocessed request %p failed with status %s, "
                "no need to cancel\n",
                request, ucs_status_string(status));
        } else if (request->handle) {
            priskv_log_debug("priskv_ucx_request_cancel: unprocessed request %p with handle %p, "
                             "canceling...\n",
                             request, request->handle);
            ucp_request_cancel(request->worker->handle, request->handle);
            return UCS_OK;
        }
    } else {
        priskv_log_debug("priskv_ucx_request_cancel: request %p already completed with status %s, "
                         "no need to cancel\n",
                         request, ucs_status_string(request->status));
    }
    request->handle = NULL;

    free(request);
    return UCS_OK;
}

static inline ucs_status_t priskv_ucx_request_common_cb_intl(priskv_ucx_request *req, void *request,
                                                             ucs_status_t status,
                                                             const ucp_tag_recv_info_t *info)
{
    priskv_ucx_request_complete(req, status, info);
    free(req);
    return UCS_OK;
}

static void priskv_ucx_request_tag_recv_cb_intl(void *request, ucs_status_t status,
                                                const ucp_tag_recv_info_t *info, void *arg)
{
    priskv_ucx_request *req = (priskv_ucx_request *)arg;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_request_tag_recv_cb_intl: failed to get request\n");
        return;
    }

    if (req->ep) {
        priskv_log_debug("priskv_ucx_request_tag_recv_cb_intl: ep %p request %p [buf=%p, len=%zu, "
                         "tag=%lu, mask=%lu] completed "
                         "with status %s\n",
                         req->ep, request, req->payload.buffer, req->payload.length,
                         req->payload.tag_recv.tag, req->payload.tag_recv.mask,
                         ucs_status_string(status));
    } else {
        priskv_log_debug(
            "priskv_ucx_request_tag_recv_cb_intl: worker %p request %p [buf=%p, len=%zu, "
            "tag=%lu, mask=%lu] completed "
            "with status %s\n",
            req->worker, request, req->payload.buffer, req->payload.length,
            req->payload.tag_recv.tag, req->payload.tag_recv.mask, ucs_status_string(status));
    }

    priskv_ucx_request_common_cb_intl(req, request, status, info);
}

static void priskv_ucx_request_tag_send_cb_intl(void *request, ucs_status_t status, void *arg)
{
    priskv_ucx_request *req = (priskv_ucx_request *)arg;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_request_tag_send_cb_intl: failed to get request\n");
        return;
    }

    priskv_log_debug("priskv_ucx_request_tag_send_cb_intl: ep %p request %p [buf=%p, len=%zu, "
                     "tag=%lu] completed "
                     "with status %s\n",
                     req->ep, request, req->payload.buffer, req->payload.length,
                     req->payload.tag_send.tag, ucs_status_string(status));

    priskv_ucx_request_common_cb_intl(req, request, status, NULL);
}

static void priskv_ucx_request_put_cb_intl(void *request, ucs_status_t status, void *arg)
{
    priskv_ucx_request *req = (priskv_ucx_request *)arg;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_request_put_cb_intl: failed to get request\n");
        return;
    }

    priskv_log_debug(
        "priskv_ucx_request_put_cb_intl: ep %p request %p [buf=%p, len=%zu, raddr=%lu] completed "
        "with status %s\n",
        req->ep, request, req->payload.buffer, req->payload.length, req->payload.rma.raddr,
        ucs_status_string(status));

    priskv_ucx_request_common_cb_intl(req, request, status, NULL);
}

static void priskv_ucx_request_get_cb_intl(void *request, ucs_status_t status, void *arg)
{
    priskv_ucx_request *req = (priskv_ucx_request *)arg;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_request_get_cb_intl: failed to get request\n");
        return;
    }

    priskv_log_debug(
        "priskv_ucx_request_get_cb_intl: ep %p request %p [buf=%p, len=%zu, raddr=%lu] completed "
        "with status %s\n",
        req->ep, request, req->payload.buffer, req->payload.length, req->payload.rma.raddr,
        ucs_status_string(status));

    priskv_ucx_request_common_cb_intl(req, request, status, NULL);
}

static void priskv_ucx_request_flush_cb_intl(void *request, ucs_status_t status, void *arg)
{
    priskv_ucx_request *req = (priskv_ucx_request *)arg;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_request_flush_cb_intl: failed to get request\n");
        return;
    }

    if (req->ep) {
        priskv_log_debug(
            "priskv_ucx_request_flush_cb_intl: ep %p request %p completed with status %s\n",
            req->ep, request, ucs_status_string(status));
    } else {
        priskv_log_debug(
            "priskv_ucx_request_flush_cb_intl: worker %p request %p completed with status %s\n",
            req->worker, request, ucs_status_string(status));
    }

    priskv_ucx_request_common_cb_intl(req, request, status, NULL);
}

static ucs_status_ptr_t priskv_ucx_post_tag_recv(priskv_ucx_worker *worker, priskv_ucx_ep *ep,
                                                 void *buffer, size_t count, ucp_tag_t tag,
                                                 ucp_tag_t mask, priskv_ucx_tag_recv_cb cb,
                                                 void *cb_data)
{
    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    ucs_status_t status;
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_post_tag_recv: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                 UCP_OP_ATTR_FIELD_DATATYPE |
                                                 UCP_OP_ATTR_FIELD_USER_DATA,
                                 .datatype = ucp_dt_make_contig(1),
                                 .cb.recv = priskv_ucx_request_tag_recv_cb_intl,
                                 .user_data = request};
    request->handle = ucp_tag_recv_nbx(worker->handle, buffer, count, tag, mask, &param);
    request->name = "tag_recv";
    request->status = UCS_INPROGRESS;
    request->worker = worker;
    request->ep = ep;
    request->payload.buffer = buffer;
    request->payload.length = count;
    request->payload.tag_recv.tag = tag;
    request->payload.tag_recv.mask = mask;
    request->tag_recv_cb = cb;
    request->cb_data = cb_data;

    if (ep) {
        priskv_log_debug("priskv_ucx_post_tag_recv: ep %p request %p [buf=%p, len=%zu, tag=%lu, "
                         "mask=%lu] posted\n",
                         ep, request, buffer, count, tag, mask);
    } else {
        priskv_log_debug("priskv_ucx_post_tag_recv: worker %p request %p [buf=%p, len=%zu, "
                         "tag=%lu, mask=%lu] posted\n",
                         worker, request, buffer, count, tag, mask);
    }

    return priskv_ucx_request_progress(request, &param);

callback:
    if (cb) {
        cb(status, tag & mask, 0, cb_data);
    }
    return NULL;
}

ucs_status_ptr_t priskv_ucx_worker_post_tag_recv(priskv_ucx_worker *worker, void *buffer,
                                                 size_t count, ucp_tag_t tag, ucp_tag_t mask,
                                                 priskv_ucx_tag_recv_cb cb, void *cb_data)
{
    return priskv_ucx_post_tag_recv(worker, NULL, buffer, count, tag, mask, cb, cb_data);
}

ucs_status_ptr_t priskv_ucx_worker_post_flush(priskv_ucx_worker *worker, priskv_ucx_request_cb cb,
                                              void *cb_data)
{
    ucs_status_t status;
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_worker_post_flush: worker is NULL\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_worker_post_flush: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask =
                                     UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA,
                                 .cb.send = priskv_ucx_request_flush_cb_intl,
                                 .user_data = request};
    request->handle = ucp_worker_flush_nbx(worker->handle, &param);
    request->name = "flush";
    request->status = UCS_INPROGRESS;
    request->worker = worker;
    request->ep = NULL;
    request->cb = cb;
    request->cb_data = cb_data;

    priskv_log_debug("priskv_ucx_worker_post_flush: worker %p request %p posted\n", worker,
                     request);

    return priskv_ucx_request_progress(request, &param);
callback:
    if (cb) {
        cb(status, cb_data);
    }
    return NULL;
}

static void priskv_ucx_worker_drain_cb_intl(void *request, ucs_status_t status,
                                            const ucp_tag_recv_info_t * /* info */,
                                            void * /* arg */)
{
    // do nothing
}

static void priskv_ucx_worker_drain_tag_recv(priskv_ucx_worker *worker)
{
    if (ucs_unlikely(worker == NULL || worker->handle == NULL)) {
        return;
    }

    ucs_status_ptr_t status;
    ucp_tag_message_h message;
    ucp_tag_recv_info_t info;
    ucp_worker_h handle = worker->handle;
    ucp_request_param_t param = {.op_attr_mask =
                                     UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_DATATYPE,
                                 .cb = {.recv = priskv_ucx_worker_drain_cb_intl},
                                 .datatype = ucp_dt_make_contig(1)};

    while ((message = ucp_tag_probe_nb(handle, 0, 0, 1, &info)) != NULL) {
        priskv_log_debug("draining tag receive message, tag: 0x%lx, length: %lu\n", info.sender_tag,
                         info.length);

        char *buf = malloc(info.length);
        if (ucs_unlikely(buf == NULL)) {
            priskv_log_error("priskv_ucx_worker_drain_tag_recv: failed to malloc %lu bytes\n",
                             info.length);
            return;
        }

        status = ucp_tag_msg_recv_nbx(handle, buf, info.length, message, &param);

        if (status != NULL) {
            while (UCS_PTR_STATUS(status) == UCS_INPROGRESS) {
                priskv_ucx_worker_progress(worker);
            }
        }
        free(buf);
    }
}

int priskv_ucx_worker_progress(priskv_ucx_worker *worker)
{
    int ret = 0, in_progress = 0;
    ucs_status_t status;
    for (;;) {
        if ((in_progress = ucp_worker_progress(worker->handle)) != 0) {
            ret += in_progress;
            continue; // some progress happened but condition not met
        }

        // arm the worker and clean-up fd
        status = ucp_worker_arm(worker->handle);
        if (UCS_OK == status) {
            return ret;
        } else if (UCS_ERR_BUSY == status) {
            continue; // could not arm, need to progress more
        } else {
            return ret;
        }
    }
    return ret;
}

void priskv_ucx_worker_signal(priskv_ucx_worker *worker)
{
    ucp_worker_signal(worker->handle);
}

ucs_status_t priskv_ucx_worker_destroy(priskv_ucx_worker *worker)
{
    priskv_ucx_worker_drain_tag_recv(worker);
    if (worker->address) {
        ucp_worker_release_address(worker->handle, worker->address);
        worker->address = NULL;
    }
    ucp_worker_destroy(worker->handle);
    priskv_log_debug("priskv_ucx_worker_destroy: worker %p destroyed\n", worker);
    free(worker);
    return UCS_OK;
}

static inline struct addrinfo *priskv_ucx_get_addrinfo(const char *ip_or_hostname, uint16_t port)
{
    char port_str[6];
    struct addrinfo *result = NULL;
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_NUMERICSERV | AI_PASSIVE;

    int port_str_len = snprintf(port_str, sizeof(port_str), "%u", port);
    if (port_str_len < 0 || port_str_len > sizeof(port_str)) {
        priskv_log_error("priskv_ucx_get_addrinfo: invalid port %u\n", port);
        return NULL;
    }
    if (getaddrinfo(ip_or_hostname, port_str, &hints, &result)) {
        priskv_log_error("priskv_ucx_get_addrinfo: invalid IP or hostname\n");
        return NULL;
    }

    return result;
}

static void priskv_ucx_conn_cb_intl(ucp_conn_request_h conn_request, void *arg)
{
    priskv_ucx_conn_request *conn_req = arg;
    conn_req->handle = conn_request;
    conn_req->attr.field_mask =
        UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ADDR | UCP_CONN_REQUEST_ATTR_FIELD_CLIENT_ID;
    ucs_status_t status = ucp_conn_request_query(conn_request, &conn_req->attr);
    if (ucs_unlikely(status != UCS_OK)) {
        priskv_log_error("priskv_ucx_conn_cb_intl: failed to query conn_request %p, status %s\n",
                         conn_request, ucs_status_string(status));
    } else {
        priskv_inet_ntop((struct sockaddr *)&conn_req->attr.client_address, conn_req->peer_addr);
    }

    if (conn_req->cb != NULL) {
        priskv_log_debug(
            "priskv_ucx_conn_cb_intl: conn_request id %d peer_addr %s cb %p cb_data %p\n",
            conn_req->attr.client_id, conn_req->peer_addr, conn_req->cb, conn_req->cb_data);
        conn_req->cb(conn_req, conn_req->cb_data);
    }
}

priskv_ucx_listener *priskv_ucx_listener_create(priskv_ucx_worker *worker,
                                                const char *ip_or_hostname, uint16_t port,
                                                priskv_ucx_conn_cb conn_cb, void *conn_cb_data)
{
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_listener_create: worker is NULL\n");
        return NULL;
    }

    struct addrinfo *info = priskv_ucx_get_addrinfo(ip_or_hostname, port);
    if (ucs_unlikely(info == NULL)) {
        priskv_log_error("priskv_ucx_listener_create: failed to get addrinfo\n");
        return NULL;
    }

    priskv_ucx_conn_request *conn_req = malloc(sizeof(priskv_ucx_conn_request));
    if (ucs_unlikely(conn_req == NULL)) {
        priskv_log_error("priskv_ucx_listener_create: failed to malloc conn_req\n");
        freeaddrinfo(info);
        return NULL;
    }

    conn_req->handle = NULL;
    conn_req->cb = conn_cb;
    conn_req->cb_data = conn_cb_data;

    ucp_listener_params_t params = {
        .field_mask = UCP_LISTENER_PARAM_FIELD_SOCK_ADDR | UCP_LISTENER_PARAM_FIELD_CONN_HANDLER,
        .sockaddr = {.addr = info->ai_addr, .addrlen = info->ai_addrlen},
        .conn_handler = {.cb = priskv_ucx_conn_cb_intl, .arg = conn_req}};

    freeaddrinfo(info);

    priskv_ucx_listener *listener = malloc(sizeof(priskv_ucx_listener));
    if (ucs_unlikely(listener == NULL)) {
        priskv_log_error("priskv_ucx_listener_create: failed to malloc listener\n");
        free(conn_req);
        return NULL;
    }

    listener->conn_request = conn_req;
    ucs_status_t status = ucp_listener_create(worker->handle, &params, &listener->handle);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_listener_create: failed to create listener",
        {
            free(conn_req);
            free(listener);
        },
        NULL);

    ucp_listener_attr_t attr = {.field_mask = UCP_LISTENER_ATTR_FIELD_SOCKADDR};
    status = ucp_listener_query(listener->handle, &attr);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_listener_create: failed to query listener",
        {
            ucp_listener_destroy(listener->handle);
            free(conn_req);
            free(listener);
        },
        NULL);

    priskv_inet_ntop((struct sockaddr *)&attr.sockaddr, listener->addr);

    listener->worker = worker;

    priskv_log_debug("priskv_ucx_listener_create: listener %s created\n", listener->addr);

    return listener;
}

priskv_ucx_ep *priskv_ucx_listener_accept(priskv_ucx_listener *listener,
                                          priskv_ucx_conn_request *conn_req,
                                          priskv_ucx_ep_close_cb close_cb, void *close_cb_data)
{
    if (ucs_unlikely(listener == NULL)) {
        priskv_log_error("priskv_ucx_listener_accept: listener is NULL\n");
        return NULL;
    }

    if (ucs_unlikely(conn_req == NULL)) {
        priskv_log_error("priskv_ucx_listener_accept: conn_req is NULL\n");
        return NULL;
    }

    if (ucs_unlikely(conn_req->handle == NULL)) {
        priskv_log_error("priskv_ucx_listener_accept: conn_req handle is NULL\n");
        return NULL;
    }

    ucp_ep_params_t params = {.field_mask =
                                  UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_CONN_REQUEST,
                              .flags = UCP_EP_PARAMS_FLAGS_NO_LOOPBACK,
                              .conn_request = conn_req->handle};

    priskv_ucx_worker *worker = priskv_ucx_worker_create(listener->worker->context, 0);
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_listener_accept: failed to create worker\n");
        return NULL;
    }
    return priskv_ucx_ep_create(worker, &params, close_cb, close_cb_data, 1);
}

ucs_status_t priskv_ucx_listener_reject(priskv_ucx_listener *listener,
                                        priskv_ucx_conn_request *conn_req)
{
    if (ucs_unlikely(listener == NULL)) {
        priskv_log_error("priskv_ucx_listener_reject: listener is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    if (ucs_unlikely(conn_req == NULL)) {
        priskv_log_error("priskv_ucx_listener_reject: conn_req is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    if (ucs_unlikely(conn_req->handle == NULL)) {
        priskv_log_error("priskv_ucx_listener_reject: conn_req handle is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    return ucp_listener_reject(listener->handle, conn_req->handle);
}

ucs_status_t priskv_ucx_listener_destroy(priskv_ucx_listener *listener)
{
    if (ucs_unlikely(listener == NULL)) {
        priskv_log_error("priskv_ucx_listener_destroy: listener is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    ucp_listener_destroy(listener->handle);

    priskv_ucx_worker_progress(listener->worker);

    priskv_log_debug("priskv_ucx_listener_destroy: listener %s destroyed\n", listener->addr);

    if (listener->conn_request) {
        free(listener->conn_request);
        listener->conn_request = NULL;
    }

    free(listener);

    return UCS_OK;
}

static void priskv_ucx_ep_error_cb_intl(void *arg, ucp_ep_h handle, ucs_status_t status)
{
    priskv_ucx_ep *ep = (priskv_ucx_ep *)arg;
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_error_cb_intl: ep is NULL\n");
        return;
    }

    if (atomic_exchange(&ep->closing, 1) == 1) {
        // ep is closing, ignore the error
        return;
    }

    ep->status = status;

    if (ep->close_cb != NULL) {
        priskv_log_debug("priskv_ucx_ep_error_cb_intl: call close_cb %p\n", ep->close_cb);
        ep->close_cb(status, ep->close_cb_data);
        ep->close_cb = NULL;
    }

    // Connection reset and timeout often represent just a normal remote
    // endpoint disconnect, log only in debug mode.
    if (status == UCS_ERR_CONNECTION_RESET || status == UCS_ERR_ENDPOINT_TIMEOUT) {
        priskv_log_debug("priskv_ucx_ep_error_cb_intl: ep %p error %s\n", ep,
                         ucs_status_string(status));
    } else {
        priskv_log_error("priskv_ucx_ep_error_cb_intl: ep %p error %s\n", ep,
                         ucs_status_string(status));
    }
}

static priskv_ucx_ep *priskv_ucx_ep_create(priskv_ucx_worker *worker, ucp_ep_params_t *params,
                                           priskv_ucx_ep_close_cb close_cb, void *close_cb_data,
                                           uint8_t paired_worker_ep)
{
    priskv_ucx_ep *ep = malloc(sizeof(priskv_ucx_ep));
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_create: failed to malloc ep\n");
        return NULL;
    }

    params->field_mask |= UCP_EP_PARAM_FIELD_ERR_HANDLING_MODE | UCP_EP_PARAM_FIELD_ERR_HANDLER;
    // sm does not support peer error handling mode
    // params->err_mode = UCP_ERR_HANDLING_MODE_PEER;
    params->err_mode = UCP_ERR_HANDLING_MODE_NONE;
    params->err_handler.cb = priskv_ucx_ep_error_cb_intl;
    params->err_handler.arg = ep;
    ucs_status_t status = ucp_ep_create(worker->handle, params, &ep->handle);
    PRISKV_UCX_RETURN_IF_ERROR(
        status, "priskv_ucx_ep_create: failed to create ep", { free(ep); }, NULL);
    atomic_init(&ep->closing, 0);
    ep->worker = worker;
    ep->close_cb = close_cb;
    ep->close_cb_data = close_cb_data;
    ep->paired_worker_ep = paired_worker_ep;
    ep->status = UCS_INPROGRESS;

    ucp_ep_attr_t attr = {.field_mask = UCP_EP_ATTR_FIELD_NAME};
    status = ucp_ep_query(ep->handle, &attr);
    if (ucs_unlikely(status != UCS_OK)) {
        priskv_log_error("priskv_ucx_ep_create: failed to query ep name, status: %s\n",
                         ucs_status_string(status));
        strcpy(ep->name, "nil");
    } else {
        strcpy(ep->name, attr.name);
    }

    return ep;
}

priskv_ucx_ep *priskv_ucx_ep_create_from_worker_addr(priskv_ucx_worker *worker, ucp_address_t *addr,
                                                     priskv_ucx_ep_close_cb close_cb,
                                                     void *close_cb_data)
{
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_ep_create_from_worker_addr: "
                         "worker is NULL\n");
        return NULL;
    }

    if (ucs_unlikely(addr == NULL)) {
        priskv_log_error("priskv_ucx_ep_create_from_worker_addr: addr is NULL\n");
        return NULL;
    }

    ucp_ep_params_t params = {
        .field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_REMOTE_ADDRESS,
        .flags = UCP_EP_PARAMS_FLAGS_SEND_CLIENT_ID, // send worker's client id
        .address = addr,
    };

    return priskv_ucx_ep_create(worker, &params, close_cb, close_cb_data, 0);
}

priskv_ucx_ep *priskv_ucx_ep_create_from_addr(priskv_ucx_worker *worker, const char *ip_or_hostname,
                                              uint16_t port, priskv_ucx_ep_close_cb close_cb,
                                              void *close_cb_data)
{
    if (ucs_unlikely(worker == NULL)) {
        priskv_log_error("priskv_ucx_ep_create_from_hostname: worker is NULL\n");
        return NULL;
    }

    struct addrinfo *info = priskv_ucx_get_addrinfo(ip_or_hostname, port);
    if (ucs_unlikely(info == NULL)) {
        priskv_log_error("priskv_ucx_ep_create_from_hostname: failed to get addrinfo\n");
        return NULL;
    }

    ucp_ep_params_t params = {.field_mask = UCP_EP_PARAM_FIELD_FLAGS | UCP_EP_PARAM_FIELD_SOCK_ADDR,
                              .flags =
                                  UCP_EP_PARAMS_FLAGS_CLIENT_SERVER |
                                  UCP_EP_PARAMS_FLAGS_SEND_CLIENT_ID, // send worker's client id
                              .sockaddr = {.addrlen = info->ai_addrlen, .addr = info->ai_addr}};

    priskv_ucx_ep *ep = priskv_ucx_ep_create(worker, &params, close_cb, close_cb_data, 0);
    freeaddrinfo(info);
    return ep;
}

ucs_status_ptr_t priskv_ucx_ep_post_tag_recv(priskv_ucx_ep *ep, void *buffer, size_t count,
                                             ucp_tag_t tag, ucp_tag_t mask,
                                             priskv_ucx_tag_recv_cb cb, void *cb_data)
{
    return priskv_ucx_post_tag_recv(ep->worker, ep, buffer, count, tag, mask, cb, cb_data);
}

ucs_status_ptr_t priskv_ucx_ep_post_tag_send(priskv_ucx_ep *ep, void *buffer, size_t count,
                                             ucp_tag_t tag, priskv_ucx_request_cb cb, void *cb_data)
{
    ucs_status_t status;
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_tag_send: ep is NULL\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    if (ucs_unlikely(ep->status != UCS_INPROGRESS)) {
        priskv_log_error("priskv_ucx_ep_post_tag_send: ep is closed\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_tag_send: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                 UCP_OP_ATTR_FIELD_DATATYPE |
                                                 UCP_OP_ATTR_FIELD_USER_DATA,
                                 .datatype = ucp_dt_make_contig(1),
                                 .cb.send = priskv_ucx_request_tag_send_cb_intl,
                                 .user_data = request};
    request->handle = ucp_tag_send_nbx(ep->handle, buffer, count, tag, &param);
    request->name = "tag_send";
    request->status = UCS_INPROGRESS;
    request->worker = ep->worker;
    request->ep = ep;
    request->payload.buffer = buffer;
    request->payload.length = count;
    request->payload.tag_send.tag = tag;
    request->cb = cb;
    request->cb_data = cb_data;

    priskv_log_debug(
        "priskv_ucx_ep_post_tag_send: ep %p request %p [buf=%p, len=%zu, tag=%lu] posted\n", ep,
        request, buffer, count, tag);

    return priskv_ucx_request_progress(request, &param);
callback:
    if (cb) {
        cb(status, cb_data);
    }
    return NULL;
}

ucs_status_ptr_t priskv_ucx_ep_post_put(priskv_ucx_ep *ep, void *buffer, size_t count,
                                        priskv_ucx_rkey *rkey, uint64_t raddr,
                                        priskv_ucx_request_cb cb, void *cb_data)
{
    ucs_status_t status;
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_put: ep is NULL\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    if (ucs_unlikely(ep->status != UCS_INPROGRESS)) {
        priskv_log_error("priskv_ucx_ep_post_put: ep is closed\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_put: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                 UCP_OP_ATTR_FIELD_DATATYPE |
                                                 UCP_OP_ATTR_FIELD_USER_DATA,
                                 .datatype = ucp_dt_make_contig(1),
                                 .cb.send = priskv_ucx_request_put_cb_intl,
                                 .user_data = request};
    request->handle = ucp_put_nbx(ep->handle, buffer, count, raddr, rkey->handle, &param);
    request->name = "put";
    request->status = UCS_INPROGRESS;
    request->worker = ep->worker;
    request->ep = ep;
    request->payload.buffer = buffer;
    request->payload.length = count;
    request->payload.rma.raddr = raddr;
    request->cb = cb;
    request->cb_data = cb_data;

    priskv_log_debug(
        "priskv_ucx_ep_post_put: ep %p request %p [buf=%p, len=%zu, raddr=%lu] posted\n", ep,
        request, buffer, count, raddr);

    return priskv_ucx_request_progress(request, &param);
callback:
    if (cb) {
        cb(status, cb_data);
    }
    return NULL;
}

ucs_status_ptr_t priskv_ucx_ep_post_get(priskv_ucx_ep *ep, void *buffer, size_t count,
                                        priskv_ucx_rkey *rkey, uint64_t raddr,
                                        priskv_ucx_request_cb cb, void *cb_data)
{
    ucs_status_t status;
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_get: ep is NULL\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    if (ucs_unlikely(ep->status != UCS_INPROGRESS)) {
        priskv_log_error("priskv_ucx_ep_post_get: ep is closed\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_get: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask = UCP_OP_ATTR_FIELD_CALLBACK |
                                                 UCP_OP_ATTR_FIELD_DATATYPE |
                                                 UCP_OP_ATTR_FIELD_USER_DATA,
                                 .datatype = ucp_dt_make_contig(1),
                                 .cb.send = priskv_ucx_request_get_cb_intl,
                                 .user_data = request};
    request->handle = ucp_get_nbx(ep->handle, buffer, count, raddr, rkey->handle, &param);
    request->name = "get";
    request->status = UCS_INPROGRESS;
    request->worker = ep->worker;
    request->ep = ep;
    request->payload.buffer = buffer;
    request->payload.length = count;
    request->payload.rma.raddr = raddr;
    request->cb = cb;
    request->cb_data = cb_data;

    priskv_log_debug(
        "priskv_ucx_ep_post_get: ep %p request %p [buf=%p, len=%zu, raddr=%lu] posted\n", ep,
        request, buffer, count, raddr);

    return priskv_ucx_request_progress(request, &param);
callback:
    if (cb) {
        cb(status, cb_data);
    }
    return NULL;
}

ucs_status_ptr_t priskv_ucx_ep_post_flush(priskv_ucx_ep *ep, priskv_ucx_request_cb cb,
                                          void *cb_data)
{
    ucs_status_t status;
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_flush: ep is NULL\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    if (ucs_unlikely(ep->status != UCS_INPROGRESS)) {
        priskv_log_error("priskv_ucx_ep_post_flush: ep is closed\n");
        status = UCS_ERR_INVALID_PARAM;
        goto callback;
    }

    priskv_ucx_request *request = malloc(sizeof(priskv_ucx_request));
    if (ucs_unlikely(request == NULL)) {
        priskv_log_error("priskv_ucx_ep_post_flush: failed to malloc request\n");
        status = UCS_ERR_NO_MEMORY;
        goto callback;
    }

    priskv_ucx_request_init(request);

    ucp_request_param_t param = {.op_attr_mask =
                                     UCP_OP_ATTR_FIELD_CALLBACK | UCP_OP_ATTR_FIELD_USER_DATA,
                                 .cb.send = priskv_ucx_request_flush_cb_intl,
                                 .user_data = request};
    request->handle = ucp_ep_flush_nbx(ep->handle, &param);
    request->name = "flush";
    request->status = UCS_INPROGRESS;
    request->worker = ep->worker;
    request->ep = ep;
    request->cb = cb;
    request->cb_data = cb_data;

    priskv_log_debug("priskv_ucx_ep_post_flush: ep %p request %p posted\n", ep, request);

    return priskv_ucx_request_progress(request, &param);
callback:
    if (cb) {
        cb(status, cb_data);
    }
    return NULL;
}

struct sockaddr_storage *priskv_ucx_ep_get_local_addr(priskv_ucx_ep *ep)
{
    ep->attr.field_mask = UCP_EP_ATTR_FIELD_LOCAL_SOCKADDR;
    ucs_status_t status = ucp_ep_query(ep->handle, &ep->attr);
    if (ucs_unlikely(status != UCS_OK)) {
        priskv_log_error(
            "priskv_ucx_ep_get_local_addr: failed to query ep local addr, status: %s\n",
            ucs_status_string(status));
        return NULL;
    } else {
        return &ep->attr.local_sockaddr;
    }
}

struct sockaddr_storage *priskv_ucx_ep_get_peer_addr(priskv_ucx_ep *ep)
{
    ep->attr.field_mask = UCP_EP_ATTR_FIELD_REMOTE_SOCKADDR;
    ucs_status_t status = ucp_ep_query(ep->handle, &ep->attr);
    if (ucs_unlikely(status != UCS_OK)) {
        priskv_log_error("priskv_ucx_ep_get_peer_addr: failed to query ep peer addr, status: %s\n",
                         ucs_status_string(status));
        return NULL;
    } else {
        return &ep->attr.remote_sockaddr;
    }
}

ucs_status_t priskv_ucx_ep_destroy(priskv_ucx_ep *ep)
{
    if (atomic_exchange(&ep->closing, 1) || ep->handle == NULL) {
        return UCS_OK;
    }

    ucp_request_param_t param = {.op_attr_mask = UCP_OP_ATTR_FIELD_FLAGS,
                                 .flags = UCP_EP_CLOSE_FLAG_FORCE};

    ucs_status_ptr_t status = ucp_ep_close_nbx(ep->handle, &param);
    if (UCS_PTR_IS_PTR(status)) {
        // wait for close request to complete
        ucs_status_t s;
        while ((s = ucp_request_check_status(status)) == UCS_INPROGRESS) {
            priskv_ucx_worker_progress(ep->worker);
        }
        ep->status = s;
    } else if (UCS_PTR_STATUS(status) != UCS_OK) {
        priskv_log_error("priskv_ucx_ep_destroy: failed to close ep %p, status %s\n", ep,
                         ucs_status_string(UCS_PTR_STATUS(status)));
    }

    if (UCS_PTR_IS_PTR(status)) {
        ucp_request_free(status);
    }

    if (ep->close_cb != NULL) {
        priskv_log_debug("priskv_ucx_ep_destroy: call close_cb %p\n", ep->close_cb);
        ep->close_cb(ep->status, ep->close_cb_data);
        ep->close_cb = NULL;
    }

    priskv_ucx_worker *worker = ep->worker;
    uint8_t paired_worker_ep = ep->paired_worker_ep;
    free(ep);

    if (paired_worker_ep) {
        return priskv_ucx_worker_destroy(worker);
    }

    return UCS_OK;
}

priskv_ucx_rkey *priskv_ucx_rkey_create(priskv_ucx_ep *ep, const void *packed_rkey)
{
    if (ucs_unlikely(ep == NULL)) {
        priskv_log_error("priskv_ucx_rkey_create: ep is NULL\n");
        return NULL;
    }

    priskv_ucx_rkey *rkey = malloc(sizeof(priskv_ucx_rkey));
    if (ucs_unlikely(rkey == NULL)) {
        priskv_log_error("priskv_ucx_rkey_create: failed to malloc rkey\n");
        return NULL;
    }

    rkey->ep = ep;
    ucs_status_t status = ucp_ep_rkey_unpack(ep->handle, packed_rkey, &rkey->handle);
    if (ucs_unlikely(status != UCS_OK)) {
        priskv_log_error("priskv_ucx_rkey_create: failed to unpack rkey\n");
        free(rkey);
        return NULL;
    }

    return rkey;
}

ucs_status_t priskv_ucx_rkey_destroy(priskv_ucx_rkey *rkey)
{
    if (ucs_unlikely(rkey == NULL)) {
        priskv_log_error("priskv_ucx_rkey_destroy: rkey is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    if (ucs_unlikely(rkey->handle == NULL)) {
        priskv_log_error("priskv_ucx_rkey_destroy: rkey handle is NULL\n");
        return UCS_ERR_INVALID_PARAM;
    }

    ucp_rkey_destroy(rkey->handle);

    free(rkey);

    return UCS_OK;
}
