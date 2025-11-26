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

#ifndef __PRISKV_UCX__
#define __PRISKV_UCX__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdatomic.h>
#include <ucp/api/ucp.h>

#include "priskv-utils.h"
#include "uthash.h"

// forward declaration
typedef struct priskv_ucx_worker priskv_ucx_worker;
typedef struct priskv_ucx_ep priskv_ucx_ep;
typedef struct priskv_ucx_conn_request priskv_ucx_conn_request;

typedef struct priskv_ucx_context {
    ucp_context_h handle;
    uint8_t busy_polling;
} priskv_ucx_context;

typedef struct priskv_ucx_payload {
    void *buffer;
    size_t length;
    union {
        struct {
            ucp_tag_t tag;
        } tag_send;
        struct {
            ucp_tag_t tag;
            ucp_tag_t mask;
        } tag_recv;
        struct {
            uint64_t raddr;
        } rma;
    };
} priskv_ucx_payload;

typedef struct priskv_ucx_rkey {
    priskv_ucx_ep *ep;
    ucp_rkey_h handle;
} priskv_ucx_rkey;

typedef struct priskv_ucx_memh {
    priskv_ucx_context *context;
    ucp_mem_h handle;
    ucs_memory_type_t type;
    uint64_t addr;
    size_t len;
    void *rkey_buffer;
    size_t rkey_length;
} priskv_ucx_memh;

typedef void (*priskv_ucx_request_cb)(ucs_status_t status, void *arg);
typedef void (*priskv_ucx_tag_recv_cb)(ucs_status_t status, ucp_tag_t sender_tag, size_t length,
                                       void *arg);
typedef struct priskv_ucx_request {
    const char *name;
    void *key;
    void *handle;
    ucs_status_t status;
    priskv_ucx_worker *worker;
    priskv_ucx_ep *ep;
    priskv_ucx_payload payload;
    union {
        priskv_ucx_request_cb cb;
        priskv_ucx_tag_recv_cb tag_recv_cb;
    };
    void *cb_data;
    UT_hash_handle hh;
} priskv_ucx_request;

typedef struct priskv_ucx_worker {
    priskv_ucx_context *context;
    ucp_worker_h handle;
    int efd;
    uint64_t client_id;
    ucp_address_t *address;
    uint32_t address_len;
} priskv_ucx_worker;

typedef struct priskv_ucx_listener {
    priskv_ucx_worker *worker;
    ucp_listener_h handle;
    priskv_ucx_conn_request *conn_request;
    char addr[PRISKV_ADDR_LEN];
} priskv_ucx_listener;

typedef void (*priskv_ucx_conn_cb)(priskv_ucx_conn_request *conn_request, void *arg);
typedef struct priskv_ucx_conn_request {
    ucp_conn_request_h handle;
    ucp_conn_request_attr_t attr;
    priskv_ucx_conn_cb cb;
    void *cb_data;
    char peer_addr[PRISKV_ADDR_LEN];
} priskv_ucx_conn_request;

typedef void (*priskv_ucx_ep_close_cb)(ucs_status_t status, void *arg);
typedef struct priskv_ucx_ep {
    char name[UCP_ENTITY_NAME_MAX];
    priskv_ucx_worker *worker;
    ucp_ep_h handle;
    ucs_status_t status;
    atomic_uint closing;
    priskv_ucx_ep_close_cb close_cb;
    void *close_cb_data;
    uint8_t paired_worker_ep;
    ucp_ep_attr_t attr;
} priskv_ucx_ep;

/**
 * @brief Check if a UCX operation is successful, and handle errors if not.
 *
 * @param STATUS The status code to check.
 * @param MSG The error message to log if the status is not UCS_OK or UCS_INPROGRESS.
 * @param CLEANUP The cleanup code to execute if the status is not UCS_OK or UCS_INPROGRESS.
 * @param RET The return value to use if the status is not UCS_OK or UCS_INPROGRESS.
 */
#define PRISKV_UCX_RETURN_IF_ERROR(STATUS, MSG, CLEANUP, RET)                                      \
    {                                                                                              \
        switch (STATUS) {                                                                          \
        case UCS_OK:                                                                               \
            break;                                                                                 \
        case UCS_INPROGRESS:                                                                       \
            break;                                                                                 \
        default:                                                                                   \
            const char *status_str = ucs_status_string(STATUS);                                    \
            priskv_log_error(MSG ", status: %s\n", status_str);                                    \
            CLEANUP;                                                                               \
            return RET;                                                                            \
        }                                                                                          \
    }

/**
 * @brief Check if CUDA is disabled by UCX_TLS.
 *
 * @param tls The UCX_TLS environment variable value.
 * @return int 1 if CUDA is disabled, 0 otherwise.
 */
int priskv_ucx_tls_has_cuda_disabled(const char *tls);

/**
 * @brief Initialize a UCX context.
 *
 * @param busy_polling Whether to enable busy polling.
 * @return priskv_ucx_context* The initialized UCX context.
 */
priskv_ucx_context *priskv_ucx_context_init(uint8_t busy_polling);

/**
 * @brief Check if a UCX context has CUDA support.
 *
 * @param context The UCX context.
 * @return int 1 if CUDA support is enabled, 0 otherwise.
 */
int priskv_ucx_context_has_cuda_support(priskv_ucx_context *context);

/**
 * @brief Map or allocate memory for zero-copy operations.
 *
 * @param context The UCX context.
 * @param buffer The memory buffer to mmap.
 * @param count The size of the memory buffer in bytes.
 * @param type The memory type.
 * @return priskv_ucx_memh* The created UCX memory handle.
 */
priskv_ucx_memh *priskv_ucx_mmap(priskv_ucx_context *context, void *buffer, size_t count,
                                 ucs_memory_type_t type);

/**
 * @brief Unmap memory segment.
 *
 * @param memh The UCX memory handle.
 * @return ucs_status_t The status of the unmap operation.
 */
ucs_status_t priskv_ucx_munmap(priskv_ucx_memh *memh);

/**
 * @brief Cancel a UCX request.
 *
 * @param request The UCX request to cancel.
 * @return ucs_status_t The status of the request cancellation.
 */
ucs_status_t priskv_ucx_request_cancel(priskv_ucx_request *request);

/**
 * @brief Create a UCX worker.
 *
 * @param context The UCX context.
 * @param client_id The client ID.
 * @return priskv_ucx_worker* The created UCX worker.
 */
priskv_ucx_worker *priskv_ucx_worker_create(priskv_ucx_context *context, uint64_t client_id);

/**
 * @brief Post a non-blocking tag receive request.
 *
 * @param worker The UCX worker.
 * @param buffer The buffer to store the received data.
 * @param count The number of bytes to receive.
 * @param tag The tag to match.
 * @param mask The mask to apply to the tag.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_worker_post_tag_recv(priskv_ucx_worker *worker, void *buffer,
                                                 size_t count, ucp_tag_t tag, ucp_tag_t mask,
                                                 priskv_ucx_tag_recv_cb cb, void *cb_data);

/**
 * @brief Post a non-blocking flush request.
 *
 * @param worker The UCX worker.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_worker_post_flush(priskv_ucx_worker *worker, priskv_ucx_request_cb cb,
                                              void *cb_data);

/**
 * @brief Progress a UCX worker.
 *
 * @param worker The UCX worker to progress.
 * @return int 0 on success, other values on error.
 */
int priskv_ucx_worker_progress(priskv_ucx_worker *worker);

/**
 * @brief Signal a UCX worker.
 *
 * @param worker The UCX worker to signal.
 */
void priskv_ucx_worker_signal(priskv_ucx_worker *worker);

/**
 * @brief Destroy a UCX worker.
 *
 * @param worker The UCX worker to destroy.
 * @return ucs_status_t The status of the destroy operation.
 */
ucs_status_t priskv_ucx_worker_destroy(priskv_ucx_worker *worker);

/**
 * @brief Create a UCX listener.
 *
 * @param worker The UCX worker.
 * @param ip_or_hostname The IP address or hostname to listen on.
 * @param port The port to listen on.
 * @param conn_cb The callback function to invoke when a connection is accepted.
 * @param conn_cb_data User data to pass to the callback.
 * @return priskv_ucx_listener* The created UCX listener.
 */
priskv_ucx_listener *priskv_ucx_listener_create(priskv_ucx_worker *worker,
                                                const char *ip_or_hostname, uint16_t port,
                                                priskv_ucx_conn_cb conn_cb, void *conn_cb_data);

/**
 * @brief Accept a connection request on a UCX listener.
 *
 * @param listener The UCX listener.
 * @param conn_req The connection request handle.
 * @param close_cb The callback function to invoke when the endpoint is closed.
 * @param close_cb_data User data to pass to the callback.
 * @return priskv_ucx_ep* The created UCX endpoint.
 */
priskv_ucx_ep *priskv_ucx_listener_accept(priskv_ucx_listener *listener,
                                          priskv_ucx_conn_request *conn_req,
                                          priskv_ucx_ep_close_cb close_cb, void *close_cb_data);

/**
 * @brief Reject a connection request on a UCX listener.
 *
 * @param listener The UCX listener.
 * @param conn_req The connection request handle.
 * @return ucs_status_t The status of the reject operation.
 */
ucs_status_t priskv_ucx_listener_reject(priskv_ucx_listener *listener,
                                        priskv_ucx_conn_request *conn_req);

/**
 * @brief Destroy a UCX listener.
 *
 * @param listener The UCX listener to destroy.
 * @return ucs_status_t The status of the destroy operation.
 */
ucs_status_t priskv_ucx_listener_destroy(priskv_ucx_listener *listener);

/**
 * @brief Create a UCX endpoint from a worker address.
 *
 * @param worker The UCX worker.
 * @param addr The worker address.
 * @param close_cb The callback function to invoke when the endpoint is closed.
 * @param close_cb_data User data to pass to the callback.
 * @return priskv_ucx_ep* The created UCX endpoint.
 */
priskv_ucx_ep *priskv_ucx_ep_create_from_worker_addr(priskv_ucx_worker *worker, ucp_address_t *addr,
                                                     priskv_ucx_ep_close_cb close_cb,
                                                     void *close_cb_data);
/**
 * @brief Create a UCX endpoint from an IP address or hostname.
 *
 * @param worker The UCX worker.
 * @param ip_or_hostname The IP address or hostname to connect to.
 * @param port The port to connect to.
 * @param close_cb The callback function to invoke when the endpoint is closed.
 * @param close_cb_data User data to pass to the callback.
 * @return priskv_ucx_ep* The created UCX endpoint.
 */
priskv_ucx_ep *priskv_ucx_ep_create_from_addr(priskv_ucx_worker *worker, const char *ip_or_hostname,
                                              uint16_t port, priskv_ucx_ep_close_cb close_cb,
                                              void *close_cb_data);

/**
 * @brief Post a non-blocking tag receive request.
 *
 * @param ep The UCX endpoint.
 * @param buffer The buffer to store the received data.
 * @param count The number of bytes to receive.
 * @param tag The tag to match.
 * @param mask The mask to apply to the tag.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_ep_post_tag_recv(priskv_ucx_ep *ep, void *buffer, size_t count,
                                             ucp_tag_t tag, ucp_tag_t mask,
                                             priskv_ucx_tag_recv_cb cb, void *cb_data);

/**
 * @brief Post a non-blocking tag send operation.
 *
 * @param ep The UCX endpoint.
 * @param buffer The buffer to send.
 * @param count The number of bytes to send.
 * @param tag The tag to send.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_ep_post_tag_send(priskv_ucx_ep *ep, void *buffer, size_t count,
                                             ucp_tag_t tag, priskv_ucx_request_cb cb,
                                             void *cb_data);

/**
 * @brief Post a non-blocking put operation.
 *
 * @param ep The UCX endpoint.
 * @param buffer The buffer to send.
 * @param count The number of bytes to send.
 * @param rkey The remote key to use.
 * @param raddr The remote address to write to.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_ep_post_put(priskv_ucx_ep *ep, void *buffer, size_t count,
                                        priskv_ucx_rkey *rkey, uint64_t raddr,
                                        priskv_ucx_request_cb cb, void *cb_data);

/**
 * @brief Post a non-blocking get operation.
 *
 * @param ep The UCX endpoint.
 * @param buffer The buffer to receive.
 * @param count The number of bytes to receive.
 * @param rkey The remote key to use.
 * @param raddr The remote address to read from.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_ep_post_get(priskv_ucx_ep *ep, void *buffer, size_t count,
                                        priskv_ucx_rkey *rkey, uint64_t raddr,
                                        priskv_ucx_request_cb cb, void *cb_data);

/**
 * @brief Post a non-blocking flush operation.
 *
 * @param ep The UCX endpoint.
 * @param cb The callback function to invoke when the operation completes.
 * @param cb_data User data to pass to the callback.
 * @return ucs_status_ptr_t The status or pointer to the request handle.
 */
ucs_status_ptr_t priskv_ucx_ep_post_flush(priskv_ucx_ep *ep, priskv_ucx_request_cb cb,
                                          void *cb_data);

/**
 * @brief Get the local address of a UCX endpoint.
 *
 * @param ep The UCX endpoint.
 * @return struct sockaddr_storage* The local address of the endpoint.
 */
struct sockaddr_storage *priskv_ucx_ep_get_local_addr(priskv_ucx_ep *ep);

/**
 * @brief Get the peer address of a UCX endpoint.
 *
 * @param ep The UCX endpoint.
 * @return struct sockaddr_storage* The peer address of the endpoint.
 */
struct sockaddr_storage *priskv_ucx_ep_get_peer_addr(priskv_ucx_ep *ep);

/**
 * @brief Destroy a UCX endpoint.
 *
 * @param ep The UCX endpoint to destroy.
 * @return ucs_status_t The status of the destroy operation.
 */
ucs_status_t priskv_ucx_ep_destroy(priskv_ucx_ep *ep);

/**
 * @brief Create a UCX remote key from a packed remote key.
 *
 * @param ep The UCX endpoint.
 * @param packed_rkey The packed remote key to unpack.
 * @return priskv_ucx_rkey* The unpacked remote key.
 */
priskv_ucx_rkey *priskv_ucx_rkey_create(priskv_ucx_ep *ep, const void *packed_rkey);

/**
 * @brief Destroy a UCX remote key.
 *
 * @param rkey The UCX remote key to destroy.
 * @return ucs_status_t The status of the destroy operation.
 */
ucs_status_t priskv_ucx_rkey_destroy(priskv_ucx_rkey *rkey);

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_UCX__ */
