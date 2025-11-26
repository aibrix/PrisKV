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

#include "transport.h"

#include <inttypes.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/mman.h>

#include <ucs/sys/string.h>
#include <ucs/sys/sock.h>

#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "priskv-threads.h"
#include "priskv-event.h"
#include "priskv-ucx.h"
#include "transport.h"
#include "uthash.h"

#define PRISKV_UCX_DEFAULT_INFLIGHT_COMMAND 128

static priskv_ucx_context *g_ucx_ctx = NULL;

typedef struct priskv_ucx_conn_resp {
    priskv_transport_conn *conn;
    priskv_response *resp;
} priskv_ucx_conn_resp;

static int priskv_ucx_req_cb_intl(void *arg);
static inline void priskv_ucx_req_complete(priskv_transport_conn *conn);
static inline void priskv_ucx_req_free(priskv_transport_req *ucx_req);
static inline priskv_transport_req *
priskv_ucx_req_new(priskv_client *client, priskv_transport_conn *conn, uint64_t request_id,
                   const char *key, uint16_t keylen, priskv_sgl *sgl, uint16_t nsgl,
                   uint64_t timeout, priskv_req_command cmd, priskv_generic_cb usercb);
static inline void priskv_ucx_req_reset(priskv_transport_req *ucx_req);
static inline void priskv_ucx_req_done(priskv_transport_conn *conn, priskv_transport_req *ucx_req);
static int priskv_ucx_recv_resp(priskv_transport_conn *conn, priskv_response *resp);
static void priskv_ucx_req_delay_send(priskv_transport_conn *conn);
static int priskv_ucx_send_req(void *arg);

static int priskv_ucx_init(void)
{
    g_ucx_ctx = priskv_ucx_context_init(0);
    if (g_ucx_ctx == NULL) {
        priskv_log_error("ucx context init failed\n");
        return -1;
    }

    return 0;
}

static int priskv_ucx_mem_new(priskv_transport_conn *conn, priskv_transport_mem *rmem,
                              const char *name, uint32_t size, bool remote_write)
{
    uint32_t page_size = getpagesize();
    uint8_t *buf;
    int ret;

    size = ALIGN_UP(size, page_size);
    buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf == MAP_FAILED) {
        priskv_log_error("UCX: failed to allocate %s buffer: %m\n", name);
        ret = -ENOMEM;
        goto error;
    }

    rmem->memh = priskv_ucx_mmap(g_ucx_ctx, buf, size, UCS_MEMORY_TYPE_HOST);
    if (!rmem->memh) {
        priskv_log_error("UCX: failed to reg MR for %s buffer: %m\n", name);
        ret = -errno;
        goto free_mem;
    }

    strncpy(rmem->name, name, PRISKV_TRANSPORT_MEM_NAME_LEN - 1);
    rmem->buf = buf;
    rmem->buf_size = size;

    priskv_log_info("UCX: new rmem %s, size %d\n", name, size);
    priskv_log_debug("UCX: new rmem %s, buf %p\n", name, buf);
    return 0;

free_mem:
    munmap(rmem->buf, rmem->buf_size);

error:
    memset(rmem, 0x00, sizeof(priskv_transport_mem));

    return ret;
}

static inline void priskv_ucx_mem_free(priskv_transport_conn *conn, priskv_transport_mem *rmem)
{
    if (rmem->memh) {
        priskv_ucx_munmap(rmem->memh);
        rmem->memh = NULL;
    }

    if (rmem->buf) {
        priskv_log_debug("UCX: free rmem %s, buf %p\n", rmem->name, rmem->buf);
        munmap(rmem->buf, rmem->buf_size);
    }

    priskv_log_info("UCX: free rmem %s, size %d\n", rmem->name, rmem->buf_size);
    memset(rmem, 0x00, sizeof(priskv_transport_mem));
}

static inline void priskv_ucx_mem_free_all(priskv_transport_conn *conn)
{
    for (int i = 0; i < PRISKV_TRANSPORT_MEM_MAX; i++) {
        priskv_transport_mem *rmem = &conn->rmem[i];

        priskv_ucx_mem_free(conn, rmem);
    }
}

#define PRISKV_UCX_REQUEST_FREE_COMMAND 0xffff
static void priskv_ucx_request_free(priskv_request *req, priskv_transport_conn *conn)
{
    uint8_t *ptr = (uint8_t *)req;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];

    assert(ptr >= rmem->buf);
    assert(ptr < rmem->buf + rmem->buf_size);
    assert(!((ptr - rmem->buf) %
             priskv_ucx_max_request_size_aligned(conn->param.max_sgl, conn->param.max_key_length)));

    req->command = PRISKV_UCX_REQUEST_FREE_COMMAND;
}

static int priskv_ucx_mem_new_all(priskv_transport_conn *conn)
{
    uint32_t page_size = getpagesize(), size;

    /* #step 1, prepare buffer & MR for request to server */
    int reqsize =
        priskv_ucx_max_request_size_aligned(conn->param.max_sgl, conn->param.max_key_length);
    size = reqsize * conn->param.max_inflight_command;
    if (priskv_ucx_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_REQ], "Request", size, false)) {
        goto error;
    }

    /* additional work: set priskv_request::command as PRISKV_UCX_REQUEST_FREE_COMMAND */
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];
    for (uint16_t i = 0; i < conn->param.max_inflight_command; i++) {
        priskv_request *req = (priskv_request *)(rmem->buf + i * reqsize);
        priskv_ucx_request_free(req, conn);
    }

    /* #step 2, prepare buffer & MR for response from server */
    size = sizeof(priskv_response) * conn->param.max_inflight_command;
    if (priskv_ucx_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_RESP], "Response", size, false)) {
        goto error;
    }

    /* #step 3, prepare buffer & MR for keys */
    size = page_size;
    if (priskv_ucx_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS], "Keys", size, true)) {
        goto error;
    }

    return 0;

error:
    priskv_ucx_mem_free_all(conn);

    return -ENOMEM;
}

static priskv_request *priskv_ucx_unused_command(priskv_transport_conn *conn, uint16_t *idx)
{
    uint16_t req_buf_size =
        priskv_ucx_max_request_size_aligned(conn->param.max_sgl, conn->param.max_key_length);
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];

    for (uint16_t i = 0; i < conn->param.max_inflight_command; i++) {
        priskv_request *req = (priskv_request *)(rmem->buf + i * req_buf_size);
        if (req->command == PRISKV_UCX_REQUEST_FREE_COMMAND) {
            priskv_log_debug("UCX: use request %d\n", i);
            req->command = 0xc001;
            *idx = i;
            return req;
        }
    }

    return NULL;
}

static void priskv_ucx_close_conn(priskv_transport_conn *conn)
{
    if (conn->state == PRISKV_TRANSPORT_CONN_STATE_CLOSED) {
        return;
    }

    priskv_transport_req *req, *tmp;
    priskv_ucx_request *ucx_req, *ucx_tmp;
    size_t total_inflight = 0;

    if (conn->state == PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED) {
        priskv_log_notice(
            "UCX: <%s - %s> ep %s close. Requests GET %ld, SET %ld, TEST %ld, DELETE %ld, "
            "Responses %ld\n",
            conn->local_addr, conn->peer_addr, conn->ep->name, conn->stats[PRISKV_COMMAND_GET],
            conn->stats[PRISKV_COMMAND_SET], conn->stats[PRISKV_COMMAND_TEST],
            conn->stats[PRISKV_COMMAND_DELETE], conn->resps);
    }

    conn->state = PRISKV_TRANSPORT_CONN_STATE_CLOSED;

    HASH_ITER(hh, conn->inflight_reqs, ucx_req, ucx_tmp)
    {
        total_inflight++;
        HASH_DEL(conn->inflight_reqs, ucx_req);
        priskv_ucx_request_cancel(ucx_req);
    }
    if (total_inflight > 0) {
        priskv_log_notice("UCX: <%s - %s> ep %s close. %ld requests are still inflight\n",
                          conn->local_addr, conn->peer_addr, conn->ep->name, total_inflight);
    }

    priskv_ucx_req_complete(conn);

    list_for_each_safe (&conn->inflight_list, req, tmp, entry) {
        list_del(&req->entry);

        priskv_ucx_request_free(req->req, conn);
        req->status = PRISKV_STATUS_DISCONNECTED;
        req->cb(req);
    }

    if (conn->ep) {
        priskv_ucx_ep_destroy(conn->ep);
        conn->ep = NULL;
    }

    if (conn->worker) {
        priskv_ucx_worker_destroy(conn->worker);
        conn->worker = NULL;
    }

    if (conn->connfd >= 0) {
        ucs_close_fd(&conn->connfd);
        conn->connfd = -1;
    }

    priskv_ucx_mem_free_all(conn);

    free(conn->keys_mems.memhs);
    free(conn);
}

static void priskv_ucx_recv_resp_cb(ucs_status_t status, ucp_tag_t sender_tag, size_t length,
                                    void *arg)
{
    if (ucs_unlikely(arg == NULL)) {
        priskv_log_error("UCX: priskv_ucx_recv_resp_cb, arg is NULL\n");
        return;
    }

    priskv_ucx_conn_resp *conn_resp = arg;
    priskv_transport_conn *conn = conn_resp->conn;
    priskv_response *resp = conn_resp->resp;
    free(conn_resp);

    priskv_ucx_request *handle = NULL;
    HASH_FIND_PTR(conn->inflight_reqs, &conn_resp, handle);
    if (handle) {
        priskv_log_debug("UCX: remove request %p from inflight_reqs\n", handle);
        HASH_DEL(conn->inflight_reqs, handle);
    }

    if (ucs_unlikely(status != UCS_OK)) {
        if (status == UCS_ERR_CANCELED) {
            priskv_log_debug("UCX: priskv_ucx_recv_resp_cb, status: %s\n",
                             ucs_status_string(status));
        } else {
            priskv_log_error("UCX: priskv_ucx_recv_resp_cb, status: %s\n",
                             ucs_status_string(status));
        }
        return;
    }

    if (length != sizeof(priskv_response)) {
        priskv_log_warn("UCX: recv %d, expected %ld\n", length, sizeof(priskv_response));
        return;
    }

    uint64_t request_id = be64toh(resp->request_id);
    if (status != UCS_OK) {
        priskv_log_error("UCX: priskv_ucx_recv_resp_cb, status: %s, resp: %p, request_id: 0x%lx\n",
                         ucs_status_string(status), resp, request_id);
        return;
    }

    uint16_t resp_status = be16toh(resp->status);
    uint32_t resp_length = be32toh(resp->length);
    priskv_transport_req *ucx_req;

    priskv_log_debug("Response request_id 0x%lx, status(%d) %s, length %d\n", request_id,
                     resp_status, priskv_resp_status_str(resp_status), resp_length);
    ucx_req = (priskv_transport_req *)request_id;
    ucx_req->status = resp_status;
    ucx_req->length = resp_length;

    if (ucx_req->cmd != PRISKV_COMMAND_KEYS) {
        ucx_req->result = &ucx_req->length;
    }

    ucx_req->flags |= PRISKV_TRANSPORT_REQ_FLAG_RECV;
    priskv_ucx_req_done(conn, ucx_req);

    conn->wc_recv++;
    conn->resps++;
    priskv_ucx_recv_resp(conn, resp);
}

/* return negative number on failure, return received buffer size on success */
static int priskv_ucx_recv_resp(priskv_transport_conn *conn, priskv_response *resp)
{
    uint16_t resp_buf_size = sizeof(priskv_response);

    priskv_log_debug("UCX: priskv_ucx_recv_resp addr %p, length %d\n", resp, resp_buf_size);
    priskv_ucx_conn_resp *conn_resp = malloc(sizeof(priskv_ucx_conn_resp));
    if (ucs_unlikely(conn_resp == NULL)) {
        priskv_log_error("UCX: priskv_ucx_recv_resp, malloc conn_resp failed\n");
        return -ENOMEM;
    }

    conn_resp->conn = conn;
    conn_resp->resp = resp;
    ucs_status_ptr_t handle =
        priskv_ucx_ep_post_tag_recv(conn->ep, resp, resp_buf_size, PRISKV_PROTO_TAG_CTRL,
                                    PRISKV_PROTO_FULL_TAG_MASK, priskv_ucx_recv_resp_cb, conn_resp);
    if (UCS_PTR_IS_ERR(handle)) {
        ucs_status_t status = UCS_PTR_STATUS(handle);
        priskv_log_error("UCX: <%s - %s> priskv_ucx_ep_post_tag_recv failed, status: %s\n",
                         conn->local_addr, conn->peer_addr, ucs_status_string(status));
        return -EIO;
    } else if (UCS_PTR_IS_PTR(handle)) {
        // still in progress
        priskv_ucx_request *req = (priskv_ucx_request *)handle;
        if (req->status == UCS_INPROGRESS) {
            req->key = conn_resp;
            HASH_ADD_PTR(conn->inflight_reqs, key, req);
        }
    } else {
        // Operation completed immediately
    }

    return resp_buf_size;
}

static int priskv_ucx_modify_max_inflight_command(priskv_transport_conn *conn,
                                                  uint16_t max_inflight_command)
{
    /* auto detect max_inflight_command from server */
    if (max_inflight_command == PRISKV_UCX_DEFAULT_INFLIGHT_COMMAND) {
        conn->param.max_inflight_command = PRISKV_UCX_DEFAULT_INFLIGHT_COMMAND;
        return 0; /* no need to change */
    }

    priskv_log_warn("UCX: not support modify max_inflight_command\n");
    return 0;
}

static void priskv_ucx_conn_close_cb(ucs_status_t status, void *arg)
{
    priskv_transport_conn *conn = arg;
    if (status != UCS_OK) {
        priskv_log_error("UCX: <%s - %s> ep close, status: %s\n", conn->local_addr, conn->peer_addr,
                         ucs_status_string(status));
    }

    // mark conn as closing
    conn->state = PRISKV_TRANSPORT_CONN_STATE_CLOSING;
}

static int priskv_ucx_handshake(priskv_transport_conn *conn, ucp_address_t **address,
                                uint32_t *address_len)
{
    int ret;
    uint8_t *peer_worker_address = NULL;

    size_t hs_size = sizeof(priskv_cm_ucx_handshake) + conn->worker->address_len;
    priskv_cm_ucx_handshake *hs = malloc(hs_size);
    if (ucs_unlikely(hs == NULL)) {
        priskv_log_error("UCX: priskv_ucx_handshake, malloc hs failed\n");
        ret = -1;
        goto error;
    }

    /* send handshake msg to server */
    if (priskv_get_log_level() >= priskv_log_debug) {
        size_t print_len = conn->worker->address_len > 128 ? 128 : conn->worker->address_len;
        char worker_address_hex[print_len * 2 + 1];
        priskv_ucx_to_hex(worker_address_hex, conn->worker->address, print_len);
        priskv_log_debug(
            "UCX: send worker address to server, address_len %d, address (first %d) %s\n",
            conn->worker->address_len, print_len, worker_address_hex);
    }

    if (priskv_set_block(conn->connfd)) {
        priskv_log_error("UCX: failed to set block mode for connfd\n");
        ret = -1;
        goto error;
    }

    hs->cap.version = htobe16(PRISKV_CM_VERSION);
    hs->cap.max_sgl = htobe16(conn->param.max_sgl);
    hs->cap.max_key_length = htobe16(conn->param.max_key_length);
    hs->cap.max_inflight_command = htobe16(conn->param.max_inflight_command);
    hs->address_len = htobe32(conn->worker->address_len);
    memcpy(hs->address, conn->worker->address, conn->worker->address_len);
    ret = priskv_safe_send(conn->connfd, hs, hs_size, NULL, NULL);
    free(hs);
    if (ret) {
        priskv_log_error("UCX: failed to send capability to server\n");
        ret = -1;
        goto error;
    }

    /* receive response from server */
    priskv_cm_ucx_handshake peer_hs;
    ret = priskv_safe_recv(conn->connfd, &peer_hs, sizeof(peer_hs), NULL, NULL);
    if (ret) {
        priskv_log_error("UCX: failed to receive handshake msg from server\n");
        ret = -1;
        goto error;
    }

    /* check reject message */
    if (peer_hs.flag == 0) {
        uint16_t version = be16toh(peer_hs.version);
        priskv_cm_status status = be16toh(peer_hs.status);
        uint64_t value = be64toh(peer_hs.value);
        priskv_log_error(
            "UCX: reject version %d, status: %s(%d), supported value %ld from server\n", version,
            priskv_cm_status_str(status), status, value);
        ret = -1;
        goto error;
    }

    /* accept */
    uint16_t version = be16toh(peer_hs.cap.version);
    conn->param.max_sgl = be16toh(peer_hs.cap.max_sgl);
    conn->param.max_key_length = be16toh(peer_hs.cap.max_key_length);
    uint16_t max_inflight_command = be16toh(peer_hs.cap.max_inflight_command);
    conn->capacity = be64toh(peer_hs.cap.capacity);
    uint32_t peer_worker_address_len = be32toh(peer_hs.address_len);
    if (peer_worker_address_len > 0) {
        peer_worker_address = malloc(peer_worker_address_len);
        if (peer_worker_address == NULL) {
            priskv_log_error("UCX: failed to allocate memory for peer_worker_address\n");
            ret = -1;
            goto error;
        }
        ret = priskv_safe_recv(conn->connfd, peer_worker_address, peer_worker_address_len, NULL,
                               NULL);
        if (ret) {
            priskv_log_error("UCX: failed to receive peer_worker_address from server\n");
            ret = -1;
            goto error;
        }
    }

    if (!peer_worker_address) {
        priskv_log_error("UCX: peer_worker_address is NULL\n");
        ret = -1;
        goto error;
    }

    if (priskv_set_nonblock(conn->connfd)) {
        priskv_log_error("UCX: failed to set nonblock mode for connfd\n");
        ret = -1;
        goto error;
    }

    if (priskv_get_log_level() >= priskv_log_debug) {
        size_t print_len = peer_worker_address_len > 128 ? 128 : peer_worker_address_len;
        char worker_address_hex[print_len * 2 + 1];
        priskv_ucx_to_hex(worker_address_hex, peer_worker_address, print_len);
        priskv_log_debug(
            "UCX: got peer worker address from server, address_len %d, address (first %d) %s\n",
            peer_worker_address_len, print_len, worker_address_hex);
    }

    priskv_log_info(
        "UCX: got response version %d, max_sgl %d, max_key_length %d, max_inflight_command "
        "%d, capacity %ld, address_len %d from server\n",
        version, conn->param.max_sgl, conn->param.max_key_length, max_inflight_command,
        conn->capacity, peer_worker_address_len);

    ret = priskv_ucx_modify_max_inflight_command(conn, max_inflight_command);
    if (ret) {
        goto error;
    }

    priskv_log_info("UCX: update connection parameters, max_sgl %d, max_key_length %d, "
                    "max_inflight_command %d\n",
                    conn->param.max_sgl, conn->param.max_key_length,
                    conn->param.max_inflight_command);

    *address_len = peer_worker_address_len;
    *address = peer_worker_address;

    return 0;
error:
    priskv_log_error("UCX: <%s - %s> connect failed\n", conn->local_addr, conn->peer_addr);
    if (peer_worker_address) {
        free(peer_worker_address);
        peer_worker_address = NULL;
    }
    return ret;
}

static inline void priskv_ucx_conn_pollin_progress(int fd, void *opaque, uint32_t ev)
{
    priskv_log_debug("UCX: pollin event %d\n", ev);
    priskv_transport_conn *conn = opaque;
    priskv_ucx_worker_progress(conn->worker);
    if (conn->state != PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED) {
        priskv_ucx_close_conn(conn);
        return;
    }

    priskv_ucx_req_complete(conn);
    priskv_ucx_req_delay_send(conn);
}

static inline void priskv_ucx_conn_connfd_progress(int fd, void *opaque, uint32_t ev)
{
    priskv_transport_conn *conn = opaque;
    priskv_ucx_close_conn(conn);
}

static int priskv_ucx_bind(const char *addr, int port, int *connfd)
{
    int ret;
    int sockfd = -1;
    int optval = 1;
    struct addrinfo hints, *res, *t;
    char service[8];
    char err_str[64];
    ucs_status_t status;

    ucs_snprintf_safe(service, sizeof(service), "%u", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = 0;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    ret = getaddrinfo(addr, service, &hints, &res);
    if (ret < 0) {
        priskv_log_error("UCX: getaddrinfo failed, addr %s, port %s, error %s\n", addr, service,
                         gai_strerror(ret));
        ret = -1;
        goto out;
    }

    if (res == NULL) {
        priskv_log_error("UCX: getaddrinfo returned empty list\n");
        ret = -1;
        goto out;
    }

    for (t = res; t != NULL; t = t->ai_next) {
        sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (sockfd < 0) {
            snprintf(err_str, 64, "socket failed: %m");
            continue;
        }

        status = ucs_socket_setopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
        if (status != UCS_OK) {
            snprintf(err_str, 64, "setopt failed: %m");
            continue;
        }

        if (bind(sockfd, t->ai_addr, t->ai_addrlen) == 0) {
            break;
        }

        snprintf(err_str, 64, "bind failed: %m");
        ucs_close_fd(&sockfd);
        sockfd = -1;
    }

    if (sockfd < 0) {
        priskv_log_error("UCX: bind failed, addr %s, port %s, error %s\n", addr, service, err_str);
        ret = -1;
        goto out_free_res;
    }

    *connfd = sockfd;
    return 0;

out_free_res:
    freeaddrinfo(res);
out:
    return ret;
}

static int priskv_ucx_connect(int sockfd, const struct sockaddr *addr, socklen_t addrlen,
                              int timeout_ms, char *err_str)
{
    int epoll_fd, ret;

    ret = priskv_set_nonblock(sockfd);
    if (ret) {
        snprintf(err_str, 64, "set NONBLOCK failed: %m");
        return -1;
    }

    ret = connect(sockfd, addr, addrlen);
    if (ret == 0) {
        return 0;
    }

    if (errno != EINPROGRESS) {
        snprintf(err_str, 64, "connect failed: %m");
        return -1;
    }

    epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) {
        snprintf(err_str, 64, "epoll_create1 failed: %m");
        return -1;
    }

    struct epoll_event ev, event;
    ev.events = EPOLLOUT;
    ev.data.fd = sockfd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sockfd, &ev) < 0) {
        snprintf(err_str, 64, "epoll_ctl failed: %m");
        ret = -1;
        goto out_free_res;
    }

poll_again:
    ret = epoll_wait(epoll_fd, &event, 1, timeout_ms);
    if (ret == 0) {
        snprintf(err_str, 64, "connect timeout");
        errno = ETIMEDOUT;
        ret = -1;
        goto out_free_res;
    } else if (ret < 0) {
        if (errno == EINTR) {
            goto poll_again;
        }
        snprintf(err_str, 64, "connect failed: %m");
        ret = -1;
        goto out_free_res;
    }

    int opt = 0;
    socklen_t optlen = sizeof(opt);
    if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &opt, &optlen) < 0) {
        snprintf(err_str, 64, "getsockopt failed: %m");
        ret = -1;
        goto out_free_res;
    }

    if (opt != 0) {
        errno = opt;
        snprintf(err_str, 64, "connect failed: %m");
        ret = -1;
        goto out_free_res;
    }

    ret = 0;
out_free_res:
    if (epoll_fd >= 0) {
        close(epoll_fd);
    }
    return ret;
}

static priskv_transport_conn *priskv_ucx_conn_connect(const char *raddr, int rport,
                                                      const char *laddr, int lport)
{
    priskv_transport_conn *conn = NULL;
    int ret;
    int connfd = -1;
    int connected = -1;
    struct addrinfo hints, *res, *t;
    char service[8];
    char err_str[64];

    /* bind local address if user specify one */
    if (laddr) {
        ret = priskv_ucx_bind(laddr, lport, &connfd);
        if (ret) {
            return NULL;
        }
    }

    ucs_snprintf_safe(service, sizeof(service), "%u", rport);
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = 0;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    ret = getaddrinfo(raddr, service, &hints, &res);
    if (ret < 0) {
        priskv_log_error("UCX: getaddrinfo failed, server %s, port %s, error %s\n", raddr, service,
                         gai_strerror(ret));
        goto out_free_res;
    }

    if (res == NULL) {
        priskv_log_error("UCX: getaddrinfo returned empty list\n");
        goto out_free_res;
    }

    for (t = res; t != NULL; t = t->ai_next) {
        if (!laddr) {
            connfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
            if (connfd < 0) {
                snprintf(err_str, 64, "socket failed: %m");
                continue;
            }
        }

        if (priskv_ucx_connect(connfd, t->ai_addr, t->ai_addrlen, 1000, err_str) == 0) {
            connected = 1;
            break;
        }

        if (!laddr) {
            ucs_close_fd(&connfd);
            connfd = -1;
        }
    }

    if (connfd < 0 || connected != 1) {
        priskv_log_error("UCX: connect failed, server %s, port %s, error %s\n", raddr, service,
                         err_str);
        goto out_free_res;
    }

    conn = calloc(sizeof(struct priskv_transport_conn), 1);
    if (!conn) {
        priskv_log_error("UCX: failed to allocate memory for UCX connection\n");
        goto out_free_res;
    }

    conn->param.max_sgl = 0;
    conn->param.max_key_length = 0;
    conn->param.max_inflight_command = PRISKV_UCX_DEFAULT_INFLIGHT_COMMAND;

    list_head_init(&conn->inflight_list);
    list_head_init(&conn->complete_list);

    conn->keys_running_req = NULL;
    conn->keys_mems.count = 1;
    conn->keys_mems.memhs = calloc(sizeof(priskv_ucx_memh *), 1);
    conn->state = PRISKV_TRANSPORT_CONN_STATE_INIT;
    conn->connfd = connfd;
    connfd = -1;

    ucs_socket_getname_str(conn->connfd, conn->local_addr, sizeof(conn->local_addr));
    priskv_inet_ntop(t->ai_addr, conn->peer_addr);

    conn->worker = priskv_ucx_worker_create(g_ucx_ctx, 0);
    if (!conn->worker) {
        priskv_log_error("UCX: failed to create worker\n");
        goto err_free_address;
    }

    ucp_address_t *address;
    uint32_t address_len;
    ret = priskv_ucx_handshake(conn, &address, &address_len);
    if (ret) {
        goto error;
    }

    conn->ep = priskv_ucx_ep_create_from_worker_addr(conn->worker, address,
                                                     priskv_ucx_conn_close_cb, conn);
    if (!conn->ep) {
        priskv_log_error("UCX: failed to create endpoint\n");
        goto err_free_address;
    }

    free(address);

    priskv_set_fd_handler(conn->worker->efd, priskv_ucx_conn_pollin_progress, NULL, conn);
    priskv_set_fd_handler(conn->connfd, priskv_ucx_conn_connfd_progress, NULL, conn);
    conn->epollfd = conn->worker->efd;

    ret = priskv_ucx_mem_new_all(conn);
    if (ret) {
        goto error;
    }

    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];
    priskv_response *resp = (priskv_response *)rmem->buf;
    for (int i = 0; i < conn->param.max_inflight_command; i++) {
        ret = priskv_ucx_recv_resp(conn, resp + i);
        if (ret < 0) {
            goto error;
        }
    }

    conn->state = PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED;

    priskv_log_notice("UCX: <%s - %s> established \n", conn->local_addr, conn->peer_addr);

    return conn;

err_free_address:
    free(address);
error:
    priskv_ucx_close_conn(conn);
    conn = NULL;
out_free_res:
    if (connfd >= 0) {
        ucs_close_fd(&connfd);
    }
    freeaddrinfo(res);
    return conn;
}

int priskv_ucx_conn_close(void *conn)
{
    if (!conn) {
        return 0;
    }

    priskv_ucx_close_conn(conn);

    return 0;
}

static priskv_ucx_memh *priskv_ucx_conn_reg_memory(priskv_transport_conn *conn, uint64_t offset,
                                                   size_t length, uint64_t iova, int fd)
{
    priskv_ucx_memh *memh = NULL;

    if (fd >= 0) {
        priskv_log_error("UCX: not support register memory with fd\n");
    } else {
        memh = priskv_ucx_mmap(g_ucx_ctx, (void *)offset, length, UCS_MEMORY_TYPE_HOST);
    }

    if (!memh) {
        priskv_log_error(
            "UCX: failed to reg mr 0x%lx:%ld %m. If you are using GPU memory, check if "
            "the nvidia_peermem module is installed\n",
            offset, length);
    }

    return memh;
}

static void priskv_ucx_conn_dereg_memory(priskv_ucx_memh *memh)
{
    priskv_ucx_munmap(memh);
}

static int priskv_ucx_mq_init(priskv_client *client, const char *raddr, int rport,
                              const char *laddr, int lport, int nqueue)
{
    client->wq = priskv_workqueue_create(client->epollfd);
    if (!client->wq) {
        priskv_log_error("UCX: failed to create workqueue\n");
        return -1;
    }

    client->pool = priskv_threadpool_create("priskv", nqueue, 0, 0);
    if (!client->pool) {
        priskv_log_error("UCX: failed to create threadpool\n");
        return -1;
    }

    client->conns = calloc(nqueue, sizeof(priskv_transport_conn *));
    if (!client->conns) {
        priskv_log_error("UCX: failed to allocate memory for connections\n");
        return -1;
    }
    client->nqueue = nqueue;

    for (uint8_t i = 0; i < nqueue; i++) {
        client->conns[i] = priskv_ucx_conn_connect(raddr, rport, laddr, lport);
        if (!client->conns[i]) {
            priskv_log_error("UCX: failed to connect to %s:%d\n", raddr, rport);
            return -1;
        }

        client->conns[i]->id = i;
        client->conns[i]->thread = priskv_threadpool_get_iothread(client->pool, i);

        priskv_thread_add_event_handler(client->conns[i]->thread, client->conns[i]->epollfd);
        priskv_thread_add_event_handler(client->conns[i]->thread, client->conns[i]->connfd);
    }

    client->cur_conn = 0;

    return 0;
}

static void priskv_ucx_mq_deinit(priskv_client *client)
{
    if (client->conns) {
        for (int i = 0; i < client->nqueue; i++) {
            priskv_thread_call_function(priskv_threadpool_get_iothread(client->pool, i),
                                        priskv_ucx_conn_close, client->conns[i]);
        }
    }

    priskv_threadpool_destroy(client->pool);
    priskv_workqueue_destroy(client->wq);
    free(client->conns);
}

static priskv_transport_conn *priskv_ucx_mq_select_conn(priskv_client *client)
{
    return client->conns[client->cur_conn++ % client->nqueue];
}

static priskv_memory *priskv_ucx_mq_reg_memory(priskv_client *client, uint64_t offset,
                                               size_t length, uint64_t iova, int fd)
{
    priskv_memory *mem = malloc(sizeof(priskv_memory));

    mem->client = client;
    mem->count = client->nqueue;
    mem->memhs = malloc(client->nqueue * sizeof(priskv_ucx_memh *));

    for (int i = 0; i < mem->count; i++) {
        mem->memhs[i] = priskv_ucx_conn_reg_memory(client->conns[i], offset, length, iova, fd);
    }

    return mem;
}

static void priskv_ucx_mq_dereg_memory(priskv_memory *mem)
{
    for (int i = 0; i < mem->count; i++) {
        priskv_ucx_conn_dereg_memory(mem->memhs[i]);
    }
    free(mem->memhs);
    free(mem);
}

static priskv_ucx_memh *priskv_ucx_mq_get_memh(priskv_memory *mem, int connid)
{
    return mem->memhs[connid];
}

static void priskv_ucx_mq_req_submit(priskv_transport_req *ucx_req)
{
    priskv_thread_submit_function(ucx_req->conn->thread, priskv_ucx_send_req, ucx_req);
}

static void priskv_ucx_mq_req_cb(priskv_transport_req *ucx_req)
{
    priskv_workqueue_submit(ucx_req->main_wq, priskv_ucx_req_cb_intl, ucx_req);
}

static priskv_conn_operation priskv_ucx_mq_ops = {
    .init = priskv_ucx_mq_init,
    .deinit = priskv_ucx_mq_deinit,
    .select_conn = priskv_ucx_mq_select_conn,
    .reg_memory = priskv_ucx_mq_reg_memory,
    .dereg_memory = priskv_ucx_mq_dereg_memory,
    .get_memh = priskv_ucx_mq_get_memh,
    .submit_req = priskv_ucx_mq_req_submit,
    .req_cb = priskv_ucx_mq_req_cb,
    .new_req = priskv_ucx_req_new,
};

static int priskv_ucx_sq_init(priskv_client *client, const char *raddr, int rport,
                              const char *laddr, int lport, int nqueue)
{
    client->conns = calloc(1, sizeof(priskv_transport_conn *));
    if (!client->conns) {
        priskv_log_error("UCX: failed to allocate memory for connections\n");
        return -1;
    }

    client->conns[0] = priskv_ucx_conn_connect(raddr, rport, laddr, lport);
    if (!client->conns[0]) {
        priskv_log_error("UCX: failed to connect to %s:%d\n", raddr, rport);
        return -1;
    }

    priskv_add_event_fd(client->epollfd, client->conns[0]->epollfd);
    priskv_add_event_fd(client->epollfd, client->conns[0]->connfd);

    return 0;
}

static void priskv_ucx_sq_deinit(priskv_client *client)
{
    priskv_ucx_conn_close(client->conns[0]);
    free(client->conns);
}

static priskv_transport_conn *priskv_ucx_sq_select_conn(priskv_client *client)
{
    return client->conns[0];
}

static priskv_memory *priskv_ucx_sq_reg_memory(priskv_client *client, uint64_t offset,
                                               size_t length, uint64_t iova, int fd)
{
    priskv_memory *mem = malloc(sizeof(priskv_memory));

    mem->client = client;
    mem->count = 1;
    mem->memhs = malloc(sizeof(priskv_ucx_memh *));

    mem->memhs[0] = priskv_ucx_conn_reg_memory(client->conns[0], offset, length, iova, fd);

    return mem;
}

static void priskv_ucx_sq_dereg_memory(priskv_memory *mem)
{
    priskv_ucx_conn_dereg_memory(mem->memhs[0]);
    free(mem->memhs);
    free(mem);
}

static priskv_ucx_memh *priskv_ucx_sq_get_memh(priskv_memory *mem, int connid)
{
    return mem->memhs[0];
}

static void priskv_ucx_sq_req_submit(priskv_transport_req *ucx_req)
{
    priskv_ucx_send_req(ucx_req);
}

static void priskv_ucx_sq_req_cb(priskv_transport_req *ucx_req)
{
    priskv_ucx_req_cb_intl(ucx_req);
}

static priskv_conn_operation priskv_ucx_sq_ops = {
    .init = priskv_ucx_sq_init,
    .deinit = priskv_ucx_sq_deinit,
    .select_conn = priskv_ucx_sq_select_conn,
    .reg_memory = priskv_ucx_sq_reg_memory,
    .dereg_memory = priskv_ucx_sq_dereg_memory,
    .get_memh = priskv_ucx_sq_get_memh,
    .submit_req = priskv_ucx_sq_req_submit,
    .req_cb = priskv_ucx_sq_req_cb,
    .new_req = priskv_ucx_req_new,
};

static inline void priskv_ucx_fillup_sgl(priskv_transport_req *req, priskv_keyed_sgl *keyed_sgl)
{
    priskv_transport_conn *conn = req->conn;
    uint8_t *keyed_sgl_base = (uint8_t *)keyed_sgl;

    for (uint16_t i = 0; i < req->nsgl; i++) {
        priskv_sgl_private *_sgl = &req->sgl[i];
        priskv_ucx_memh *memh;

        if (!_sgl->sgl.mem) {
            priskv_log_warn("UCX: SGL %d without registered memory, iova 0x%lx, length 0x%zx\n", i,
                            _sgl->sgl.iova, _sgl->sgl.length);
            memh = _sgl->memh = priskv_ucx_conn_reg_memory(conn, _sgl->sgl.iova, _sgl->sgl.length,
                                                           _sgl->sgl.iova, -1);
        } else {
            if (req->cmd != PRISKV_COMMAND_KEYS) {
                memh = req->ops->get_memh(_sgl->sgl.mem, conn->id);
            } else {
                memh = req->ops->get_memh(_sgl->sgl.mem, 0);
            }
        }

        assert(memh->rkey_length <= priskv_ucx_max_rkey_len);

        priskv_keyed_sgl *_keyed_sgl = (priskv_keyed_sgl *)keyed_sgl_base;

        _keyed_sgl->addr = htobe64(_sgl->sgl.iova);
        _keyed_sgl->length = htobe32(_sgl->sgl.length);
        _keyed_sgl->packed_rkey_len = htobe32(memh->rkey_length);
        memcpy(_keyed_sgl->packed_rkey, memh->rkey_buffer, memh->rkey_length);
        keyed_sgl_base += sizeof(priskv_keyed_sgl) + memh->rkey_length;

        if (priskv_get_log_level() >= priskv_log_debug) {
            char rkey_hex[priskv_ucx_max_rkey_len * 2 + 1];
            priskv_ucx_to_hex(rkey_hex, _keyed_sgl->packed_rkey, memh->rkey_length);
            priskv_log_debug("UCX: addr 0x%lx@%x rkey (%d) %s\n", _sgl->sgl.iova, _sgl->sgl.length,
                             memh->rkey_length, rkey_hex);
        }
    }
}

static int priskv_ucx_req_cb_intl(void *arg)
{
    priskv_transport_req *req = arg;

    if (req->usercb) {
        req->usercb(req->request_id, req->status, req->result);
    }

    priskv_ucx_req_free(req);

    return 0;
}

static void priskv_ucx_req_cb(priskv_transport_req *ucx_req)
{
    ucx_req->ops->req_cb(ucx_req);
}

static void priskv_ucx_keys_req_cb(priskv_transport_req *ucx_req)
{
    priskv_transport_conn *conn = ucx_req->conn;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
    uint32_t valuelen = ucx_req->length;

    if (ucx_req->status == PRISKV_STATUS_OK) {
        uint32_t nkey = 0;
        uint16_t keylen;
        uint8_t *buf = rmem->buf;

        while ((buf - rmem->buf) < valuelen) {
            priskv_keys_resp *keys_resp = (priskv_keys_resp *)buf;
            keylen = be16toh(keys_resp->keylen);

            buf += sizeof(priskv_keys_resp);
            buf += keylen;
            nkey++;
        }

        if ((buf - rmem->buf) != valuelen) {
            priskv_log_error("UCX: KEYS protocol error\n");
            ucx_req->status = PRISKV_STATUS_PROTOCOL_ERROR;
            goto exit;
        }

        priskv_keyset *keyset = calloc(1, sizeof(priskv_keyset));
        keyset->keys = calloc(nkey, sizeof(priskv_key));
        keyset->nkey = nkey;
        priskv_key *curkey = &keyset->keys[0];
        buf = rmem->buf;
        while ((buf - rmem->buf) < valuelen) {
            priskv_keys_resp *keys_resp = (priskv_keys_resp *)buf;
            keylen = be16toh(keys_resp->keylen);
            curkey->valuelen = be32toh(keys_resp->valuelen);

            buf += sizeof(priskv_keys_resp);
            curkey->key = malloc(keylen + 1);
            memcpy(curkey->key, buf, keylen);
            curkey->key[keylen] = '\0';
            buf += keylen;
            curkey++;
        }

        ucx_req->result = keyset;
    } else if (ucx_req->status == PRISKV_STATUS_VALUE_TOO_BIG) {
        priskv_log_info("UCX: resize KEYS buffer to valuelen %d\n", valuelen);
        priskv_ucx_mem_free(conn, rmem);
        if (priskv_ucx_mem_new(conn, rmem, "Keys", valuelen + valuelen / 8, true)) {
            priskv_log_error("UCX: failed to resize KEYS buffer to valuelen %d\n", valuelen);
            ucx_req->status = PRISKV_STATUS_TRANSPORT_ERROR;
            goto exit;
        }

        conn->keys_mems.count = 1;
        conn->keys_mems.memhs[0] = rmem->memh;

        priskv_ucx_req_reset(ucx_req);

        ucx_req->nsgl = 1;
        ucx_req->sgl[0].sgl.iova = (uint64_t)(rmem->buf);
        ucx_req->sgl[0].sgl.length = rmem->buf_size;
        ucx_req->sgl[0].sgl.mem = &conn->keys_mems;

        priskv_ucx_send_req(ucx_req);
        return;
    }

exit:
    conn->keys_running_req = NULL;
    priskv_ucx_req_cb(ucx_req);
}

static inline priskv_transport_req *
priskv_ucx_req_new(priskv_client *client, priskv_transport_conn *conn, uint64_t request_id,
                   const char *key, uint16_t keylen, priskv_sgl *sgl, uint16_t nsgl,
                   uint64_t timeout, priskv_req_command cmd, priskv_generic_cb usercb)
{
    priskv_transport_req *ucx_req = calloc(1, sizeof(priskv_transport_req));
    if (!ucx_req) {
        return NULL;
    }

    ucx_req->conn = conn;
    ucx_req->ops = client->ops;
    ucx_req->main_wq = client->wq;
    ucx_req->cmd = cmd;
    ucx_req->timeout = timeout;
    ucx_req->key = strdup(key);
    ucx_req->keylen = keylen;
    ucx_req->request_id = request_id;
    ucx_req->usercb = usercb;
    ucx_req->cb = priskv_ucx_req_cb;
    ucx_req->delaying = false;
    list_node_init(&ucx_req->entry);

    if (sgl && nsgl) {
        ucx_req->nsgl = nsgl;
        ucx_req->sgl = calloc(nsgl, sizeof(priskv_sgl_private));
        for (int i = 0; i < nsgl; i++) {
            memcpy(&ucx_req->sgl[i], &sgl[i], sizeof(priskv_sgl));
        }
    } else if (cmd == PRISKV_COMMAND_KEYS) {
        priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
        conn->keys_mems.count = 1;
        conn->keys_mems.memhs[0] = rmem->memh;

        ucx_req->nsgl = 1;
        ucx_req->sgl = malloc(sizeof(priskv_sgl_private));
        ucx_req->sgl[0].sgl.iova = (uint64_t)(rmem->buf);
        ucx_req->sgl[0].sgl.length = rmem->buf_size;
        ucx_req->sgl[0].sgl.mem = &conn->keys_mems;
        ucx_req->sgl[0].memh = NULL;
        ucx_req->cb = priskv_ucx_keys_req_cb;
    }

    return ucx_req;
}

static inline void priskv_ucx_req_free(priskv_transport_req *req)
{
    for (int i = 0; i < req->nsgl; i++) {
        priskv_sgl_private *_sgl = &req->sgl[i];
        if (_sgl->memh) {
            priskv_ucx_conn_dereg_memory(_sgl->memh);
            _sgl->memh = NULL;
        }
    }

    free(req->sgl);
    free(req->key);
    free(req);
}

static inline void priskv_ucx_req_reset(priskv_transport_req *ucx_req)
{
    ucx_req->flags = 0;
    ucx_req->req = NULL;
    ucx_req->status = PRISKV_STATUS_OK;
    ucx_req->length = 0;
    ucx_req->delaying = false;
}

static void priskv_ucx_send_req_cb(ucs_status_t status, void *arg)
{
    if (ucs_unlikely(arg == NULL)) {
        priskv_log_error("UCX: priskv_ucx_send_req_cb, arg is NULL\n");
        return;
    }

    priskv_transport_req *ucx_req = (priskv_transport_req *)arg;
    priskv_transport_conn *conn = ucx_req->conn;
    priskv_ucx_request *req;

    HASH_FIND_PTR(conn->inflight_reqs, &ucx_req, req);
    if (req) {
        priskv_log_debug("UCX: remove request %p from inflight_reqs\n", req);
        HASH_DEL(conn->inflight_reqs, req);
    }

    if (status != UCS_OK) {
        priskv_log_error("UCX: priskv_ucx_send_req_cb, status: %s, request: %p, request_id: %lu\n",
                         ucs_status_string(status), ucx_req, ucx_req->request_id);
        ucx_req->status = PRISKV_STATUS_TRANSPORT_ERROR;
        ucx_req->cb(ucx_req);
        return;
    } else {
        conn->wc_send++;
        ucx_req->flags |= PRISKV_TRANSPORT_REQ_FLAG_SEND;
        priskv_ucx_req_done(ucx_req->conn, ucx_req);
    }
}

static int priskv_ucx_send_req(void *arg)
{
    priskv_transport_req *ucx_req = arg;
    priskv_transport_conn *conn = ucx_req->conn;
    priskv_request *req;
    uint16_t req_idx;

    if (conn->state != PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED) {
        ucx_req->status = PRISKV_STATUS_DISCONNECTED;
        ucx_req->cb(ucx_req);
        return -1;
    }

    if (ucx_req->cmd == PRISKV_COMMAND_KEYS) {
        if (conn->keys_running_req && conn->keys_running_req != ucx_req) {
            ucx_req->status = PRISKV_STATUS_BUSY;
            ucx_req->cb(ucx_req);
            return -1;
        } else {
            conn->keys_running_req = ucx_req;
        }
    }

    req = priskv_ucx_unused_command(conn, &req_idx);
    if (!req) {
        if (ucx_req->delaying) {
            list_add(&conn->inflight_list, &ucx_req->entry);
        } else {
            list_add_tail(&conn->inflight_list, &ucx_req->entry);
            ucx_req->delaying = true;
        }
        return EAGAIN;
    }

    req->request_id = htobe64((uint64_t)ucx_req);
    req->command = htobe16(ucx_req->cmd);
    req->nsgl = htobe16(ucx_req->nsgl);
    req->timeout = htobe64(ucx_req->timeout);
    req->key_length = htobe16(ucx_req->keylen);

    struct timeval client_metadata_send_time;
    gettimeofday(&client_metadata_send_time, NULL);
    req->runtime.client_metadata_send_time = client_metadata_send_time;

    priskv_ucx_fillup_sgl(ucx_req, req->sgls);
    memcpy(priskv_ucx_request_key(req), ucx_req->key, ucx_req->keylen);

    uint16_t req_length = priskv_ucx_request_size(req);
    if (priskv_get_log_level() >= priskv_log_debug) {
        char key_short[16] = {0};
        priskv_string_shorten(ucx_req->key, ucx_req->keylen, key_short, sizeof(key_short));

        priskv_log_debug(
            "UCX: Request command length %u, request_id 0x%lx, %s[0x%x], nsgl %u, key[%u] %s\n",
            req_length, ucx_req->request_id, priskv_command_str(ucx_req->cmd), ucx_req->cmd,
            ucx_req->nsgl, ucx_req->keylen, key_short);
    }

    ucx_req->req = req;

    ucs_status_ptr_t handle = priskv_ucx_ep_post_tag_send(
        conn->ep, req, req_length, PRISKV_PROTO_TAG_CTRL, priskv_ucx_send_req_cb, ucx_req);
    if (UCS_PTR_IS_ERR(handle)) {
        priskv_log_notice(
            "UCX: <%s - %s> ep %s close. Requests GET %ld, SET %ld, TEST %ld, DELETE %ld, "
            "Responses %ld\n",
            conn->local_addr, conn->peer_addr, conn->ep->name, conn->stats[PRISKV_COMMAND_GET],
            conn->stats[PRISKV_COMMAND_SET], conn->stats[PRISKV_COMMAND_TEST],
            conn->stats[PRISKV_COMMAND_DELETE], conn->resps);

        ucs_status_t status = UCS_PTR_STATUS(handle);
        priskv_log_error("UCX: priskv_ucx_ep_post_tag_send addr %p, length %d. wc_recv %ld, "
                         "wc_send %ld, failed: %s\n",
                         req, req_length, conn->wc_recv, conn->wc_send, ucs_status_string(status));
        return -1;
    } else if (UCS_PTR_IS_PTR(handle)) {
        // still in progress
        priskv_ucx_request *request = handle;
        if (request->status == UCS_INPROGRESS) {
            request->key = ucx_req;
            HASH_ADD_PTR(conn->inflight_reqs, key, request);
        }
    } else {
        // Operation completed immediately
    }

    conn->stats[ucx_req->cmd]++;

    return 0;
}

static inline void priskv_ucx_req_submit(priskv_transport_req *ucx_req)
{
    ucx_req->ops->submit_req(ucx_req);
}

static void priskv_ucx_req_delay_send(priskv_transport_conn *conn)
{
    priskv_transport_req *ucx_req, *tmp;

    list_for_each_safe (&conn->inflight_list, ucx_req, tmp, entry) {
        list_del(&ucx_req->entry);

        if (priskv_ucx_send_req(ucx_req) == EAGAIN) {
            return;
        }
    }
}

static inline void priskv_ucx_req_done(priskv_transport_conn *conn, priskv_transport_req *ucx_req)
{
    if ((ucx_req->flags & PRISKV_TRANSPORT_REQ_FLAG_DONE) == PRISKV_TRANSPORT_REQ_FLAG_DONE) {
        list_add_tail(&conn->complete_list, &ucx_req->entry);
    }
}

static inline void priskv_ucx_req_complete(priskv_transport_conn *conn)
{
    priskv_transport_req *req, *tmp;

    list_for_each_safe (&conn->complete_list, req, tmp, entry) {
        list_del(&req->entry);

        priskv_ucx_request_free(req->req, conn);
        req->cb(req);
    }
}

priskv_conn_operation *priskv_ucx_get_sq_ops(void)
{
    return &priskv_ucx_sq_ops;
}

priskv_conn_operation *priskv_ucx_get_mq_ops(void)
{
    return &priskv_ucx_mq_ops;
}

priskv_transport_driver priskv_transport_driver_ucx = {
    .name = "ucx",
    .init = priskv_ucx_init,
    .get_sq_ops = priskv_ucx_get_sq_ops,
    .get_mq_ops = priskv_ucx_get_mq_ops,
};
