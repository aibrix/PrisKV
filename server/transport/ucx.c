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
#include <sys/eventfd.h>
#include <unistd.h>

#include <ucs/sys/string.h>
#include <ucs/sys/sock.h>

#include "../acl.h"
#include "../memory.h"
#include "../kv.h"
#include "../backend/backend.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "priskv-threads.h"
#include "priskv-event.h"
#include "priskv-ucx.h"
#include "transport.h"
#include "uthash.h"

extern priskv_transport_server g_transport_server;
extern priskv_threadpool *g_threadpool;

static uint32_t priskv_ucx_max_rw_size = 1024 * 1024 * 1024;

// forward declaration
static int priskv_ucx_new_ctrl_buffer(priskv_transport_conn *conn);
static inline void priskv_ucx_free_ctrl_buffer(priskv_transport_conn *conn);

typedef struct priskv_ucx_conn_aux {
    priskv_transport_conn *conn;
    void *aux;
} priskv_ucx_conn_aux;

static inline uint32_t priskv_ucx_wr_size(priskv_transport_conn *client)
{
    return client->conn_cap.max_inflight_command * (2 + client->conn_cap.max_sgl);
}

#define PRISKV_UCX_RESPONSE_FREE_STATUS 0xffff
static inline int priskv_ucx_response_free(priskv_response *resp)
{
    if (resp->status == PRISKV_UCX_RESPONSE_FREE_STATUS) {
        return -EPROTO;
    }

    resp->status = PRISKV_UCX_RESPONSE_FREE_STATUS;
    return 0;
}

static int priskv_ucx_init(void)
{
    g_transport_server.context = priskv_ucx_context_init(0);
    if (g_transport_server.context == NULL) {
        priskv_log_error("ucx context init failed\n");
        return -1;
    }

    return 0;
}

static inline void priskv_ucx_listener_signal(priskv_transport_conn *listener)
{
    uint64_t u = 1;

    write(listener->efd, &u, sizeof(u));
}

static inline void priskv_ucx_mark_client_closed(priskv_transport_conn *client)
{
    priskv_transport_mark_client_closed(client);
    priskv_ucx_listener_signal(client->c.listener);
}

static void priskv_ucx_recv_req_cb(ucs_status_t status, ucp_tag_t sender_tag, size_t length,
                                   void *arg)
{
    if (ucs_unlikely(arg == NULL)) {
        priskv_log_error("UCX: priskv_ucx_recv_req_cb, arg is NULL\n");
        return;
    }

    priskv_ucx_conn_aux *conn_req = arg;
    priskv_transport_conn *conn = conn_req->conn;
    priskv_request *req = conn_req->aux;
    free(conn_req);

    priskv_ucx_request *handle = NULL;
    HASH_FIND_PTR(conn->inflight_reqs, &conn_req, handle);
    if (handle) {
        priskv_log_debug("UCX: remove request %p from inflight_reqs\n", handle);
        HASH_DEL(conn->inflight_reqs, handle);
    }

    if (ucs_unlikely(status != UCS_OK)) {
        if (status == UCS_ERR_CANCELED) {
            priskv_log_debug("UCX: priskv_ucx_recv_req_cb, status: %s, req: %p\n",
                             ucs_status_string(status), arg);
        } else {
            priskv_log_error("UCX: priskv_ucx_recv_req_cb, status: %s, req: %p\n",
                             ucs_status_string(status), arg);
        }
        priskv_ucx_mark_client_closed(conn);
        return;
    }

    struct timeval server_metadata_recv_time;

    gettimeofday(&server_metadata_recv_time, NULL);
    req->runtime.server_metadata_recv_time = server_metadata_recv_time;

    if (priskv_transport_handle_recv(conn, req, length)) {
        priskv_ucx_mark_client_closed(conn);
    }
}

static int priskv_ucx_recv_req(priskv_transport_conn *conn, uint8_t *req)
{
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];
    uint16_t req_buf_size =
        priskv_ucx_max_request_size_aligned(conn->conn_cap.max_sgl, conn->conn_cap.max_key_length);

    priskv_log_debug("UCX: priskv_ucx_recv_req addr %p, length %d\n", req, req_buf_size);
    assert((req >= rmem->buf) && (req < rmem->buf + rmem->buf_size));

    priskv_ucx_conn_aux *conn_req = malloc(sizeof(priskv_ucx_conn_aux));
    if (ucs_unlikely(conn_req == NULL)) {
        priskv_log_error("UCX: priskv_ucx_recv_req, malloc conn_req failed\n");
        return -ENOMEM;
    }

    conn_req->conn = conn;
    conn_req->aux = req;
    ucs_status_ptr_t handle =
        priskv_ucx_ep_post_tag_recv(conn->ep, req, req_buf_size, PRISKV_PROTO_TAG_CTRL,
                                    PRISKV_PROTO_FULL_TAG_MASK, priskv_ucx_recv_req_cb, conn_req);
    if (UCS_PTR_IS_ERR(handle)) {
        ucs_status_t status = UCS_PTR_STATUS(handle);
        priskv_log_error("UCX: <%s - %s> priskv_ucx_ep_post_tag_recv failed, status: %s\n",
                         conn->local_addr, conn->peer_addr, ucs_status_string(status));
        return -EIO;
    } else if (UCS_PTR_IS_PTR(handle)) {
        // still in progress
        priskv_ucx_request *request = (priskv_ucx_request *)handle;
        if (request->status == UCS_INPROGRESS) {
            request->key = conn_req;
            HASH_ADD_PTR(conn->inflight_reqs, key, request);
        }
    } else {
        // Operation completed immediately
    }

    return req_buf_size;
}

static void priskv_ucx_conn_close_cb(ucs_status_t status, void *arg)
{
    priskv_transport_conn *client = arg;
    priskv_log_error("UCX: <%s - %s> ep close, name %s, status: %s\n", client->local_addr,
                     client->peer_addr, client->ep->name, ucs_status_string(status));
    priskv_ucx_mark_client_closed(client);
}

static inline void priskv_ucx_client_efd_progress(int fd, void *opaque, uint32_t ev)
{
    priskv_log_debug("UCX: client efd progress event %d, worker %p, efd %d\n", ev, opaque, fd);
    priskv_ucx_worker_progress(opaque);
}

static inline void priskv_ucx_client_connfd_progress(int fd, void *opaque, uint32_t ev)
{
    priskv_transport_conn *client = opaque;
    priskv_log_error("UCX: <%s - %s> ep close, name %s\n", client->local_addr, client->peer_addr,
                     client->ep->name);
    priskv_ucx_mark_client_closed(client);
}

static inline int priskv_ucx_verify_conn_cap(priskv_transport_conn_cap *client,
                                             priskv_transport_conn_cap *listener, uint64_t *val)
{
    if (!client->max_sgl) {
        client->max_sgl = listener->max_sgl;
    } else if (client->max_sgl > listener->max_sgl) {
        *val = listener->max_sgl;
        return PRISKV_CM_REJ_STATUS_INVALID_SGL;
    }

    if (!client->max_key_length) {
        client->max_key_length = listener->max_key_length;
    } else if (client->max_key_length > listener->max_key_length) {
        *val = listener->max_key_length;
        return PRISKV_CM_REJ_STATUS_INVALID_KEY_LENGTH;
    }

    if (!client->max_inflight_command) {
        client->max_inflight_command = listener->max_inflight_command;
    } else if (client->max_inflight_command > listener->max_inflight_command) {
        *val = listener->max_inflight_command;
        return PRISKV_CM_REJ_STATUS_INVALID_INFLIGHT_COMMAND;
    }

    return 0;
}

static inline void priskv_ucx_reject(priskv_transport_conn *client, priskv_cm_status status,
                                     uint64_t value)
{
    priskv_cm_ucx_handshake rej_msg_be = {
        .flag = 0,
        .version = htobe16(PRISKV_CM_VERSION),
        .status = htobe16(status),
        .value = htobe64(value),
    };
    int ret = priskv_safe_send(client->connfd, &rej_msg_be, sizeof(rej_msg_be), NULL, NULL);
    if (ret < 0) {
        priskv_log_error("UCX: send reject message failed: %m\n");
    }
}

static inline int priskv_ucx_accept(priskv_transport_conn *client)
{
    uint32_t address_len = client->worker->address_len;
    size_t hs_size = sizeof(priskv_cm_ucx_handshake) + address_len;
    priskv_cm_ucx_handshake *hs = malloc(hs_size);
    if (ucs_unlikely(!hs)) {
        priskv_log_error("UCX: malloc accept message failed: %m\n");
        return -1;
    }

    hs->flag = 1;
    hs->cap.version = htobe16(client->conn_cap.version);
    hs->cap.max_sgl = htobe16(client->conn_cap.max_sgl);
    hs->cap.max_key_length = htobe16(client->conn_cap.max_key_length);
    hs->cap.max_inflight_command = htobe16(client->conn_cap.max_inflight_command);
    hs->cap.capacity = htobe64(client->conn_cap.capacity);
    hs->cap.shm_pid = htobe32(client->conn_cap.shm_pid);
    hs->cap.shm_fd = htobe32(client->conn_cap.shm_fd);
    hs->address_len = htobe32(address_len);
    memcpy(hs->address, client->worker->address, address_len);

    if (priskv_get_log_level() >= priskv_log_debug) {
        size_t print_len = address_len > 128 ? 128 : address_len;
        char worker_address_hex[print_len * 2 + 1];
        priskv_ucx_to_hex(worker_address_hex, client->worker->address, print_len);
        priskv_log_debug(
            "UCX: send worker address to client %s, address_len %d, address (first %d) %s\n",
            client->peer_addr, address_len, print_len, worker_address_hex);
    }

    int ret = priskv_safe_send(client->connfd, hs, hs_size, NULL, NULL);
    if (ret < 0) {
        priskv_log_error("UCX: send accept message failed: %m\n");
        goto out_free_msg;
    }

    priskv_log_info("UCX: <%s - %s> accept connect request, name %s\n", client->local_addr,
                    client->peer_addr, client->ep->name);

out_free_msg:
    free(hs);
    return ret;
}

static inline void priskv_ucx_handle_cm(int fd, void *opaque, uint32_t ev)
{
    priskv_log_debug("UCX: listener efd progress event %d, listener %p, efd %d\n", ev, opaque, fd);

    priskv_transport_conn *listener = opaque;
    int ret;
    struct sockaddr_storage client_addr;
    socklen_t client_addr_len;
    int connfd;
    char peer_addr[PRISKV_ADDR_LEN];
    priskv_cm_ucx_handshake peer_hs;
    priskv_cm_status status;
    uint64_t value = 0;
    uint8_t *peer_worker_address = NULL;

    assert(listener->listenfd == fd);

    client_addr_len = sizeof(client_addr);
    connfd = accept(listener->listenfd, (struct sockaddr *)&client_addr, &client_addr_len);
    if (connfd < 0) {
        priskv_log_error("UCX: accept on listenfd %d failed: %m\n", listener->listenfd);
        return;
    }

    priskv_inet_ntop(&client_addr, peer_addr);
    priskv_log_info("UCX: accept on listenfd %d, connfd %d, client addr %s\n", listener->listenfd,
                    connfd, peer_addr);

    priskv_transport_conn *client = calloc(1, sizeof(priskv_transport_conn));
    assert(client);
    client->ep = NULL;
    client->inflight_reqs = NULL;
    client->c.listener = listener;
    client->c.thread = NULL;
    client->c.closing = false;
    client->connfd = connfd;
    list_node_init(&client->c.node);
    pthread_spin_init(&client->lock, 0);

    const char *local_addr = listener->local_addr;
    snprintf(client->local_addr, PRISKV_ADDR_LEN, "%s", local_addr);
    snprintf(client->peer_addr, PRISKV_ADDR_LEN, "%s", peer_addr);

    pthread_spin_lock(&listener->lock);
    list_add_tail(&listener->s.head, &client->c.node);
    listener->s.nclients++;
    pthread_spin_unlock(&listener->lock);

    /* #step0, recv handshake msg */
    ret = priskv_safe_recv(connfd, &peer_hs, sizeof(peer_hs), NULL, NULL);
    if (ret < 0) {
        priskv_log_error("UCX: recv handshake msg failed: %m\n");
        ucs_close_fd(&connfd);
        return;
    }

    client->conn_cap.version = be16toh(peer_hs.cap.version);
    client->conn_cap.max_sgl = be16toh(peer_hs.cap.max_sgl);
    client->conn_cap.max_key_length = be32toh(peer_hs.cap.max_key_length);
    client->conn_cap.max_inflight_command = be16toh(peer_hs.cap.max_inflight_command);
    client->conn_cap.shm_pid = listener->conn_cap.shm_pid;
    client->conn_cap.shm_fd = listener->conn_cap.shm_fd;
    size_t peer_worker_address_len = be32toh(peer_hs.address_len);

    if (peer_worker_address_len > 0) {
        peer_worker_address = malloc(peer_worker_address_len);
        if (!peer_worker_address) {
            priskv_log_error("UCX: malloc peer address failed: %m\n");
            ucs_close_fd(&connfd);
            return;
        }
        ret = priskv_safe_recv(connfd, peer_worker_address, peer_worker_address_len, NULL, NULL);
        if (ret < 0) {
            priskv_log_error("UCX: recv peer address failed: %m\n");
            ucs_close_fd(&connfd);
            return;
        }
    }

    priskv_log_info("UCX: <%s - %s> incoming connect request - version %d, max_sgl %d, "
                    "max_key_length %d, max_inflight_command %d, address_len %d\n",
                    local_addr, peer_addr, client->conn_cap.version, client->conn_cap.max_sgl,
                    client->conn_cap.max_key_length, client->conn_cap.max_inflight_command,
                    peer_worker_address_len);

    if (client->conn_cap.version != PRISKV_CM_VERSION) {
        status = PRISKV_CM_REJ_STATUS_INVALID_VERSION;
        value = PRISKV_CM_VERSION;
        goto rej;
    }

    if (!peer_worker_address) {
        priskv_log_error("UCX: <%s - %s> peer worker address is empty\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_INVALID_WORKER_ADDR;
        value = 0;
        goto rej;
    }

    if (priskv_get_log_level() >= priskv_log_debug) {
        size_t print_len = peer_worker_address_len > 128 ? 128 : peer_worker_address_len;
        char worker_address_hex[print_len * 2 + 1];
        priskv_ucx_to_hex(worker_address_hex, peer_worker_address, print_len);
        priskv_log_debug(
            "UCX: got peer worker address from client %s, address_len %d, address (first %d) %s\n",
            peer_addr, peer_worker_address_len, print_len, worker_address_hex);
    }

    status = priskv_ucx_verify_conn_cap(&client->conn_cap, &listener->conn_cap, &value);
    if (status) {
        goto rej;
    }

    /* #step1, ACL verification */
    if (priskv_acl_verify((struct sockaddr *)&client_addr)) {
        priskv_log_error("UCX: <%s - %s> ACL verification failed\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_ACL_REFUSE;
        value = 0;
        goto rej;
    }

    client->worker = priskv_ucx_worker_create(g_transport_server.context, 0);
    if (client->worker == NULL) {
        priskv_log_error("UCX: <%s - %s> create worker failed\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        value = 0;
        goto rej;
    }

    client->ep = priskv_ucx_ep_create_from_worker_addr(client->worker, peer_worker_address,
                                                       priskv_ucx_conn_close_cb, client);
    if (client->ep == NULL) {
        priskv_log_error("UCX: <%s - %s> create ep failed\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    free(peer_worker_address);
    peer_worker_address = NULL;

    /* #step2, create related resources */
    if (priskv_ucx_new_ctrl_buffer(client)) {
        priskv_log_error("UCX: <%s - %s> create ctrl buffer failed, name %s\n", local_addr,
                         peer_addr, client->ep->name);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    /* #step3, post tag recv */
    uint32_t wr_size = priskv_ucx_wr_size(client);
    uint8_t *recv_req = client->rmem[PRISKV_TRANSPORT_MEM_REQ].buf;
    for (uint16_t i = 0; i < wr_size; i++) {
        int recvsize = priskv_ucx_recv_req(client, recv_req);
        if (recvsize < 0) {
            status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
            goto rej;
        }

        recv_req += recvsize;
    }

    /* #step4, initialize KV of client */
    client->value_base = client->c.listener->value_base;
    client->kv = client->c.listener->kv;
    client->value_memh.ucx_memh = client->c.listener->value_memh.ucx_memh;

    /* use the idlest worker thread to drive the progress */
    priskv_set_fd_handler(client->worker->efd, priskv_ucx_client_efd_progress, NULL,
                          client->worker);
    client->c.thread = priskv_threadpool_find_iothread(g_threadpool);
    priskv_thread_add_event_handler(client->c.thread, client->worker->efd);
    priskv_log_debug("UCX: <%s - %s> assign worker efd %d to thread %d\n", local_addr, peer_addr,
                     client->worker->efd, client->c.thread);

    if (priskv_set_nonblock(client->connfd)) {
        priskv_log_error("UCX: <%s - %s> failed to set nonblock mode for connfd\n", local_addr,
                         peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    if (priskv_ucx_accept(client) < 0) {
        priskv_log_error("UCX: <%s - %s> accept failed, name %s\n", local_addr, peer_addr,
                         client->ep->name);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    priskv_set_fd_handler(client->connfd, priskv_ucx_client_connfd_progress, NULL, client);
    priskv_thread_add_event_handler(client->c.thread, client->connfd);
    priskv_log_debug("UCX: <%s - %s> assign connfd %d to thread %d\n", local_addr, peer_addr,
                     client->connfd, client->c.thread);

    priskv_log_notice("UCX: <%s - %s> established\n", local_addr, peer_addr);

    return;

rej:
    priskv_log_warn("UCX: <%s - %s> %s, reject\n", local_addr, peer_addr,
                    priskv_cm_status_str(status));
    if (peer_worker_address) {
        free(peer_worker_address);
        peer_worker_address = NULL;
    }
    priskv_ucx_reject(client, status, value);
    priskv_ucx_mark_client_closed(client);
}

static int priskv_ucx_listen_one(char *addr, int port, void *kv, priskv_transport_conn_cap *cap)
{
    int ret;
    int listenfd = -1;
    int optval = 1;
    struct addrinfo hints, *res, *t;
    char service[8];
    char err_str[64];
    ucs_status_t status;

    ucs_snprintf_safe(service, sizeof(service), "%u", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    ret = getaddrinfo(addr, service, &hints, &res);
    if (ret < 0) {
        priskv_log_error("UCX: getaddrinfo failed, server %s, port %s, error %s\n", addr, service,
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
        listenfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
        if (listenfd < 0) {
            snprintf(err_str, 64, "socket failed: %m\n");
            continue;
        }

        status = ucs_socket_setopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));
        if (status != UCS_OK) {
            snprintf(err_str, 64, "setopt failed: %m\n");
            continue;
        }

        ret = priskv_set_nonblock(listenfd);
        if (ret) {
            snprintf(err_str, 64, "set NONBLOCK failed: %m\n");
            continue;
        }

        if (bind(listenfd, t->ai_addr, t->ai_addrlen) == 0) {
            break;
        }

        snprintf(err_str, 64, "bind failed: %m\n");
        ucs_close_fd(&listenfd);
        listenfd = -1;
    }

    if (listenfd < 0) {
        priskv_log_error("UCX: bind failed, server %s, port %s, error %s\n", addr, service,
                         err_str);
        ret = -1;
        goto out_free_res;
    }

    ret = listen(listenfd, 0);
    if (ret < 0) {
        priskv_log_error("UCX: listen failed: %m\n");
        ret = -1;
        goto err_close_listenfd;
    }

    priskv_transport_conn *listener = &g_transport_server.listeners[g_transport_server.nlisteners];

    uint8_t *value_base = priskv_get_value_base(kv);
    assert(value_base);
    uint64_t size = priskv_get_value_blocks(kv) * priskv_get_value_block_size(kv);
    assert(size);
    listener->value_memh.ucx_memh =
        priskv_ucx_mmap(g_transport_server.context, value_base, size, UCS_MEMORY_TYPE_HOST);
    if (!listener->value_memh.ucx_memh) {
        ret = -1;
        priskv_log_error(
            "UCX: failed to reg MR for value: %m [%p, %p], value block %ld, value block size %d\n",
            value_base, value_base + size, priskv_get_value_blocks(kv),
            priskv_get_value_block_size(kv));
        goto err_close_listenfd;
    }

    priskv_log_debug("UCX: Value buffer %p, length %ld\n", value_base, size);

    listener->listenfd = listenfd;
    listener->value_base = value_base;
    listener->kv = kv;
    listener->conn_cap = *cap;
    listener->conn_cap.capacity = size;
    listener->conn_cap.shm_pid = getpid();
    listener->conn_cap.shm_fd = priskv_get_shm_fd(kv);
    listener->s.nclients = 0;
    list_head_init(&listener->s.head);
    pthread_spin_init(&listener->lock, 0);

    priskv_log_debug("UCX: shm pid %d, shm fd %d\n", listener->conn_cap.shm_pid,
                     listener->conn_cap.shm_fd);

    listener->efd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (listener->efd < 0) {
        priskv_log_error("UCX: failed to create eventfd %m\n");
        ret = -1;
        goto err_close_listenfd;
    }

    priskv_inet_ntop(t->ai_addr, listener->local_addr);
    priskv_log_info("UCX: <%s> listener starts\n", listener->local_addr);

    g_transport_server.nlisteners++;
    ret = 0;
    goto out_free_res;

err_close_listenfd:
    ucs_close_fd(&listenfd);
out_free_res:
    freeaddrinfo(res);
out:
    return ret;
}

static int priskv_ucx_listen(char **addrs, int naddrs, int port, void *kv,
                             priskv_transport_conn_cap *cap)
{
    priskv_transport_conn *listener;
    int efd;

    for (int i = 0; i < naddrs; i++) {
        int ret = priskv_ucx_listen_one(addrs[i], port, kv, cap);
        if (ret) {
            return ret;
        }
    }

    g_transport_server.kv = kv;

    g_transport_server.epollfd = epoll_create(g_transport_server.nlisteners);
    if (g_transport_server.epollfd == -1) {
        priskv_log_error("UCX: failed to create epoll fd %m\n");
        return -1;
    }

    for (int i = 0; i < g_transport_server.nlisteners; i++) {
        listener = &g_transport_server.listeners[i];

        priskv_set_fd_handler(listener->listenfd, priskv_ucx_handle_cm, NULL, listener);
        if (priskv_add_event_fd(g_transport_server.epollfd, listener->listenfd)) {
            priskv_log_error("UCX: failed to add listenfd into epoll fd %m\n");
            return -1;
        }

        priskv_set_fd_handler(listener->efd, NULL, NULL, NULL);
        if (priskv_add_event_fd(g_transport_server.epollfd, listener->efd)) {
            priskv_log_error("UCX: failed to add eventfd into epoll fd %m\n");
            return -1;
        }

        priskv_log_notice("UCX: <%s> ready\n", listener->local_addr);
    }

    return 0;
}

static int priskv_ucx_get_fd(void)
{
    return g_transport_server.epollfd;
}

static void *priskv_ucx_get_kv(void)
{
    return g_transport_server.kv;
}

static int priskv_ucx_mem_new(priskv_transport_conn *conn, priskv_transport_mem *rmem,
                              const char *name, uint32_t size)
{
    bool guard = true; /* always enable memory guard */
    uint8_t *buf;
    int ret;

    buf = priskv_mem_malloc(size, MAP_PRIVATE | MAP_ANONYMOUS, -1, guard);
    if (!buf) {
        priskv_log_error("UCX: failed to allocate %s buffer: %m\n", name);
        ret = -ENOMEM;
        goto error;
    }

    rmem->memh.ucx_memh =
        priskv_ucx_mmap(g_transport_server.context, buf, size, UCS_MEMORY_TYPE_HOST);
    if (!rmem->memh.ucx_memh) {
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
    priskv_mem_free(rmem->buf, rmem->buf_size, guard);

error:
    memset(rmem, 0x00, sizeof(priskv_transport_mem));

    return ret;
}

static inline void priskv_ucx_mem_free(priskv_transport_conn *conn, priskv_transport_mem *rmem)
{
    if (rmem->memh.ucx_memh) {
        priskv_ucx_munmap(rmem->memh.ucx_memh);
        rmem->memh.ucx_memh = NULL;
    }

    if (rmem->buf) {
        priskv_log_debug("UCX: free rmem %s, buf %p\n", rmem->name, rmem->buf);
        priskv_mem_free(rmem->buf, rmem->buf_size, true);
    }

    priskv_log_info("UCX: free rmem %s, size %d\n", rmem->name, rmem->buf_size);
    memset(rmem, 0x00, sizeof(priskv_transport_mem));
}

static int priskv_ucx_new_ctrl_buffer(priskv_transport_conn *conn)
{
    uint16_t size;
    uint32_t buf_size;

    /* #step 1, prepare buffer & MR for request from client */
    size =
        priskv_ucx_max_request_size_aligned(conn->conn_cap.max_sgl, conn->conn_cap.max_key_length);
    buf_size = (uint32_t)size * priskv_ucx_wr_size(conn);
    if (priskv_ucx_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_REQ], "Request", buf_size)) {
        goto error;
    }

    /* #step 2, prepare buffer & MR for response to client */
    size = sizeof(priskv_response);
    buf_size = size * priskv_ucx_wr_size(conn);
    if (priskv_ucx_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_RESP], "Response", buf_size)) {
        goto error;
    }

    for (uint16_t i = 0; i < priskv_ucx_wr_size(conn); i++) {
        priskv_response *resp =
            (priskv_response *)(conn->rmem[PRISKV_TRANSPORT_MEM_RESP].buf + i * size);
        priskv_ucx_response_free(resp);
    }

    return 0;

error:
    priskv_ucx_free_ctrl_buffer(conn);
    return -ENOMEM;
}

static inline void priskv_ucx_free_ctrl_buffer(priskv_transport_conn *conn)
{
    for (int i = 0; i < PRISKV_TRANSPORT_MEM_MAX; i++) {
        priskv_transport_mem *rmem = &conn->rmem[i];

        priskv_ucx_mem_free(conn, rmem);
    }
}

static inline void priskv_ucx_cancel_client_requests(priskv_transport_conn *client)
{
    priskv_ucx_request *req, *tmp;

    HASH_ITER(hh, client->inflight_reqs, req, tmp)
    {
        HASH_DEL(client->inflight_reqs, req);
        if (!req->handle) {
            continue;
        }
        priskv_ucx_request_cancel(req);
    }
}

static void priskv_ucx_close_client(priskv_transport_conn *client)
{
    priskv_log_notice("UCX: <%s - %s> close. name %s. Requests GET %ld, SET %ld, TEST %ld, "
                      "DELETE %ld, Responses %ld\n",
                      client->local_addr, client->peer_addr, client->ep->name,
                      client->c.stats[PRISKV_COMMAND_GET].ops,
                      client->c.stats[PRISKV_COMMAND_SET].ops,
                      client->c.stats[PRISKV_COMMAND_TEST].ops,
                      client->c.stats[PRISKV_COMMAND_DELETE].ops, client->c.resps);

    if ((client->worker) && (client->c.thread != NULL)) {
        priskv_thread_call_function(client->c.thread, priskv_ucx_cancel_client_requests, client);
        priskv_thread_del_event_handler(client->c.thread, client->worker->efd);
        priskv_thread_del_event_handler(client->c.thread, client->connfd);
        priskv_set_fd_handler(client->worker->efd, NULL, NULL, NULL); /* clear fd handler */
        priskv_set_fd_handler(client->connfd, NULL, NULL, NULL);      /* clear fd handler */
        client->c.thread = NULL;
    }

    priskv_ucx_free_ctrl_buffer(client);

    if (client->ep) {
        priskv_ucx_ep_destroy(client->ep);
        client->ep = NULL;
    }

    if (client->worker) {
        priskv_ucx_worker_destroy(client->worker);
        client->worker = NULL;
    }

    free(client);
}

static void priskv_ucx_get_clients(priskv_transport_conn *listener,
                                   priskv_transport_client **clients, int *nclients)
{
    priskv_transport_conn *client;
    *nclients = 0;

    pthread_spin_lock(&listener->lock);
    *clients = calloc(listener->s.nclients, sizeof(priskv_transport_client));
    list_for_each (&listener->s.head, client, c.node) {
        const char *peer_addr = client->peer_addr;
        memcpy((*clients)[*nclients].address, peer_addr, strlen(peer_addr) + 1);
        memcpy((*clients)[*nclients].stats, client->c.stats,
               PRISKV_COMMAND_MAX * sizeof(priskv_transport_stats));
        (*clients)[*nclients].resps = client->c.resps;
        (*clients)[*nclients].closing = client->c.closing;
        (*nclients)++;

        if (*nclients == listener->s.nclients) {
            break;
        }
    }
    pthread_spin_unlock(&listener->lock);
}

static priskv_transport_listener *priskv_ucx_get_listeners(int *nlisteners)
{
    priskv_transport_listener *listeners;

    *nlisteners = g_transport_server.nlisteners;
    listeners = calloc(*nlisteners, sizeof(priskv_transport_listener));

    for (int i = 0; i < *nlisteners; i++) {
        const char *local_addr = g_transport_server.listeners[i].local_addr;
        memcpy(listeners[i].address, local_addr, strlen(local_addr) + 1);
        priskv_ucx_get_clients(&g_transport_server.listeners[i], &listeners[i].clients,
                               &listeners[i].nclients);
    }

    return listeners;
}

void priskv_ucx_free_listeners(priskv_transport_listener *listeners, int nlisteners)
{
    for (int i = 0; i < nlisteners; i++) {
        free(listeners[i].clients);
    }
    free(listeners);
}

static priskv_response *priskv_ucx_unused_response(priskv_transport_conn *conn)
{
    uint16_t resp_buf_size = sizeof(priskv_response);
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];

    for (uint16_t i = 0; i < priskv_ucx_wr_size(conn); i++) {
        priskv_response *resp = (priskv_response *)(rmem->buf + i * resp_buf_size);
        if (resp->status == PRISKV_UCX_RESPONSE_FREE_STATUS) {
            priskv_log_debug("UCX: use response %d\n", i);
            resp->status = PRISKV_RESP_STATUS_OK;
            return resp;
        }
    }

    priskv_log_error("UCX: <%s - %s> inflight response exceeds %d\n", conn->local_addr,
                     conn->peer_addr, priskv_ucx_wr_size(conn));
    return NULL;
}

static void priskv_ucx_send_response_cb(ucs_status_t status, void *arg)
{
    if (ucs_unlikely(arg == NULL)) {
        priskv_log_error("UCX: priskv_ucx_send_response_cb, arg is NULL\n");
        return;
    }

    priskv_ucx_conn_aux *conn_resp = arg;
    priskv_transport_conn *conn = conn_resp->conn;
    priskv_response *resp = conn_resp->aux;
    priskv_ucx_request *req;
    free(conn_resp);

    HASH_FIND_PTR(conn->inflight_reqs, &resp, req);
    if (req) {
        priskv_log_debug("UCX: remove request %p from inflight_reqs\n", req);
        HASH_DEL(conn->inflight_reqs, req);
    }

    if (status != UCS_OK) {
        priskv_log_error(
            "UCX: priskv_ucx_send_response_cb, status: %s, response: %p, request_id: %lu\n",
            ucs_status_string(status), resp, resp->request_id);
        priskv_ucx_mark_client_closed(conn);
        return;
    }
    priskv_ucx_response_free(resp);
}

static int priskv_ucx_send_response(priskv_transport_conn *conn, uint64_t request_id,
                                    priskv_resp_status status, uint32_t length)
{
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];
    priskv_response *resp;

    resp = priskv_ucx_unused_response(conn);
    if (!resp) {
        return -EPROTO;
    }

    assert(((uint8_t *)resp >= rmem->buf) && ((uint8_t *)resp < rmem->buf + rmem->buf_size));

    resp->request_id = request_id; /* be64 */
    resp->status = htobe16(status);
    resp->length = htobe32(length);

    priskv_ucx_conn_aux *conn_resp = malloc(sizeof(priskv_ucx_conn_aux));
    if (ucs_unlikely(conn_resp == NULL)) {
        priskv_log_error("UCX: priskv_ucx_send_response, malloc conn_resp failed\n");
        return -ENOMEM;
    }

    conn_resp->conn = conn;
    conn_resp->aux = resp;
    ucs_status_ptr_t handle =
        priskv_ucx_ep_post_tag_send(conn->ep, resp, sizeof(priskv_response), PRISKV_PROTO_TAG_CTRL,
                                    priskv_ucx_send_response_cb, conn_resp);
    if (UCS_PTR_IS_ERR(handle)) {
        ucs_status_t ucs_status = UCS_PTR_STATUS(handle);
        priskv_log_error("UCX: <%s - %s> priskv_ucx_ep_post_tag_send response failed: addr 0x%lx, "
                         "length 0x%x status %s\n",
                         conn->local_addr, conn->peer_addr, (uint64_t)resp, sizeof(priskv_response),
                         ucs_status_string(ucs_status));
        return -EIO;
    } else if (UCS_PTR_IS_PTR(handle)) {
        // still in progress
        priskv_ucx_request *req = handle;
        if (req->status == UCS_INPROGRESS) {
            req->key = conn;
            HASH_ADD_PTR(conn->inflight_reqs, key, req);
        }
    } else {
        // Operation completed immediately
    }

    conn->c.resps++;
    return 0;
}

static void priskv_ucx_rw_req_cb(ucs_status_t status, void *arg)
{
    struct timeval server_data_recv_time;

    priskv_transport_rw_work *work = arg;
    priskv_request *req = (priskv_request *)work->req;
    priskv_transport_conn *conn = work->conn;

    priskv_ucx_request *handle = NULL;
    HASH_FIND_PTR(conn->inflight_reqs, &work, handle);
    if (handle) {
        priskv_log_debug("UCX: remove request %p from inflight_reqs\n", handle);
        HASH_DEL(conn->inflight_reqs, handle);
    }

    gettimeofday(&server_data_recv_time, NULL);
    req->runtime.server_data_recv_time = server_data_recv_time;

    if (work->ucx_rkey) {
        priskv_ucx_rkey_destroy(work->ucx_rkey);
    }

    if (priskv_transport_handle_rw(conn, work)) {
        priskv_ucx_mark_client_closed(conn);
    }
}

static int priskv_ucx_rw_req(priskv_transport_conn *conn, priskv_request *req,
                             priskv_transport_memh *memh, uint8_t *val, uint32_t valuelen, bool set,
                             void (*cb)(void *), void *cbarg, bool defer_resp,
                             priskv_transport_rw_work **work_out)
{
    priskv_transport_rw_work *work;
    uint32_t offset = 0;
    uint16_t nsgl = be16toh(req->nsgl);
    const char *cmdstr = set ? "READ" : "WRITE";

    if (work_out) {
        *work_out = NULL;
    }

    work = calloc(1, sizeof(priskv_transport_rw_work));
    if (!work) {
        priskv_log_error("UCX: failed to allocate memory for %s request\n", cmdstr);
        return -ENOMEM;
    }

    const char *local_addr = conn->local_addr;
    const char *peer_addr = conn->peer_addr;

    work->conn = conn;
    work->req = req;
    work->memh.ucx_memh = memh->ucx_memh;
    work->ucx_rkey = NULL;
    work->request_id = req->request_id; /* be64 */
    work->valuelen = valuelen;
    work->completed = 0;
    work->cb = cb;
    work->cbarg = cbarg;
    work->defer_resp = defer_resp;

    uint8_t *keyed_sgl_base = (uint8_t *)req->sgls;
    for (uint16_t i = 0; i < nsgl; i++) {
        priskv_keyed_sgl *sgl = (priskv_keyed_sgl *)keyed_sgl_base;

        uint64_t remote_base = be64toh(sgl->addr);
        uint32_t sgl_length = be32toh(sgl->length);
        uint32_t packed_rkey_len = be32toh(sgl->packed_rkey_len);
        uint32_t runtime_offset = 0;
        keyed_sgl_base += sizeof(priskv_keyed_sgl) + packed_rkey_len;

        if (priskv_get_log_level() >= priskv_log_debug) {
            char rkey_hex[priskv_ucx_max_rkey_len * 2 + 1];
            priskv_ucx_to_hex(rkey_hex, sgl->packed_rkey, packed_rkey_len);
            priskv_log_debug("UCX: got rkey (%d) %s\n", packed_rkey_len, rkey_hex);
        }

        work->ucx_rkey = priskv_ucx_rkey_create(conn->ep, sgl->packed_rkey);
        if (ucs_unlikely(!work->ucx_rkey)) {
            priskv_log_error("UCX: <%s - %s> priskv_ucx_rkey_create failed: %m\n", local_addr,
                             peer_addr);
            free(work);
            return -EPROTO;
        }

        do {
            uint64_t local_base = (uint64_t)val + offset + runtime_offset;
            uint32_t length = priskv_min_u32(sgl_length - runtime_offset, valuelen);
            length = priskv_min_u32(length, priskv_ucx_max_rw_size);
            // need to go before post_get/put in case of immediate completion,
            // which will check nsgl
            work->nsgl++;

            ucs_status_ptr_t handle = NULL;
            if (set) {
                handle = priskv_ucx_ep_post_get(conn->ep, local_base, length, work->ucx_rkey,
                                                remote_base + runtime_offset, priskv_ucx_rw_req_cb,
                                                work);
            } else {
                handle = priskv_ucx_ep_post_put(conn->ep, local_base, length, work->ucx_rkey,
                                                remote_base + runtime_offset, priskv_ucx_rw_req_cb,
                                                work);
            }

            if (UCS_PTR_IS_ERR(handle)) {
                ucs_status_t status = UCS_PTR_STATUS(handle);
                priskv_log_error("UCX: <%s - %s> priskv_ucx_ep_post_put/get failed: addr 0x%lx, "
                                 "length 0x%x status %s\n",
                                 local_addr, peer_addr, local_base, length,
                                 ucs_status_string(status));
                // ucx will return UCS_ERR_ALREADY_EXISTS if the same buffer is posted again
                // to avoid memory corruption, we only log the error and consider it as completed
                if (status != UCS_ERR_ALREADY_EXISTS) {
                    return -EIO;
                }
            } else if (UCS_PTR_IS_PTR(handle)) {
                // still in progress
                priskv_ucx_request *request = (priskv_ucx_request *)handle;
                if (request->status == UCS_INPROGRESS) {
                    request->key = work;
                    HASH_ADD_PTR(conn->inflight_reqs, key, request);
                }
            } else {
                // Operation completed immediately
            }

            priskv_log_debug("UCX: %s [%d/%d]:[%d/%d], val %p, length 0x%x\n", cmdstr, i, nsgl,
                             runtime_offset, sgl_length, val + offset + runtime_offset, length);

            runtime_offset += length;
        } while (runtime_offset < priskv_min_u32(sgl_length, valuelen));

        offset += sgl_length;
        valuelen -= sgl_length;
    }

    if (work_out) {
        *work_out = work;
    }

    return 0;
}

priskv_transport_driver priskv_transport_driver_ucx = {
    .name = "ucx",
    .init = priskv_ucx_init,
    .listen = priskv_ucx_listen,
    .get_fd = priskv_ucx_get_fd,
    .get_kv = priskv_ucx_get_kv,
    .get_listeners = priskv_ucx_get_listeners,
    .free_listeners = priskv_ucx_free_listeners,
    .send_response = priskv_ucx_send_response,
    .rw_req = priskv_ucx_rw_req,
    .recv_req = priskv_ucx_recv_req,
    .mem_new = priskv_ucx_mem_new,
    .mem_free = priskv_ucx_mem_free,
    .request_key_off = priskv_ucx_request_key_off,
    .request_key = priskv_ucx_request_key,
    .close_client = priskv_ucx_close_client,
};
