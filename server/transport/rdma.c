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

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>
#include <rdma/rdma_cma.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <sys/time.h>

#include "../acl.h"
#include "../backend/backend.h"
#include "../crc.h"
#include "../kv.h"
#include "../memory.h"
#include "transport.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "priskv-event.h"
#include "priskv-threads.h"
#include "list.h"

#define PRISKV_RDMA_DEF_ADDR(id)                                                                   \
    char local_addr[PRISKV_ADDR_LEN] = {0};                                                        \
    char peer_addr[PRISKV_ADDR_LEN] = {0};                                                         \
    priskv_inet_ntop(rdma_get_local_addr(id), local_addr);                                         \
    priskv_inet_ntop(rdma_get_peer_addr(id), peer_addr);

extern priskv_transport_server g_transport_server;
extern priskv_threadpool *g_threadpool;

static uint32_t priskv_rdma_max_rw_size = 1024 * 1024 * 1024;

static void priskv_rdma_handle_cm(int fd, void *opaque, uint32_t events);

static int priskv_rdma_mem_new(priskv_transport_conn *conn, priskv_transport_mem *rmem,
                               const char *name, uint32_t size)
{
    uint32_t flags = IBV_ACCESS_LOCAL_WRITE;
    bool guard = true; /* always enable memory guard */
    uint8_t *buf;
    int ret;

    buf = priskv_mem_malloc(size, MAP_PRIVATE | MAP_ANONYMOUS, -1, guard);
    if (!buf) {
        priskv_log_error("RDMA: failed to allocate %s buffer: %m\n", name);
        ret = -ENOMEM;
        goto error;
    }

    rmem->memh.rdma_mr = ibv_reg_mr(conn->cm_id->pd, buf, size, flags);
    if (!rmem->memh.rdma_mr) {
        priskv_log_error("RDMA: failed to reg MR for %s buffer: %m\n", name);
        ret = -errno;
        goto free_mem;
    }

    strncpy(rmem->name, name, PRISKV_TRANSPORT_MEM_NAME_LEN - 1);
    rmem->buf = buf;
    rmem->buf_size = size;

    priskv_log_info("RDMA: new rmem %s, size %d\n", name, size);
    priskv_log_debug("RDMA: new rmem %s, buf %p\n", name, buf);
    return 0;

free_mem:
    priskv_mem_free(rmem->buf, rmem->buf_size, guard);

error:
    memset(rmem, 0x00, sizeof(priskv_transport_mem));

    return ret;
}

static void priskv_rdma_mem_free(priskv_transport_conn *conn, priskv_transport_mem *rmem)
{
    if (rmem->memh.rdma_mr) {
        ibv_dereg_mr(rmem->memh.rdma_mr);
    }

    if (rmem->buf) {
        priskv_log_debug("RDMA: free rmem %s, buf %p\n", rmem->name, rmem->buf);
        priskv_mem_free(rmem->buf, rmem->buf_size, true);
    }

    priskv_log_info("RDMA: free rmem %s, size %d\n", rmem->name, rmem->buf_size);
    memset(rmem, 0x00, sizeof(priskv_transport_mem));
}

static inline void priskv_rdma_free_ctrl_buffer(priskv_transport_conn *conn)
{
    for (int i = 0; i < PRISKV_TRANSPORT_MEM_MAX; i++) {
        priskv_transport_mem *rmem = &conn->rmem[i];

        priskv_rdma_mem_free(conn, rmem);
    }
}

static int priskv_rdma_listen_one(char *addr, int port, void *kv, priskv_transport_conn_cap *cap)
{
    int ret = 0, afonly = 1;
    char _port[6]; /* strlen("65535") */
    struct rdma_addrinfo hints, *servinfo;
    struct rdma_cm_id *listen_cmid = NULL;
    struct rdma_event_channel *listen_channel = NULL;
    priskv_transport_conn *listener;

    snprintf(_port, 6, "%d", port);
    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = RAI_PASSIVE;
    hints.ai_port_space = RDMA_PS_TCP;
    ret = rdma_getaddrinfo(addr, _port, &hints, &servinfo);
    if (ret) {
        priskv_log_error("RDMA: getaddrinfo %s failed: %s\n", addr, gai_strerror(ret));
        return ret;
    } else if (!servinfo) {
        priskv_log_error("RDMA: getaddrinfo %s: no availabe address\n", addr);
        return -EINVAL;
    }

    listen_channel = rdma_create_event_channel();
    if (!listen_channel) {
        ret = -errno;
        priskv_log_error("RDMA: create event channel failed\n");
        goto freeaddr;
    }

    ret = priskv_set_nonblock(listen_channel->fd);
    if (ret) {
        priskv_log_error("RDMA: failed to set NONBLOCK on event channel fd\n");
        goto error;
    }

    if (rdma_create_id(listen_channel, &listen_cmid, NULL, RDMA_PS_TCP)) {
        ret = -errno;
        priskv_log_error("RDMA: create listen cm id error\n");
        goto error;
    }

    rdma_set_option(listen_cmid, RDMA_OPTION_ID, RDMA_OPTION_ID_AFONLY, &afonly, sizeof(afonly));

    if (rdma_bind_addr(listen_cmid, servinfo->ai_src_addr)) {
        ret = -errno;
        priskv_log_error("RDMA: Bind addr error on %s\n", addr);
        goto error;
    }

    if (rdma_listen(listen_cmid, 0)) {
        ret = -errno;
        priskv_log_error("RDMA: listen addr error on %s\n", addr);
        goto error;
    }

    /* TODO split into several MRs, because of max_mr_size of IB device */
    uint8_t *value_base = priskv_get_value_base(kv);
    assert(value_base);
    uint64_t size = priskv_get_value_blocks(kv) * priskv_get_value_block_size(kv);
    assert(size);
    uint32_t access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    struct ibv_mr *value_mr = ibv_reg_mr(listen_cmid->pd, value_base, size, access);
    if (!value_mr) {
        ret = -errno;
        priskv_log_error(
            "RDMA: failed to reg MR for value: %m [%p, %p], value block %ld, value block size %d\n",
            value_base, value_base + size, priskv_get_value_blocks(kv),
            priskv_get_value_block_size(kv));
        goto error;
    }

    priskv_log_debug("RDMA: Value buffer %p, length %ld\n", value_base, size);

    listener = &g_transport_server.listeners[g_transport_server.nlisteners++];
    listener->cm_id = listen_cmid;
    listener->value_base = value_base;
    listener->kv = kv;
    listener->value_memh.rdma_mr = value_mr;
    listener->conn_cap = *cap;
    listener->s.nclients = 0;
    list_head_init(&listener->s.head);
    pthread_spin_init(&listener->lock, 0);

    priskv_log_info("RDMA: <%s:%d> listener starts\n", addr, port);

    ret = 0;
    goto freeaddr;

error:
    if (listen_cmid) {
        rdma_destroy_id(listen_cmid);
    }
    if (listen_channel) {
        rdma_destroy_event_channel(listen_channel);
    }

freeaddr:
    rdma_freeaddrinfo(servinfo);
    return ret;
}

int priskv_rdma_listen(char **addr, int naddr, int port, void *kv, priskv_transport_conn_cap *cap)
{
    priskv_transport_conn *listener;

    for (int i = 0; i < naddr; i++) {
        int ret = priskv_rdma_listen_one(addr[i], port, kv, cap);
        if (ret) {
            return ret;
        }
    }

    g_transport_server.kv = kv;

    g_transport_server.epollfd = epoll_create(g_transport_server.nlisteners);
    if (g_transport_server.epollfd == -1) {
        priskv_log_error("RDMA: failed to create epoll fd %m\n");
        return -1;
    }

    for (int i = 0; i < g_transport_server.nlisteners; i++) {
        listener = &g_transport_server.listeners[i];
        PRISKV_RDMA_DEF_ADDR(listener->cm_id);

        priskv_set_fd_handler(listener->cm_id->channel->fd, priskv_rdma_handle_cm, NULL, listener);
        if (priskv_add_event_fd(g_transport_server.epollfd, listener->cm_id->channel->fd)) {
            priskv_log_error("RDMA: failed to add listen fd into epoll fd %m\n");
            return -1;
        }

        priskv_log_notice("RDMA: <%s> ready\n", local_addr);
    }

    return 0;
}

static void priskv_rdma_get_clients(priskv_transport_conn *listener,
                                    priskv_transport_client **clients, int *nclients)
{
    priskv_transport_conn *client;
    *nclients = 0;

    pthread_spin_lock(&listener->lock);
    *clients = calloc(listener->s.nclients, sizeof(priskv_transport_client));
    list_for_each (&listener->s.head, client, c.node) {
        PRISKV_RDMA_DEF_ADDR(client->cm_id);

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

static void priskv_rdma_free_clients(priskv_transport_client *clients)
{
    free(clients);
}

priskv_transport_listener *priskv_rdma_get_listeners(int *nlisteners)
{
    priskv_transport_listener *listeners;

    *nlisteners = g_transport_server.nlisteners;
    listeners = calloc(*nlisteners, sizeof(priskv_transport_listener));

    for (int i = 0; i < *nlisteners; i++) {
        PRISKV_RDMA_DEF_ADDR(g_transport_server.listeners[i].cm_id);

        memcpy(listeners[i].address, local_addr, strlen(local_addr) + 1);
        priskv_rdma_get_clients(&g_transport_server.listeners[i], &listeners[i].clients,
                                &listeners[i].nclients);
    }

    return listeners;
}

void priskv_rdma_free_listeners(priskv_transport_listener *listeners, int nlisteners)
{
    for (int i = 0; i < nlisteners; i++) {
        priskv_rdma_free_clients(listeners[i].clients);
    }
    free(listeners);
}

int priskv_rdma_get_fd(void)
{
    return g_transport_server.epollfd;
}

void *priskv_rdma_get_kv(void)
{
    return g_transport_server.kv;
}

#define PRISKV_RDMA_RESPONSE_FREE_STATUS 0xffff
static inline int priskv_rdma_response_free(priskv_response *resp)
{
    if (resp->status == PRISKV_RDMA_RESPONSE_FREE_STATUS) {
        return -EPROTO;
    }

    resp->status = PRISKV_RDMA_RESPONSE_FREE_STATUS;
    return 0;
}

static inline uint32_t priskv_rdma_wr_size(priskv_transport_conn *client)
{
    return client->conn_cap.max_inflight_command * (2 + client->conn_cap.max_sgl);
}

static int priskv_rdma_new_ctrl_buffer(priskv_transport_conn *conn)
{
    uint16_t size;
    uint32_t buf_size;

    /* #step 1, prepare buffer & MR for request from client */
    size =
        priskv_rdma_max_request_size_aligned(conn->conn_cap.max_sgl, conn->conn_cap.max_key_length);
    buf_size = (uint32_t)size * priskv_rdma_wr_size(conn);
    if (priskv_rdma_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_REQ], "Request", buf_size)) {
        goto error;
    }

    /* #step 2, prepare buffer & MR for response to client */
    size = sizeof(priskv_response);
    buf_size = size * priskv_rdma_wr_size(conn);
    if (priskv_rdma_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_RESP], "Response", buf_size)) {
        goto error;
    }

    for (uint16_t i = 0; i < priskv_rdma_wr_size(conn); i++) {
        priskv_response *resp =
            (priskv_response *)(conn->rmem[PRISKV_TRANSPORT_MEM_RESP].buf + i * size);
        priskv_rdma_response_free(resp);
    }

    return 0;

error:
    priskv_rdma_free_ctrl_buffer(conn);
    return -ENOMEM;
}

static void priskv_rdma_close_client(priskv_transport_conn *client)
{
    PRISKV_RDMA_DEF_ADDR(client->cm_id)
    priskv_log_notice(
        "RDMA: <%s - %s> close. Requests GET %ld, SET %ld, TEST %ld, DELETE %ld, Responses %ld\n",
        local_addr, peer_addr, client->c.stats[PRISKV_COMMAND_GET].ops,
        client->c.stats[PRISKV_COMMAND_SET].ops, client->c.stats[PRISKV_COMMAND_TEST].ops,
        client->c.stats[PRISKV_COMMAND_DELETE].ops, client->c.resps);

    if ((client->comp_channel) && (client->c.thread != NULL)) {
        priskv_thread_del_event_handler(client->c.thread, client->comp_channel->fd);
        priskv_set_fd_handler(client->comp_channel->fd, NULL, NULL, NULL); /* clear fd handler */
        client->c.thread = NULL;
    }

    if (client->cm_id && client->cm_id->qp) {
        rdma_destroy_qp(client->cm_id);
        client->cm_id->qp = NULL;
    }

    if (client->cq) {
        if (ibv_destroy_cq(client->cq)) {
            priskv_log_warn("ibv_destroy_cq failed\n");
        }
        client->cq = NULL;
    }

    if (client->comp_channel) {
        if (ibv_destroy_comp_channel(client->comp_channel)) {
            priskv_log_warn("ibv_destroy_comp_channel failed\n");
        }
        client->comp_channel = NULL;
    }

    priskv_rdma_free_ctrl_buffer(client);

    if (client->cm_id) {
        rdma_destroy_id(client->cm_id);
    }

    free(client);
}

static priskv_response *priskv_rdma_unused_response(priskv_transport_conn *conn)
{
    uint16_t resp_buf_size = sizeof(priskv_response);
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];

    for (uint16_t i = 0; i < priskv_rdma_wr_size(conn); i++) {
        priskv_response *resp = (priskv_response *)(rmem->buf + i * resp_buf_size);
        if (resp->status == PRISKV_RDMA_RESPONSE_FREE_STATUS) {
            priskv_log_debug("RDMA: use response %d\n", i);
            resp->status = PRISKV_RESP_STATUS_OK;
            return resp;
        }
    }

    priskv_log_error("RDMA: <%s - %s> inflight response exceeds %d\n", conn->local_addr,
                     conn->peer_addr, priskv_rdma_wr_size(conn));
    return NULL;
}

static int priskv_rdma_send_response(priskv_transport_conn *conn, uint64_t request_id,
                                     priskv_resp_status status, uint32_t length)
{
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];
    struct ibv_send_wr wr = {0}, *bad_wr;
    struct ibv_sge rsge;
    priskv_response *resp;

    resp = priskv_rdma_unused_response(conn);
    if (!resp) {
        return -EPROTO;
    }

    assert(((uint8_t *)resp >= rmem->buf) && ((uint8_t *)resp < rmem->buf + rmem->buf_size));

    resp->request_id = request_id; /* be64 */
    resp->status = htobe16(status);
    resp->length = htobe32(length);

    rsge.addr = (uint64_t)resp;
    rsge.length = sizeof(priskv_response);
    rsge.lkey = rmem->memh.rdma_mr->lkey;

    wr.wr_id = (uint64_t)resp;
    wr.sg_list = &rsge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED;

    int ret = ibv_post_send(conn->cm_id->qp, &wr, &bad_wr);
    if (ret) {
        PRISKV_RDMA_DEF_ADDR(conn->cm_id)
        priskv_log_error(
            "RDMA: <%s - %s> ibv_post_send response failed: addr 0x%lx, length 0x%x ret %d\n",
            local_addr, peer_addr, rsge.addr, rsge.length, ret);
    } else {
        conn->c.resps++;
    }

    return ret;
}

static int priskv_rdma_rw_req(priskv_transport_conn *conn, priskv_request *req,
                              priskv_transport_memh *memh, uint8_t *val, uint32_t valuelen,
                              bool set, void (*cb)(void *), void *cbarg, bool defer_resp,
                              priskv_transport_rw_work **work_out)
{
    priskv_transport_rw_work *work;
    uint32_t offset = 0;
    struct ibv_send_wr wr = {0}, *bad_wr;
    struct ibv_sge sge;
    uint16_t nsgl = be16toh(req->nsgl);
    const char *cmdstr = set ? "READ" : "WRITE";

    if (work_out) {
        *work_out = NULL;
    }

    work = calloc(1, sizeof(priskv_transport_rw_work));
    if (!work) {
        priskv_log_error("RDMA: failed to allocate memory for %s request\n", cmdstr);
        return -ENOMEM;
    }

    work->conn = conn;
    work->req = req;
    work->memh.rdma_mr = memh->rdma_mr;
    work->request_id = req->request_id; /* be64 */
    work->valuelen = valuelen;
    work->completed = 0;
    work->cb = cb;
    work->cbarg = cbarg;
    work->defer_resp = defer_resp;

    wr.wr_id = (uint64_t)work;
    wr.next = NULL;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.opcode = set ? IBV_WR_RDMA_READ : IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_SIGNALED;

    for (uint16_t i = 0; i < nsgl; i++) {
        priskv_keyed_sgl *sgl = &req->sgls[i];

        uint32_t sgl_length = be32toh(sgl->length);
        uint32_t sgl_offset = 0;

        wr.wr.rdma.rkey = be32toh(sgl->key);
        work->rdma_rkey = wr.wr.rdma.rkey;
        do {
            wr.wr.rdma.remote_addr = be64toh(sgl->addr) + sgl_offset;

            sge.addr = (uint64_t)val + offset + sgl_offset;
            sge.lkey = memh->rdma_mr->lkey;
            sge.length = priskv_min_u32(sgl_length - sgl_offset, valuelen);
            sge.length = priskv_min_u32(sge.length, priskv_rdma_max_rw_size);

            if (ibv_post_send(conn->cm_id->qp, &wr, &bad_wr)) {
                PRISKV_RDMA_DEF_ADDR(conn->cm_id)
                priskv_log_error("RDMA: <%s - %s> ibv_post_send RDMA failed: %m\n", local_addr,
                                 peer_addr);
                free(work);
                return -errno;
            }

            priskv_log_debug(
                "RDMA: %s [%d/%d]:[%d/%d] wr_id 0x%lx, val %p, length 0x%x, addr 0x%lx, "
                "rkey 0x%x\n",
                cmdstr, i, nsgl, sgl_offset, sgl_length, wr.wr_id, val + offset + sgl_offset,
                sge.length, wr.wr.rdma.remote_addr, wr.wr.rdma.rkey);
            sgl_offset += sge.length;

            work->nsgl++;
        } while (sgl_offset < priskv_min_u32(sgl_length, valuelen));

        offset += sgl_length;
        valuelen -= sgl_length;
    }

    if (work_out) {
        *work_out = work;
    }

    return 0;
}

static int priskv_rdma_recv_req(priskv_transport_conn *conn, uint8_t *req);

static inline int priskv_rdma_handle_send(priskv_transport_conn *conn, priskv_response *resp,
                                          uint32_t len)
{
    return priskv_rdma_response_free(resp);
}

/* return negative number on failure, return received buffer size on success */
static int priskv_rdma_recv_req(priskv_transport_conn *conn, uint8_t *req)
{
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];
    uint32_t lkey = rmem->memh.rdma_mr->lkey;
    uint16_t req_buf_size =
        priskv_rdma_max_request_size_aligned(conn->conn_cap.max_sgl, conn->conn_cap.max_key_length);
    int ret;

    assert((req >= rmem->buf) && (req < rmem->buf + rmem->buf_size));
    sge.addr = (uint64_t)req;
    sge.length = req_buf_size;
    sge.lkey = lkey;

    recv_wr.wr_id = (uint64_t)req;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    priskv_log_debug("RDMA: ibv_post_recv addr %p, length %d\n", req, req_buf_size);
    ret = ibv_post_recv(conn->cm_id->qp, &recv_wr, &bad_wr);
    if (ret) {
        PRISKV_RDMA_DEF_ADDR(conn->cm_id)
        priskv_log_error("RDMA: <%s - %s> ibv_post_recv failed: %m\n", local_addr, peer_addr);
        return -errno;
    }

    return req_buf_size;
}

static void priskv_rdma_handle_cq(int fd, void *opaque, uint32_t events)
{
    priskv_transport_conn *conn = opaque;
    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc;
    priskv_request *req;
    priskv_transport_rw_work *work;
    priskv_response *resp;
    int ret;

    assert(conn->comp_channel->fd == fd);

    if (ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx) < 0) {
        if (errno != EAGAIN) {
            priskv_log_warn("RDMA: ibv_get_cq_event failed: %m\n");
        }
        goto error_close;
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
        priskv_log_warn("RDMA: ibv_req_notify_cq failed: %m\n");
        goto error_close;
    }

    ibv_ack_cq_events(conn->cq, 1);

poll_cq:
    ret = ibv_poll_cq(conn->cq, 1, &wc);
    if (ret < 0) {
        priskv_log_warn("RDMA: ibv_poll_cq failed: %m\n");
        goto error_close;
    } else if (ret == 0) {
        return;
    }

    priskv_log_debug("RDMA: CQ handle status: %s[0x%x], wr_id: %p, opcode: 0x%x, byte_len: %u\n",
                     ibv_wc_status_str(wc.status), wc.status, (void *)wc.wr_id, wc.opcode,
                     wc.byte_len);
    if (wc.status != IBV_WC_SUCCESS) {
        PRISKV_RDMA_DEF_ADDR(conn->cm_id)
        priskv_log_error("RDMA: <%s - %s> CQ error status: wr_id 0x%lx, %s[0x%x], opcode : 0x%x, "
                         "byte_len : %ld\n",
                         local_addr, peer_addr, wc.wr_id, ibv_wc_status_str(wc.status), wc.status,
                         wc.opcode, wc.byte_len);
        if (wc.status == IBV_WC_LOC_QP_OP_ERR) {
            priskv_log_error("RDMA: possible remote command size exceeds\n");
        }
        goto error_close;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV: {
        struct timeval server_metadata_recv_time;

        req = (priskv_request *)wc.wr_id;

        gettimeofday(&server_metadata_recv_time, NULL);
        req->runtime.server_metadata_recv_time = server_metadata_recv_time;

        if (priskv_transport_handle_recv(conn, req, wc.byte_len)) {
            goto error_close;
        }
        break;
    }

    case IBV_WC_RDMA_READ:
    case IBV_WC_RDMA_WRITE: {
        struct timeval server_data_recv_time;

        work = (priskv_transport_rw_work *)wc.wr_id;
        req = (priskv_request *)work->req;

        gettimeofday(&server_data_recv_time, NULL);
        req->runtime.server_data_recv_time = server_data_recv_time;

        if (priskv_transport_handle_rw(conn, work)) {
            goto error_close;
        }

        break;
    }

    case IBV_WC_SEND: {
        resp = (priskv_response *)wc.wr_id;
        priskv_rdma_handle_send(conn, resp, wc.byte_len);
        break;
    }

    default:
        priskv_log_error("unexpected opcode 0x%x\n", wc.opcode);
        goto error_close;
    }

    goto poll_cq;

error_close:
    priskv_transport_mark_client_closed(conn);
}

static void priskv_rdma_reject(struct rdma_cm_id *cm_id, uint16_t status, uint64_t val)
{
    priskv_cm_rej rej = {0};

    rej.version = htobe16(PRISKV_CM_VERSION);
    rej.status = htobe16(status);
    rej.value = htobe64(val);

    rdma_reject(cm_id, &rej, sizeof(priskv_cm_rej));
}

static int priskv_rdma_resp(priskv_transport_conn *client, struct rdma_cm_id *cm_id)
{
    void *kv = client->c.listener->kv;
    uint64_t capacity = priskv_get_value_blocks(kv) * priskv_get_value_block_size(kv);

    priskv_cm_cap rep = {0};
    rep.version = htobe16(PRISKV_CM_VERSION);
    rep.max_sgl = htobe16(client->conn_cap.max_sgl);
    rep.max_key_length = htobe16(client->conn_cap.max_key_length);
    rep.max_inflight_command = htobe16(client->conn_cap.max_inflight_command);
    rep.capacity = htobe64(capacity);
    // We don't use shared memory for kv transfer if RDMA is available
    rep.shm_fd = htobe32(-1);

    struct rdma_conn_param resp_param = {0};
    resp_param.responder_resources = 1;
    resp_param.initiator_depth = 1;
    resp_param.retry_count = 5;
    resp_param.private_data = &rep;
    resp_param.private_data_len = sizeof(rep);

    int ret = rdma_accept(cm_id, &resp_param);
    if (ret) {
        PRISKV_RDMA_DEF_ADDR(client->cm_id)
        priskv_log_error("RDMA: <%s - %s> rdma_accept failed: %m\n", local_addr, peer_addr);
    }

    return ret;
}

static int priskv_rdma_verify_conn_cap(priskv_transport_conn_cap *client,
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

static void priskv_rdma_handle_connect_request(struct rdma_cm_event *ev,
                                               priskv_transport_conn *listener)
{
    priskv_transport_conn *client;
    struct rdma_cm_id *id = ev->id;
    struct ibv_qp_init_attr init_attr = {0};
    struct rdma_conn_param *req_param = &ev->param.conn;
    unsigned char exp_len = sizeof(struct rdma_conn_param) + sizeof(priskv_cm_cap);
    priskv_cm_status status;
    uint64_t value = 0;

    PRISKV_RDMA_DEF_ADDR(id);

    client = calloc(1, sizeof(priskv_transport_conn));
    assert(client);
    id->context = client;
    client->cm_id = id;
    client->c.listener = listener;
    client->c.thread = NULL;
    client->c.closing = false;
    list_node_init(&client->c.node);
    pthread_spin_init(&client->lock, 0);

    snprintf(client->local_addr, PRISKV_ADDR_LEN, "%s", local_addr);
    snprintf(client->peer_addr, PRISKV_ADDR_LEN, "%s", peer_addr);

    pthread_spin_lock(&listener->lock);
    list_add_tail(&listener->s.head, &client->c.node);
    listener->s.nclients++;
    pthread_spin_unlock(&listener->lock);

    /* #step0, ACL verification */
    if (priskv_acl_verify(rdma_get_peer_addr(id))) {
        priskv_log_error("RDMA: <%s - %s> ACL verification failed\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_ACL_REFUSE;
        value = 0;
        goto rej;
    }

    /* #step1, check incoming request parameters */
    if (req_param->private_data_len != exp_len) {
        priskv_log_error("RDMA: <%s - %s> unexpected CM REQ length %d, expetected %d\n", local_addr,
                         peer_addr, req_param->private_data_len, exp_len);
        status = PRISKV_CM_REJ_STATUS_INVALID_CM_REP;
        value = exp_len;
        goto rej;
    }

    const priskv_cm_cap *req = req_param->private_data;
    uint16_t version = be16toh(req->version);
    if (version != PRISKV_CM_VERSION) {
        status = PRISKV_CM_REJ_STATUS_INVALID_VERSION;
        value = PRISKV_CM_VERSION;
        goto rej;
    }

    client->conn_cap.max_sgl = be16toh(req->max_sgl);
    client->conn_cap.max_key_length = be16toh(req->max_key_length);
    client->conn_cap.max_inflight_command = be16toh(req->max_inflight_command);
    priskv_log_info("RDMA: <%s - %s> incoming connect request - version %d, max_sgl %d, "
                    "max_key_length %d, max_inflight_command %d\n",
                    local_addr, peer_addr, version, client->conn_cap.max_sgl,
                    client->conn_cap.max_key_length, client->conn_cap.max_inflight_command);

    status = priskv_rdma_verify_conn_cap(&client->conn_cap, &listener->conn_cap, &value);
    if (status) {
        goto rej;
    }

    /* #step2, create QP and related resources */
    client->comp_channel = ibv_create_comp_channel(id->verbs);
    if (!client->comp_channel) {
        priskv_log_error("RDMA: <%s - %s> ibv_create_comp_channel failed: %m\n", local_addr,
                         peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    priskv_set_nonblock(client->comp_channel->fd);
    uint32_t wr_size = priskv_rdma_wr_size(client);
    client->cq = ibv_create_cq(id->verbs, wr_size * 2 * 4, NULL, client->comp_channel, 0);
    if (!client->cq) {
        priskv_log_error("RDMA: <%s - %s> ibv_create_cq failed: %m\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    ibv_req_notify_cq(client->cq, 0);

    init_attr.cap.max_send_wr = wr_size * 4;
    init_attr.cap.max_recv_wr = wr_size * 4;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = client->cq;
    init_attr.recv_cq = client->cq;
    if (rdma_create_qp(id, NULL, &init_attr)) {
        priskv_log_error("RDMA: <%s - %s> rdma_create_qp failed: %m\n", local_addr, peer_addr);
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    /* #step3, create QP and related resources */
    if (priskv_rdma_new_ctrl_buffer(client)) {
        status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
        goto rej;
    }

    /* #step4, post recv all the request commands */
    uint8_t *recv_req = client->rmem[PRISKV_TRANSPORT_MEM_REQ].buf;
    for (uint16_t i = 0; i < wr_size; i++) {
        int recvsize = priskv_rdma_recv_req(client, recv_req);
        if (recvsize < 0) {
            status = PRISKV_CM_REJ_STATUS_SERVER_ERROR;
            goto rej;
        }

        recv_req += recvsize;
    }

    /* #step5, accept the new client */
    if (priskv_rdma_resp(client, id)) {
        goto close_client;
    }

    priskv_log_info("RDMA: <%s - %s>  accept connect request - version %d, max_sgl %d, "
                    "max_key_length %d, max_inflight_command %d\n",
                    local_addr, peer_addr, version, client->conn_cap.max_sgl,
                    client->conn_cap.max_key_length, client->conn_cap.max_inflight_command);
    return;

rej:
    priskv_log_warn("RDMA: <%s - %s> %s, reject\n", local_addr, peer_addr,
                    priskv_cm_status_str(status));
    priskv_rdma_reject(id, status, value);

close_client:
    priskv_transport_mark_client_closed(client);
}

static void priskv_rdma_handle_established(struct rdma_cm_event *ev,
                                           priskv_transport_conn *listener)
{
    struct rdma_cm_id *id = ev->id;
    priskv_transport_conn *client = id->context;

    PRISKV_RDMA_DEF_ADDR(id);

    /* initialize KV of client */
    client->value_base = listener->value_base;
    client->kv = listener->kv;
    client->value_memh.rdma_mr = listener->value_memh.rdma_mr;

    /* use the idlest worker thread handle CQ event(CM event is still handled by main thread) */
    priskv_set_fd_handler(client->comp_channel->fd, priskv_rdma_handle_cq, NULL, client);
    client->c.thread = priskv_threadpool_find_iothread(g_threadpool);
    priskv_thread_add_event_handler(client->c.thread, client->comp_channel->fd);

    priskv_log_notice("RDMA: <%s - %s> established\n", local_addr, peer_addr);
    priskv_log_debug("RDMA: <%s - %s> assign CQ fd %d to thread %d\n", local_addr, peer_addr,
                     client->comp_channel->fd, client->c.thread);
}

static void priskv_rdma_handle_disconnected(struct rdma_cm_event *ev,
                                            priskv_transport_conn *listener)
{
    struct rdma_cm_id *id = ev->id;
    priskv_transport_conn *client = id->context;

    priskv_transport_mark_client_closed(client);
}

static void priskv_rdma_handle_cm(int fd, void *opaque, uint32_t events)
{
    priskv_transport_conn *listener = opaque;
    struct rdma_cm_event *ev;
    int ret;

    assert(listener->cm_id->channel->fd == fd);

again:
    ret = rdma_get_cm_event(listener->cm_id->channel, &ev);
    if (ret) {
        if (errno != EAGAIN) {
            priskv_log_error("RDMA: listener rdma_get_cm_event failed: %m\n");
        }
        return;
    }

    const char *evstr = rdma_event_str(ev->event);
    char addrbuf[64] = {0};
    priskv_inet_ntop(rdma_get_local_addr(listener->cm_id), addrbuf);
    priskv_log_debug("RDMA: listener<%s> cm event: %s\n", addrbuf, evstr);

    switch (ev->event) {
    case RDMA_CM_EVENT_CONNECT_REQUEST:
        priskv_rdma_handle_connect_request(ev, listener);
        break;

    case RDMA_CM_EVENT_ESTABLISHED:
        priskv_rdma_handle_established(ev, listener);
        break;

    case RDMA_CM_EVENT_DISCONNECTED:
        priskv_rdma_handle_disconnected(ev, listener);
        break;

    default:
        priskv_log_error("RDMA: listener<%s> listener unexpected cm event: %s\n", addrbuf, evstr);
    }

    rdma_ack_cm_event(ev);

    goto again;
}

priskv_transport_driver priskv_transport_driver_rdma = {
    .name = "rdma",
    .init = NULL,
    .listen = priskv_rdma_listen,
    .get_fd = priskv_rdma_get_fd,
    .get_kv = priskv_rdma_get_kv,
    .get_listeners = priskv_rdma_get_listeners,
    .free_listeners = priskv_rdma_free_listeners,
    .send_response = priskv_rdma_send_response,
    .rw_req = priskv_rdma_rw_req,
    .recv_req = priskv_rdma_recv_req,
    .mem_new = priskv_rdma_mem_new,
    .mem_free = priskv_rdma_mem_free,
    .request_key_off = priskv_rdma_request_key_off,
    .request_key = priskv_rdma_request_key,
    .close_client = priskv_rdma_close_client,
};
