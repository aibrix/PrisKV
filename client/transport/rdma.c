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
#include <sys/time.h>

#include "../priskv.h"
#include "priskv-threads.h"
#include "priskv-event.h"
#include "priskv-workqueue.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-utils.h"
#include "priskv-log.h"
#include "list.h"
#include "transport.h"

#define PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND 128

#define PRISKV_RDMA_DEF_ADDR(id)                                                                   \
    char local_addr[PRISKV_ADDR_LEN] = {0};                                                        \
    char peer_addr[PRISKV_ADDR_LEN] = {0};                                                         \
    priskv_inet_ntop(rdma_get_local_addr(id), local_addr);                                         \
    priskv_inet_ntop(rdma_get_peer_addr(id), peer_addr);

static int priskv_rdma_handle_cq(priskv_transport_conn *conn);
static int priskv_rdma_req_cb_intl(void *arg);
static int priskv_rdma_req_send(void *arg);
static inline void priskv_rdma_req_free(priskv_transport_req *rdma_req);
static inline void priskv_rdma_req_complete(priskv_transport_conn *conn);
static inline void priskv_rdma_req_reset(priskv_transport_req *rdma_req);
static inline priskv_transport_req *
priskv_rdma_req_new(priskv_client *client, priskv_transport_conn *conn, uint64_t request_id,
                    const char *key, uint16_t keylen, priskv_sgl *sgl, uint16_t nsgl,
                    uint64_t timeout, priskv_req_command cmd, priskv_generic_cb usercb);

static int priskv_rdma_mem_new(priskv_transport_conn *conn, priskv_transport_mem *rmem,
                               const char *name, uint32_t size, bool remote_write)
{
    uint32_t page_size = getpagesize();
    uint8_t *buf;
    uint32_t flags = IBV_ACCESS_LOCAL_WRITE;
    int ret;

    size = ALIGN_UP(size, page_size);
    buf = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (buf == MAP_FAILED) {
        priskv_log_error("RDMA: failed to allocate %s buffer: %m\n", name);
        ret = -ENOMEM;
        goto error;
    }

    if (remote_write) {
        flags |= IBV_ACCESS_REMOTE_WRITE;
    }

    rmem->mr = ibv_reg_mr(conn->cm_id->pd, buf, size, flags);
    if (!rmem->mr) {
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
    munmap(rmem->buf, rmem->buf_size);

error:
    memset(rmem, 0x00, sizeof(priskv_transport_mem));

    return ret;
}

static void priskv_rdma_mem_free(priskv_transport_conn *conn, priskv_transport_mem *rmem)
{
    if (rmem->mr) {
        ibv_dereg_mr(rmem->mr);
    }

    if (rmem->buf) {
        priskv_log_debug("RDMA: free rmem %s, buf %p\n", rmem->name, rmem->buf);
        munmap(rmem->buf, rmem->buf_size);
    }

    priskv_log_info("RDMA: free rmem %s, size %d\n", rmem->name, rmem->buf_size);
    memset(rmem, 0x00, sizeof(priskv_transport_mem));
}

static inline void priskv_rdma_mem_free_all(priskv_transport_conn *conn)
{
    for (int i = 0; i < PRISKV_TRANSPORT_MEM_MAX; i++) {
        priskv_transport_mem *rmem = &conn->rmem[i];

        priskv_rdma_mem_free(conn, rmem);
    }
}

#define PRISKV_RDMA_REQUEST_FREE_COMMAND 0xffff
static void priskv_rdma_request_free(priskv_request *req, priskv_transport_conn *conn)
{
    uint8_t *ptr = (uint8_t *)req;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];

    assert(ptr >= rmem->buf);
    assert(ptr < rmem->buf + rmem->buf_size);
    assert(!((ptr - rmem->buf) % priskv_rdma_max_request_size_aligned(conn->param.max_sgl,
                                                                      conn->param.max_key_length)));

    req->command = PRISKV_RDMA_REQUEST_FREE_COMMAND;
}

static int priskv_rdma_mem_new_all(priskv_transport_conn *conn)
{
    uint32_t page_size = getpagesize(), size;

    /* #step 1, prepare buffer & MR for request to server */
    int reqsize =
        priskv_rdma_max_request_size_aligned(conn->param.max_sgl, conn->param.max_key_length);
    size = reqsize * conn->param.max_inflight_command;
    if (priskv_rdma_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_REQ], "Request", size, false)) {
        goto error;
    }

    /* additional work: set priskv_request::command as PRISKV_RDMA_REQUEST_FREE_COMMAND */
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];
    for (uint16_t i = 0; i < conn->param.max_inflight_command; i++) {
        priskv_request *req = (priskv_request *)(rmem->buf + i * reqsize);
        priskv_rdma_request_free(req, conn);
    }

    /* #step 2, prepare buffer & MR for response from server */
    size = sizeof(priskv_response) * conn->param.max_inflight_command;
    if (priskv_rdma_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_RESP], "Response", size,
                            false)) {
        goto error;
    }

    /* #step 3, prepare buffer & MR for keys */
    size = page_size;
    if (priskv_rdma_mem_new(conn, &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS], "Keys", size, true)) {
        goto error;
    }

    return 0;

error:
    priskv_rdma_mem_free_all(conn);

    return -ENOMEM;
}

static priskv_request *priskv_rdma_unused_command(priskv_transport_conn *conn, uint16_t *idx)
{
    uint16_t req_buf_size =
        priskv_rdma_max_request_size_aligned(conn->param.max_sgl, conn->param.max_key_length);
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];

    for (uint16_t i = 0; i < conn->param.max_inflight_command; i++) {
        priskv_request *req = (priskv_request *)(rmem->buf + i * req_buf_size);
        if (req->command == PRISKV_RDMA_REQUEST_FREE_COMMAND) {
            priskv_log_debug("RDMA: use request %d\n", i);
            req->command = 0xc001;
            *idx = i;
            return req;
        }
    }

    return NULL;
}

static void priskv_rdma_close_conn(priskv_transport_conn *conn)
{
    priskv_transport_req *rdma_req, *tmp;

    if (conn->state == PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED) {
        PRISKV_RDMA_DEF_ADDR(conn->cm_id)
        priskv_log_notice("RDMA: <%s - %s> close. Requests GET %ld, SET %ld, TEST %ld, DELETE %ld, "
                          "Responses %ld\n",
                          local_addr, peer_addr, conn->stats[PRISKV_COMMAND_GET],
                          conn->stats[PRISKV_COMMAND_SET], conn->stats[PRISKV_COMMAND_TEST],
                          conn->stats[PRISKV_COMMAND_DELETE], conn->resps);
    }

    conn->state = PRISKV_TRANSPORT_CONN_STATE_CLOSED;

    priskv_rdma_req_complete(conn);

    list_for_each_safe (&conn->inflight_list, rdma_req, tmp, entry) {
        list_del(&rdma_req->entry);

        priskv_rdma_request_free(rdma_req->req, conn);
        rdma_req->status = PRISKV_STATUS_DISCONNECTED;
        rdma_req->cb(rdma_req);
    }

    if (conn->epollfd > 0) {
        close(conn->epollfd);
        conn->epollfd = -1;
    }

    if (conn->qp) {
        if (ibv_destroy_qp(conn->qp)) {
            priskv_log_warn("ibv_destroy_qp failed\n");
        }
        conn->qp = NULL;
    }

    if (conn->cq) {
        if (ibv_destroy_cq(conn->cq)) {
            priskv_log_warn("ibv_destroy_cq failed\n");
        }
        conn->cq = NULL;
    }

    if (conn->comp_channel) {
        if (ibv_destroy_comp_channel(conn->comp_channel)) {
            priskv_log_warn("ibv_destroy_comp_channel failed\n");
        }
        conn->comp_channel = NULL;
    }

    priskv_rdma_mem_free_all(conn);

    if (conn->cm_id) {
        if (rdma_destroy_id(conn->cm_id)) {
            priskv_log_warn("rdma_destroy_id failed\n");
        }
        conn->cm_id = NULL;
    }

    if (conn->cm_channel) {
        rdma_destroy_event_channel(conn->cm_channel);
        conn->cm_channel = NULL;
    }

    free(conn->keys_mems.mrs);
}

/* return negative number on failure, return received buffer size on success */
static int priskv_rdma_recv_resp(priskv_transport_conn *conn, priskv_response *resp)
{
    struct ibv_sge sge;
    struct ibv_recv_wr recv_wr, *bad_wr;
    uint16_t resp_buf_size = sizeof(priskv_response);
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];
    int ret;

    sge.addr = (uint64_t)resp;
    sge.length = resp_buf_size;
    sge.lkey = rmem->mr->lkey;

    recv_wr.wr_id = (uint64_t)resp;
    recv_wr.sg_list = &sge;
    recv_wr.num_sge = 1;
    recv_wr.next = NULL;

    priskv_log_debug("RDMA: ibv_post_recv addr %p, length %d\n", resp, resp_buf_size);
    ret = ibv_post_recv(conn->qp, &recv_wr, &bad_wr);
    if (ret) {
        priskv_log_error("RDMA: ibv_post_recv failed: ret %d, errno %m\n", ret);
        return -1;
    }

    return resp_buf_size;
}

static void priskv_rdma_cq_process(int fd, void *opaque, uint32_t ev)
{
    priskv_transport_conn *conn = opaque;

    priskv_rdma_handle_cq(conn);
}

static int priskv_rdma_new_qp(priskv_transport_conn *conn)
{
    struct ibv_qp_init_attr init_attr = {0};

    conn->comp_channel = ibv_create_comp_channel(conn->cm_id->verbs);
    if (!conn->comp_channel) {
        priskv_log_error("RDMA: ibv_create_comp_channel failed: %m\n");
        return -errno;
    }

    priskv_set_nonblock(conn->comp_channel->fd);

    priskv_set_fd_handler(conn->comp_channel->fd, priskv_rdma_cq_process, NULL, conn);
    priskv_add_event_fd(conn->epollfd, conn->comp_channel->fd);

    uint16_t depth = conn->param.max_inflight_command;
    if (!depth) {
        depth = PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND;
    }

    conn->cq = ibv_create_cq(conn->cm_id->verbs, depth * 2, NULL, conn->comp_channel, 0);
    if (!conn->cq) {
        priskv_log_error("RDMA: ibv_create_cq failed: %m\n");
        return -errno;
    }

    if (ibv_req_notify_cq(conn->cq, 0)) {
        priskv_log_error("RDMA: ibv_req_notify_cq failed: %m\n");
        return -errno;
    }

    init_attr.cap.max_send_wr = depth;
    init_attr.cap.max_recv_wr = depth;
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.send_cq = conn->cq;
    init_attr.recv_cq = conn->cq;
    conn->qp = ibv_create_qp(conn->cm_id->pd, &init_attr);
    if (!conn->qp) {
        priskv_log_error("RDMA: ibv_create_qp failed: %m\n");
        return -errno;
    }

    return 0;
}

static int priskv_rdma_connect(priskv_transport_conn *conn)
{
    struct rdma_cm_id *cm_id = conn->cm_id;
    struct rdma_conn_param conn_param = {0};
    priskv_connect_param *param = &conn->param;
    priskv_cm_cap cm_req = {0};
    int ret;

    ret = priskv_rdma_new_qp(conn);
    if (ret) {
        return ret;
    }

    cm_req.version = htobe16(PRISKV_CM_VERSION);
    cm_req.max_sgl = htobe16(param->max_sgl);
    cm_req.max_key_length = htobe16(param->max_key_length);
    cm_req.max_inflight_command = htobe16(param->max_inflight_command);

    conn_param.private_data = &cm_req;
    conn_param.private_data_len = sizeof(cm_req);
    conn_param.responder_resources = 1;
    conn_param.initiator_depth = 1;
    conn_param.retry_count = 2;
    conn_param.rnr_retry_count = 2;
    conn_param.qp_num = conn->qp->qp_num;
    ret = rdma_connect(cm_id, &conn_param);
    if (ret) {
        priskv_log_error("RDMA: rdma_connect failed: %m\n");
        return ret;
    }

    return 0;
}

static int priskv_rdma_establish_qp(priskv_transport_conn *conn)
{
    struct ibv_qp_attr qp_attr;
    int qp_attr_mask, ret;

    qp_attr.qp_state = IBV_QPS_INIT;
    ret = rdma_init_qp_attr(conn->cm_id, &qp_attr, &qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: rdma_init_qp_attr to INIT failed: %m\n");
        return ret;
    }

    ret = ibv_modify_qp(conn->qp, &qp_attr, qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: ibv_modify_qp to INIT failed: %m\n");
        return ret;
    }

    qp_attr.qp_state = IBV_QPS_RTR;
    ret = rdma_init_qp_attr(conn->cm_id, &qp_attr, &qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: rdma_init_qp_attr to RTR failed: %m\n");
        return ret;
    }

    ret = ibv_modify_qp(conn->qp, &qp_attr, qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: ibv_modify_qp to RTR failed: %m\n");
        return ret;
    }

    qp_attr.qp_state = IBV_QPS_RTS;
    ret = rdma_init_qp_attr(conn->cm_id, &qp_attr, &qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: rdma_init_qp_attr to RTS failed: %m\n");
        return ret;
    }

    ret = ibv_modify_qp(conn->qp, &qp_attr, qp_attr_mask);
    if (ret) {
        priskv_log_error("RDMA: ibv_modify_qp to RTS failed: %m\n");
        return ret;
    }

    ret = rdma_establish(conn->cm_id);
    if (ret) {
        priskv_log_error("RDMA: rdma_establish failed: %m\n");
        return ret;
    }

    return 0;
}

static int priskv_rdma_modify_max_inflight_command(priskv_transport_conn *conn,
                                                   uint16_t max_inflight_command)
{
    /* auto detect max_inflight_command from server */
    if (max_inflight_command == PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND) {
        conn->param.max_inflight_command = PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND;
        return 0; /* no need to change */
    }

    struct ibv_device_attr device_attr;
    if (ibv_query_device(conn->cm_id->verbs, &device_attr)) {
        priskv_log_warn("RDMA: ignore ibv_query_device failed: %m\n");
        return 0; /* not fatal error */
    }

    if (!(device_attr.device_cap_flags & IBV_DEVICE_RESIZE_MAX_WR)) {
        conn->param.max_inflight_command =
            priskv_min_u16(max_inflight_command, PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND);
        priskv_log_warn("RDMA: ignore modify max_inflight_command, use %d\n",
                        conn->param.max_inflight_command);
        return 0; /* not fatal error */
    }

    conn->param.max_inflight_command = max_inflight_command;

    int ret = ibv_resize_cq(conn->cq, conn->param.max_inflight_command * 2);
    if (ret) {
        priskv_log_error("RDMA: ibv_resize_cq failed %m\n");
        return ret;
    }

    priskv_log_info("RDMA: resize CQ to %d\n", conn->param.max_inflight_command);
    struct ibv_qp_attr qp_attr = {0};

    qp_attr.cap.max_send_wr = conn->param.max_inflight_command;
    qp_attr.cap.max_recv_wr = conn->param.max_inflight_command;
    qp_attr.cap.max_send_sge = 1;
    qp_attr.cap.max_recv_sge = 1;
    ret = ibv_modify_qp(conn->qp, &qp_attr, IBV_QP_CAP);
    if (ret) {
        priskv_log_error("RDMA: ibv_modify_qp CAP failed %m\n");
        return -ret;
    }

    priskv_log_info("RDMA: modify QP WR to %d\n", conn->param.max_inflight_command);

    return 0;
}

static int priskv_rdma_responsed(struct rdma_cm_event *ev, priskv_transport_conn *conn)
{
    struct rdma_conn_param *rep_param = &ev->param.conn;
    unsigned char exp_len = sizeof(priskv_cm_cap);
    int ret = -EPROTO;

    if (rep_param->private_data_len < exp_len) {
        priskv_log_error("RDMA: unexpected CM REQ length %d, expetected %d\n",
                         rep_param->private_data_len, exp_len);
        return -EPROTO;
    }

    priskv_cm_cap *rep = (priskv_cm_cap *)rep_param->private_data;
    uint16_t version = be16toh(rep->version);
    conn->param.max_sgl = be16toh(rep->max_sgl);
    conn->param.max_key_length = be16toh(rep->max_key_length);
    uint16_t max_inflight_command = be16toh(rep->max_inflight_command);
    conn->capacity = be64toh(rep->capacity);
    priskv_log_info(
        "RDMA: response version %d, max_sgl %d, max_key_length %d, max_inflight_command "
        "%d, capacity %ld from server\n",
        version, conn->param.max_sgl, conn->param.max_key_length, max_inflight_command,
        conn->capacity);

    ret = priskv_rdma_modify_max_inflight_command(conn, max_inflight_command);
    if (ret) {
        return ret;
    }

    priskv_log_info("RDMA: update connection parameters, max_sgl %d, max_key_length %d, "
                    "max_inflight_command %d\n",
                    conn->param.max_sgl, conn->param.max_key_length,
                    conn->param.max_inflight_command);

    ret = priskv_rdma_establish_qp(conn);
    if (ret) {
        return ret;
    }

    ret = priskv_rdma_mem_new_all(conn);
    if (ret) {
        return ret;
    }

    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_RESP];
    priskv_response *resp = (priskv_response *)rmem->buf;
    for (int i = 0; i < conn->param.max_inflight_command; i++) {
        ret = priskv_rdma_recv_resp(conn, resp + i);
        if (ret < 0) {
            return ret;
        }
    }

    return 0;
}

static int priskv_rdma_rejected(struct rdma_cm_event *ev, priskv_transport_conn *conn)
{
    struct rdma_conn_param *rep_param = &ev->param.conn;
    unsigned char exp_len = sizeof(priskv_cm_rej);

    if (rep_param->private_data_len < exp_len) {
        priskv_log_error("RDMA: unexpected REJECT REQ length %d, expetected %d\n",
                         rep_param->private_data_len, exp_len);
        return -EPROTO;
    }

    priskv_cm_rej *rej = (priskv_cm_rej *)rep_param->private_data;
    uint16_t version = be16toh(rej->version);
    uint16_t status = be16toh(rej->status);
    uint64_t value = be64toh(rej->value);
    priskv_log_error("RDMA: reject version %d, status: %s(%d), supported value %ld from server\n",
                     version, priskv_cm_status_str(status), status, value);

    return -ECONNREFUSED;
}

static int priskv_rdma_handle_cm_event(priskv_transport_conn *conn)
{
    struct rdma_event_channel *cm_channel = conn->cm_id->channel;
    struct rdma_cm_event *ev;
    enum rdma_cm_event_type ev_type;
    int ret;

again:
    ret = rdma_get_cm_event(cm_channel, &ev);
    if (ret) {
        if (errno == EAGAIN) {
            return 0;
        }
        return ret;
    }

    ev_type = ev->event;
    priskv_log_info("RDMA: conn %p, cm event: %s\n", conn, rdma_event_str(ev_type));

    switch (ev_type) {
    case RDMA_CM_EVENT_ADDR_RESOLVED:
        ret = rdma_resolve_route(ev->id, 1000);
        if (ret) {
            priskv_log_error("RMDA: rdma_resolve_route failed: %m\n");
        }
        break;

    case RDMA_CM_EVENT_ROUTE_RESOLVED:
        ret = priskv_rdma_connect(conn);
        break;

    case RDMA_CM_EVENT_CONNECT_RESPONSE:
        ret = priskv_rdma_responsed(ev, conn);
        conn->state = PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED;
        break;

    case RDMA_CM_EVENT_REJECTED:
        ret = priskv_rdma_rejected(ev, conn);
        break;

    case RDMA_CM_EVENT_DISCONNECTED:
        conn->state = PRISKV_TRANSPORT_CONN_STATE_CLOSED;
        break;

    default:
        priskv_log_error("RDMA: unexpected cm event: %s\n", rdma_event_str(ev_type));
        ret = -ECONNREFUSED;
    };

    rdma_ack_cm_event(ev);

    if (ret) {
        return ret;
    }

    goto again;
}

static int priskv_rdma_wait_established(priskv_transport_conn *conn)
{
    struct epoll_event event;
    struct timeval start, end;
    int ret;

    gettimeofday(&start, NULL);

    while (conn->state == PRISKV_TRANSPORT_CONN_STATE_INIT) {
        ret = epoll_wait(conn->epollfd, &event, 1, 1000);
        if (ret < 0) {
            if (errno == EINTR) {
                continue;
            }
            priskv_log_error("RDMA: epoll_wait failed on CM event\n");
            return -errno;
        } else if (ret == 0) {
            priskv_log_error("RDMA: connect timeout\n");
            return -ETIMEDOUT;
        }

        ret = priskv_rdma_handle_cm_event(conn);
        if (ret) {
            return ret;
        }
    }

    gettimeofday(&end, NULL);
    PRISKV_RDMA_DEF_ADDR(conn->cm_id)
    priskv_log_debug("RDMA: <%s - %s> wait established delay %d us\n", local_addr, peer_addr,
                     priskv_time_elapsed_us(&start, &end));

    return 0;
}

static void priskv_rdma_cm_process(int fd, void *opaque, uint32_t ev)
{
    priskv_transport_conn *conn = opaque;

    priskv_rdma_handle_cm_event(conn);

    priskv_rdma_handle_cq(conn);
}

static priskv_transport_conn *priskv_rdma_conn_connect(const char *raddr, int rport,
                                                       const char *laddr, int lport)
{
    struct rdma_addrinfo hints = {0}, *addrinfo = NULL;
    priskv_transport_conn *conn = NULL;
    char _port[6]; /* strlen("65535") */

    conn = calloc(sizeof(struct priskv_transport_conn), 1);
    if (!conn) {
        priskv_log_error("RDMA: failed to allocate memory for RDMA connection\n");
        return NULL;
    }

    conn->param.max_sgl = 0;
    conn->param.max_key_length = 0;
    conn->param.max_inflight_command = PRISKV_RDMA_DEFAULT_INFLIGHT_COMMAND;

    list_head_init(&conn->inflight_list);
    list_head_init(&conn->complete_list);

    conn->keys_running_req = NULL;
    conn->keys_mems.count = 1;
    conn->keys_mems.mrs = calloc(sizeof(struct ibv_mr *), 1);

    conn->state = PRISKV_TRANSPORT_CONN_STATE_INIT;
    conn->epollfd = epoll_create1(0);
    if (conn->epollfd < 0) {
        priskv_log_error("RDMA: failed to create epoll fd\n");
        return NULL;
    }

    priskv_set_fd_handler(conn->epollfd, priskv_transport_conn_process, NULL, conn);

    priskv_set_nonblock(conn->epollfd);
    conn->cm_channel = rdma_create_event_channel();
    if (!conn->cm_channel) {
        priskv_log_error("RDMA: failed to create event channel\n");
        goto error;
    }

    if (priskv_set_nonblock(conn->cm_channel->fd)) {
        priskv_log_error("RDMA: failed to set NONBLOCK on event channel fd\n");
        goto error;
    }

    priskv_set_fd_handler(conn->cm_channel->fd, priskv_rdma_cm_process, NULL, conn);
    priskv_add_event_fd(conn->epollfd, conn->cm_channel->fd);

    if (rdma_create_id(conn->cm_channel, &conn->cm_id, NULL, RDMA_PS_TCP)) {
        priskv_log_error("RDMA: failed to create CM ID\n");
        goto error;
    }

    /* bind local address if user specify one */
    if (laddr) {
        snprintf(_port, 6, "%d", lport);
        hints.ai_flags = RAI_PASSIVE;
        hints.ai_port_space = RDMA_PS_TCP;
        if (rdma_getaddrinfo(laddr, _port, &hints, &addrinfo)) {
            priskv_log_error("RDMA: failed to get local addr info %s:%d\n", laddr, lport);
            goto error;
        }

        if (rdma_bind_addr(conn->cm_id, addrinfo->ai_src_addr)) {
            priskv_log_error("RDMA: failed to bind local addr info %s:%d\n", laddr, lport);
            goto error;
        }

        memset(&hints, 0x00, sizeof(hints));
        rdma_freeaddrinfo(addrinfo);
        addrinfo = NULL;
    }

    /* resolve remote address */
    snprintf(_port, 6, "%d", rport);
    hints.ai_port_space = RDMA_PS_TCP;
    if (rdma_getaddrinfo(raddr, _port, &hints, &addrinfo)) {
        priskv_log_error("RDMA: failed to get remote addr info %s:%d\n", raddr, rport);
        goto error;
    }

    if (rdma_resolve_addr(conn->cm_id, NULL, (struct sockaddr *)addrinfo->ai_dst_addr, 1000)) {
        priskv_log_error("RDMA: failed to resolve remote addr %s:%d\n", raddr, rport);
        goto error;
    }

    if (priskv_rdma_wait_established(conn)) {
        priskv_log_error("RDMA: failed to connect to %s:%d\n", raddr, rport);
        goto error;
    }

    goto free_addrinfo;

error:
    priskv_rdma_close_conn(conn);
    free(conn);
    conn = NULL;

free_addrinfo:
    if (addrinfo) {
        rdma_freeaddrinfo(addrinfo);
    }

    return conn;
}

int priskv_rdma_conn_close(void *conn)
{
    if (!conn) {
        return 0;
    }

    priskv_rdma_close_conn(conn);
    free(conn);

    return 0;
}

static struct ibv_mr *priskv_rdma_conn_reg_memory(priskv_transport_conn *conn, uint64_t offset,
                                                  size_t length, uint64_t iova, int fd)
{
    struct ibv_pd *pd = conn->cm_id->pd;
    unsigned int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    struct ibv_mr *mr = NULL;

    if (fd >= 0) {
#if LIBIBVERBS_VERSION_MINOR >= 12
        mr = ibv_reg_dmabuf_mr(pd, offset, length, iova, fd, access);
#else
        priskv_log_error("RDMA: the libibverbs before 1.12 does not support ibv_reg_dmabuf_mr\n");
#endif
    } else {
        mr = ibv_reg_mr_iova(pd, (void *)offset, length, iova, access);
    }

    if (!mr) {
        priskv_log_error(
            "RDMA: failed to reg mr 0x%lx:%ld %m. If you are using GPU memory, check if "
            "the nvidia_peermem module is installed\n",
            offset, length);
    }

    return mr;
}

static void priskv_rdma_conn_dereg_memory(struct ibv_mr *mr)
{
    ibv_dereg_mr(mr);
}

static int priskv_rdma_mq_init(priskv_client *client, const char *raddr, int rport,
                               const char *laddr, int lport, int nqueue)
{
    client->wq = priskv_workqueue_create(client->epollfd);
    if (!client->wq) {
        priskv_log_error("RDMA: failed to create workqueue\n");
        return -1;
    }

    client->pool = priskv_threadpool_create("priskv", nqueue, 0, 0);
    if (!client->pool) {
        priskv_log_error("RDMA: failed to create threadpool\n");
        return -1;
    }

    client->conns = calloc(nqueue, sizeof(priskv_transport_conn *));
    if (!client->conns) {
        priskv_log_error("RDMA: failed to allocate memory for connections\n");
        return -1;
    }
    client->nqueue = nqueue;

    for (uint8_t i = 0; i < nqueue; i++) {
        client->conns[i] = priskv_rdma_conn_connect(raddr, rport, laddr, lport);
        if (!client->conns[i]) {
            priskv_log_error("RDMA: failed to connect to %s:%d\n", raddr, rport);
            return -1;
        }

        client->conns[i]->id = i;
        client->conns[i]->thread = priskv_threadpool_get_iothread(client->pool, i);

        priskv_thread_add_event_handler(client->conns[i]->thread, client->conns[i]->epollfd);
    }

    client->cur_conn = 0;

    return 0;
}

static void priskv_rdma_mq_deinit(priskv_client *client)
{
    if (client->conns) {
        for (int i = 0; i < client->nqueue; i++) {
            priskv_thread_call_function(priskv_threadpool_get_iothread(client->pool, i),
                                        priskv_rdma_conn_close, client->conns[i]);
        }
    }

    priskv_threadpool_destroy(client->pool);
    priskv_workqueue_destroy(client->wq);
    free(client->conns);
}

static priskv_transport_conn *priskv_rdma_mq_select_conn(priskv_client *client)
{
    return client->conns[client->cur_conn++ % client->nqueue];
}

static priskv_memory *priskv_rdma_mq_reg_memory(priskv_client *client, uint64_t offset,
                                                size_t length, uint64_t iova, int fd)
{
    priskv_memory *mem = malloc(sizeof(priskv_memory));

    mem->client = client;
    mem->count = client->nqueue;
    mem->mrs = malloc(client->nqueue * sizeof(struct ibv_mr *));

    for (int i = 0; i < mem->count; i++) {
        mem->mrs[i] = priskv_rdma_conn_reg_memory(client->conns[i], offset, length, iova, fd);
    }

    return mem;
}

static void priskv_rdma_mq_dereg_memory(priskv_memory *mem)
{
    for (int i = 0; i < mem->count; i++) {
        priskv_rdma_conn_dereg_memory(mem->mrs[i]);
    }
    free(mem->mrs);
    free(mem);
}

static struct ibv_mr *priskv_rdma_mq_get_mr(priskv_memory *mem, int connid)
{
    return mem->mrs[connid];
}

static void priskv_rdma_mq_req_submit(priskv_transport_req *rdma_req)
{
    priskv_thread_submit_function(rdma_req->conn->thread, priskv_rdma_req_send, rdma_req);
}

static void priskv_rdma_mq_req_cb(priskv_transport_req *rdma_req)
{
    priskv_workqueue_submit(rdma_req->main_wq, priskv_rdma_req_cb_intl, rdma_req);
}

static priskv_conn_operation priskv_rdma_mq_ops = {
    .init = priskv_rdma_mq_init,
    .deinit = priskv_rdma_mq_deinit,
    .select_conn = priskv_rdma_mq_select_conn,
    .reg_memory = priskv_rdma_mq_reg_memory,
    .dereg_memory = priskv_rdma_mq_dereg_memory,
    .get_mr = priskv_rdma_mq_get_mr,
    .submit_req = priskv_rdma_mq_req_submit,
    .req_cb = priskv_rdma_mq_req_cb,
    .new_req = priskv_rdma_req_new,
};

static int priskv_rdma_sq_init(priskv_client *client, const char *raddr, int rport,
                               const char *laddr, int lport, int nqueue)
{
    client->conns = calloc(1, sizeof(priskv_transport_conn *));
    if (!client->conns) {
        priskv_log_error("RDMA: failed to allocate memory for connections\n");
        return -1;
    }

    client->conns[0] = priskv_rdma_conn_connect(raddr, rport, laddr, lport);
    if (!client->conns[0]) {
        priskv_log_error("RDMA: failed to connect to %s:%d\n", raddr, rport);
        return -1;
    }

    priskv_add_event_fd(client->epollfd, client->conns[0]->epollfd);

    return 0;
}

static void priskv_rdma_sq_deinit(priskv_client *client)
{
    priskv_rdma_conn_close(client->conns[0]);
    free(client->conns);
}

static priskv_transport_conn *priskv_rdma_sq_select_conn(priskv_client *client)
{
    return client->conns[0];
}

static priskv_memory *priskv_rdma_sq_reg_memory(priskv_client *client, uint64_t offset,
                                                size_t length, uint64_t iova, int fd)
{
    priskv_memory *mem = malloc(sizeof(priskv_memory));

    mem->client = client;
    mem->count = 1;
    mem->mrs = malloc(sizeof(struct ibv_mr *));

    mem->mrs[0] = priskv_rdma_conn_reg_memory(client->conns[0], offset, length, iova, fd);

    return mem;
}

static void priskv_rdma_sq_dereg_memory(priskv_memory *mem)
{
    priskv_rdma_conn_dereg_memory(mem->mrs[0]);
    free(mem->mrs);
    free(mem);
}

static struct ibv_mr *priskv_rdma_sq_get_mr(priskv_memory *mem, int connid)
{
    return mem->mrs[0];
}

static void priskv_rdma_sq_req_submit(priskv_transport_req *rdma_req)
{
    priskv_rdma_req_send(rdma_req);
}

static void priskv_rdma_sq_req_cb(priskv_transport_req *rdma_req)
{
    priskv_rdma_req_cb_intl(rdma_req);
}

static priskv_conn_operation priskv_rdma_sq_ops = {
    .init = priskv_rdma_sq_init,
    .deinit = priskv_rdma_sq_deinit,
    .select_conn = priskv_rdma_sq_select_conn,
    .reg_memory = priskv_rdma_sq_reg_memory,
    .dereg_memory = priskv_rdma_sq_dereg_memory,
    .get_mr = priskv_rdma_sq_get_mr,
    .submit_req = priskv_rdma_sq_req_submit,
    .req_cb = priskv_rdma_sq_req_cb,
    .new_req = priskv_rdma_req_new,
};

static inline void priskv_rdma_fillup_sgl(priskv_transport_req *rdma_req,
                                          priskv_keyed_sgl *keyed_sgl)
{
    priskv_transport_conn *conn = rdma_req->conn;

    for (uint16_t i = 0; i < rdma_req->nsgl; i++) {
        priskv_sgl_private *_sgl = &rdma_req->sgl[i];
        struct ibv_mr *mr;

        if (!_sgl->sgl.mem) {
            mr = _sgl->mr = priskv_rdma_conn_reg_memory(conn, _sgl->sgl.iova, _sgl->sgl.length,
                                                        _sgl->sgl.iova, -1);
        } else {
            if (rdma_req->cmd != PRISKV_COMMAND_KEYS) {
                mr = rdma_req->ops->get_mr(_sgl->sgl.mem, conn->id);
            } else {
                mr = rdma_req->ops->get_mr(_sgl->sgl.mem, 0);
            }
        }

        priskv_keyed_sgl *_keyed_sgl = &keyed_sgl[i];

        _keyed_sgl->addr = htobe64(_sgl->sgl.iova);
        _keyed_sgl->length = htobe32(_sgl->sgl.length);
        _keyed_sgl->key = htobe32(mr->rkey);

        priskv_log_debug("RDMA: addr 0x%lx@%x rkey 0x%x\n", _sgl->sgl.iova, _sgl->sgl.length,
                         mr->rkey);
    }
}

static int priskv_rdma_req_cb_intl(void *arg)
{
    priskv_transport_req *rdma_req = arg;

    if (rdma_req->usercb) {
        rdma_req->usercb(rdma_req->request_id, rdma_req->status, rdma_req->result);
    }

    priskv_rdma_req_free(rdma_req);

    return 0;
}

static void priskv_rdma_req_cb(priskv_transport_req *rdma_req)
{
    rdma_req->ops->req_cb(rdma_req);
}

static void priskv_rdma_keys_req_cb(priskv_transport_req *rdma_req)
{
    priskv_transport_conn *conn = rdma_req->conn;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
    uint32_t valuelen = rdma_req->length;

    if (rdma_req->status == PRISKV_STATUS_OK) {
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
            priskv_log_error("RDMA: KEYS protocol error\n");
            rdma_req->status = PRISKV_STATUS_PROTOCOL_ERROR;
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

        rdma_req->result = keyset;
    } else if (rdma_req->status == PRISKV_STATUS_VALUE_TOO_BIG) {
        priskv_log_info("RDMA: resize KEYS buffer to valuelen %d\n", valuelen);
        priskv_rdma_mem_free(conn, rmem);
        if (priskv_rdma_mem_new(conn, rmem, "Keys", valuelen + valuelen / 8, true)) {
            priskv_log_error("RDMA: failed to resize KEYS buffer to valuelen %d\n", valuelen);
            rdma_req->status = PRISKV_STATUS_TRANSPORT_ERROR;
            goto exit;
        }

        conn->keys_mems.count = 1;
        conn->keys_mems.mrs[0] = rmem->mr;

        priskv_rdma_req_reset(rdma_req);

        rdma_req->nsgl = 1;
        rdma_req->sgl[0].sgl.iova = (uint64_t)(rmem->buf);
        rdma_req->sgl[0].sgl.length = rmem->buf_size;
        rdma_req->sgl[0].sgl.mem = &conn->keys_mems;

        priskv_rdma_req_send(rdma_req);
        return;
    }

exit:
    conn->keys_running_req = NULL;
    priskv_rdma_req_cb(rdma_req);
}

static inline priskv_transport_req *
priskv_rdma_req_new(priskv_client *client, priskv_transport_conn *conn, uint64_t request_id,
                    const char *key, uint16_t keylen, priskv_sgl *sgl, uint16_t nsgl,
                    uint64_t timeout, priskv_req_command cmd, priskv_generic_cb usercb)
{
    priskv_transport_req *rdma_req = calloc(1, sizeof(priskv_transport_req));
    if (!rdma_req) {
        return NULL;
    }

    rdma_req->conn = conn;
    rdma_req->ops = client->ops;
    rdma_req->main_wq = client->wq;
    rdma_req->cmd = cmd;
    rdma_req->timeout = timeout;
    rdma_req->key = strdup(key);
    rdma_req->keylen = keylen;
    rdma_req->request_id = request_id;
    rdma_req->usercb = usercb;
    rdma_req->cb = priskv_rdma_req_cb;
    rdma_req->delaying = false;
    list_node_init(&rdma_req->entry);

    if (sgl && nsgl) {
        rdma_req->nsgl = nsgl;
        rdma_req->sgl = calloc(nsgl, sizeof(priskv_sgl_private));
        for (int i = 0; i < nsgl; i++) {
            memcpy(&rdma_req->sgl[i], &sgl[i], sizeof(priskv_sgl));
        }
    } else if (cmd == PRISKV_COMMAND_KEYS) {
        priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
        conn->keys_mems.count = 1;
        conn->keys_mems.mrs[0] = rmem->mr;

        rdma_req->nsgl = 1;
        rdma_req->sgl = malloc(sizeof(priskv_sgl_private));
        rdma_req->sgl[0].sgl.iova = (uint64_t)(rmem->buf);
        rdma_req->sgl[0].sgl.length = rmem->buf_size;
        rdma_req->sgl[0].sgl.mem = &conn->keys_mems;
        rdma_req->sgl[0].mr = NULL;
        rdma_req->cb = priskv_rdma_keys_req_cb;
    }

    return rdma_req;
}

static inline void priskv_rdma_req_free(priskv_transport_req *rdma_req)
{
    for (int i = 0; i < rdma_req->nsgl; i++) {
        priskv_sgl_private *_sgl = &rdma_req->sgl[i];
        if (_sgl->mr) {
            priskv_rdma_conn_dereg_memory(_sgl->mr);
            _sgl->mr = NULL;
        }
    }

    free(rdma_req->sgl);
    free(rdma_req->key);
    free(rdma_req);
}

static inline void priskv_rdma_req_reset(priskv_transport_req *rdma_req)
{
    rdma_req->flags = 0;
    rdma_req->req = NULL;
    rdma_req->status = PRISKV_STATUS_OK;
    rdma_req->length = 0;
    rdma_req->delaying = false;
}

static int priskv_rdma_req_send(void *arg)
{
    priskv_transport_req *rdma_req = arg;
    priskv_transport_conn *conn = rdma_req->conn;
    struct ibv_send_wr wr = {0}, *bad_wr;
    struct ibv_sge rsge;
    priskv_request *req;
    uint16_t req_idx;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_REQ];

    if (conn->state != PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED) {
        rdma_req->status = PRISKV_STATUS_DISCONNECTED;
        rdma_req->cb(rdma_req);
        return -1;
    }

    if (rdma_req->cmd == PRISKV_COMMAND_KEYS) {
        if (conn->keys_running_req && conn->keys_running_req != rdma_req) {
            rdma_req->status = PRISKV_STATUS_BUSY;
            rdma_req->cb(rdma_req);
            return -1;
        } else {
            conn->keys_running_req = rdma_req;
        }
    }

    req = priskv_rdma_unused_command(conn, &req_idx);
    if (!req) {
        if (rdma_req->delaying) {
            list_add(&conn->inflight_list, &rdma_req->entry);
        } else {
            list_add_tail(&conn->inflight_list, &rdma_req->entry);
            rdma_req->delaying = true;
        }
        return EAGAIN;
    }

    char key_short[16] = {0};
    priskv_string_shorten(rdma_req->key, rdma_req->keylen, key_short, sizeof(key_short));

    priskv_log_debug(
        "RDMA: Request command length %u, request_id 0x%lx, %s[0x%x], nsgl %u, key[%u] %s\n",
        rsge.length, rdma_req->request_id, priskv_command_str(rdma_req->cmd), rdma_req->cmd,
        rdma_req->nsgl, rdma_req->keylen, key_short);

    req->request_id = htobe64((uint64_t)rdma_req);
    req->command = htobe16(rdma_req->cmd);
    req->nsgl = htobe16(rdma_req->nsgl);
    req->timeout = htobe64(rdma_req->timeout);
    req->key_length = htobe16(rdma_req->keylen);

    struct timeval client_metadata_send_time;
    gettimeofday(&client_metadata_send_time, NULL);
    req->runtime.client_metadata_send_time = client_metadata_send_time;

    priskv_rdma_fillup_sgl(rdma_req, req->sgls);
    memcpy(priskv_rdma_request_key(req), rdma_req->key, rdma_req->keylen);

    rdma_req->req = req;

    rsge.addr = (uint64_t)req;
    rsge.length = priskv_rdma_request_size(req);
    rsge.lkey = rmem->mr->lkey;

    wr.wr_id = (uint64_t)req;
    wr.sg_list = &rsge;
    wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_SIGNALED; /* MLX needs a SIGNALED to kick SQ */

    int ret = ibv_post_send(conn->qp, &wr, &bad_wr);
    if (ret) {
        PRISKV_RDMA_DEF_ADDR(conn->cm_id)
        priskv_log_notice("RDMA: <%s - %s> close. Requests GET %ld, SET %ld, TEST %ld, DELETE %ld, "
                          "Responses %ld\n",
                          local_addr, peer_addr, conn->stats[PRISKV_COMMAND_GET],
                          conn->stats[PRISKV_COMMAND_SET], conn->stats[PRISKV_COMMAND_TEST],
                          conn->stats[PRISKV_COMMAND_DELETE], conn->resps);

        priskv_log_error(
            "RDMA: ibv_post_send addr %p, length %d. wc_recv %ld, wc_send %ld, failed: %d\n", req,
            rsge.length, conn->wc_recv, conn->wc_send, ret);
        rdma_req->status = PRISKV_STATUS_TRANSPORT_ERROR;
        rdma_req->cb(rdma_req);
        return -1;
    }

    conn->stats[rdma_req->cmd]++;

    return 0;
}


static inline void priskv_rdma_req_delay_send(priskv_transport_conn *conn)
{
    priskv_transport_req *rdma_req, *tmp;

    list_for_each_safe (&conn->inflight_list, rdma_req, tmp, entry) {
        list_del(&rdma_req->entry);

        if (priskv_rdma_req_send(rdma_req) == EAGAIN) {
            return;
        }
    }
}

static inline void priskv_rdma_req_done(priskv_transport_conn *conn, priskv_transport_req *rdma_req)
{
    if ((rdma_req->flags & PRISKV_TRANSPORT_REQ_FLAG_DONE) == PRISKV_TRANSPORT_REQ_FLAG_DONE) {
        list_add_tail(&conn->complete_list, &rdma_req->entry);
    }
}

static inline void priskv_rdma_req_complete(priskv_transport_conn *conn)
{
    priskv_transport_req *rdma_req, *tmp;

    list_for_each_safe (&conn->complete_list, rdma_req, tmp, entry) {
        list_del(&rdma_req->entry);

        priskv_rdma_request_free(rdma_req->req, conn);
        rdma_req->cb(rdma_req);
    }
}

static int priskv_rdma_handle_recv(priskv_transport_conn *conn, priskv_response *resp, uint32_t len)
{
    uint64_t request_id = be64toh(resp->request_id);
    uint16_t status = be16toh(resp->status);
    uint32_t length = be32toh(resp->length);
    priskv_transport_req *rdma_req;

    if (len != sizeof(priskv_response)) {
        priskv_log_warn("RDMA: recv %d, expected %ld\n", len, sizeof(priskv_response));
        return -EPROTO;
    }

    priskv_log_debug("Response request_id 0x%lx, status(%d) %s, length %d\n", request_id, status,
                     priskv_resp_status_str(status), length);
    rdma_req = (priskv_transport_req *)request_id;
    rdma_req->status = status;
    rdma_req->length = length;

    if (rdma_req->cmd != PRISKV_COMMAND_KEYS) {
        rdma_req->result = &rdma_req->length;
    }

    rdma_req->flags |= PRISKV_TRANSPORT_REQ_FLAG_RECV;
    priskv_rdma_req_done(conn, rdma_req);

    conn->resps++;
    priskv_rdma_recv_resp(conn, resp);

    return 0;
}

static void priskv_rdma_handle_send(priskv_transport_conn *conn, priskv_request *req)
{
    priskv_transport_req *rdma_req = (priskv_transport_req *)be64toh(req->request_id);

    rdma_req->flags |= PRISKV_TRANSPORT_REQ_FLAG_SEND;
    priskv_rdma_req_done(conn, rdma_req);
}

static int priskv_rdma_handle_cq(priskv_transport_conn *conn)
{

    struct ibv_cq *ev_cq = NULL;
    void *ev_ctx = NULL;
    struct ibv_wc wc = {0};
    priskv_response *resp;
    int ret;

    if (ibv_get_cq_event(conn->comp_channel, &ev_cq, &ev_ctx) < 0) {
        if (errno != EAGAIN) {
            priskv_log_warn("RDMA: ibv_get_cq_event failed: %m\n");
        }
        return -errno;
    } else if (ibv_req_notify_cq(ev_cq, 0)) {
        priskv_log_warn("ibv_req_notify_cq failed: %m\n");
        return -errno;
    }

    ibv_ack_cq_events(conn->cq, 1);

poll_cq:
    ret = ibv_poll_cq(conn->cq, 1, &wc);
    if (ret < 0) {
        priskv_log_warn("ibv_poll_cq failed: %m\n");
        return -errno;
    } else if (ret == 0) {
        priskv_rdma_req_complete(conn);
        priskv_rdma_req_delay_send(conn);
        return 0;
    }

    priskv_log_debug("RDMA: CQ handle status: %s[0x%x], wr_id: %p, opcode: 0x%x, byte_len: %d\n",
                     ibv_wc_status_str(wc.status), wc.status, (void *)wc.wr_id, wc.opcode,
                     wc.byte_len);
    if (wc.status != IBV_WC_SUCCESS) {
        priskv_log_error("CQ handle error status: %s[0x%x], opcode : 0x%x\n",
                         ibv_wc_status_str(wc.status), wc.status, wc.opcode);
        return -EIO;
    }

    switch (wc.opcode) {
    case IBV_WC_RECV:
        conn->wc_recv++;
        resp = (priskv_response *)wc.wr_id;
        ret = priskv_rdma_handle_recv(conn, resp, wc.byte_len);
        if (ret < 0) {
            return ret;
        }
        break;

    case IBV_WC_SEND:
        priskv_rdma_handle_send(conn, (priskv_request *)wc.wr_id);
        conn->wc_send++;
        break;

    default:
        priskv_log_error("unexpected opcode 0x%x\n", wc.opcode);
        return -EIO;
    }

    goto poll_cq;
}

priskv_conn_operation *priskv_rdma_get_sq_ops(void)
{
    return &priskv_rdma_sq_ops;
}

priskv_conn_operation *priskv_rdma_get_mq_ops(void)
{
    return &priskv_rdma_mq_ops;
}

priskv_transport_driver priskv_transport_driver_rdma = {
    .name = "rdma",
    .init = NULL,
    .get_sq_ops = priskv_rdma_get_sq_ops,
    .get_mq_ops = priskv_rdma_get_mq_ops,
};
