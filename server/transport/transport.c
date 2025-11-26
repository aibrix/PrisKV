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
#include <strings.h>

#include "../kv.h"
#include "priskv-event.h"
#include "priskv-log.h"
#include "priskv-threads.h"
#include "priskv-protocol-helper.h"

priskv_transport_driver *g_transport_driver = NULL;
priskv_threadpool *g_threadpool = NULL;
priskv_transport_server g_transport_server = {
    .nlisteners = 0,
    .epollfd = -1,
    .context = NULL,
};

extern priskv_transport_driver priskv_transport_driver_ucx;
extern priskv_transport_driver priskv_transport_driver_rdma;

uint32_t g_slow_query_threshold_latency_us = SLOW_QUERY_THRESHOLD_LATENCY_US;

// forward declaration
void priskv_tiering_get(priskv_tiering_req *treq);
void priskv_tiering_test(priskv_tiering_req *treq);
void priskv_tiering_set(priskv_tiering_req *treq);
void priskv_tiering_del(priskv_tiering_req *treq);

static void __attribute__((constructor)) priskv_server_transport_init(void)
{
    const char *transport_env = getenv("PRISKV_TRANSPORT");
    priskv_transport_backend backend = PRISKV_TRANSPORT_BACKEND_RDMA;
    if (transport_env) {
        if (strcasecmp(transport_env, "UCX") == 0) {
            backend = PRISKV_TRANSPORT_BACKEND_UCX;
        } else if (strcasecmp(transport_env, "RDMA") == 0) {
            backend = PRISKV_TRANSPORT_BACKEND_RDMA;
        } else {
            priskv_log_error("Unknown transport backend: %s\n", transport_env);
        }
    }

    priskv_transport_driver *driver = NULL;
    switch (backend) {
    case PRISKV_TRANSPORT_BACKEND_UCX:
        driver = &priskv_transport_driver_ucx;
        priskv_log_notice("Using UCX transport backend\n");
        break;
    case PRISKV_TRANSPORT_BACKEND_RDMA:
        driver = &priskv_transport_driver_rdma;
        priskv_log_notice("Using RDMA transport backend\n");
        break;
    default:
        priskv_log_error("Unknown transport backend: %d\n", backend);
        break;
    }

    if (driver && driver->init) {
        if (driver->init() != 0) {
            priskv_log_error("Failed to initialize transport driver: %s\n", driver->name);
            driver = NULL;
        }
    }

    if (driver) {
        g_transport_driver = driver;
        return 0;
    }

    return -1;
}

int priskv_transport_listen(char **addr, int naddr, int port, void *kv,
                            priskv_transport_conn_cap *cap)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return -1;
    }
    return g_transport_driver->listen(addr, naddr, port, kv, cap);
}

int priskv_transport_get_fd(void)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return -1;
    }
    return g_transport_driver->get_fd();
}

void *priskv_transport_get_kv(void)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return NULL;
    }
    return g_transport_driver->get_kv();
}

priskv_transport_listener *priskv_transport_get_listeners(int *nlisteners)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return NULL;
    }
    return g_transport_driver->get_listeners(nlisteners);
}

void priskv_transport_free_listeners(priskv_transport_listener *listeners, int nlisteners)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return;
    }
    g_transport_driver->free_listeners(listeners, nlisteners);
}

int priskv_transport_send_response(priskv_transport_conn *conn, uint64_t request_id,
                                   priskv_resp_status status, uint32_t length)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return -1;
    }
    return g_transport_driver->send_response(conn, request_id, status, length);
}

int priskv_transport_rw_req(priskv_transport_conn *conn, priskv_request *req,
                            priskv_transport_memh *memh, uint8_t *val, uint32_t valuelen, bool set,
                            void (*cb)(void *), void *cbarg, bool defer_resp,
                            priskv_transport_rw_work **work_out)
{
    if (!g_transport_driver) {
        priskv_log_error("Transport driver is NULL\n");
        return -1;
    }
    return g_transport_driver->rw_req(conn, req, memh, val, valuelen, set, cb, cbarg, defer_resp,
                                      work_out);
}

void priskv_check_and_log_slow_query(priskv_transport_rw_work *work)
{
    struct timeval server_resp_send_time;
    priskv_request *req = (priskv_request *)work->req;
    uint16_t command = be16toh(req->command);
    uint8_t *key = g_transport_driver->request_key(req);
    uint16_t keylen = be16toh(req->key_length);

    gettimeofday(&server_resp_send_time, NULL);
    req->runtime.server_resp_send_time = server_resp_send_time;
    if (priskv_time_elapsed_us(&req->runtime.client_metadata_send_time,
                               &req->runtime.server_resp_send_time) >
        g_slow_query_threshold_latency_us) {
        char key_short[128] = {0};
        priskv_string_shorten((const char *)key, keylen, key_short, sizeof(key_short));
        priskv_log_notice(
            "Slow Query Encountered . "
            "Slow Query threshold latency is %ld us |"
            "Command %s key[%u] = \"%s\" |"
            "thread id is %lu |"
            "Client send metadata: %ld.%06ld us | "
            "Server recv metadata: %ld.%06ld us | "
            "Server RW KV: %ld.%06ld us | "
            "Server send data: %ld.%06ld us | "
            "Server recv data: %ld.%06ld us | "
            "Server send resp: %ld.%06ld us | "
            "Total: %ld us | "
            "Steps: "
            "Client->Server metadata: %ld us | "
            "Server metadata->RW KV: %ld us | "
            "RW KV->Send data: %ld us | "
            "Send data->Recv data: %ld us | "
            "Recv data->Resp send: %ld us \n",

            g_slow_query_threshold_latency_us, priskv_command_str(command), keylen, key_short,
            pthread_self(), req->runtime.client_metadata_send_time.tv_sec,
            req->runtime.client_metadata_send_time.tv_usec,
            req->runtime.server_metadata_recv_time.tv_sec,
            req->runtime.server_metadata_recv_time.tv_usec, req->runtime.server_rw_kv_time.tv_sec,
            req->runtime.server_rw_kv_time.tv_usec, req->runtime.server_data_send_time.tv_sec,
            req->runtime.server_data_send_time.tv_usec, req->runtime.server_data_recv_time.tv_sec,
            req->runtime.server_data_recv_time.tv_usec, req->runtime.server_resp_send_time.tv_sec,
            req->runtime.server_resp_send_time.tv_usec,

            priskv_time_elapsed_us(&req->runtime.client_metadata_send_time,
                                   &req->runtime.server_resp_send_time),

            priskv_time_elapsed_us(&req->runtime.client_metadata_send_time,
                                   &req->runtime.server_metadata_recv_time),
            priskv_time_elapsed_us(&req->runtime.server_metadata_recv_time,
                                   &req->runtime.server_rw_kv_time),
            priskv_time_elapsed_us(&req->runtime.server_rw_kv_time,
                                   &req->runtime.server_data_send_time),
            priskv_time_elapsed_us(&req->runtime.server_data_send_time,
                                   &req->runtime.server_data_recv_time),
            priskv_time_elapsed_us(&req->runtime.server_data_recv_time,
                                   &req->runtime.server_resp_send_time));
    }
}

int priskv_transport_handle_recv(priskv_transport_conn *conn, priskv_request *req, uint32_t len)
{
    uint16_t command = be16toh(req->command);
    uint16_t nsgl = be16toh(req->nsgl);
    uint64_t timeout = be64toh(req->timeout);
    uint8_t *key;
    uint16_t keylen;
    uint16_t keyoff = g_transport_driver->request_key_off(req);
    uint8_t *val;
    uint32_t valuelen = 0, nkeys = 0;
    uint32_t remote_valuelen;
    uint64_t bytes = 0;
    void *keynode;
    priskv_resp_status status;
    int ret = 0;
    bool tiering_inflight = false;
    priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
    priskv_transport_driver *driver = g_transport_driver;

    if (len < keyoff) {
        priskv_log_warn("Transport: <%s - %s> invalid command. recv %d, less than %d, nsgl 0x%x\n",
                        conn->local_addr, conn->peer_addr, len, keyoff, nsgl);
        driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_INVALID_COMMAND, 0);
        return -EPROTO;
    }

    keylen = len - keyoff;
    if (!keylen) {
        priskv_log_warn("Transport: <%s - %s> empty key. recv %d, less than %d, nsgl 0x%x\n",
                        conn->local_addr, conn->peer_addr, len, keyoff, nsgl);

        driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_KEY_EMPTY, 0);
        return -EPROTO;
    }

    if (keylen > conn->conn_cap.max_key_length) {
        priskv_log_warn("Transport: <%s - %s> invalid key. key(%d) exceeds max_key_length(%d)\n",
                        conn->local_addr, conn->peer_addr, keylen, conn->conn_cap.max_key_length);
        driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_KEY_TOO_BIG, 0);
        return -EPROTO;
    }

    if (nsgl > conn->conn_cap.max_sgl) {
        priskv_log_warn("Transport: <%s - %s> invalid nsgl. nsgl(%d) exceeds max_sgl(%d)\n",
                        conn->local_addr, conn->peer_addr, nsgl, conn->conn_cap.max_sgl);
        driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_INVALID_SGL, 0);
        return -EPROTO;
    }

    key = driver->request_key(req);

    if (priskv_get_log_level() >= priskv_log_debug) {
        char key_short[128] = {0};
        priskv_string_shorten((const char *)key, keylen, key_short, sizeof(key_short));
        priskv_log_debug("Transport: <%s - %s> %s key[%u] = \"%s\"\n", conn->local_addr,
                         conn->peer_addr, priskv_command_str(command), keylen, key_short);
    }

    switch (command) {
    case PRISKV_COMMAND_GET: {
        struct timeval server_rw_kv_time, server_data_send_time;
        remote_valuelen = priskv_sgl_size_from_be(req->sgls, nsgl);

        if (!priskv_backend_tiering_enabled()) {
            status = priskv_get_key(conn->kv, key, keylen, &val, &valuelen, &keynode);
            if (status != PRISKV_RESP_STATUS_OK || !keynode) {
                ret = driver->send_response(conn, req->request_id, status, 0);
                priskv_get_key_end(keynode);
                break;
            }

            gettimeofday(&server_rw_kv_time, NULL);
            req->runtime.server_rw_kv_time = server_rw_kv_time;

            if (remote_valuelen < valuelen) {
                ret = driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_VALUE_TOO_BIG,
                                            valuelen);
                priskv_get_key_end(keynode);
                break;
            }

            ret = driver->rw_req(conn, req, &conn->value_memh, val, valuelen, false,
                                 priskv_get_key_end, keynode, false, NULL);

            gettimeofday(&server_data_send_time, NULL);
            req->runtime.server_data_send_time = server_data_send_time;

            bytes = valuelen;
        } else {
            priskv_resp_status alloc_status = PRISKV_RESP_STATUS_OK;
            priskv_tiering_req *treq =
                priskv_tiering_req_new(conn, req, key, keylen, PRISKV_KEY_MAX_TIMEOUT,
                                       PRISKV_COMMAND_GET, remote_valuelen, &alloc_status);
            if (!treq) {
                ret = driver->send_response(conn, req->request_id, alloc_status, 0);
                break;
            }

            tiering_inflight = true;
            priskv_tiering_get(treq);
        }
        break;
    }
    case PRISKV_COMMAND_SET: {
        struct timeval server_rw_kv_time, server_data_send_time;

        remote_valuelen = priskv_sgl_size_from_be(req->sgls, nsgl);
        if (!remote_valuelen) {
            ret = driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_VALUE_EMPTY, 0);
            break;
        }

        if (!priskv_backend_tiering_enabled()) {
            status =
                priskv_set_key(conn->kv, key, keylen, &val, remote_valuelen, timeout, &keynode);
            if (status != PRISKV_RESP_STATUS_OK || !keynode) {
                ret = driver->send_response(conn, req->request_id, status, 0);
                priskv_set_key_end(keynode);
                break;
            }

            gettimeofday(&server_rw_kv_time, NULL);
            req->runtime.server_rw_kv_time = server_rw_kv_time;

            ret = driver->rw_req(conn, req, &conn->value_memh, val, remote_valuelen, true,
                                 priskv_set_key_end, keynode, false, NULL);

            gettimeofday(&server_data_send_time, NULL);
            req->runtime.server_data_send_time = server_data_send_time;

            bytes = remote_valuelen;
        } else {
            priskv_resp_status alloc_status = PRISKV_RESP_STATUS_OK;
            priskv_tiering_req *treq =
                priskv_tiering_req_new(conn, req, key, keylen, timeout, PRISKV_COMMAND_SET,
                                       remote_valuelen, &alloc_status);
            if (!treq) {
                ret = driver->send_response(conn, req->request_id, alloc_status, 0);
                break;
            }

            tiering_inflight = true;
            priskv_tiering_set(treq);
        }
        break;
    }

    case PRISKV_COMMAND_TEST: {
        if (!priskv_backend_tiering_enabled()) {
            status = priskv_get_key(conn->kv, key, keylen, &val, &valuelen, &keynode);
            ret = driver->send_response(conn, req->request_id, status, valuelen);
            priskv_get_key_end(keynode);
            break;
        }

        priskv_resp_status alloc_status = PRISKV_RESP_STATUS_OK;
        priskv_tiering_req *treq = priskv_tiering_req_new(conn, req, key, keylen, timeout,
                                                          PRISKV_COMMAND_TEST, 0, &alloc_status);
        if (!treq) {
            ret = driver->send_response(conn, req->request_id, alloc_status, 0);
            break;
        }

        tiering_inflight = true;
        priskv_tiering_test(treq);
        break;
    }

    case PRISKV_COMMAND_DELETE: {
        if (!priskv_backend_tiering_enabled()) {
            status = priskv_delete_key(conn->kv, key, keylen);
            ret = driver->send_response(conn, req->request_id, status, 0);
            break;
        }

        priskv_resp_status alloc_status = PRISKV_RESP_STATUS_OK;
        priskv_tiering_req *treq = priskv_tiering_req_new(conn, req, key, keylen, timeout,
                                                          PRISKV_COMMAND_DELETE, 0, &alloc_status);
        if (!treq) {
            ret = driver->send_response(conn, req->request_id, alloc_status, 0);
            break;
        }

        tiering_inflight = true;
        priskv_tiering_del(treq);
        break;
    }

    case PRISKV_COMMAND_EXPIRE:
        status = priskv_expire_key(conn->kv, key, keylen, timeout);
        ret = driver->send_response(conn, req->request_id, status, 0);
        break;

    case PRISKV_COMMAND_KEYS:
        if (rmem->memh.handle) {
            /* a single KEYS command is allowed inflight with a connection */
            driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_NO_MEM, 0);
            ret = 0;
            break;
        }

        remote_valuelen = priskv_sgl_size_from_be(req->sgls, nsgl);
        if (driver->mem_new(conn, rmem, "Keys", remote_valuelen)) {
            ret = driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_NO_MEM, valuelen);
            break;
        }

        status =
            priskv_get_keys(conn->kv, key, keylen, rmem->buf, remote_valuelen, &valuelen, &nkeys);
        if ((status != PRISKV_RESP_STATUS_OK) || !valuelen) {
            driver->mem_free(conn, rmem);
            ret = driver->send_response(conn, req->request_id, status, valuelen);
            break;
        }

        ret = driver->rw_req(conn, req, &rmem->memh, rmem->buf, valuelen, false, NULL, NULL, false,
                             NULL);
        if (ret) {
            driver->mem_free(conn, rmem);
            ret = driver->send_response(conn, req->request_id, status, valuelen);
        }
        break;

    case PRISKV_COMMAND_NRKEYS:
        status = priskv_get_keys(conn->kv, key, keylen, NULL, 0, &valuelen, &nkeys);
        /* PRISKV_RESP_STATUS_VALUE_TOO_BIG is expected */
        if (status == PRISKV_RESP_STATUS_VALUE_TOO_BIG) {
            ret = driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_OK, nkeys);
            break;
        }
        ret = driver->send_response(conn, req->request_id, status, 0);
        break;

    case PRISKV_COMMAND_FLUSH:
        status = priskv_flush_keys(conn->kv, key, keylen, &nkeys);
        ret = driver->send_response(conn, req->request_id, status, nkeys);
        break;

    default:
        priskv_log_warn("Transport: <%s - %s> unknown command %d\n", conn->local_addr,
                        conn->peer_addr, command);
        ret = driver->send_response(conn, req->request_id, PRISKV_RESP_STATUS_NO_SUCH_COMMAND, 0);
    }

    if (!tiering_inflight) {
        conn->c.stats[command].ops++;
        if (!ret) {
            driver->recv_req(conn, (uint8_t *)req);
            conn->c.stats[command].bytes += bytes;
        }
    }

    return ret;
}

static int priskv_transport_complete_rw_work(priskv_transport_rw_work *work,
                                             priskv_resp_status status, uint32_t length)
{
    if (!work) {
        return -EINVAL;
    }

    priskv_transport_conn *conn = work->conn;

    int ret = g_transport_driver->send_response(conn, work->request_id, status, length);

    if (work->memh.handle != conn->value_memh.handle) {
        priskv_transport_mem *rmem = &conn->rmem[PRISKV_TRANSPORT_MEM_KEYS];
        assert(work->memh.handle == rmem->memh.handle);

        g_transport_driver->mem_free(conn, rmem);
        priskv_log_debug("Transport: KEYS done\n");
    }

    priskv_check_and_log_slow_query(work);

    free(work);
    return ret;
}

int priskv_transport_handle_rw(priskv_transport_conn *conn, priskv_transport_rw_work *work)
{
    work->completed++;
    assert(work->completed <= work->nsgl);

    if (work->completed < work->nsgl) {
        return 0;
    }

    if (work->cb) {
        work->cb(work->cbarg);
    }

    if (work->defer_resp) {
        return 0;
    }

    return priskv_transport_complete_rw_work(work, PRISKV_RESP_STATUS_OK, work->valuelen);
}

void priskv_transport_mark_client_closed(priskv_transport_conn *client)
{
    pthread_spin_lock(&client->lock);
    if (client->c.closing) {
        pthread_spin_unlock(&client->lock);
        return;
    }

    client->c.closing = true;
    pthread_spin_unlock(&client->lock);

    priskv_log_notice("Transport: <%s - %s> async close client\n", client->local_addr,
                      client->peer_addr);
}

void priskv_transport_close_disconnected(priskv_transport_conn *listener)
{
    priskv_transport_conn *client, *tmp;

    pthread_spin_lock(&listener->lock);
    list_for_each_safe (&listener->s.head, client, tmp, c.node) {
        if (client->c.closing) {
            listener->s.nclients--;
            list_del(&client->c.node);
            pthread_spin_unlock(&listener->lock);
        } else {
            continue;
        }

        g_transport_driver->close_client(client);

        pthread_spin_lock(&listener->lock);
    }
    pthread_spin_unlock(&listener->lock);
}

void priskv_transport_process(void)
{
    priskv_transport_conn *listener;
#define PRISKV_EPOLL_MAX_CM_EVENT 32
    struct epoll_event events[PRISKV_EPOLL_MAX_CM_EVENT];
    int nevents;

    nevents = epoll_wait(g_transport_server.epollfd, events, PRISKV_EPOLL_MAX_CM_EVENT, 1000);
    if (!nevents) {
        goto close_disconnected;
    }

    if (nevents < 0) {
        assert(errno == EINTR);
        goto close_disconnected;
    }

    for (int n = 0; n < nevents; n++) {
        struct epoll_event *event = &events[n];
        priskv_fd_handler_event(event);
    }

close_disconnected:
    for (int i = 0; i < g_transport_server.nlisteners; i++) {
        listener = &g_transport_server.listeners[i];
        priskv_transport_close_disconnected(listener);
    }
}
