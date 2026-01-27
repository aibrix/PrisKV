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

#ifndef __PRISKV_SERVER_TRANSPORT__
#define __PRISKV_SERVER_TRANSPORT__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <limits.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>

#include "list.h"
#include "priskv-protocol.h"
#include "priskv-threads.h"
#include "priskv-ucx.h"
#include "priskv-utils.h"
#include "uthash.h"

#define PRISKV_TRANSPORT_MAX_BIND_ADDR 32
#define PRISKV_TRANSPORT_DEFAULT_PORT ('H' << 8 | 'P')
#define PRISKV_TRANSPORT_MAX_INFLIGHT_COMMAND 4096
#define PRISKV_TRANSPORT_DEFAULT_INFLIGHT_COMMAND 128
#define PRISKV_TRANSPORT_MAX_SGL 8
#define PRISKV_TRANSPORT_DEFAULT_SGL 4
#define PRISKV_TRANSPORT_MAX_KEY (1 << 30)
#define PRISKV_TRANSPORT_DEFAULT_KEY (16 * 1024)
#define PRISKV_TRANSPORT_MAX_KEY_LENGTH 1024
#define PRISKV_TRANSPORT_DEFAULT_KEY_LENGTH 128
#define PRISKV_TRANSPORT_MAX_VALUE_BLOCK_SIZE (1 << 20)
#define PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK_SIZE 4096
#define PRISKV_TRANSPORT_MAX_VALUE_BLOCK (1UL << 30)
#define PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK (1024UL * 1024)
#define SLOW_QUERY_THRESHOLD_LATENCY_US 1000000

extern uint32_t g_slow_query_threshold_latency_us;

typedef struct priskv_transport_stats {
    uint64_t ops;
    uint64_t bytes;
} priskv_transport_stats;

typedef priskv_cm_cap priskv_transport_conn_cap;

typedef struct priskv_transport_client {
    char address[PRISKV_ADDR_LEN];
    priskv_transport_stats stats[PRISKV_COMMAND_MAX];
    uint64_t resps;
    bool closing;
} priskv_transport_client;

typedef struct priskv_transport_listener {
    char address[PRISKV_ADDR_LEN];
    int nclients;
    priskv_transport_client *clients;
} priskv_transport_listener;

typedef union priskv_transport_memh {
    void *handle;
    struct ibv_mr *rdma_mr;    // rdma
    priskv_ucx_memh *ucx_memh; // ucx
} priskv_transport_memh;

typedef struct priskv_transport_mem {
#define PRISKV_TRANSPORT_MEM_NAME_LEN 32
    char name[PRISKV_TRANSPORT_MEM_NAME_LEN];
    uint8_t *buf;
    uint32_t buf_size;
    priskv_transport_memh memh;
} priskv_transport_mem;

typedef enum priskv_transport_mem_type {
    PRISKV_TRANSPORT_MEM_REQ,
    PRISKV_TRANSPORT_MEM_RESP,
    PRISKV_TRANSPORT_MEM_KEYS,

    PRISKV_TRANSPORT_MEM_MAX
} priskv_transport_mem_type;

typedef struct priskv_transport_conn {
    priskv_transport_conn_cap conn_cap;
    char local_addr[PRISKV_ADDR_LEN];
    char peer_addr[PRISKV_ADDR_LEN];
    pthread_spinlock_t lock;

    priskv_transport_memh value_memh;
    union {
        struct {
            struct rdma_cm_id *cm_id;
            struct ibv_comp_channel *comp_channel;
            struct ibv_cq *cq;
        }; // rdma
        struct {
            union {
                struct {
                    int listenfd;
                    int efd;
                    priskv_transport_conn_cap conn_cap_be;
                }; // listener
                struct {
                    int connfd;
                    priskv_ucx_worker *worker;
                    priskv_ucx_ep *ep;
                    priskv_ucx_request *inflight_reqs;
                }; // client
            };
        }; // ucx
    };

    union {
        struct {
            struct list_head head;
            uint32_t nclients;
        } s; /* for listener */
        struct {
            struct priskv_transport_conn *listener;
            struct list_node node;
            priskv_thread *thread;
            bool closing;
            priskv_transport_stats stats[PRISKV_COMMAND_MAX];
            uint64_t resps;
        } c; /* for client */
    };

    void *kv;
    uint8_t *value_base;
    priskv_transport_mem rmem[PRISKV_TRANSPORT_MEM_MAX];
} priskv_transport_conn;

typedef struct priskv_transport_rw_work {
    priskv_transport_conn *conn;
    uint64_t request_id; /* be64 type */
    priskv_request *req;
    priskv_transport_memh memh;
    union {
        uint32_t rdma_rkey;        // rdma
        priskv_ucx_rkey *ucx_rkey; // ucx
    };

    uint32_t valuelen;
    uint16_t nsgl;
    uint16_t completed;
    bool defer_resp;
    void (*cb)(void *);
    void *cbarg;
} priskv_transport_rw_work;

typedef struct priskv_transport_server {
    int epollfd;
    void *kv;
    int nlisteners;
    priskv_transport_conn listeners[PRISKV_TRANSPORT_MAX_BIND_ADDR];
    priskv_transport_conn_cap cap;
    union {
        struct {}; // rdma
        struct {
            priskv_ucx_context *context;
        }; // ucx
    };
} priskv_transport_server;

typedef struct priskv_transport_driver {
    const char *name;
    int (*init)(void);
    int (*listen)(char **addr, int naddr, int port, void *kv, priskv_transport_conn_cap *cap);

    int (*get_fd)(void);
    void *(*get_kv)(void);

    // listeners
    priskv_transport_listener *(*get_listeners)(int *nlisteners);
    void (*free_listeners)(priskv_transport_listener *listeners, int nlisteners);

    // req/resp
    int (*send_response)(priskv_transport_conn *conn, uint64_t request_id,
                         priskv_resp_status status, uint32_t length);
    int (*rw_req)(priskv_transport_conn *conn, priskv_request *req, priskv_transport_memh *memh,
                  uint8_t *val, uint32_t valuelen, bool set, void (*cb)(void *), void *cbarg,
                  bool defer_resp, priskv_transport_rw_work **work_out);
    int (*recv_req)(priskv_transport_conn *conn, uint8_t *req);

    // mem
    int (*mem_new)(priskv_transport_conn *conn, priskv_transport_mem *rmem, const char *name,
                   uint32_t size);
    void (*mem_free)(priskv_transport_conn *conn, priskv_transport_mem *rmem);

    // request layout
    uint16_t (*request_key_off)(priskv_request *req);
    uint8_t *(*request_key)(priskv_request *req);

    void (*close_client)(priskv_transport_conn *client);
} priskv_transport_driver;

/**
 * @brief Listen on the specified addresses and port.
 *
 * @param addr The addresses to listen on.
 * @param naddr The number of addresses.
 * @param port The port to listen on.
 * @param kv The key-value store to use.
 * @param cap The connection capacity.
 * @return int 0 on success, others on error.
 */
int priskv_transport_listen(char **addr, int naddr, int port, void *kv,
                            priskv_transport_conn_cap *cap);

/**
 * @brief Get the file descriptor for the transport driver.
 *
 * @return int The file descriptor.
 */
int priskv_transport_get_fd(void);

/**
 * @brief Process events for the transport driver.
 */
void priskv_transport_process(void);

/**
 * @brief Get the key-value store associated with the transport driver.
 *
 * @return void* The key-value store.
 */
void *priskv_transport_get_kv(void);

/**
 * @brief Get the listeners associated with the transport driver.
 *
 * @param nlisteners The number of listeners.
 * @return priskv_transport_listener* The listeners.
 */
priskv_transport_listener *priskv_transport_get_listeners(int *nlisteners);

/**
 * @brief Free the listeners associated with the transport driver.
 *
 * @param listeners The listeners to free.
 * @param nlisteners The number of listeners.
 */
void priskv_transport_free_listeners(priskv_transport_listener *listeners, int nlisteners);

/**
 * @brief Send a response to the client.
 *
 * @param conn The transport connection.
 * @param request_id The request ID.
 * @param status The response status.
 * @param length The response length.
 * @return int 0 on success, others on error.
 */
int priskv_transport_send_response(priskv_transport_conn *conn, uint64_t request_id,
                                   priskv_resp_status status, uint32_t length);

/**
 * @brief Submit a read or write request to the transport driver.
 *
 * @param conn The transport connection.
 * @param req The request to submit.
 * @param memh The memory handle to use.
 * @param val The value to read or write.
 * @param valuelen The length of the value.
 * @param set Whether to perform a write operation.
 * @param cb The callback function to invoke when the request completes.
 * @param cbarg The argument to pass to the callback function.
 * @param defer_resp Whether to defer sending the response.
 * @param work_out The output parameter to store the submitted work.
 * @return int 0 on success, others on error.
 */
int priskv_transport_rw_req(priskv_transport_conn *conn, priskv_request *req,
                            priskv_transport_memh *memh, uint8_t *val, uint32_t valuelen, bool set,
                            void (*cb)(void *), void *cbarg, bool defer_resp,
                            priskv_transport_rw_work **work_out);

/**
 * @brief Check and log slow queries.
 *
 * @param work The transport rw work.
 */
void priskv_check_and_log_slow_query(priskv_transport_rw_work *work);

/**
 * @brief Handle a received request.
 *
 * @param conn The transport connection.
 * @param req The request to handle.
 * @param len The length of the request.
 * @return int 0 on success, others on error.
 */
int priskv_transport_handle_recv(priskv_transport_conn *conn, priskv_request *req, uint32_t len);

/**
 * @brief Handle a read or write request.
 *
 * @param conn The transport connection.
 * @param work The transport rw work.
 * @return int 0 on success, others on error.
 */
int priskv_transport_handle_rw(priskv_transport_conn *conn, priskv_transport_rw_work *work);

/**
 * @brief Mark a client connection as closed.
 *
 * @param client The transport connection.
 */
void priskv_transport_mark_client_closed(priskv_transport_conn *client);

/**
 * @brief Close all disconnected client connections.
 *
 * @param listener The transport listener.
 */
void priskv_transport_close_disconnected(priskv_transport_conn *listener);

#if defined(__cplusplus)
}
#endif

#endif
