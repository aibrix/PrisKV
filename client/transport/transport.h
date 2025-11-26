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

#ifndef __PRISKV_CLIENT_TRANSPORT__
#define __PRISKV_CLIENT_TRANSPORT__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <limits.h>
#include <rdma/rdma_cma.h>
#include <stdbool.h>

#include "../priskv.h"
#include "list.h"
#include "priskv-protocol.h"
#include "priskv-threads.h"
#include "priskv-ucx.h"
#include "priskv-utils.h"
#include "priskv-workqueue.h"
#include "uthash.h"

// forward declaration
typedef struct priskv_conn_operation priskv_conn_operation;
typedef struct priskv_transport_conn priskv_transport_conn;

typedef enum priskv_transport_backend {
    PRISKV_TRANSPORT_BACKEND_UCX,
    PRISKV_TRANSPORT_BACKEND_RDMA,
    PRISKV_TRANSPORT_BACKEND_MAX,
} priskv_transport_backend;

typedef enum priskv_transport_mem_type {
    PRISKV_TRANSPORT_MEM_REQ,
    PRISKV_TRANSPORT_MEM_RESP,
    PRISKV_TRANSPORT_MEM_KEYS,

    PRISKV_TRANSPORT_MEM_MAX
} priskv_transport_mem_type;

typedef struct priskv_transport_mem {
#define PRISKV_TRANSPORT_MEM_NAME_LEN 32
    char name[PRISKV_TRANSPORT_MEM_NAME_LEN];
    uint8_t *buf;
    uint32_t buf_size;
    union {
        struct {
            struct ibv_mr *mr;
        }; // rdma
        struct {
            priskv_ucx_memh *memh;
        }; // ucx
    };
} priskv_transport_mem;

typedef struct priskv_connect_param {
    /* the maxium count of @priskv_sgl */
    uint16_t max_sgl;
    /* the maxium length of a KEY in bytes */
    uint16_t max_key_length;
    /* the maxium command in flight, aka depth of commands */
    uint16_t max_inflight_command;
} priskv_connect_param;

typedef struct priskv_memory {
    priskv_client *client;
    int count;
    struct {
        struct ibv_mr **mrs;
    }; // rdma
    struct {
        priskv_ucx_memh **memhs;
    }; // ucx
} priskv_memory;

typedef struct priskv_sgl_private {
    priskv_sgl sgl;
    /* used for automatic registration memory */
    union {
        struct ibv_mr *mr;     // rdma
        priskv_ucx_memh *memh; // ucx
    };
} priskv_sgl_private;

typedef struct priskv_transport_req {
    priskv_transport_conn *conn;
    priskv_conn_operation *ops;
    priskv_workqueue *main_wq;
    struct list_node entry;
    priskv_request *req;
    uint64_t request_id;
    char *key;
    priskv_sgl_private *sgl;
    uint16_t nsgl;
    uint16_t keylen;
    uint64_t timeout;
    priskv_req_command cmd;
    void (*cb)(struct priskv_transport_req *req);
    priskv_generic_cb usercb;
#define PRISKV_TRANSPORT_REQ_FLAG_SEND (1 << 0)
#define PRISKV_TRANSPORT_REQ_FLAG_RECV (1 << 2)
#define PRISKV_TRANSPORT_REQ_FLAG_DONE                                                             \
    (PRISKV_TRANSPORT_REQ_FLAG_SEND | PRISKV_TRANSPORT_REQ_FLAG_RECV)
    uint8_t flags;
    uint16_t status;
    uint32_t length;
    void *result;
    bool delaying;
} priskv_transport_req;

typedef enum priskv_transport_conn_state {
    PRISKV_TRANSPORT_CONN_STATE_INIT,
    PRISKV_TRANSPORT_CONN_STATE_ESTABLISHED,
    PRISKV_TRANSPORT_CONN_STATE_CLOSING,
    PRISKV_TRANSPORT_CONN_STATE_CLOSED,
} priskv_transport_conn_state;

typedef struct priskv_transport_conn {
    char local_addr[PRISKV_ADDR_LEN];
    char peer_addr[PRISKV_ADDR_LEN];
    union {
        struct {
            struct rdma_cm_id *cm_id;
            struct rdma_event_channel *cm_channel;
            struct ibv_comp_channel *comp_channel;
            struct ibv_cq *cq;
            struct ibv_qp *qp;
        }; // rdma
        struct {
            int connfd;
            priskv_ucx_worker *worker;
            priskv_ucx_ep *ep;
            priskv_ucx_request *inflight_reqs;
        }; // ucx
    };

    uint8_t id;
    priskv_thread *thread;

    priskv_transport_mem rmem[PRISKV_TRANSPORT_MEM_MAX];

    priskv_connect_param param;
    uint64_t capacity;
    int epollfd;
    priskv_transport_conn_state state;
    struct list_head inflight_list;
    struct list_head complete_list;

    priskv_transport_req *keys_running_req;
    priskv_memory keys_mems;

    uint64_t stats[PRISKV_COMMAND_MAX];
    uint64_t resps;
    uint64_t wc_recv;
    uint64_t wc_send;
} priskv_transport_conn;

typedef struct priskv_client {
    priskv_threadpool *pool;
    priskv_transport_conn **conns;
    int nqueue;
    int cur_conn;
    int epollfd;
    priskv_workqueue *wq;
    priskv_conn_operation *ops;
} priskv_client;

typedef struct priskv_conn_operation {
    int (*init)(priskv_client *client, const char *raddr, int rport, const char *laddr, int lport,
                int nqueue);
    void (*deinit)(priskv_client *client);
    priskv_transport_conn *(*select_conn)(priskv_client *client);
    priskv_memory *(*reg_memory)(priskv_client *client, uint64_t offset, size_t length,
                                 uint64_t iova, int fd);
    void (*dereg_memory)(priskv_memory *mem);
    union {
        struct ibv_mr *(*get_mr)(priskv_memory *mem, int connid);
        priskv_ucx_memh *(*get_memh)(priskv_memory *mem, int connid);
    };
    void (*submit_req)(priskv_transport_req *req);
    void (*req_cb)(priskv_transport_req *req);
    priskv_transport_req *(*new_req)(priskv_client *client, priskv_transport_conn *conn,
                                     uint64_t request_id, const char *key, uint16_t keylen,
                                     priskv_sgl *sgl, uint16_t nsgl, uint64_t timeout,
                                     priskv_req_command cmd, priskv_generic_cb usercb);
} priskv_conn_operation;

typedef struct priskv_transport_driver {
    const char *name;
    int (*init)(void);
    priskv_conn_operation *(*get_sq_ops)(void);
    priskv_conn_operation *(*get_mq_ops)(void);
} priskv_transport_driver;

/**
 * @brief Process events for the connection.
 *
 * @param fd The file descriptor.
 * @param opaque The opaque pointer.
 * @param ev The event.
 */
void priskv_transport_conn_process(int fd, void *opaque, uint32_t ev);

#if defined(__cplusplus)
}
#endif

#endif
