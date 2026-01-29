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

#ifndef __PRISKV_PROTOCOL__
#define __PRISKV_PROTOCOL__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdint.h>
#include <sys/time.h>

/*
 * a Scatter Gather List (SGL) is a data structure in memory address space used to describe a
 * data buffer.
 */
typedef struct priskv_keyed_sgl {
    uint64_t addr;
    uint32_t length;
    uint32_t key;
} priskv_keyed_sgl;

/*
 * response header of PRISKV_COMMAND_KEYS.
 * [priskv_keys_resp][key][priskv_keys_resp][key] ... [priskv_keys_resp][key]
 */
typedef struct priskv_keys_resp {
    uint16_t keylen;
    uint16_t reserved;
    uint32_t valuelen;
} priskv_keys_resp;

/*
 * command of request
 */
typedef enum priskv_req_command {
    PRISKV_COMMAND_GET = 0x00,    /* get value of a key */
    PRISKV_COMMAND_SET = 0x01,    /* set value of a key */
    PRISKV_COMMAND_TEST = 0x02,   /* test key exist or not */
    PRISKV_COMMAND_DELETE = 0x03, /* delete a key */
    PRISKV_COMMAND_EXPIRE = 0x04, /* set expire time of a key */
    PRISKV_COMMAND_KEYS = 0x05,   /* get keys by regex */
    /* get the number of keys by regex, priskv_keys_resp::valuelen indicates it. */
    PRISKV_COMMAND_NRKEYS = 0x06,
    PRISKV_COMMAND_FLUSH = 0x07,         /* flush keys by regex */
    PRISKV_COMMAND_PIN = 0x08,           /* pin keys to prevent LRU eviction */
    PRISKV_COMMAND_UNPIN = 0x09,         /* unpin keys using token */
    PRISKV_COMMAND_GET_AND_PIN = 0x0A,   /* get value and pin the key in one operation */
    PRISKV_COMMAND_SET_AND_PIN = 0x0B,   /* set value and pin the key in one operation */
    PRISKV_COMMAND_GET_AND_UNPIN = 0x0C, /* get value and unpin the key in one operation */

    PRISKV_COMMAND_MAX /* not a part of protocol, keep last */
} priskv_req_command;

/*
 * request runtime in whole query
 */
typedef struct priskv_request_runtime {
    struct timeval server_metadata_recv_time; // Step 2: Server receives metadata request time
    struct timeval server_rw_kv_time;         // Step 3: Server reads/writes KV data time
    struct timeval server_data_send_time;     // Step 4: Server sends data request time
    struct timeval server_data_recv_time;     // Step 5: Server receives data request completion time
    struct timeval server_resp_send_time;     // Step 6: Server sends response time

    struct timeval client_metadata_send_time; // Step 1: Client sends metadata request time
} priskv_request_runtime;

/*
 * request from client, submitted by @IBV_WR_SEND
 */
typedef struct priskv_request {
    uint64_t request_id;
    uint64_t timeout; /* in ms */
    uint16_t command; /* priskv_req_command */
    uint8_t reserved[4];
    uint16_t nsgl; /* how many SGL contains following */
    uint16_t key_length;
    uint64_t token; /* token for unpin */
    priskv_request_runtime runtime;
    priskv_keyed_sgl sgls[0];
} priskv_request;

/*
 * response status to client
 */
typedef enum priskv_resp_status {
    PRISKV_RESP_STATUS_OK = 0x00,

    PRISKV_RESP_STATUS_INVALID_COMMAND = 0x100,
    PRISKV_RESP_STATUS_KEY_EMPTY,
    PRISKV_RESP_STATUS_KEY_TOO_BIG,
    PRISKV_RESP_STATUS_VALUE_EMPTY,
    PRISKV_RESP_STATUS_VALUE_TOO_BIG,
    PRISKV_RESP_STATUS_NO_SUCH_COMMAND,
    PRISKV_RESP_STATUS_NO_SUCH_KEY,
    PRISKV_RESP_STATUS_INVALID_SGL,
    PRISKV_RESP_STATUS_INVALID_REGEX,
    PRISKV_RESP_STATUS_KEY_UPDATING,
    PRISKV_RESP_STATUS_CONNECT_ERROR,
    PRISKV_RESP_STATUS_SERVER_ERROR,

    PRISKV_RESP_STATUS_NO_MEM = 0x200
} priskv_resp_status;

/*
 * response to client, submitted by @IBV_WR_SEND
 */
typedef struct priskv_response {
    uint64_t request_id;
    uint64_t timeout; /* in ms */
    uint64_t pin_token; /* the token of operation */
    uint32_t length;  /* the length of value */
    uint16_t status;  /* priskv_resp_status */
    uint8_t reserved[10];
} priskv_response;

/*
 * currently version 0x01 is supported only.
 */
#define PRISKV_RDMA_CM_VERSION 0x01

/*
 * rdma connect request
 *
 * @version: must be PRISKV_RDMA_CM_VERSION
 * @max_sgl: request max SGLs from client.
 * @max_key_length: request max key length in bytes from client.
 * @max_inflight_command: request max inflight command(aka command depth) from client.
 *
 * @max_sgl, @max_key_length, @max_inflight_command must be less than or equal to the
 * limitations from the server side, otherwise the server rejects connection. Or specify 0 to
 * use the maximum value from server.
 */
typedef struct priskv_rdma_cm_req {
    uint16_t version;
    uint16_t max_sgl;
    uint16_t max_key_length;
    uint16_t max_inflight_command;
    uint8_t reserved[24];
} priskv_rdma_cm_req;

/*
 * rdma connect reply
 */
typedef struct priskv_rdma_cm_rep {
    uint16_t version;
    uint16_t max_sgl;
    uint16_t max_key_length;
    uint16_t max_inflight_command;
    uint64_t capacity;
    uint8_t reserved[16];
} priskv_rdma_cm_rep;

/*
 * status on RDMA CM rejection
 */
typedef enum priskv_rdma_cm_status {
    PRISKV_RDMA_CM_REJ_STATUS_INVALID_CM_REP = 0x01,
    PRISKV_RDMA_CM_REJ_STATUS_INVALID_VERSION = 0x02,
    PRISKV_RDMA_CM_REJ_STATUS_INVALID_SGL = 0x03,
    PRISKV_RDMA_CM_REJ_STATUS_INVALID_KEY_LENGTH = 0x04,
    PRISKV_RDMA_CM_REJ_STATUS_INVALID_INFLIGHT_COMMAND = 0x05,
    PRISKV_RDMA_CM_REJ_STATUS_ACL_REFUSE = 0x06,

    PRISKV_RDMA_CM_REJ_STATUS_SERVER_ERROR = 0x10
} priskv_rdma_cm_status;

/*
 * rdma connect reject
 */
typedef struct priskv_rdma_cm_rej {
    uint16_t version;
    uint16_t status; /* priskv_rdma_cm_status */
    uint8_t reserved[4];
    uint64_t value; /* indicate the supported value */
} priskv_rdma_cm_rej;

/*
 *assuming max timeout means no timeout
 */
#define PRISKV_KEY_MAX_TIMEOUT 0xffffffffffffffffUL

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_PROTOCOL__ */
