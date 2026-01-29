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
    union {
        struct {
            uint32_t key;
        };  // rdma
        struct {
            uint32_t packed_rkey_len;
            uint8_t packed_rkey[0];
        };  // ucx
    };
} priskv_keyed_sgl;

/*
 * response header of PRISKV_COMMAND_KEYS.
 * [priskv_keys_resp][key][priskv_keys_resp][key] ... [priskv_keys_resp][key]
 */
typedef struct priskv_keys_resp {
    uint16_t keylen;
    uint16_t reserved;
    uint32_t valuelen;
    uint64_t addr_offset;
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
    PRISKV_COMMAND_FLUSH = 0x07, /* flush keys by regex */
    /* for the zero copy mode */
    PRISKV_COMMAND_ALLOC = 0x08,   /* alloc memory region if support zero copy */
    PRISKV_COMMAND_SEAL = 0x09,    /* seal memory region*/
    PRISKV_COMMAND_ACQUIRE = 0x0a, /* acquire memory region if support zero copy */
    PRISKV_COMMAND_RELEASE = 0x0b, /* release memory region */

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
    uint32_t alloc_length;
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
    uint64_t addr_offset; /* the address offset of memory region */
    uint32_t length;  /* the length of value */
    uint16_t status;  /* priskv_resp_status */
    uint8_t reserved[10];
} priskv_response;

/*
 * currently version 0x02 is supported only.
 */
#define PRISKV_CM_VERSION 0x02

/*
 * CM capability
 *
 * @version: must be PRISKV_CM_VERSION
 * @max_sgl: max SGLs supported by server/client.
 * @max_key_length: max key length in bytes supported by server/client.
 * @max_inflight_command: max inflight command(aka command depth) supported by
 * server/client.
 * @capacity: max capacity in bytes supported by server.
 *
 * @max_sgl, @max_key_length, @max_inflight_command of client must be less than
 * or equal to the limitations from the server side, otherwise the connection
 * will be disconnected. Or specify 0 to use the maximum value from server.
 */
typedef struct priskv_cm_cap {
    uint16_t version;
    uint16_t max_sgl;
    uint16_t max_key_length;
    uint16_t max_inflight_command;
    uint64_t capacity;
    uint8_t reserved[16];
} priskv_cm_cap;

typedef struct __attribute__((packed)) priskv_cm_ucx_handshake {
    uint8_t flag;  // 0: reject, 1: others
    union {
        struct __attribute__((packed)) {
            uint16_t version;
            uint16_t status;
            uint64_t value;
        };  // reject
        struct __attribute__((packed)) {
            priskv_cm_cap cap;
            uint32_t address_len;
            uint8_t address[0];
        };  // others
    };
} priskv_cm_ucx_handshake;

/*
 * CM status
 */
typedef enum priskv_cm_status {
    PRISKV_CM_REJ_STATUS_INVALID_CM_REP = 0x01,
    PRISKV_CM_REJ_STATUS_INVALID_VERSION = 0x02,
    PRISKV_CM_REJ_STATUS_INVALID_SGL = 0x03,
    PRISKV_CM_REJ_STATUS_INVALID_KEY_LENGTH = 0x04,
    PRISKV_CM_REJ_STATUS_INVALID_INFLIGHT_COMMAND = 0x05,
    PRISKV_CM_REJ_STATUS_ACL_REFUSE = 0x06,
    PRISKV_CM_REJ_STATUS_INVALID_WORKER_ADDR = 0x07,

    PRISKV_CM_REJ_STATUS_SERVER_ERROR = 0x10
} priskv_cm_status;

/*
 * connect reject
 */
typedef struct priskv_cm_rej {
    uint16_t version;
    uint16_t status; /* priskv_cm_status */
    uint8_t reserved[4];
    uint64_t value; /* indicate the supported value */
} priskv_cm_rej;

typedef enum priskv_proto_tag {
    PRISKV_PROTO_TAG_HANDSHAKE = 0x01,
    PRISKV_PROTO_TAG_CTRL = 0x02,
    PRISKV_PROTO_FULL_TAG_MASK = ~0LL,
} priskv_proto_tag;

/*
 *assuming max timeout means no timeout
 */
#define PRISKV_KEY_MAX_TIMEOUT 0xffffffffffffffffUL

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_PROTOCOL__ */
