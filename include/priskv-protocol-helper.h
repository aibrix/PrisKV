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

#ifndef __PRISKV_PROTOCOL_HELPER__
#define __PRISKV_PROTOCOL_HELPER__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stddef.h>
#include "priskv-protocol.h"
#include "priskv-utils.h"

static uint32_t priskv_ucx_max_rkey_len = 256;

static inline uint16_t priskv_rdma_request_key_off(priskv_request *req)
{
    return sizeof(priskv_request) + sizeof(priskv_keyed_sgl) * be16toh(req->nsgl);
}

static inline uint16_t priskv_rdma_request_size(priskv_request *req)
{
    return priskv_rdma_request_key_off(req) + be16toh(req->key_length);
}

static inline unsigned int priskv_rdma_max_request_size_aligned(uint16_t max_sgl,
                                                                uint16_t max_key_length)
{
    uint16_t s = sizeof(priskv_request) + sizeof(priskv_keyed_sgl) * max_sgl + max_key_length;

    return ALIGN_UP(s, 64);
}

static inline uint8_t *priskv_rdma_request_key(priskv_request *req)
{
    unsigned char *base = (unsigned char *)req;
    return base + priskv_rdma_request_key_off(req);
}

static inline uint16_t priskv_ucx_request_key_off(priskv_request *req)
{
    uint16_t nsgl = be16toh(req->nsgl);
    uint16_t off = sizeof(priskv_request);
    uint16_t i = 0;
    for (; i < nsgl; i++) {
        off += sizeof(priskv_keyed_sgl) + be32toh(req->sgls[i].packed_rkey_len);
    }
    return off;
}

static inline uint16_t priskv_ucx_request_size(priskv_request *req)
{
    return priskv_ucx_request_key_off(req) + be16toh(req->key_length);
}

static inline unsigned int priskv_ucx_max_request_size_aligned(uint16_t max_sgl,
                                                               uint16_t max_key_length)
{
    uint16_t s = sizeof(priskv_request) +
                 (sizeof(priskv_keyed_sgl) + priskv_ucx_max_rkey_len) * max_sgl + max_key_length;

    return ALIGN_UP(s, 64);
}

static inline uint8_t *priskv_ucx_request_key(priskv_request *req)
{
    unsigned char *base = (unsigned char *)req;
    return base + priskv_ucx_request_key_off(req);
}

static inline void priskv_ucx_to_hex(uint8_t *hex, uint8_t *str, uint32_t str_len)
{
    for (size_t j = 0; j < str_len; j++) {
        unsigned char b = str[j];
        hex[j * 2] = "0123456789abcdef"[b >> 4];
        hex[j * 2 + 1] = "0123456789abcdef"[b & 0xF];
    }
    hex[str_len * 2] = '\0';
}

static inline const char *priskv_command_str(priskv_req_command cmd)
{
    static const char *cmd_str[] = {"GET",    "SET",  "TEST",   "DELETE",
                                    "EXPIRE", "KEYS", "NRKEYS", "FLUSH"};

    if (cmd >= PRISKV_COMMAND_MAX) {
        return "unknown";
    }

    return cmd_str[cmd];
}

static inline const char *priskv_resp_status_str(priskv_resp_status status)
{
    switch (status) {
    case PRISKV_RESP_STATUS_OK:
        return "OK";

    case PRISKV_RESP_STATUS_INVALID_COMMAND:
        return "Invalid command";

    case PRISKV_RESP_STATUS_KEY_EMPTY:
        return "Empty key";

    case PRISKV_RESP_STATUS_KEY_TOO_BIG:
        return "Key too big";

    case PRISKV_RESP_STATUS_VALUE_EMPTY:
        return "Empty Value";

    case PRISKV_RESP_STATUS_VALUE_TOO_BIG:
        return "Value too big";

    case PRISKV_RESP_STATUS_NO_SUCH_COMMAND:
        return "No such command";

    case PRISKV_RESP_STATUS_NO_SUCH_KEY:
        return "No such key";

    case PRISKV_RESP_STATUS_INVALID_SGL:
        return "Invalid SGL";

    case PRISKV_RESP_STATUS_INVALID_REGEX:
        return "Invalid regex";

    case PRISKV_RESP_STATUS_KEY_UPDATING:
        return "Key is updating";

    case PRISKV_RESP_STATUS_CONNECT_ERROR:
        return "Connect error";

    case PRISKV_RESP_STATUS_SERVER_ERROR:
        return "Server internal error";

    case PRISKV_RESP_STATUS_NO_MEM:
        return "No memory";
    }

    return "Unknown";
}

static inline const char *priskv_cm_status_str(priskv_cm_status status)
{
    switch (status) {
    case PRISKV_CM_REJ_STATUS_INVALID_CM_REP:
        return "Invalid CM Reply";

    case PRISKV_CM_REJ_STATUS_INVALID_VERSION:
        return "Invalid version";

    case PRISKV_CM_REJ_STATUS_INVALID_SGL:
        return "Invalid SGL";

    case PRISKV_CM_REJ_STATUS_INVALID_KEY_LENGTH:
        return "Invalid Key length";

    case PRISKV_CM_REJ_STATUS_INVALID_INFLIGHT_COMMAND:
        return "Invalid inflight command";

    case PRISKV_CM_REJ_STATUS_ACL_REFUSE:
        return "ACL refuse";

    case PRISKV_CM_REJ_STATUS_INVALID_WORKER_ADDR:
        return "Invalid worker address";

    case PRISKV_CM_REJ_STATUS_SERVER_ERROR:
        return "server error";
    }

    return "Unknown";
}

static inline uint32_t priskv_sgl_size_from_be(priskv_keyed_sgl *sgls, uint16_t nsgl)
{
    uint32_t size = 0;

    for (uint16_t i = 0; i < nsgl; i++) {
        priskv_keyed_sgl *sgl = &sgls[i];
        size += be32toh(sgl->length);
    }

    return size;
}

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_PROTOCOL_HELPER__ */
