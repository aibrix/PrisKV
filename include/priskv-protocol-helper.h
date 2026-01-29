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

static inline uint16_t priskv_request_key_off(uint16_t nsgl)
{
    // size_t offset = offsetof(priskv_request, sgls);
    // return (uint16_t)(offset + nsgl * sizeof(priskv_keyed_sgl));
    return sizeof(priskv_request) + sizeof(priskv_keyed_sgl) * nsgl;
}

static inline uint16_t priskv_request_size(uint16_t nsgl, uint16_t keylen)
{
    return priskv_request_key_off(nsgl) + keylen;
}

static inline uint8_t *priskv_request_key(priskv_request *req, uint16_t nsgl)
{
    unsigned char *base = (unsigned char *)req;
    return base + priskv_request_key_off(nsgl);
}

static inline const char *priskv_command_str(priskv_req_command cmd)
{
    static const char *cmd_str[] = {"GET",   "SET", "TEST",  "DELETE", "EXPIRE", "KEYS", "NRKEYS",
                                    "FLUSH", "PIN", "UNPIN", "PGET",   "UGET",   "PSET"};

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

static inline const char *priskv_rdma_cm_status_str(priskv_rdma_cm_status status)
{
    switch (status) {
    case PRISKV_RDMA_CM_REJ_STATUS_INVALID_CM_REP:
        return "Invalid CM Reply";

    case PRISKV_RDMA_CM_REJ_STATUS_INVALID_VERSION:
        return "Invalid version";

    case PRISKV_RDMA_CM_REJ_STATUS_INVALID_SGL:
        return "Invalid SGL";

    case PRISKV_RDMA_CM_REJ_STATUS_INVALID_KEY_LENGTH:
        return "Invalid Key length";

    case PRISKV_RDMA_CM_REJ_STATUS_INVALID_INFLIGHT_COMMAND:
        return "Invalid inflight command";

    case PRISKV_RDMA_CM_REJ_STATUS_ACL_REFUSE:
        return "ACL refuse";

    case PRISKV_RDMA_CM_REJ_STATUS_SERVER_ERROR:
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
