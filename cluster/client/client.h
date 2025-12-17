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

#include <stdint.h>
#include <stdio.h>
#include <errno.h>
#include "priskv.h"

typedef struct priskvClusterClient priskvClusterClient;
typedef struct priskvClusterMemory priskvClusterMemory;

priskvClusterClient *priskvClusterConnect(const char *raddr, int rport, const char *password);
void priskvClusterClose(priskvClusterClient *client);
int priskvClusterClientGetFd(priskvClusterClient *client);
void priskvClusterClientProcess(priskvClusterClient *client, int timeout);

priskvClusterMemory *priskvClusterRegMemory(priskvClusterClient *client, uint64_t offset, size_t length,
                                        uint64_t iova, int fd);
void priskvClusterDeregMemory(priskvClusterMemory *mem);
typedef struct priskvClusterSGL {
    uint64_t iova;
    uint32_t length;
    priskvClusterMemory *mem;
} priskvClusterSGL;

typedef enum priskvClusterStatus {
    PRISKV_CLUSTER_STATUS_OK = 0x00,
    /* unrecognized command, typically protocol error */
    PRISKV_CLUSTER_STATUS_INVALID_COMMAND = 0x100,

    /* zero length key in use? Note that '\0' is allowed */
    PRISKV_CLUSTER_STATUS_KEY_EMPTY,

    /* does the length of key exceed @max_key_length? */
    PRISKV_CLUSTER_STATUS_KEY_TOO_BIG,

    /* zero length value in use? Note that '\0' is allowed */
    PRISKV_CLUSTER_STATUS_VALUE_EMPTY,

    /* the length of @priskv_sgl is smaller than the length of value */
    PRISKV_CLUSTER_STATUS_VALUE_TOO_BIG,

    /* no such command */
    PRISKV_CLUSTER_STATUS_NO_SUCH_COMMAND,

    /* no such key */
    PRISKV_CLUSTER_STATUS_NO_SUCH_KEY,

    /* invalid SGL. the number of SGL within a command must not exceed @max_sgl */
    PRISKV_CLUSTER_STATUS_INVALID_SGL,

    /* invalid regex */
    PRISKV_CLUSTER_STATUS_INVALID_REGEX,

    /* key is updating */
    PRISKV_CLUSTER_STATUS_KEY_UPDATING,

    /* no enough memory reported by server side */
    PRISKV_CLUSTER_STATUS_NO_MEM = 0x200,

    /* RDMA disconnected from the server side */
    PRISKV_CLUSTER_STATUS_DISCONNECTED = 0xF00,

    /* local RDMA error occurs */
    PRISKV_CLUSTER_STATUS_RDMA_ERROR,

    /* does inflight requests exceed @max_inflight_command? */
    PRISKV_CLUSTER_STATUS_BUSY,

    /* unexpected protocol error */
    PRISKV_CLUSTER_STATUS_PROTOCOL_ERROR,
} priskvClusterStatus;

static inline const char *priskv_cluster_status_str(priskvClusterStatus status)
{
    switch (status) {
    case PRISKV_CLUSTER_STATUS_OK:
        return "OK";

    case PRISKV_CLUSTER_STATUS_INVALID_COMMAND:
        return "Invalid command";

    case PRISKV_CLUSTER_STATUS_KEY_EMPTY:
        return "Empty key";

    case PRISKV_CLUSTER_STATUS_KEY_TOO_BIG:
        return "Key too big";

    case PRISKV_CLUSTER_STATUS_VALUE_EMPTY:
        return "Empty Value";

    case PRISKV_CLUSTER_STATUS_VALUE_TOO_BIG:
        return "Value too big";

    case PRISKV_CLUSTER_STATUS_NO_SUCH_COMMAND:
        return "No such command";

    case PRISKV_CLUSTER_STATUS_NO_SUCH_KEY:
        return "No such key";

    case PRISKV_CLUSTER_STATUS_INVALID_SGL:
        return "Invalid SGL";

    case PRISKV_CLUSTER_STATUS_INVALID_REGEX:
        return "Invalid regex";

    case PRISKV_CLUSTER_STATUS_KEY_UPDATING:
        return "Key is updating";

    case PRISKV_CLUSTER_STATUS_NO_MEM:
        return "No memory";

    case PRISKV_CLUSTER_STATUS_DISCONNECTED:
        return "Disconnected";

    case PRISKV_CLUSTER_STATUS_RDMA_ERROR:
        return "RDMA error";

    case PRISKV_CLUSTER_STATUS_BUSY:
        return "Busy";

    case PRISKV_CLUSTER_STATUS_PROTOCOL_ERROR:
        return "Protocol error";
    }

    return "Unknown";
}

#define PRISKV_CLUSTER_KEY_MAX_TIMEOUT 0xffffffffffffffffUL

typedef void (*priskvClusterCallback)(priskvClusterStatus status, uint32_t valuelen, void *cbarg);

/* async APIs */
int priskvClusterAsyncGet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                        uint16_t nsgl, priskvClusterCallback cb, void *cbarg);
int priskvClusterAsyncSet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                        uint16_t nsgl, uint64_t timeout, priskvClusterCallback cb, void *cbarg);
int priskvClusterAsyncTest(priskvClusterClient *client, const char *key, priskvClusterCallback cb,
                         void *cbarg);
int priskvClusterAsyncDelete(priskvClusterClient *client, const char *key, priskvClusterCallback cb,
                           void *cbarg);
int priskvClusterAsyncGetAndPin(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                                uint16_t nsgl, uint64_t *pin_token, priskvClusterCallback cb,
                                void *cbarg);
int priskvClusterAsyncGetAndUnPin(priskvClusterClient *client, const char *key,
                                  priskvClusterSGL *sgl, uint16_t nsgl, uint64_t *pin_token,
                                  priskvClusterCallback cb, void *cbarg);
int priskvClusterAsyncSetAndPin(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                                uint16_t nsgl, uint64_t timeout, uint64_t *pin_token,
                                priskvClusterCallback cb, void *cbarg);

/* sync APIs */
priskvClusterStatus priskvClusterGet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                                 uint16_t nsgl, uint32_t *value_len);
priskvClusterStatus priskvClusterSet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                                 uint16_t nsgl, uint64_t timeout);
priskvClusterStatus priskvClusterTest(priskvClusterClient *client, const char *key, uint32_t *value_len);
priskvClusterStatus priskvClusterDelete(priskvClusterClient *client, const char *key);
priskvClusterStatus priskvClusterKeys(priskvClusterClient *client, const char *regex,
                                  priskv_keyset **keyset);
priskvClusterStatus priskvClusterStatusFromPRISKVStatus(priskv_status status);

int priskvClusterGetAndPin(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                           uint16_t nsgl, uint32_t *value_len, uint64_t *pin_token);
int priskvClusterGetAndUnPin(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                             uint16_t nsgl, uint32_t *value_len, uint64_t *pin_token);
int priskvClusterSetAndPin(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                           uint16_t nsgl, uint64_t timeout, uint64_t *pin_token);
