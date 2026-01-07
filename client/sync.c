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

#include <errno.h>
#include <sys/epoll.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "priskv-protocol-helper.h"
#include "priskv-log.h"
#include "priskv.h"

typedef struct priskv_rdma_req_sync {
    priskv_status status;
    uint32_t valuelen;
    uint64_t pin_token;
    bool done;
} priskv_rdma_req_sync;

static void priskv_common_sync_cb(uint64_t request_id, priskv_status status, void *result,
                                  void *result_token)
{
    priskv_rdma_req_sync *rdma_req_sync = (priskv_rdma_req_sync *)request_id;
    uint32_t valuelen = result ? *(uint32_t *)result : 0;
    uint64_t token = result_token ? *(uint64_t *)result_token : 0;

    priskv_log_debug("RDMA: callback request_id 0x%lx, status: %s[0x%x], length %d\n", request_id,
                   priskv_resp_status_str(status), status, valuelen);
    rdma_req_sync->status = status;
    rdma_req_sync->valuelen = valuelen;
    rdma_req_sync->pin_token = token;
    rdma_req_sync->done = true;
}

static inline int priskv_sync_wait(priskv_client *client, bool *done)
{
    while (!*done) {
        priskv_process(client, 0);
    }

    return 0;
}

int priskv_get(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl, uint32_t *valuelen)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_get_async(client, key, sgl, nsgl, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *valuelen = rdma_req_sync.valuelen;

    return rdma_req_sync.status;
}

int priskv_get_and_pin(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                       uint64_t *pin_token, uint32_t *valuelen)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_get_and_pin_async(client, key, sgl, nsgl, *pin_token, (uint64_t)&rdma_req_sync,
                             priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *valuelen = rdma_req_sync.valuelen;
    *pin_token = rdma_req_sync.pin_token;

    return rdma_req_sync.status;
}

int priskv_get_and_unpin(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                         uint64_t pin_token, uint32_t *valuelen)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_get_and_unpin_async(client, key, sgl, nsgl, pin_token, (uint64_t)&rdma_req_sync,
                               priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *valuelen = rdma_req_sync.valuelen;

    return rdma_req_sync.status;
}

int priskv_set(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl, uint64_t timeout)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_set_async(client, key, sgl, nsgl, timeout, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);

    return rdma_req_sync.status;
}

int priskv_test(priskv_client *client, const char *key, uint32_t *valuelen)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_test_async(client, key, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *valuelen = rdma_req_sync.valuelen;

    return rdma_req_sync.status;
}

int priskv_delete(priskv_client *client, const char *key)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_delete_async(client, key, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);

    return rdma_req_sync.status;
}

int priskv_expire(priskv_client *client, const char *key, uint64_t timeout)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_expire_async(client, key, timeout, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);

    return rdma_req_sync.status;
}

int priskv_nrkeys(priskv_client *client, const char *regex, uint32_t *nkey)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_nrkeys_async(client, regex, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *nkey = rdma_req_sync.valuelen;

    return rdma_req_sync.status;
}

int priskv_flush(priskv_client *client, const char *regex, uint32_t *nkey)
{
    priskv_rdma_req_sync rdma_req_sync = {.status = 0xffff, .done = false};

    priskv_flush_async(client, regex, (uint64_t)&rdma_req_sync, priskv_common_sync_cb);
    priskv_sync_wait(client, &rdma_req_sync.done);
    *nkey = rdma_req_sync.valuelen;

    return rdma_req_sync.status;
}

typedef struct priskv_rdma_keys_sync {
    priskv_status status;
    bool done;
    priskv_keyset **keyset;
} priskv_rdma_keys_sync;

static void priskv_keys_sync_cb(uint64_t request_id, priskv_status status, void *result)
{
    priskv_rdma_keys_sync *keys_req_sync = (priskv_rdma_keys_sync *)request_id;

    priskv_log_debug("RDMA: callback request_id 0x%lx, status: %s[0x%x]\n", request_id,
                   priskv_resp_status_str(status), status);
    keys_req_sync->status = status;
    keys_req_sync->done = true;

    if (status == PRISKV_STATUS_OK) {
        *keys_req_sync->keyset = (priskv_keyset *)result;
    }
}

int priskv_keys(priskv_client *client, const char *regex, priskv_keyset **keyset)
{
    priskv_rdma_keys_sync keys_req_sync = {0};

    keys_req_sync.keyset = keyset;
    priskv_keys_async(client, regex, (uint64_t)&keys_req_sync, priskv_keys_sync_cb);
    priskv_sync_wait(client, &keys_req_sync.done);

    if (keys_req_sync.status == PRISKV_STATUS_OK) {
        return keys_req_sync.status;
    }

    return keys_req_sync.status;
}
