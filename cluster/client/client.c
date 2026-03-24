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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/epoll.h>
#include <hiredis/hiredis.h>
#include <sys/timerfd.h>
#include <time.h>

#include "client.h"
#include "priskv-config.h"
#include "priskv-log.h"
#include "priskv-utils.h"
#include "priskv-event.h"
#include "priskv-codec.h"
#include "jsonobjs.h"
#include "crc16.h"
#include "list.h"

#define PRISKV_CLUSTER_SLOTS 4096

typedef struct priskvClusterMetaServer priskvClusterMetaServer;
typedef struct priskvClusterMetaData priskvClusterMetaData;
typedef struct priskvClusterNode priskvClusterNode;
typedef struct priskvClusterMemoryCtx priskvClusterMemoryCtx;
typedef struct priskvClusterRequest priskvClusterRequest;

struct list_head retry_req_list = LIST_HEAD_INIT(retry_req_list);
typedef enum { GET, SET, TEST, DELETE, ALLOC, SEAL, ACQUIRE, RELEASE, DROP } RequestType;

struct priskvClusterMetaServer {
    char *addr;
    int port;
    redisContext *redisCtx;
};

struct priskvClusterNode {
    int id;
    char *addr;
    int port;
    priskv_client *client;
    int refCount;
};

struct priskvClusterMetaData {
    priskvClusterNode *slots[PRISKV_CLUSTER_SLOTS];
};

struct priskvClusterMemoryCtx {
    priskvClusterMemory *mem;
    uint64_t offset;
    size_t length;
    uint64_t iova;
    int fd;
};

struct priskvClusterClient {
    int epollfd;
    priskvClusterMetaServer metaServer;
    priskvClusterMetaData metaData;
    priskv_codec *codec;
    priskvClusterNode *nodes;
    int nodeCount;
    bool metaUpdating;              /* 元数据更新标志位 */
    int metaTimerFd;                /* 定时器文件描述符 */
    int metaVersion;                /* 当前元数据版本 */
    priskvClusterMemoryCtx *memCtx; /* 集群通信内存上下文 */
};

struct priskvClusterMemory {
    priskv_memory **mems;
    int count;
};

struct priskvClusterRequest {
    priskv_sgl *sgl; /* Cluster communication SGL: stores offsets into communication memory blocks */
    uint16_t nsgl;
    priskvClusterCallback cb;
    priskvClusterZeroCopyCallback zero_copy_cb;
    void *cbarg;
    RequestType type;
    const char *key;
    priskvClusterSGL *cluster_sgl; /* Cluster communication SGL: stores base addresses of communication memory blocks (used for retry) */
    uint64_t timeout;
    uint64_t pin_ttl_ms; /* PIN TTL in ms; 0 means server default */
    bool pin_key;        /* Whether to PIN the key (interpreted by request type) */
    bool unpin_key;      /* Whether to UNPIN the key (interpreted by request type) */
    uint32_t alloc_length;
    uint64_t token; /* Token used for SEAL/RELEASE */
    priskvClusterNode *node;
    priskvClusterClient *client;
    struct list_node entry;
};

static int priskvClusterMetaServerConnect(priskvClusterMetaServer *metaServer, const char *addr,
                                          int port, const char *password)
{
    struct timeval timeout = {g_config.client.meta_server_connect_timeout_sec, 0};

    metaServer->redisCtx = redisConnectWithTimeout(addr, port, timeout);
    if (!metaServer->redisCtx || metaServer->redisCtx->err) {
        if (metaServer->redisCtx) {
            priskv_log_error("Failed to connect to meta server %s:%d, %s\n", addr, port,
                             metaServer->redisCtx->errstr);
            redisFree(metaServer->redisCtx);
            metaServer->redisCtx = NULL;
        } else {
            priskv_log_error(
                "Failed to connect to meta server %s:%d, can't allocate redis context\n", addr,
                port);
        }

        return -1;
    }

    metaServer->addr = strdup(addr);
    metaServer->port = port;

    if (password != NULL) {
        redisReply *reply = redisCommand(metaServer->redisCtx, "AUTH %s", password);
        if (!reply || reply->type == REDIS_REPLY_ERROR) {
            priskv_log_error("Failed to authenticate to meta server %s:%d, %s\n", addr, port,
                             reply ? reply->str : "unknown error");

            redisFree(metaServer->redisCtx);
            metaServer->redisCtx = NULL;
            freeReplyObject(reply);
            return -1;
        }

        freeReplyObject(reply);
    }

    return 0;
}

static void priskvClusterMetaServerClose(priskvClusterMetaServer *metaServer)
{
    if (metaServer->redisCtx) {
        redisFree(metaServer->redisCtx);
        metaServer->redisCtx = NULL;
    }
    free(metaServer->addr);
    metaServer->addr = NULL;
    metaServer->port = 0;
}

static priskvClusterMetaDataInfo *priskvClusterMetaDataGetFromServer(priskvClusterClient *client)
{
    priskvClusterMetaDataInfo *info = NULL;
    redisReply *reply;

    reply = redisCommand(client->metaServer.redisCtx, "GET %s", g_config.client.metadata_key);

    priskv_log_debug("priskvClusterMetaData: %s\n", reply->str);

    info = priskv_codec_decode(client->codec, reply->str, &priskvClusterMetaDataInfo_obj);
    if (!info) {
        priskv_log_error("Failed to decode meta data: %s, error: %s\n", reply->str,
                         priskv_codec_get_error(client->codec));
        return NULL;
    }

    freeReplyObject(reply);

    return info;
}

static void priskvClusterMetaDataInfoFree(priskvClusterClient *client,
                                          priskvClusterMetaDataInfo *metaDataInfo)
{
    priskv_codec_free_struct(client->codec, metaDataInfo, &priskvClusterMetaDataInfo_obj);
}

static inline int priskvClusterSlotAdd(priskvClusterClient *client, priskvClusterNode *node,
                                       int slot)
{
    assert(slot < PRISKV_CLUSTER_SLOTS);
    assert(!client->metaData.slots[slot]);

    client->metaData.slots[slot] = node;
    return 0;
}

static inline int priskvClusterSlotDelete(priskvClusterClient *client, int slot)
{
    assert(slot < PRISKV_CLUSTER_SLOTS);
    assert(client->metaData.slots[slot]);

    client->metaData.slots[slot] = NULL;
    return 0;
}

static void priskvClusterNodeHandler(int fd, void *opaque, uint32_t events)
{
    priskvClusterNode *node = opaque;

    priskv_process(node->client, EPOLLIN);
}

static inline int priskvClusterNodeOpen(priskvClusterClient *client, priskvClusterNode *node,
                                        priskvClusterMetaDataNodeInfo *nodeInfo, int id)
{
    node->id = id;
    node->addr = strdup(nodeInfo->addr);
    node->port = nodeInfo->port;
    node->client = priskv_connect(nodeInfo->addr, nodeInfo->port, NULL, 0, g_config.client.nqueue);
    node->refCount = 0;
    if (!node->client) {
        priskv_log_error("Failed to connect to node %s:%d\n", nodeInfo->addr, nodeInfo->port);
        return -1;
    }

    int fd = priskv_get_fd(node->client);
    priskv_set_fd_handler(fd, priskvClusterNodeHandler, NULL, node);
    priskv_add_event_fd(client->epollfd, fd);

    return 0;
}

static inline void priskvClusterNodeClose(priskvClusterClient *client, priskvClusterNode *node)
{
    if (node->client) {
        priskv_close(node->client);
        node->client = NULL;
    }
    if (node->addr) {
        free(node->addr);
        node->addr = NULL;
    }
    node->port = 0;
}

static inline int priskvClusterMetaDataNodeLoad(priskvClusterClient *client,
                                                priskvClusterNode *node,
                                                priskvClusterMetaDataNodeInfo *nodeInfo, int id)
{
    if (priskvClusterNodeOpen(client, node, nodeInfo, id)) {
        priskv_log_error("Failed to open node %s:%d\n", nodeInfo->addr, nodeInfo->port);
        return -1;
    }

    for (int i = 0; i < nodeInfo->slotRangeCount; i++) {
        priskvClusterMetaDataNodeSlotRange *slotRange = &nodeInfo->slotRanges[i];
        int start = slotRange->start;
        int end = slotRange->end;

        assert(start <= end && end < PRISKV_CLUSTER_SLOTS);

        for (int slot = start; slot <= end; slot++) {
            if (priskvClusterSlotAdd(client, node, slot)) {
                priskv_log_error("Failed to add node %s:%d to slot %d\n", nodeInfo->addr,
                                 nodeInfo->port, slot);
                return -1;
            }
        }
    }

    return 0;
}

static inline void priskvClusterMetaDataNodeUnload(priskvClusterClient *client,
                                                   priskvClusterNode *node)
{
    for (int i = 0; i < PRISKV_CLUSTER_SLOTS; i++) {
        if (client->metaData.slots[i] == node) {
            priskvClusterSlotDelete(client, i);
        }
    }

    priskvClusterNodeClose(client, node);
}

static int priskvClusterMetaDataLoad(priskvClusterClient *client,
                                     priskvClusterMetaDataInfo *metaDataInfo)
{
    client->nodes = realloc(client->nodes, sizeof(priskvClusterNode) * metaDataInfo->nodeCount);
    client->nodeCount = metaDataInfo->nodeCount;
    client->metaVersion = metaDataInfo->version;

    for (int i = 0; i < metaDataInfo->nodeCount; i++) {
        priskvClusterNode *node = &client->nodes[i];
        priskvClusterMetaDataNodeInfo *nodeInfo = &metaDataInfo->nodes[i];
        assert(!priskvClusterMetaDataNodeLoad(client, node, nodeInfo, i));
    }

    return 0;
}

static void priskvClusterMetaDataUnload(priskvClusterClient *client)
{
    for (int i = 0; i < client->nodeCount; i++) {
        priskvClusterMetaDataNodeUnload(client, &client->nodes[i]);
    }

    free(client->nodes);
    client->nodes = NULL;
    client->nodeCount = 0;
}

static int priskvClusterMetaDataUpdate(priskvClusterClient *client)
{
    priskvClusterMetaDataInfo *metaDataInfo = priskvClusterMetaDataGetFromServer(client);
    if (!metaDataInfo) {

        return -1;
    }

    if (priskvClusterMetaDataLoad(client, metaDataInfo)) {
        return -1;
    }

    priskvClusterMetaDataInfoFree(client, metaDataInfo);

    return 0;
}

static priskv_memory *priskvClusterGetMemory(priskvClusterMemory *mem, uint64_t nodeIdx)
{
    if (!mem) {
        return NULL;
    }

    assert(nodeIdx < mem->count);
    return mem->mems[nodeIdx];
}

/* Modified from redis */
static unsigned int clusterManagerKeyHashSlot(const char *key, int keylen)
{
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++) {
        if (key[s] == '{') {
            break;
        }
    }

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) {
        return crc16(key, keylen) & 0x0FFF;
    }

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++) {
        if (key[e] == '}') {
            break;
        }
    }
    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) {
        return crc16(key, keylen) & 0x0FFF;
    }

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x0FFF;
}

static inline priskvClusterNode *priskvClusterGetNode(priskvClusterClient *client, const char *key)
{
    if (g_config.client.direct_mode) {
        // direct mode, return the only node
        assert(client->codec == NULL);
        return client->nodes;
    }

    uint16_t slot = clusterManagerKeyHashSlot((char *)key, strlen(key));
    return client->metaData.slots[slot];
}

static priskvClusterRequest *
priskvClusterRequestNewBase(priskvClusterNode *node, priskvClusterSGL *sgl, uint16_t nsgl,
                            priskvClusterCallback cb, priskvClusterZeroCopyCallback zero_copy_cb,
                            void *cbarg, RequestType type, const char *key, uint64_t timeout,
                            bool pin_key, uint64_t pin_ttl_ms, bool unpin_key,
                            uint32_t alloc_length, uint64_t token, priskvClusterClient *client)
{
    priskvClusterRequest *req = malloc(sizeof(priskvClusterRequest));

    req->sgl = malloc(sizeof(priskv_sgl) * nsgl);
    req->nsgl = nsgl;
    req->cb = cb;
    req->zero_copy_cb = zero_copy_cb;
    req->cbarg = cbarg;
    req->type = type;
    req->key = key;
    req->cluster_sgl = malloc(sizeof(priskvClusterSGL) * nsgl);
    req->timeout = timeout;
    req->pin_key = pin_key;
    req->pin_ttl_ms = pin_ttl_ms;
    req->unpin_key = unpin_key;
    req->alloc_length = alloc_length;
    req->node = node;
    req->token = token;
    req->client = client;

    for (uint16_t i = 0; i < nsgl; i++) {
        req->cluster_sgl[i].iova = sgl[i].iova;
        req->cluster_sgl[i].length = sgl[i].length;
        req->cluster_sgl[i].mem = sgl[i].mem;
    }

    for (uint16_t i = 0; i < nsgl; i++) {
        req->sgl[i].iova = sgl[i].iova;
        req->sgl[i].length = sgl[i].length;
        req->sgl[i].mem = priskvClusterGetMemory(sgl[i].mem, node->id);
    }

    return req;
}

static priskvClusterRequest *priskvClusterRequestNew(priskvClusterNode *node, priskvClusterSGL *sgl,
                                                     uint16_t nsgl, priskvClusterCallback cb,
                                                     void *cbarg, RequestType type, const char *key,
                                                     uint64_t timeout, priskvClusterClient *client)
{
    /* For ACQUIRE/SEAL, interpret 'timeout' from callers as pin_ttl_ms to keep API stable */
    return priskvClusterRequestNewBase(node, sgl, nsgl, cb, NULL, cbarg, type, key, timeout,
                                       false /*pin_key*/, 0 /*pin_ttl_ms*/, false /* unpin_key */,
                                       0, 0, client);
}

static priskvClusterRequest *
priskvClusterZeroCopyRequestNew(priskvClusterNode *node, priskvClusterZeroCopyCallback cb,
                                void *cbarg, RequestType type, const char *key,
                                uint32_t alloc_length, uint64_t timeout, bool pin_key,
                                uint64_t pin_ttl_ms, bool unpin_key, priskvClusterClient *client)
{
    /* Zero-copy requests never set unpin_key at construction */
    return priskvClusterRequestNewBase(node, NULL, 0, NULL, cb, cbarg, type, key, timeout, pin_key,
                                       pin_ttl_ms, unpin_key, alloc_length, 0 /*token*/, client);
}

static void priskvClusterRequestFree(priskvClusterRequest *req)
{
    if (!req) {
        return;
    }

    free(req->cluster_sgl);
    free(req->sgl);
    free(req);
}

priskvClusterStatus priskvClusterStatusFromPriskvStatus(priskv_status status)
{
    return (priskvClusterStatus)status;
}

static void priskvClusterRequestCallback(uint64_t request_id, priskv_status status, void *result)
{
    priskvClusterRequest *req = (priskvClusterRequest *)request_id;
    uint32_t valuelen = 0;

    if (status == PRISKV_STATUS_OK && result) {
        valuelen = *(uint32_t *)result;
    }

    if (req->client->metaUpdating == true && status != PRISKV_STATUS_OK) {
        list_add_tail(&retry_req_list, &req->entry);
        req->node->refCount -= 1;
        priskv_log_info("metaData is Updating, need retrying request %s, %d\n", req->key,
                        req->node->refCount);
        return;
    }

    req->cb(priskvClusterStatusFromPriskvStatus(status), valuelen, req->cbarg);
    req->node->refCount -= 1;
    priskvClusterRequestFree(req);
}

static void priskvClusterZeroCopyRequestCallback(uint64_t request_id, priskv_status status,
                                                 void *result)
{
    priskvClusterRequest *req = (priskvClusterRequest *)request_id;
    uint64_t addr = 0;
    uint32_t value_length = 0;
    uint64_t token = 0;

    if (status == PRISKV_STATUS_OK && result) {
        priskv_memory_region *region = (priskv_memory_region *)result;
        addr = region->addr;
        value_length = region->length;
        token = region->token;
        /* Optionally record token for debugging or future reuse */
        req->token = token;
    }

    priskv_log_debug("PriskvZeroCopyRequestCallback: callback request_id 0x%lx, status: [0x%x], "
                     "addr: 0x%lx, length %d\n",
                     request_id, status, addr, value_length);

    if (req->client->metaUpdating == true && status != PRISKV_STATUS_OK) {
        list_add_tail(&retry_req_list, &req->entry);
        req->node->refCount -= 1;
        priskv_log_info("metaData is Updating, need retrying request %s, %d\n", req->key,
                        req->node->refCount);
        return;
    }

    req->zero_copy_cb(priskvClusterStatusFromPriskvStatus(status), addr, value_length, token,
                      req->cbarg);
    req->node->refCount -= 1;
    priskvClusterRequestFree(req);
}

priskvClusterRequest *priskvClusterUpdateRequest(priskvClusterRequest *req)
{
    priskvClusterNode *node = priskvClusterGetNode(req->client, req->key);
    if (!node) {
        req->cb(PRISKV_CLUSTER_STATUS_NO_SUCH_KEY, 0, req->cbarg);
        return NULL;
    }

    req->node = node;
    for (uint16_t i = 0; i < req->nsgl; i++) {
        req->sgl[i].iova = req->cluster_sgl[i].iova;
        req->sgl[i].length = req->cluster_sgl[i].length;
        req->sgl[i].mem = priskvClusterGetMemory(req->cluster_sgl[i].mem, node->id);
    }

    return req;
}

priskvClusterRequest *priskvClusterGetRequest(priskvClusterClient *client, const char *key,
                                              priskvClusterSGL *sgl, uint16_t nsgl,
                                              priskvClusterCallback cb, void *cbarg, int timeout,
                                              RequestType type)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        cb(PRISKV_CLUSTER_STATUS_NO_SUCH_KEY, 0, cbarg);
        return NULL;
    }

    priskvClusterRequest *req =
        priskvClusterRequestNew(node, sgl, nsgl, cb, cbarg, type, key, timeout, client);
    return req;
}

priskvClusterRequest *priskvClusterGetZeroCopyRequest(priskvClusterClient *client, const char *key,
                                                      priskvClusterZeroCopyCallback cb, void *cbarg,
                                                      uint32_t alloc_length, uint64_t timeout,
                                                      bool pin_key, uint64_t pin_ttl_ms,
                                                      bool unpin_key, RequestType type)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        cb(PRISKV_CLUSTER_STATUS_NO_SUCH_KEY, 0, 0, 0, cbarg);
        return NULL;
    }

    /* Use explicit pin_ttl_ms for ACQUIRE/SEAL; ALLOC ignores it */
    priskvClusterRequest *req = priskvClusterZeroCopyRequestNew(
        node, cb, cbarg, type, key, alloc_length, timeout, pin_key, pin_ttl_ms, unpin_key, client);
    return req;
}

int priskvClusterSubmitRequest(priskvClusterRequest *req)
{
    if (req->client->metaUpdating == true) {
        list_add_tail(&retry_req_list, &req->entry);
    } else {
        req->node->refCount += 1;
        switch (req->type) {
        case GET:
            priskv_get_async(req->node->client, req->key, req->sgl, req->nsgl, (uint64_t)req,
                             priskvClusterRequestCallback);
            break;
        case SET:
            priskv_set_async(req->node->client, req->key, req->sgl, req->nsgl, req->timeout,
                             (uint64_t)req, priskvClusterRequestCallback);
            break;
        case TEST:
            priskv_test_async(req->node->client, req->key, (uint64_t)req,
                              priskvClusterRequestCallback);
            break;
        case DELETE:
            priskv_delete_async(req->node->client, req->key, (uint64_t)req,
                                priskvClusterRequestCallback);
            break;
        case ALLOC:
            priskv_alloc_async(req->node->client, req->key, req->alloc_length, req->timeout,
                               (uint64_t)req, priskvClusterZeroCopyRequestCallback);
            break;
        case SEAL:
            priskv_seal_async(req->node->client, &req->token, req->pin_key /* pin_on_seal */,
                              req->pin_ttl_ms /* pin_ttl_ms */, (uint64_t)req,
                              priskvClusterRequestCallback);
            break;
        case ACQUIRE:
            priskv_acquire_async(req->node->client, req->key, req->pin_ttl_ms,
                                 req->pin_key /* pin_on_acquire */, (uint64_t)req,
                                 priskvClusterZeroCopyRequestCallback);
            break;
        case RELEASE:
            priskv_release_async(req->node->client, &req->token,
                                 req->unpin_key /* unpin_on_release */, (uint64_t)req,
                                 priskvClusterRequestCallback);
            break;
        case DROP:
            priskv_drop_async(req->node->client, &req->token, (uint64_t)req,
                              priskvClusterRequestCallback);
            break;
        }
    }

    return 0;
}

/* 仅用于内部调用、只复用mem, 对比priskvClusterRegMemory */
void priskvClusterRegMemoryInternal(priskvClusterClient *client, uint64_t offset, size_t length,
                                    uint64_t iova, int fd)
{
    priskvClusterMemory *mem = client->memCtx->mem;

    mem->count = client->nodeCount;
    mem->mems = malloc(sizeof(priskv_memory *) * client->nodeCount);

    for (int i = 0; i < client->nodeCount; i++) {
        mem->mems[i] = priskv_reg_memory(client->nodes[i].client, offset, length, iova, fd);
    }
}

/* 仅用于内部调用、只复用mem, 对比priskvClusterDeregMemory */
void priskvClusterDeregMemoryInternal(priskvClusterClient *client)
{
    priskvClusterMemory *mem = client->memCtx->mem;
    for (int i = 0; i < mem->count; i++) {
        priskv_dereg_memory(mem->mems[i]);
    }
    free(mem->mems);
    mem->count = 0;
}

static void priskvClusterTimerHandler(int fd, void *opaque, uint32_t events)
{
    priskvClusterClient *client = opaque;

    uint64_t exp;
    read(client->metaTimerFd, &exp, sizeof(exp));

    priskvClusterMetaDataInfo *newMeta = priskvClusterMetaDataGetFromServer(client);
    if (!newMeta || newMeta->version <= client->metaVersion) {
        if (newMeta)
            priskvClusterMetaDataInfoFree(client, newMeta);
        return;
    }

    /* 执行元数据更新流程 */
    client->metaUpdating = true;
    priskv_log_debug("metaData update start\n");

    for (int i = 0; i < client->nodeCount; ++i) {
        if (client->nodes[i].refCount > 0) {
            priskv_log_info("Defer metadata update due to active references\n");
            priskvClusterMetaDataInfoFree(client, newMeta);
            return;
        }
    }

    priskvClusterMetaDataUnload(client);
    priskvClusterDeregMemoryInternal(client);

    if (priskvClusterMetaDataLoad(client, newMeta) != 0) {
        priskv_log_error("Failed to load new metadata\n");
    }

    priskvClusterMetaDataInfoFree(client, newMeta);
    priskvClusterRegMemoryInternal(client, client->memCtx->offset, client->memCtx->length,
                                   client->memCtx->iova, client->memCtx->fd);

    client->metaUpdating = false;
    priskv_log_debug("metaData update end\n");

    /* 处理重试队列 */
    priskvClusterRequest *req;
    int req_count = 0;
    list_for_each (&retry_req_list, req, entry) {
        req = priskvClusterUpdateRequest(req);
        if (req == NULL) {
            list_del(&req->entry);
            priskvClusterRequestFree(req);
            priskv_log_error("Failed to update request\n");
            continue;
        }

        priskvClusterSubmitRequest(req);

        list_del(&req->entry);
        req_count += 1;
    }
    priskv_log_debug("retry list count: %d\n", req_count);
}

static int createMetaTimer(priskvClusterClient *client)
{
    struct itimerspec its = {
        .it_interval = {.tv_sec = g_config.client.metadata_update_interval_sec, .tv_nsec = 0},
        .it_value = {.tv_sec = g_config.client.metadata_update_interval_sec, .tv_nsec = 0}};

    int tfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);
    if (tfd < 0) {
        priskv_log_error("Cannot create timer epoll!\n");
        return -1;
    }
    timerfd_settime(tfd, 0, &its, NULL);

    priskv_set_fd_handler(tfd, priskvClusterTimerHandler, NULL, client);
    priskv_add_event_fd(client->epollfd, tfd);

    return tfd;
}

/* TODO: support local addr */
priskvClusterClient *priskvClusterConnect(const char *raddr, int rport, const char *password)
{
    priskvClusterClient *client = calloc(sizeof(priskvClusterClient), 1);
    if (!client) {
        priskv_log_error("Failed to allocate memory for client\n");
        goto err;
    }

    client->epollfd = epoll_create1(0);
    if (client->epollfd < 0) {
        priskv_log_error("Failed to create epoll fd\n");
        goto err;
    }

    client->metaUpdating = false;
    client->metaVersion = 0;
    client->memCtx = malloc(sizeof(priskvClusterMemoryCtx));
    if (!g_config.client.direct_mode) {
        priskv_log_info("Using indirect mode\n");
        client->codec = priskv_codec_new();
        if (!client->codec) {
            priskv_log_error("Failed to create codec\n");
            goto err;
        }

        client->metaTimerFd = createMetaTimer(client);

        if (priskvClusterMetaServerConnect(&client->metaServer, raddr, rport, password)) {
            priskv_log_error("Failed to connect to meta server\n");
            goto err;
        }

        if (priskvClusterMetaDataUpdate(client)) {
            priskv_log_error("Failed to update meta data\n");
            goto err;
        }
    } else {
        // direct mode
        priskv_log_info("Using direct mode\n");
        client->nodes = realloc(client->nodes, sizeof(priskvClusterNode));
        client->nodeCount = 1;

        priskvClusterNode *node = &client->nodes[0];
        priskvClusterMetaDataNodeInfo nodeInfo = {
            .name = "PrisKV",
            .addr = raddr,
            .port = rport,
            .slotRangeCount = 0,
            .slotRanges = NULL,
        };

        if (priskvClusterMetaDataNodeLoad(client, node, &nodeInfo, 0)) {
            goto err;
        }
    }

    return client;

err:
    priskvClusterClose(client);
    return NULL;
}

void priskvClusterClose(priskvClusterClient *client)
{
    if (!client) {
        return;
    }

    if (!g_config.client.direct_mode) {
        assert(client->codec == NULL);
        priskvClusterMetaServerClose(&client->metaServer);
    }
    priskvClusterMetaDataUnload(client);

    if (client->epollfd > 0) {
        close(client->epollfd);
    }

    if (client->codec) {
        priskv_codec_destroy(client->codec);
    }

    if (client->memCtx) {
        free(client->memCtx);
    }

    free(client);
}

int priskvClusterClientGetFd(priskvClusterClient *client)
{
    return client->epollfd;
}

void priskvClusterClientProcess(priskvClusterClient *client, int timeout)
{
    const int maxevents = 256;
    struct epoll_event events[maxevents];
    int nr, i;

    nr = epoll_wait(client->epollfd, events, maxevents, timeout);
    if (nr < 0) {
        return;
    }

    for (i = 0; i < nr; i++) {
        struct epoll_event *event = &events[i];
        priskv_fd_handler_event(event);
    }
}

priskvClusterMemory *priskvClusterRegMemory(priskvClusterClient *client, uint64_t offset,
                                            size_t length, uint64_t iova, int fd)
{
    priskvClusterMemory *mem = malloc(sizeof(priskvClusterMemory));

    mem->count = client->nodeCount;
    mem->mems = malloc(sizeof(priskv_memory *) * client->nodeCount);

    for (int i = 0; i < client->nodeCount; i++) {
        mem->mems[i] = priskv_reg_memory(client->nodes[i].client, offset, length, iova, fd);
    }

    client->memCtx->mem = mem;
    client->memCtx->offset = offset;
    client->memCtx->length = length;
    client->memCtx->iova = iova;
    client->memCtx->fd = fd;

    return mem;
}

void priskvClusterDeregMemory(priskvClusterMemory *mem)
{
    for (int i = 0; i < mem->count; i++) {
        priskv_dereg_memory(mem->mems[i]);
    }
    free(mem->mems);
    free(mem);
}

int priskvClusterAsyncGet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                          uint16_t nsgl, priskvClusterCallback cb, void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetRequest(client, key, sgl, nsgl, cb, cbarg, 0, GET);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncSet(priskvClusterClient *client, const char *key, priskvClusterSGL *sgl,
                          uint16_t nsgl, uint64_t timeout, priskvClusterCallback cb, void *cbarg)
{
    priskvClusterRequest *req =
        priskvClusterGetRequest(client, key, sgl, nsgl, cb, cbarg, timeout, SET);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncTest(priskvClusterClient *client, const char *key, priskvClusterCallback cb,
                           void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetRequest(client, key, NULL, 0, cb, cbarg, 0, TEST);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncDelete(priskvClusterClient *client, const char *key, priskvClusterCallback cb,
                             void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetRequest(client, key, NULL, 0, cb, cbarg, 0, DELETE);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncAlloc(priskvClusterClient *client, const char *key, uint64_t alloc_length,
                            uint64_t timeout, priskvClusterZeroCopyCallback cb, void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetZeroCopyRequest(
        client, key, cb, cbarg, alloc_length, timeout, false /* pin_key */, 0 /* pin_ttl_ms */,
        false /* unpin_key */, ALLOC);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncSeal(priskvClusterClient *client, const char *key, const uint64_t *token,
                           bool pin_on_seal, uint64_t pin_ttl_ms, priskvClusterCallback cb,
                           void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetRequest(
        client, key, NULL, 0, cb, cbarg, pin_ttl_ms /* use timeout as pin_ttl_ms */, SEAL);
    if (req) {
        req->token = token ? *token : 0;
        req->pin_key = pin_on_seal;
    }
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncAcquire(priskvClusterClient *client, const char *key, bool pin_on_acquire,
                              uint64_t pin_ttl_ms, priskvClusterZeroCopyCallback cb, void *cbarg)
{
    priskvClusterRequest *req = priskvClusterGetZeroCopyRequest(
        client, key, cb, cbarg, 0 /* alloc_length */, 0 /* timeout */, pin_on_acquire /* pin_key */,
        pin_ttl_ms, false /* unpin_key */, ACQUIRE);
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncRelease(priskvClusterClient *client, const char *key, const uint64_t *token,
                              bool unpin_key, priskvClusterCallback cb, void *cbarg)
{
    priskvClusterRequest *req =
        priskvClusterGetRequest(client, key, NULL, 0, cb, cbarg, 0, RELEASE);
    if (req) {
        req->token = token ? *token : 0;
        req->unpin_key = unpin_key;
    }
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

int priskvClusterAsyncDrop(priskvClusterClient *client, const char *key, const uint64_t *token,
                           priskvClusterCallback cb, void *cbarg)
{
    priskvClusterRequest *req =
        priskvClusterGetRequest(client, key, NULL, 0, cb, cbarg, 0, DROP);
    if (req) {
        req->token = token ? *token : 0;
    }
    if (req == NULL)
        return -1;

    return priskvClusterSubmitRequest(req);
}

priskvClusterStatus priskvClusterGet(priskvClusterClient *client, const char *key,
                                     priskvClusterSGL *sgl, uint16_t nsgl, uint32_t *value_len)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskvClusterRequest *req =
        priskvClusterRequestNew(node, sgl, nsgl, NULL, NULL, GET, key, 0, client);

    priskv_status status = priskv_get(node->client, key, req->sgl, req->nsgl, value_len);

    priskvClusterRequestFree(req);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterSet(priskvClusterClient *client, const char *key,
                                     priskvClusterSGL *sgl, uint16_t nsgl, uint64_t timeout)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskvClusterRequest *req =
        priskvClusterRequestNew(node, sgl, nsgl, NULL, NULL, SET, key, timeout, client);

    priskv_status status = priskv_set(node->client, key, req->sgl, req->nsgl, timeout);

    priskvClusterRequestFree(req);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterTest(priskvClusterClient *client, const char *key,
                                      uint32_t *value_len)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskv_status status = priskv_test(node->client, key, value_len);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterDelete(priskvClusterClient *client, const char *key)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskv_status status = priskv_delete(node->client, key);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterAlloc(priskvClusterClient *client, const char *key,
                                       uint64_t alloc_length, uint64_t timeout, uint64_t *addr)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskv_memory_region region = {0};
    priskv_status status =
        priskv_alloc(node->client, key, (uint32_t)alloc_length, timeout, &region);
    if (status == PRISKV_STATUS_OK && addr) {
        *addr = region.addr;
    }

    return priskvClusterStatusFromPriskvStatus(status);
}

int priskvClusterAllocRegion(priskvClusterClient *client, const char *key, uint32_t alloc_length,
                             uint64_t timeout, priskv_memory_region *region)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    return priskv_alloc(node->client, key, alloc_length, timeout, region);
}

priskvClusterStatus priskvClusterSeal(priskvClusterClient *client, const char *key,
                                      const uint64_t *token, bool pin_on_seal, uint64_t pin_ttl_ms)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }
    priskv_status status = priskv_seal(node->client, token, pin_on_seal, pin_ttl_ms);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterAcquire(priskvClusterClient *client, const char *key,
                                         uint64_t timeout, bool pin_on_acquire, uint64_t pin_ttl_ms,
                                         uint64_t *addr, uint32_t *valuelen)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    priskv_memory_region region = {0};
    priskv_status status = priskv_acquire(node->client, key, pin_on_acquire, pin_ttl_ms, &region);
    if (status == PRISKV_STATUS_OK) {
        if (addr) {
            *addr = region.addr;
        }
        if (valuelen) {
            *valuelen = region.length;
        }
    }

    return priskvClusterStatusFromPriskvStatus(status);
}

int priskvClusterAcquireRegion(priskvClusterClient *client, const char *key, uint64_t timeout,
                               bool pin_on_acquire, uint64_t pin_ttl_ms, 
                               priskv_memory_region *region)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }

    /* timeout is kept for API compatibility; pin_ttl_ms controls PIN TTL (0 uses server default) */
    return priskv_acquire(node->client, key, pin_on_acquire, pin_ttl_ms, region);
}

priskvClusterStatus priskvClusterRelease(priskvClusterClient *client, const char *key,
                                         const uint64_t *token, bool unpin_on_release)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }
    priskv_status status = priskv_release(node->client, token, unpin_on_release);

    return priskvClusterStatusFromPriskvStatus(status);
}

priskvClusterStatus priskvClusterDrop(priskvClusterClient *client, const char *key,
                                      const uint64_t *token)
{
    priskvClusterNode *node = priskvClusterGetNode(client, key);
    if (!node) {
        return PRISKV_CLUSTER_STATUS_NO_SUCH_KEY;
    }
    priskv_status status = priskv_drop(node->client, token);

    return priskvClusterStatusFromPriskvStatus(status);
}

static void priskvAppendKeyset(priskv_keyset *dst, priskv_keyset *src)
{
    if (!src) {
        return;
    }

    dst->keys = realloc(dst->keys, (dst->nkey + src->nkey) * sizeof(priskv_key));
    for (int i = 0; i < src->nkey; i++) {
        size_t keylen = strlen(src->keys[i].key);
        dst->keys[dst->nkey + i].key = (char *)malloc(keylen + 1);
        memcpy(dst->keys[dst->nkey + i].key, src->keys[i].key, keylen);
        dst->keys[dst->nkey + i].key[keylen] = '\0';
        dst->keys[dst->nkey + i].valuelen = src->keys[i].valuelen;
    }

    dst->nkey += src->nkey;
    priskv_keyset_free(src);
}

priskvClusterStatus priskvClusterKeys(priskvClusterClient *client, const char *regex,
                                      priskv_keyset **keyset)
{
    priskvClusterNode *node;
    priskv_keyset *node_keyset, *all_keyset = NULL;
    priskv_status status = PRISKV_STATUS_OK;
    for (int i = 0; i < client->nodeCount; i++) {
        node = &client->nodes[i];
        if (node == NULL) {
            continue;
        }

        status = priskv_keys(node->client, regex, &node_keyset);
        if (status != PRISKV_STATUS_OK) {
            continue;
        }

        if (!all_keyset) {
            all_keyset = node_keyset;
        } else {
            priskvAppendKeyset(all_keyset, node_keyset);
        }
    }

    *keyset = all_keyset;
    return priskvClusterStatusFromPriskvStatus(status);
}
