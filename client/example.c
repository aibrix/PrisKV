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

#include <stdio.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <sys/epoll.h>
#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <string.h>

#include "priskv.h"

static const char *raddr = "192.168.122.100";
static int rport = 18512;
static priskv_client *client;
static priskv_memory *dev_sendmem, *dev_recvmem;
static void *dev_sendbuf, *dev_recvbuf;
static priskv_memory *host_sendmem, *host_recvmem;
static void *host_sendbuf, *host_recvbuf;
static int connfd;
static int epollfd;
static const char key[] = "my_key";
static const char value[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
static uint32_t value_size = sizeof(value);

static int gdr_is_support(void)
{
    CUdevice currentDev;
    int cudaDev;
    static int support_gdr = -1;
    cudaError_t err;

    if (support_gdr >= 0) {
        return support_gdr;
    }

    err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        support_gdr = 0;
        return support_gdr;
    }

    err = cuDeviceGet(&currentDev, cudaDev);
    if (err != cudaSuccess) {
        support_gdr = 0;
        return support_gdr;
    }

    err = cuDeviceGetAttribute(&support_gdr, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_SUPPORTED,
                               currentDev);
    if (err != cudaSuccess) {
        support_gdr = 0;
        return support_gdr;
    }

    printf("Cuda(%d) support GDR: %s\n", (int)currentDev, support_gdr ? "yes" : "no");

    return support_gdr;
}

static int buffer_init()
{
    assert(gdr_is_support());

    if (cudaMalloc(&dev_sendbuf, value_size) != cudaSuccess) {
        printf("Cannot allocate send buffer!\n");
        return -1;
    }

    if (cudaMalloc(&dev_recvbuf, value_size) != cudaSuccess) {
        printf("Cannot allocate recv buffer!\n");
        return -1;
    }

    cudaMemset(dev_sendbuf, 0, value_size);
    cudaMemset(dev_recvbuf, 0, value_size);
    cudaMemcpy(dev_sendbuf, value, value_size, cudaMemcpyDefault);

    host_sendbuf = malloc(value_size);
    host_recvbuf = malloc(value_size);
    memset(host_sendbuf, 0, value_size);
    memset(host_recvbuf, 0, value_size);
    memcpy(host_sendbuf, value, value_size);

    return 0;
}

static void buffer_deinit()
{
    cudaFree(dev_sendbuf);
    cudaFree(dev_recvbuf);

    free(host_sendbuf);
    free(host_recvbuf);
}

static int priskv_init()
{
    client = priskv_connect(raddr, rport, NULL, 0, 0);
    if (!client) {
        printf("Cannot connect to priskv server!\n");
        return -1;
    }

    printf("Connected to priskv server!\n");

    dev_sendmem =
        priskv_reg_memory(client, (uint64_t)dev_sendbuf, value_size, (uint64_t)dev_sendbuf, -1);
    if (!dev_sendmem) {
        printf("Cannot register device send buffer!\n");
        return -1;
    }

    dev_recvmem =
        priskv_reg_memory(client, (uint64_t)dev_recvbuf, value_size, (uint64_t)dev_recvbuf, -1);
    if (!dev_recvmem) {
        printf("Cannot register device recv buffer!\n");
        return -1;
    }

    host_sendmem =
        priskv_reg_memory(client, (uint64_t)host_sendbuf, value_size, (uint64_t)host_sendbuf, -1);
    if (!host_sendmem) {
        printf("Cannot register host send buffer!\n");
        return -1;
    }

    host_recvmem =
        priskv_reg_memory(client, (uint64_t)host_recvbuf, value_size, (uint64_t)host_recvbuf, -1);
    if (!host_recvmem) {
        printf("Cannot register host recv buffer!\n");
        return -1;
    }

    connfd = priskv_get_fd(client);
    if (connfd < 0) {
        printf("Cannot get fd from priskv connection!\n");
        return -1;
    }

    return 0;
}

static void priskv_deinit()
{
    priskv_dereg_memory(dev_sendmem);
    priskv_dereg_memory(dev_recvmem);

    priskv_dereg_memory(host_sendmem);
    priskv_dereg_memory(host_recvmem);
}

static int epoll_init()
{
    struct epoll_event event = {0};

    epollfd = epoll_create1(0);
    if (epollfd < 0) {
        printf("Cannot create epoll!\n");
        return -1;
    }

    event.events = EPOLLIN | EPOLLET;
    event.data.fd = connfd;
    if (epoll_ctl(epollfd, EPOLL_CTL_ADD, connfd, &event) < 0) {
        printf("Cannot add connfd to epoll!\n");
        return -1;
    }

    return 0;
}

static void epoll_deinit()
{
    close(epollfd);
}

static void poller_wait(int timeout)
{
    struct epoll_event events[1];
    int nevents;

    nevents = epoll_wait(epollfd, events, 1, timeout);
    if (!nevents) {
        return;
    }

    if (nevents < 0) {
        assert(errno == EINTR);
        return;
    }

    priskv_process(client, events[0].events);
}

static void priskv_req_cb(uint64_t request_id, priskv_status status, void *result)
{
    int *done = (int *)request_id;

    if (status != PRISKV_STATUS_OK) {
        printf("priskv response: status[%d]\n", status);
        *done = -1;
    } else {
        *done = 1;
    }
}

static int wait_for_done(int *done, const char *op)
{
    while (!(*done)) {
        poller_wait(1000);
    }
    if ((*done) < 0) {
        printf("priskv %s failed!\n", op);
        return -1;
    }

    return 0;
}

#define REPORT(_job, _op, _key, _value)                                                            \
    printf("(%s) [%s]: OK!\n\tkey: %s\n\tvalue: %s\n", _job, _op, _key, _value);

static int priskv_async_test()
{
    priskv_sgl sgl;
    int done = 0;

    /* Data flow: GPU Memory -> PrisKV database */
    sgl.iova = (uint64_t)dev_sendbuf;
    sgl.length = value_size;
    sgl.mem = dev_sendmem;
    priskv_set_async(client, key, &sgl, 1, PRISKV_KEY_MAX_TIMEOUT, (uint64_t)&done, priskv_req_cb);
    assert(wait_for_done(&done, "SET") == 0);
    REPORT("async", "SET", key, value);

    /* Data flow: PrisKV database -> CPU Memory */
    done = 0;
    sgl.iova = (uint64_t)host_recvbuf;
    sgl.length = value_size;
    sgl.mem = host_recvmem;
    priskv_get_async(client, key, &sgl, 1, (uint64_t)&done, priskv_req_cb);
    assert(wait_for_done(&done, "GET") == 0);
    REPORT("async", "GET", key, (char *)host_recvbuf);

    /* Check data consistency */
    assert(memcmp(host_recvbuf, value, value_size) == 0);

    done = 0;
    priskv_delete_async(client, key, (uint64_t)&done, priskv_req_cb);
    assert(wait_for_done(&done, "DELETE") == 0);
    REPORT("async", "DELETE", key, value);

    return 0;
}

static int priskv_sync_test()
{
    priskv_sgl sgl;
    uint32_t valuelen = 0;

    /* Data flow: GPU Memory -> PrisKV database */
    sgl.iova = (uint64_t)dev_sendbuf;
    sgl.length = value_size;
    sgl.mem = dev_sendmem;
    assert(priskv_set(client, key, &sgl, 1, PRISKV_KEY_MAX_TIMEOUT) == PRISKV_STATUS_OK);
    REPORT("sync", "SET", key, value);

    /* Data flow: PrisKV database -> CPU Memory */
    sgl.iova = (uint64_t)host_recvbuf;
    sgl.length = value_size;
    sgl.mem = host_recvmem;
    assert(priskv_get(client, key, &sgl, 1, &valuelen) == PRISKV_STATUS_OK);
    assert(valuelen == value_size);
    REPORT("sync", "GET", key, (char *)host_recvbuf);

    /* Check data consistency */
    assert(memcmp(host_recvbuf, value, value_size) == 0);

    assert(priskv_delete(client, key) == PRISKV_STATUS_OK);
    REPORT("sync", "DELETE", key, value);

    return 0;
}

int main(int argc, char *argv[])
{
    if (buffer_init()) {
        return -1;
    }

    if (priskv_init()) {
        return -1;
    }

    if (epoll_init()) {
        return -1;
    }

    if (priskv_async_test()) {
        return -1;
    }

    if (priskv_sync_test()) {
        return -1;
    }

    epoll_deinit();
    priskv_deinit();
    buffer_deinit();

    return 0;
}
