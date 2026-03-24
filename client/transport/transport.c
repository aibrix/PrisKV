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

#include "transport.h"

#include <sys/types.h>
#include <netdb.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <netdb.h>
#include <sys/epoll.h>
#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/time.h>
#include <sys/stat.h>
#include <endian.h>

#include "priskv-config.h"
#include "priskv-cuda.h"
#include "priskv-event.h"
#include "priskv-log.h"

priskv_transport_driver *g_client_driver = NULL;

extern priskv_transport_driver priskv_transport_driver_ucx;
extern priskv_transport_driver priskv_transport_driver_rdma;

static int priskv_build_check(void)
{
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_OK != (int)PRISKV_RESP_STATUS_OK);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_INVALID_COMMAND !=
                        (int)PRISKV_RESP_STATUS_INVALID_COMMAND);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_KEY_EMPTY != (int)PRISKV_RESP_STATUS_KEY_EMPTY);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_KEY_TOO_BIG != (int)PRISKV_RESP_STATUS_KEY_TOO_BIG);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_VALUE_EMPTY != (int)PRISKV_RESP_STATUS_VALUE_EMPTY);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_VALUE_TOO_BIG != (int)PRISKV_RESP_STATUS_VALUE_TOO_BIG);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_NO_SUCH_COMMAND !=
                        (int)PRISKV_RESP_STATUS_NO_SUCH_COMMAND);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_NO_SUCH_KEY != (int)PRISKV_RESP_STATUS_NO_SUCH_KEY);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_INVALID_SGL != (int)PRISKV_RESP_STATUS_INVALID_SGL);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_INVALID_REGEX != (int)PRISKV_RESP_STATUS_INVALID_REGEX);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_KEY_UPDATING != (int)PRISKV_RESP_STATUS_KEY_UPDATING);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_CONNECT_ERROR != (int)PRISKV_RESP_STATUS_CONNECT_ERROR);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_SERVER_ERROR != (int)PRISKV_RESP_STATUS_SERVER_ERROR);
    PRISKV_BUILD_BUG_ON((int)PRISKV_STATUS_NO_MEM != (int)PRISKV_RESP_STATUS_NO_MEM);
    return 0;
}

static void __attribute__((constructor)) priskv_client_transport_init(void)
{
    assert(!priskv_build_check());
    priskv_config_init();

    priskv_transport_backend backend = g_config.transport;

    priskv_transport_driver *driver = NULL;
    switch (backend) {
    case PRISKV_TRANSPORT_BACKEND_UCX:
        driver = &priskv_transport_driver_ucx;
        priskv_log_notice("Using UCX transport backend\n");
        break;
    case PRISKV_TRANSPORT_BACKEND_RDMA:
        driver = &priskv_transport_driver_rdma;
        priskv_log_notice("Using RDMA transport backend\n");
        break;
    default:
        priskv_log_error("Unknown transport backend: %d\n", backend);
        break;
    }

    if (driver && driver->init) {
        if (driver->init() != 0) {
            priskv_log_error("Failed to initialize transport driver: %s\n", driver->name);
            driver = NULL;
        }
    }

    if (driver) {
        g_client_driver = driver;
    }
}

/*
 * TODO: Implement Shared Memory Read-Only protection via Dual Mapping.
 *
 * Current status: In SHM mode, the client can physically write to memory acquired via
 * priskv_acquire. Next stage: The client library should map the SHM file twice:
 * 1) Read-Only mapping (PROT_READ) for 'acquire' operations.
 * 2) Read-Write mapping (PROT_READ | PROT_WRITE) for 'alloc' operations.
 * This enforces RO semantics, causing unauthorized writes to 'acquire' regions to fault.
 */
static int priskv_transport_mmap(void **addr, uint64_t *size, int *fd, uint32_t shm_pid, int shm_fd)
{
    char proc_path[256];
    snprintf(proc_path, sizeof(proc_path), "/proc/%d/fd/%d", shm_pid, shm_fd);

    struct stat statbuf;
    int ret = stat(proc_path, &statbuf);
    if (ret) {
        priskv_log_error("Transport: failed to stat file %s\n", proc_path);
        goto err;
    }

    *fd = open(proc_path, O_RDWR);
    if (*fd == -1) {
        priskv_log_error("Transport: failed to open file %s\n", proc_path);
        goto err;
    }

    *size = statbuf.st_size;
    *addr = mmap(NULL, *size, PROT_READ | PROT_WRITE, MAP_SHARED, *fd, 0);
    if (*addr == MAP_FAILED) {
        priskv_log_error("Transport: failed to mmap shm buffer %m\n");
        goto err;
    }

    if (madvise(*addr, *size, MADV_DONTDUMP)) {
        priskv_log_warn("Transport: failed to set dont dump, skip %m\n");
    }
    return 0;

err:
    if (*addr != MAP_FAILED) {
        munmap(*addr, *size);
        *addr = NULL;
    }
    if (*fd > 0) {
        close(*fd);
        *fd = -1;
    }
    *size = 0;
    return -1;
}

void priskv_keyset_free(priskv_keyset *keyset)
{
    if (!keyset) {
        return;
    }

    for (uint32_t i = 0; i < keyset->nkey; i++) {
        free(keyset->keys[i].key);
    }
    free(keyset->keys);
    free(keyset);
}

priskv_client *priskv_connect(const char *raddr, int rport, const char *laddr, int lport,
                              int nqueue)
{
    priskv_client *client = NULL;

    if (lport && nqueue > 1) {
        priskv_log_error("Transport: unable to bind local port when queues > 1\n");
        return NULL;
    }

    client = calloc(sizeof(priskv_client), 1);
    if (!client) {
        priskv_log_error("Transport: failed to allocate memory for client\n");
        return NULL;
    }

    client->shm_addr = NULL;
    client->epollfd = epoll_create1(0);
    if (client->epollfd < 0) {
        priskv_log_error("Transport: failed to create epoll fd\n");
        goto err;
    }
    priskv_set_nonblock(client->epollfd);

    if (nqueue > 0) {
        client->ops = g_client_driver->get_mq_ops();
    } else {
        client->ops = g_client_driver->get_sq_ops();
    }

    if (client->ops->init(client, raddr, rport, laddr, lport, nqueue)) {
        priskv_log_error("Transport: failed to initialize client\n");
        goto err;
    }

    priskv_log_debug("Transport: server mem.use_shm %d\n", g_config.mem.use_shm);
    // If use shm, mmap shm buffer into memory space
    if (g_config.mem.use_shm) {
        if (client->conns[0]->shm_pid < 0) {
            priskv_log_error("Transport: server shm_pid is not valid\n");
            goto err;
        }

        if (client->conns[0]->shm_fd < 0) {
            priskv_log_error("Transport: server shm_fd is not valid\n");
            goto err;
        }

        if (priskv_transport_mmap(&client->shm_addr, &client->shm_len, &client->shm_fd,
                                  client->conns[0]->shm_pid, client->conns[0]->shm_fd)) {
            goto err;
        }

        if (g_config.mem.use_cuda) {
            if (priskv_cuda_host_register(client->shm_addr, client->shm_len)) {
                goto err;
            }
        }
    }

    return client;
err:
    if (client->shm_addr) {
        munmap(client->shm_addr, client->shm_len);
        close(client->shm_fd);
        client->shm_addr = NULL;
    }

    client->ops->deinit(client);
    close(client->epollfd);
    free(client);

    return NULL;
}

void priskv_close(priskv_client *client)
{

    client->ops->deinit(client);
    close(client->epollfd);
    client->epollfd = -1;
    if (client->shm_addr) {
        if (g_config.mem.use_cuda) {
            priskv_cuda_host_unregister(client->shm_addr);
        }
        munmap(client->shm_addr, client->shm_len);
        close(client->shm_fd);
        client->shm_addr = NULL;
    }
    free(client);
}

int priskv_get_fd(priskv_client *client)
{
    return client->epollfd;
}

int priskv_process(priskv_client *client, uint32_t event)
{
    if (client->epollfd < 0) {
        return -1;
    }

    priskv_events_process(client->epollfd, -1);

    return 0;
}

priskv_memory *priskv_reg_memory(priskv_client *client, uint64_t offset, size_t length,
                                 uint64_t iova, int fd)
{
    return client->ops->reg_memory(client, offset, length, iova, fd);
}

void priskv_dereg_memory(priskv_memory *mem)
{
    priskv_client *client = mem->client;

    client->ops->dereg_memory(mem);
}

static inline priskv_transport_conn *priskv_select_conn(priskv_client *client)
{
    return client->ops->select_conn(client);
}

static void priskv_send_command(priskv_client *client, uint64_t request_id, const char *key,
                                uint32_t alloc_length, priskv_sgl *sgl, uint16_t nsgl,
                                uint64_t key_expiry_timeout, uint64_t pin_ttl_ms,
                                priskv_req_command cmd, uint32_t req_flags, priskv_generic_cb cb)
{
    priskv_transport_conn *conn = priskv_select_conn(client);
    priskv_connect_param *param = &conn->param;
    priskv_transport_req *req;
    uint16_t keylen = 0;
    const char *key_ptr = key;
    uint64_t tok_be_buf = 0;

    /* For SEAL/RELEASE/DROP, reuse the key field to carry a binary token with fixed length 8.
     * Server parses in network byte order (be64toh); client must send in big-endian. */
    if (cmd == PRISKV_COMMAND_SEAL || cmd == PRISKV_COMMAND_RELEASE || cmd == PRISKV_COMMAND_DROP) {
        keylen = sizeof(uint64_t);
        if (key) {
            uint64_t tok = *(const uint64_t *)key;
            tok_be_buf = htobe64(tok);
            key_ptr = (const char *)&tok_be_buf;
        }
    } else {
        keylen = strlen(key);
        key_ptr = key;
    }

    assert(cmd < PRISKV_COMMAND_MAX);
    if (!key || !keylen) {
        cb(request_id, PRISKV_STATUS_KEY_EMPTY, NULL);
    }

    if (keylen > param->max_key_length) {
        cb(request_id, PRISKV_STATUS_KEY_TOO_BIG, NULL);
    }

    if (nsgl > param->max_sgl) {
        priskv_log_error("Transport: nsgl %d > max_sgl %d\n", nsgl, param->max_sgl);
        cb(request_id, PRISKV_STATUS_INVALID_SGL, NULL);
    }

    req = client->ops->new_req(client, conn, request_id, key_ptr, keylen, alloc_length, sgl, nsgl,
                               key_expiry_timeout, cmd, cb);
    if (!req) {
        cb(request_id, PRISKV_STATUS_NO_MEM, NULL);
        return;
    }
    req->req_flags = req_flags;
    req->pin_ttl_ms = pin_ttl_ms;
    client->ops->submit_req(req);
}

int priskv_get_async(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                     uint64_t request_id, priskv_generic_cb cb)
{
    if (!sgl || !nsgl) {
        cb(request_id, PRISKV_STATUS_VALUE_EMPTY, 0);
        return 0;
    }

    priskv_send_command(client, request_id, key, 0 /* alloc_length */, sgl, nsgl,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_GET, 0, cb);
    return 0;
}

int priskv_set_async(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                     uint64_t timeout, uint64_t request_id, priskv_generic_cb cb)
{
    if (!sgl || !nsgl) {
        cb(request_id, PRISKV_STATUS_VALUE_EMPTY, 0);
        return 0;
    }

    priskv_send_command(client, request_id, key, 0 /* alloc_length */, sgl, nsgl,
                        timeout /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_SET, 0,
                        cb);
    return 0;
}

int priskv_test_async(priskv_client *client, const char *key, uint64_t request_id,
                      priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, key, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_TEST, 0, cb);
    return 0;
}

int priskv_delete_async(priskv_client *client, const char *key, uint64_t request_id,
                        priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, key, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_DELETE, 0,
                        cb);
    return 0;
}

int priskv_expire_async(priskv_client *client, const char *key, uint64_t timeout,
                        uint64_t request_id, priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, key, 0 /* alloc_length */, NULL, 0,
                        timeout /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_EXPIRE,
                        0, cb);
    return 0;
}

int priskv_keys_async(priskv_client *client, const char *regex, uint64_t request_id,
                      priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, regex, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_KEYS, 0, cb);
    return 0;
}

int priskv_nrkeys_async(priskv_client *client, const char *regex, uint64_t request_id,
                        priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, regex, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_NRKEYS, 0,
                        cb);
    return 0;
}

int priskv_flush_async(priskv_client *client, const char *regex, uint64_t request_id,
                       priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, regex, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_FLUSH, 0,
                        cb);
    return 0;
}

int priskv_alloc_async(priskv_client *client, const char *key, uint32_t alloc_length,
                       uint64_t timeout, uint64_t request_id, priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, key, alloc_length, NULL, 0,
                        timeout /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_ALLOC,
                        0, cb);
    return 0;
}

int priskv_seal_async(priskv_client *client, const uint64_t *token, bool pin_on_seal,
                      uint64_t pin_ttl_ms, uint64_t request_id, priskv_generic_cb cb)
{
    uint32_t flags = pin_on_seal ? PRISKV_REQ_FLAG_PIN_ON_SEAL : 0;
    priskv_send_command(client, request_id, (const char *)token, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, pin_ttl_ms /* pin_ttl_ms */,
                        PRISKV_COMMAND_SEAL, flags, cb);
    return 0;
}

int priskv_acquire_async(priskv_client *client, const char *key, bool pin_on_acquire,
                         uint64_t pin_ttl_ms, uint64_t request_id, priskv_generic_cb cb)
{
    uint32_t flags = pin_on_acquire ? PRISKV_REQ_FLAG_PIN_ON_ACQUIRE : 0;
    priskv_send_command(client, request_id, key, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, pin_ttl_ms /* pin_ttl_ms */,
                        PRISKV_COMMAND_ACQUIRE, flags, cb);
    return 0;
}

int priskv_release_async(priskv_client *client, const uint64_t *token, bool unpin_on_release,
                         uint64_t request_id, priskv_generic_cb cb)
{
    uint32_t flags = unpin_on_release ? PRISKV_REQ_FLAG_UNPIN_ON_RELEASE : 0;
    priskv_send_command(client, request_id, (const char *)token, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_RELEASE,
                        flags, cb);
    return 0;
}

int priskv_drop_async(priskv_client *client, const uint64_t *token, uint64_t request_id,
                      priskv_generic_cb cb)
{
    priskv_send_command(client, request_id, (const char *)token, 0 /* alloc_length */, NULL, 0,
                        0 /* key_expiry_timeout */, 0 /* pin_ttl_ms */, PRISKV_COMMAND_DROP, 0, cb);
    return 0;
}

uint64_t priskv_capacity(priskv_client *client)
{
    return client->conns[0]->capacity;
}

void priskv_transport_conn_process(int fd, void *opaque, uint32_t ev)
{
    priskv_transport_conn *conn = opaque;

    assert(conn->epollfd == fd);

    priskv_log_debug("Transport: process event %d\n", ev);
    priskv_events_process(conn->epollfd, -1);
}
