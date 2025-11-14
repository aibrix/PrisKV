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

#ifndef __PRISKV_H__
#define __PRISKV_H__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdint.h>

typedef struct priskv_client priskv_client;
typedef struct priskv_memory priskv_memory;

/* Get client context. Return NULL on failure
 * @raddr: remote address
 * @rport: remote port
 * @laddr: local address. Use system default routing address on NULL @laddr
 * @lport: local port. Ignore port on NULL @laddr
 * @nqueue: the number of worker threads
 */
priskv_client *priskv_connect(const char *raddr, int rport, const char *laddr, int lport, int nqueue);

/* Close a client context created by @priskv_connect */
void priskv_close(priskv_client *client);

/* Get memory handler
 * @offset: uint64_t type on valid fd(>= 0); or void * type on invalid fd.
 */
priskv_memory *priskv_reg_memory(priskv_client *client, uint64_t offset, size_t length, uint64_t iova,
                             int fd);

/* Put a memory handler
 * @mem: created by @priskv_reg_memory
 */
void priskv_dereg_memory(priskv_memory *mem);

/* Get event fd. Ex wait POLLIN event by syscall poll(), or EPOLLIN by syscall epoll_wait */
int priskv_get_fd(priskv_client *client);

/* Process the incoming event of @priskv_get_fd.
 * Return 0 on success. -ENOTCONN on disconnected.
 */
int priskv_process(priskv_client *client, uint32_t event);

/* PrisKV Scatter Gather List (SGL) is a data structure in memory address space used to describe a
 * data buffer. The memory address is either CPU memory address or GPU memory address (please
 * see GPU Direct RDMA (GDR) for more informations). */
typedef struct priskv_sgl {
    uint64_t iova;
    uint32_t length;
    priskv_memory *mem; /* memory handler created by @priskv_reg_memory */
} priskv_sgl;

typedef enum priskv_status {
    PRISKV_STATUS_OK = 0x00,

    /* unrecognized command, typically protocol error */
    PRISKV_STATUS_INVALID_COMMAND = 0x100,

    /* zero length key in use? Note that '\0' is allowed */
    PRISKV_STATUS_KEY_EMPTY,

    /* does the length of key exceed @max_key_length? */
    PRISKV_STATUS_KEY_TOO_BIG,

    /* zero length value in use? Note that '\0' is allowed */
    PRISKV_STATUS_VALUE_EMPTY,

    /* the length of @priskv_sgl is smaller than the length of value */
    PRISKV_STATUS_VALUE_TOO_BIG,

    /* no such command */
    PRISKV_STATUS_NO_SUCH_COMMAND,

    /* no such key */
    PRISKV_STATUS_NO_SUCH_KEY,

    /* invalid SGL. the number of SGL within a command must not exceed @max_sgl */
    PRISKV_STATUS_INVALID_SGL,

    /* invalid regex */
    PRISKV_STATUS_INVALID_REGEX,

    /* key is updating */
    PRISKV_STATUS_KEY_UPDATING,

    /* connect to server side failed */
    PRISKV_STATUS_CONNECT_ERROR,

    /* generic server side failure */
    PRISKV_STATUS_SERVER_ERROR,

    /* no enough memory reported by server side */
    PRISKV_STATUS_NO_MEM = 0x200,

    /* RDMA disconnected from the server side */
    PRISKV_STATUS_DISCONNECTED = 0xF00,

    /* local RDMA error occurs */
    PRISKV_STATUS_RDMA_ERROR,

    /* does inflight requests exceed @max_inflight_command? */
    PRISKV_STATUS_BUSY,

    /* unexpected protocol error */
    PRISKV_STATUS_PROTOCOL_ERROR,
} priskv_status;

static inline const char *priskv_status_str(priskv_status status)
{
    switch (status) {
    case PRISKV_STATUS_OK:
        return "OK";

    case PRISKV_STATUS_INVALID_COMMAND:
        return "Invalid command";

    case PRISKV_STATUS_KEY_EMPTY:
        return "Empty key";

    case PRISKV_STATUS_KEY_TOO_BIG:
        return "Key too big";

    case PRISKV_STATUS_VALUE_EMPTY:
        return "Empty Value";

    case PRISKV_STATUS_VALUE_TOO_BIG:
        return "Value too big";

    case PRISKV_STATUS_NO_SUCH_COMMAND:
        return "No such command";

    case PRISKV_STATUS_NO_SUCH_KEY:
        return "No such key";

    case PRISKV_STATUS_INVALID_SGL:
        return "Invalid SGL";

    case PRISKV_STATUS_INVALID_REGEX:
        return "Invalid regex";

    case PRISKV_STATUS_KEY_UPDATING:
        return "Key is updating";

    case PRISKV_STATUS_CONNECT_ERROR:
        return "Connect error";

    case PRISKV_STATUS_SERVER_ERROR:
        return "Server internal error";

    case PRISKV_STATUS_NO_MEM:
        return "No memory";

    case PRISKV_STATUS_DISCONNECTED:
        return "Disconnected";

    case PRISKV_STATUS_RDMA_ERROR:
        return "RDMA error";

    case PRISKV_STATUS_BUSY:
        return "Busy";

    case PRISKV_STATUS_PROTOCOL_ERROR:
        return "Protocol error";
    }

    return "Unknown";
}

/* async APIs */

/* generic callback function for async KV APIs.
 * @opaque: return different content according to the interface
 */
typedef void (*priskv_generic_cb)(uint64_t request_id, priskv_status status, void *result);

/* General rules:
 * @client: client context of priskv service which is created by @priskv_connect
 * @key: string format. Note that it must be NULL terminated
 * @sgl: array of @priskv_sgl
 * @nsgl: the elements of @sgl
 * @timeout: timeout for key (in milliseconds). PRISKV_KEY_MAX_TIMEOUT means no timeout
 * @request_id: identifier of request
 * @cb: callback function of @priskv_generic_cb
 */

/* Get value of a key, store the value into addresses described by @sgl * @nsgl.
 * The total length of @sgl * @nsgl is 'expected-len', and the real value length is 'value-len':
 *   1> expected-len >= value-len: the server side tries to store value into @sgl * @nsgl. Once any
 *                                 error occurs, the memory of @sgl * @nsgl is corrupted.
 *   2> expected-len < value-len: the server side would not try to store value, response
 *                                PRISKV_STATUS_VALUE_TOO_BIG as soon as possible.
 */
int priskv_get_async(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                   uint64_t request_id, priskv_generic_cb cb);

/* Set value of a key */
int priskv_set_async(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
                   uint64_t timeout, uint64_t request_id, priskv_generic_cb cb);

/* Test a key-value exist or not */
int priskv_test_async(priskv_client *client, const char *key, uint64_t request_id, priskv_generic_cb cb);

/* Delete a key-value exist or not */
int priskv_delete_async(priskv_client *client, const char *key, uint64_t request_id,
                      priskv_generic_cb cb);

/* Set expire time of a key */
int priskv_expire_async(priskv_client *client, const char *key, uint64_t timeout, uint64_t request_id,
                      priskv_generic_cb cb);

/* Get the number of keys which match the @regex */
int priskv_nrkeys_async(priskv_client *client, const char *regex, uint64_t request_id,
                      priskv_generic_cb cb);

/* Flush the keys which match the @regex */
int priskv_flush_async(priskv_client *client, const char *regex, uint64_t request_id,
                     priskv_generic_cb cb);

/* for *KEYS* command */
typedef struct priskv_key {
    char *key;
    uint32_t valuelen;
} priskv_key;

typedef struct priskv_keyset {
    uint32_t nkey;
    priskv_key *keys;
} priskv_keyset;

/* free keyset returned by @priskv_keys */
void priskv_keyset_free(priskv_keyset *keyset);

/* Get the keys which match the @regex and return the result in priskv_generic_cb */
int priskv_keys_async(priskv_client *client, const char *regex, uint64_t request_id,
                    priskv_generic_cb cb);

/* sync APIs */
int priskv_get(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl,
             uint32_t *valuelen);

int priskv_set(priskv_client *client, const char *key, priskv_sgl *sgl, uint16_t nsgl, uint64_t timeout);

int priskv_test(priskv_client *client, const char *key, uint32_t *valuelen);

int priskv_delete(priskv_client *client, const char *key);

int priskv_expire(priskv_client *client, const char *key, uint64_t timeout);

int priskv_keys(priskv_client *client, const char *regex, priskv_keyset **keyset);

int priskv_nrkeys(priskv_client *client, const char *regex, uint32_t *nkey);

int priskv_flush(priskv_client *client, const char *regex, uint32_t *nkey);

uint64_t priskv_capacity(priskv_client *client);

/*
 *assuming max timeout means no timeout
 */
#define PRISKV_KEY_MAX_TIMEOUT 0xffffffffffffffffUL

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_H__ */
