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

#include <stdlib.h>
#include <string.h>
#include <ucp/api/ucp.h>

#include "priskv-log.h"
#include "priskv-threads.h"
#include "backend/backend.h"
#include "transport/transport.h"
#include "kv.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"

static void priskv_tiering_req_repost_recv(priskv_tiering_req *treq)
{
    if (!treq || treq->recv_reposted) {
        return;
    }
    treq->recv_reposted = true;
}

static void priskv_tiering_req_free(priskv_tiering_req *treq)
{
    if (!treq) {
        return;
    }
    free(treq->key);
    free(treq);
}

priskv_tiering_req *priskv_tiering_req_new(priskv_transport_conn *conn, priskv_request *req,
                                           uint8_t *key, uint16_t keylen, uint64_t timeout,
                                           priskv_req_command cmd, uint32_t remote_valuelen,
                                           priskv_resp_status *resp_status)
{
    priskv_resp_status status = PRISKV_RESP_STATUS_NO_MEM;
    priskv_tiering_req *treq = calloc(1, sizeof(priskv_tiering_req));
    priskv_thread *thread = NULL;
    priskv_backend_device *backend = NULL;

    if (!treq) {
        goto error;
    }

    thread = NULL;
    backend = priskv_get_thread_backend(thread);
    if (!backend) {
        status = PRISKV_RESP_STATUS_SERVER_ERROR;
        goto error;
    }

    treq->key = malloc(keylen + 1);
    if (!treq->key) {
        goto error;
    }
    memcpy(treq->key, key, keylen);
    treq->key[keylen] = '\0';

    list_node_init(&treq->node);
    treq->conn = conn;
    treq->thread = thread;
    treq->backend = backend;
    treq->kv = priskv_transport_get_kv();
    treq->req = req;
    treq->request_id = req->request_id;
    treq->keylen = keylen;
    treq->timeout = timeout;
    treq->cmd = cmd;
    treq->remote_valuelen = remote_valuelen;
    treq->valuelen = 0;
    treq->execute = false;
    treq->recv_reposted = false;
    treq->hash_head_index = priskv_crc32(key, keylen) % priskv_get_bucket_count(treq->kv);
    treq->backend_status = PRISKV_BACKEND_STATUS_ERROR;

    if (resp_status) {
        *resp_status = PRISKV_RESP_STATUS_OK;
    }
    return treq;

error:
    free(treq->key);
    free(treq);
    if (resp_status) {
        *resp_status = status;
    }
    return NULL;
}

static void priskv_tiering_finish(priskv_tiering_req *treq, priskv_resp_status status,
                                  uint32_t length)
{
    if (!treq) {
        return;
    }

    if (treq->execute) {
        priskv_key_serialize_exit(treq);
        treq->execute = false;
    }

    priskv_tiering_req_repost_recv(treq);

    priskv_transport_send_response(treq->conn, treq->request_id, status, length);

    priskv_tiering_req_free(treq);
}

static void priskv_tiering_get_complete_cb(void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_get_key_end(treq->keynode);
    priskv_tiering_req_repost_recv(treq);
    priskv_tiering_req_free(treq);
}

static void priskv_tiering_get_backend_cb(priskv_backend_status backend_status, uint32_t valuelen,
                                          void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_resp_status resp_status;
    treq->backend_status = backend_status;

    switch (backend_status) {
    case PRISKV_BACKEND_STATUS_OK: {
        uint8_t *val = NULL;
        uint32_t cached_length = 0;
        void *keynode = NULL;

        priskv_update_valuelen(treq->keynode, valuelen);
        treq->valuelen = valuelen;

        priskv_get_key(treq->kv, treq->key, treq->keylen, &val, &cached_length, &keynode);
        priskv_transport_rw_req(treq->conn, treq->req, &treq->conn->value_memh, treq->value,
                                treq->valuelen, 0, priskv_tiering_get_complete_cb, treq, 0,
                                NULL);
        return;
    }
    case PRISKV_BACKEND_STATUS_VALUE_TOO_BIG:
        resp_status = PRISKV_RESP_STATUS_VALUE_TOO_BIG;
        break;
    case PRISKV_BACKEND_STATUS_NOT_FOUND:
        resp_status = PRISKV_RESP_STATUS_NO_SUCH_KEY;
        break;
    default:
        resp_status = PRISKV_RESP_STATUS_SERVER_ERROR;
        break;
    }

    priskv_delete_key(treq->kv, treq->key, treq->keylen);
    priskv_tiering_finish(treq, resp_status, 0);
}

void priskv_tiering_get(priskv_tiering_req *treq)
{
    priskv_resp_status status;
    uint8_t *val = NULL;
    uint32_t valuelen = 0;
    void *keynode = NULL;

    if (!treq->execute) {
        if (!priskv_key_serialize_enter(treq)) {
            return;
        }
        treq->execute = true;
    }

    status = priskv_get_key(treq->kv, treq->key, treq->keylen, &val, &valuelen, &keynode);
    if (status == PRISKV_RESP_STATUS_OK && keynode) {
        treq->value = val;
        treq->valuelen = valuelen;
        treq->keynode = keynode;

        if (treq->remote_valuelen < treq->valuelen) {
            priskv_tiering_finish(treq, PRISKV_RESP_STATUS_VALUE_TOO_BIG, treq->valuelen);
            return;
        }
        priskv_transport_rw_req(treq->conn, treq->req, &treq->conn->value_memh, treq->value,
                                treq->valuelen, 0, priskv_tiering_get_complete_cb, treq, 0, NULL);
        priskv_key_serialize_exit(treq);
        treq->execute = false;
        return;
    }

    status = priskv_set_key(treq->kv, treq->key, treq->keylen, &val, treq->remote_valuelen,
                            treq->timeout, &keynode);
    priskv_set_key_end(keynode);
    if (status != PRISKV_RESP_STATUS_OK || !keynode) {
        priskv_tiering_finish(treq, status, 0);
        return;
    }
    treq->value = val;
    treq->keynode = keynode;
    priskv_backend_get(treq->backend, (const char *)treq->key, treq->value, treq->remote_valuelen,
                       priskv_tiering_get_backend_cb, treq);
}

static void priskv_tiering_test_backend_cb(priskv_backend_status status, uint32_t valuelen,
                                           void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_resp_status resp_status;
    uint32_t length = 0;
    treq->backend_status = status;
    switch (status) {
    case PRISKV_BACKEND_STATUS_OK:
        resp_status = PRISKV_RESP_STATUS_OK;
        length = valuelen;
        break;
    case PRISKV_BACKEND_STATUS_NOT_FOUND:
        resp_status = PRISKV_RESP_STATUS_NO_SUCH_KEY;
        break;
    default:
        resp_status = PRISKV_RESP_STATUS_SERVER_ERROR;
        break;
    }
    priskv_tiering_finish(treq, resp_status, length);
}

void priskv_tiering_test(priskv_tiering_req *treq)
{
    priskv_resp_status status;
    uint8_t *val = NULL;
    uint32_t valuelen = 0;
    void *keynode = NULL;

    if (!treq)
        return;
    if (!treq->execute) {
        if (!priskv_key_serialize_enter(treq)) {
            return;
        }
        treq->execute = true;
    }

    status = priskv_get_key(treq->kv, treq->key, treq->keylen, &val, &valuelen, &keynode);
    if (status == PRISKV_RESP_STATUS_OK) {
        priskv_tiering_finish(treq, PRISKV_RESP_STATUS_OK, valuelen);
        priskv_get_key_end(keynode);
        return;
    }
    priskv_backend_test(treq->backend, (const char *)treq->key, priskv_tiering_test_backend_cb,
                        treq);
}

static void priskv_tiering_set_backend_cb(priskv_backend_status status, uint32_t valuelen,
                                          void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_resp_status resp_status;
    uint32_t length = 0;
    priskv_delete_key(treq->kv, treq->key, treq->keylen);
    switch (status) {
    case PRISKV_BACKEND_STATUS_OK:
        resp_status = PRISKV_RESP_STATUS_OK;
        length = treq->valuelen;
        break;
    case PRISKV_BACKEND_STATUS_NO_SPACE:
        resp_status = PRISKV_RESP_STATUS_NO_MEM;
        break;
    default:
        resp_status = PRISKV_RESP_STATUS_SERVER_ERROR;
        break;
    }
    priskv_tiering_finish(treq, resp_status, length);
}

static void priskv_tiering_set_complete_cb(void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_set_key_end(treq->keynode);
    if (!treq->backend) {
        priskv_delete_key(treq->kv, treq->key, treq->keylen);
        priskv_tiering_finish(treq, PRISKV_RESP_STATUS_SERVER_ERROR, 0);
        return;
    }
    priskv_backend_set(treq->backend, (const char *)treq->key, treq->value, treq->remote_valuelen,
                       treq->timeout, priskv_tiering_set_backend_cb, treq);
}

void priskv_tiering_set(priskv_tiering_req *treq)
{
    priskv_resp_status status;
    if (!treq->execute) {
        if (!priskv_key_serialize_enter(treq)) {
            return;
        }
        treq->execute = true;
    }
    status = priskv_set_key(treq->kv, treq->key, treq->keylen, &treq->value, treq->remote_valuelen,
                            treq->timeout, &treq->keynode);
    if (status != PRISKV_RESP_STATUS_OK || !treq->keynode) {
        priskv_set_key_end(treq->keynode);
        priskv_tiering_finish(treq, status, 0);
        return;
    }
    priskv_transport_rw_req(treq->conn, treq->req, &treq->conn->value_memh, treq->value,
                            treq->remote_valuelen, 1, priskv_tiering_set_complete_cb, treq, 0,
                            NULL);
}

static void priskv_tiering_del_backend_cb(priskv_backend_status status, uint32_t valuelen,
                                          void *arg)
{
    priskv_tiering_req *treq = arg;
    priskv_resp_status resp_status;
    priskv_delete_key(treq->kv, treq->key, treq->keylen);
    switch (status) {
    case PRISKV_BACKEND_STATUS_OK:
        resp_status = PRISKV_RESP_STATUS_OK;
        break;
    case PRISKV_BACKEND_STATUS_NOT_FOUND:
        resp_status = PRISKV_RESP_STATUS_NO_SUCH_KEY;
        break;
    default:
        resp_status = PRISKV_RESP_STATUS_SERVER_ERROR;
        break;
    }
    priskv_tiering_finish(treq, resp_status, 0);
}

void priskv_tiering_del(priskv_tiering_req *treq)
{
    if (!treq->execute) {
        if (!priskv_key_serialize_enter(treq)) {
            return;
        }
        treq->execute = true;
    }
    priskv_backend_del(treq->backend, (const char *)treq->key, priskv_tiering_del_backend_cb, treq);
}

int priskv_backend_req_resubmit(void *req)
{
    priskv_tiering_req *treq = (priskv_tiering_req *)req;
    switch (treq->cmd) {
    case PRISKV_COMMAND_GET:
        priskv_tiering_get(treq);
        break;
    case PRISKV_COMMAND_SET:
        priskv_tiering_set(treq);
        break;
    case PRISKV_COMMAND_TEST:
        priskv_tiering_test(treq);
        break;
    case PRISKV_COMMAND_DELETE:
        priskv_tiering_del(treq);
        break;
    default:
        return -1;
    }
    return 0;
}
