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

#include <stdint.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <pthread.h>

#include "memory.h"
#include "kv.h"
#include "buddy.h"
#include "priskv-protocol.h"
#include "priskv-protocol-helper.h"
#include "priskv-utils.h"
#include "../transport/transport.h"

#define MAX_KEY_LENGTH 64
#define VALUE_BLOCK_SIZE 1024
#define MAX_KEYS 1024
#define VALUE_BLOCKS (MAX_KEYS * 2)

/* --- Transport Layer Permission Tests (Negative Cases) --- */
static priskv_resp_status last_status;
static uint64_t last_token;
static int mock_send_response(priskv_transport_conn *conn, uint64_t request_id,
                              priskv_resp_status status, uint32_t length, uint64_t addr_offset,
                              uint64_t token)
{
    last_status = status;
    last_token = token;
    return 0;
}

static uint16_t mock_request_key_off(priskv_request *req)
{
    return sizeof(priskv_request);
}

static uint8_t *mock_request_key(priskv_request *req)
{
    return (uint8_t *)req + sizeof(priskv_request);
}

static int mock_recv_req(priskv_transport_conn *conn, uint8_t *req)
{
    return 0;
}

static void do_mock_req(priskv_transport_conn *conn, uint16_t cmd, void *payload,
                        uint16_t payload_len)
{
    uint8_t req_buf[1024];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, sizeof(req_buf));
    req->command = htobe16(cmd);
    if (cmd == PRISKV_COMMAND_ALLOC) {
        req->alloc_length = htobe32(*(uint32_t *)payload);
        memcpy(mock_request_key(req), "perm_test_key", strlen("perm_test_key") + 1);
        priskv_transport_handle_recv(conn, req,
                                     sizeof(priskv_request) + strlen("perm_test_key") + 1);
    } else {
        memcpy(mock_request_key(req), payload, payload_len);
        priskv_transport_handle_recv(conn, req, sizeof(priskv_request) + payload_len);
    }
}

static void do_mock_req_with_flags(priskv_transport_conn *conn, uint16_t cmd, void *payload,
                                   uint16_t payload_len, uint32_t flags)
{
    uint8_t req_buf[1024];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, sizeof(req_buf));
    req->command = htobe16(cmd);
    req->flags = htobe32(flags);
    memcpy(mock_request_key(req), payload, payload_len);
    priskv_transport_handle_recv(conn, req, sizeof(priskv_request) + payload_len);
}

static void test_kv_transport_permissions(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    const char *key = "perm_test_key";
    uint16_t keylen = strlen(key) + 1;
    uint8_t *val_ptr;
    void *keynode_alloc = NULL;
    void *keynode_acquire = NULL;

    /* 1. Manually create an ALLOC token */
    priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 1024, PRISKV_KEY_MAX_TIMEOUT,
                              &keynode_alloc);
    assert(keynode_alloc != NULL);
    uint64_t alloc_token =
        priskv_transport_token_add(&conn, keynode_alloc, PRISKV_TOKEN_TYPE_ALLOC);

    /* 2. Try to RELEASE the ALLOC token (Negative Case) */
    last_status = -1;
    uint64_t be_token = htobe64(alloc_token);
    do_mock_req(&conn, PRISKV_COMMAND_RELEASE, &be_token, sizeof(uint64_t));
    if (last_status != PRISKV_RESP_STATUS_PERMISSION_DENIED) {
        printf("TEST TRANSPORT: Negative Case (RELEASE on ALLOC token) [FAILED] status %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: Negative Case (RELEASE on ALLOC token) [OK]\n");

    /* 3. Manually create an ACQUIRE token */
    priskv_publish_node(kv, keynode_alloc);
    priskv_transport_token_del(&conn, alloc_token);

    uint32_t val_len;
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &val_len, &keynode_acquire);
    assert(keynode_acquire != NULL);
    uint64_t acquire_token =
        priskv_transport_token_add(&conn, keynode_acquire, PRISKV_TOKEN_TYPE_ACQUIRE);

    /* 4. Try to SEAL the ACQUIRE token (Negative Case) */
    last_status = -1;
    be_token = htobe64(acquire_token);
    do_mock_req(&conn, PRISKV_COMMAND_SEAL, &be_token, sizeof(uint64_t));
    if (last_status != PRISKV_RESP_STATUS_PERMISSION_DENIED) {
        printf("TEST TRANSPORT: Negative Case (SEAL on ACQUIRE token) [FAILED] status %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: Negative Case (SEAL on ACQUIRE token) [OK]\n");

    /* 5. RELEASE it normally */
    do_mock_req(&conn, PRISKV_COMMAND_RELEASE, &be_token, sizeof(uint64_t));
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* Cleanup the key from KV */
    priskv_delete_key(kv, (uint8_t *)key, keylen);

    /* Cleanup transport resources */
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- DROP behavior & permission tests --- */
static void test_kv_transport_drop_behavior(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = (priskv_transport_conn) {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    const char *key = "drop_perm_key";
    uint16_t keylen = strlen(key) + 1;
    uint8_t *val_ptr;
    void *keynode_alloc = NULL;

    /* A. Unpublished node should be invisible and not ACQUIRE-able */
    int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 512,
                                      PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);

    /* Try ACQUIRE via transport on an unpublished key: expect NO_SUCH_KEY */
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen);
    if (last_status != PRISKV_RESP_STATUS_NO_SUCH_KEY) {
        printf("TEST TRANSPORT: ACQUIRE unpublished key should be NO_SUCH_KEY [FAILED], got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: ACQUIRE unpublished key [OK]\n");

    /* Create ALLOC token and DROP it: expect OK */
    uint64_t alloc_token =
        priskv_transport_token_add(&conn, keynode_alloc, PRISKV_TOKEN_TYPE_ALLOC);
    uint64_t be_token = htobe64(alloc_token);
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_DROP, &be_token, sizeof(uint64_t));
    if (last_status != PRISKV_RESP_STATUS_OK) {
        printf("TEST TRANSPORT: DROP unpublished (ALLOC token) [FAILED], got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: DROP unpublished (ALLOC token) [OK]\n");

    /* After DROP, ACQUIRE should still be NO_SUCH_KEY */
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen);
    if (last_status != PRISKV_RESP_STATUS_NO_SUCH_KEY) {
        printf("TEST TRANSPORT: ACQUIRE after DROP (unpublished) should be NO_SUCH_KEY [FAILED], "
               "got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: ACQUIRE after DROP (unpublished) [OK]\n");

    /* B. Published node: ACQUIRE then DROP (ACQUIRE token denied) */
    /* Re-create content */
    s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 512, PRISKV_KEY_MAX_TIMEOUT,
                                  &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
    priskv_publish_node(kv, keynode_alloc);

    /* ACQUIRE via transport */
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* Build an ACQUIRE token manually for DROP permission check */
    uint32_t vlen;
    void *keynode_acq = NULL;
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &keynode_acq);
    assert(keynode_acq);
    uint64_t acquire_token =
        priskv_transport_token_add(&conn, keynode_acq, PRISKV_TOKEN_TYPE_ACQUIRE);
    be_token = htobe64(acquire_token);

    /* DROP with ACQUIRE token: expect PERMISSION_DENIED */
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_DROP, &be_token, sizeof(uint64_t));
    if (last_status != PRISKV_RESP_STATUS_PERMISSION_DENIED) {
        printf("TEST TRANSPORT: DROP published (ACQUIRE token) should be PERMISSION_DENIED "
               "[FAILED], got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: DROP published (ACQUIRE token) denied [OK]\n");

    /* Cleanup */
    priskv_delete_key(kv, (uint8_t *)key, keylen);
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- Parameter Validation: empty key, overlong key, nsgl overflow --- */
static void test_kv_transport_param_validation(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH; /* 64 */
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    /* Common request buffer */
    uint8_t req_buf[1024];

    /* 1) Empty key: len == keyoff, expect KEY_EMPTY */
    memset(req_buf, 0, sizeof(req_buf));
    priskv_request *req = (priskv_request *)req_buf;
    req->command = htobe16(PRISKV_COMMAND_GET);
    req->nsgl = htobe16(0);
    last_status = -1;
    priskv_transport_handle_recv(&conn, req, mock_request_key_off(req));
    assert(last_status == PRISKV_RESP_STATUS_KEY_EMPTY);
    printf("TEST TRANSPORT: PARAM (empty key) [OK]\n");

    /* 2) Overlong key: keylen > max_key_length, expect KEY_TOO_BIG */
    memset(req_buf, 0, sizeof(req_buf));
    req = (priskv_request *)req_buf;
    req->command = htobe16(PRISKV_COMMAND_GET);
    req->nsgl = htobe16(0);
    uint16_t too_long = conn.conn_cap.max_key_length + 1; /* 65 */
    /* Append overlong key bytes following the request */
    memset(req_buf + sizeof(priskv_request), 'A', too_long);
    last_status = -1;
    priskv_transport_handle_recv(&conn, req, sizeof(priskv_request) + too_long);
    assert(last_status == PRISKV_RESP_STATUS_KEY_TOO_BIG);
    printf("TEST TRANSPORT: PARAM (key too big) [OK]\n");

    /* 3) nsgl overflow: nsgl > max_sgl, expect INVALID_SGL (use valid key to avoid other branches) */
    memset(req_buf, 0, sizeof(req_buf));
    req = (priskv_request *)req_buf;
    req->command = htobe16(PRISKV_COMMAND_GET);
    req->nsgl = htobe16(conn.conn_cap.max_sgl + 1);
    const char *key = "param_test_key"; /* length < max_key_length */
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    memcpy(req_buf + sizeof(priskv_request), key, keylen);
    last_status = -1;
    priskv_transport_handle_recv(&conn, req, sizeof(priskv_request) + keylen);
    assert(last_status == PRISKV_RESP_STATUS_INVALID_SGL);
    printf("TEST TRANSPORT: PARAM (nsgl too big) [OK]\n");

    /* Cleanup */
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- ALLOC path: token_add fails -> expect SERVER_ERROR and private node reclaimed --- */
static void test_kv_transport_alloc_token_add_fail(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = (priskv_transport_conn){0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    /* Inject a one-shot failure for token_add */
    priskv_test_token_add_fail_once = true;

    uint32_t alloc_len = 512;
    /* Record value-buddy usage before/after ALLOC to ensure memory is reclaimed on failure */
    uint64_t used_before = priskv_get_value_blocks_inuse(kv);
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_ALLOC, &alloc_len, sizeof(uint32_t));
    if (last_status != PRISKV_RESP_STATUS_SERVER_ERROR) {
        printf("TEST TRANSPORT: ALLOC with token_add failure should be SERVER_ERROR [FAILED], got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: ALLOC with token_add failure [OK]\n");

    uint64_t used_after = priskv_get_value_blocks_inuse(kv);
    if (used_after != used_before) {
        printf("TEST TRANSPORT: value_blocks_inuse should be unchanged after failed ALLOC [FAILED], before=%lu after=%lu\n",
               used_before, used_after);
        assert(0);
    }
    printf("TEST TRANSPORT: value_blocks_inuse unchanged after failed ALLOC [OK]\n");

    /* Ensure key not published and not ACQUIRE-able */
    const char *key = "perm_test_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    last_status = -1;
    do_mock_req(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen);
    if (last_status != PRISKV_RESP_STATUS_NO_SUCH_KEY) {
        printf("TEST TRANSPORT: ACQUIRE after failed ALLOC should be NO_SUCH_KEY [FAILED], got %s\n",
               priskv_resp_status_str(last_status));
        assert(0);
    }
    printf("TEST TRANSPORT: ACQUIRE after failed ALLOC [OK]\n");

    /* Cleanup */
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- Pin on SEAL and pin_count inheritance tests --- */
static void test_kv_transport_pin_on_seal(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    const char *key = "pin_seal_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;
    void *keynode_alloc = NULL;

    /* ALLOC private node and publish with pin-on-seal */
    int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 256,
                                      PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
    uint64_t alloc_token = priskv_transport_token_add(&conn, keynode_alloc, PRISKV_TOKEN_TYPE_ALLOC);
    uint64_t be_token = htobe64(alloc_token);
    last_status = -1;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_SEAL, &be_token, sizeof(uint64_t),
                           PRISKV_REQ_FLAG_PIN_ON_SEAL);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* verify pin_count == 1 on latest */
    uint32_t vlen = 0;
    void *keynode_acq = NULL;
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &keynode_acq);
    assert(keynode_acq);
    priskv_key *kn = (priskv_key *)keynode_acq;
    assert(kn->pin_count == 1);
    priskv_get_key_end(keynode_acq);

    /* publish a new version with pin-on-seal again; pin_count should inherit and increment to 2 */
    keynode_alloc = NULL;
    s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 128,
                                  PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
    alloc_token = priskv_transport_token_add(&conn, keynode_alloc, PRISKV_TOKEN_TYPE_ALLOC);
    be_token = htobe64(alloc_token);
    last_status = -1;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_SEAL, &be_token, sizeof(uint64_t),
                           PRISKV_REQ_FLAG_PIN_ON_SEAL);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &keynode_acq);
    assert(keynode_acq);
    kn = (priskv_key *)keynode_acq;
    assert(kn->pin_count == 2);
    priskv_get_key_end(keynode_acq);

    /* cleanup */
    priskv_delete_key(kv, (uint8_t *)key, keylen);
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- Pin on ACQUIRE + Unpin on RELEASE tests --- */
static void test_kv_transport_pin_and_unpin(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    const char *key = "pin_unpin_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;
    void *keynode_alloc = NULL;

    /* publish a key */
    int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 128,
                                      PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
    priskv_publish_node(kv, keynode_alloc);

    /* ACQUIRE with pin-on-acquire */
    last_status = -1; last_token = 0;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen,
                           PRISKV_REQ_FLAG_PIN_ON_ACQUIRE);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* verify pin_count == 1 */
    uint32_t vlen = 0;
    void *keynode_acq = NULL;
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &keynode_acq);
    assert(keynode_acq);
    priskv_key *kn = (priskv_key *)keynode_acq;
    assert(kn->pin_count == 1);
    priskv_get_key_end(keynode_acq);

    /* RELEASE with unpin-on-release */
    uint64_t be_token = htobe64(last_token);
    last_status = -1;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_RELEASE, &be_token, sizeof(uint64_t),
                           PRISKV_REQ_FLAG_UNPIN_ON_RELEASE);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* verify pin_count == 0 and counters updated */
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &keynode_acq);
    assert(keynode_acq);
    kn = (priskv_key *)keynode_acq;
    assert(kn->pin_count == 0);
    priskv_get_key_end(keynode_acq);

    /* TODO(wangyi): Add PinTTL cleanup tests in transport layer
     * - Inject short TTL for pin entries (once protocol supports it) and advance timer to
     *   verify automatic unpin on latest version.
     * - Verify counters for ttl_expired and ttl_cleanup_ops.
     */

    uint64_t pin_ops = priskv_get_pin_ops(kv);
    uint64_t unpin_ops = priskv_get_unpin_ops(kv);
    uint64_t unpin_not_closed = priskv_get_unpin_not_closed(kv);
    assert(pin_ops >= 1);
    assert(unpin_ops >= 1);
    assert(unpin_not_closed == 0);

    /* cleanup */
    priskv_delete_key(kv, (uint8_t *)key, keylen);
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- Unpin when latest version is missing (deleted/expired) tests --- */
static void test_kv_transport_unpin_no_such_key(void *kv)
{
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    priskv_transport_conn conn = {0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    const char *key = "unpin_nosuch_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;
    void *keynode_alloc = NULL;

    /* publish a key */
    int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 64,
                                      PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
    assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
    priskv_publish_node(kv, keynode_alloc);

    /* ACQUIRE with pin-on-acquire to generate a token */
    last_status = -1; last_token = 0;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_ACQUIRE, (void *)key, keylen,
                           PRISKV_REQ_FLAG_PIN_ON_ACQUIRE);
    assert(last_status == PRISKV_RESP_STATUS_OK);

    /* delete the key before release */
    priskv_delete_key(kv, (uint8_t *)key, keylen);

    /* RELEASE with unpin-on-release should return NO_SUCH_KEY */
    uint64_t be_token = htobe64(last_token);
    last_status = -1;
    do_mock_req_with_flags(&conn, PRISKV_COMMAND_RELEASE, &be_token, sizeof(uint64_t),
                           PRISKV_REQ_FLAG_UNPIN_ON_RELEASE);
    assert(last_status == PRISKV_RESP_STATUS_NO_SUCH_KEY);

    /* cleanup */
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

int main()
{
    uint8_t *key_base, *value_base;
    void *kv;

    srandom(getpid());

    key_base = calloc(MAX_KEYS, priskv_mem_key_size(MAX_KEY_LENGTH));
    value_base = calloc(1, priskv_buddy_mem_size(VALUE_BLOCKS, VALUE_BLOCK_SIZE));
    kv = priskv_new_kv(key_base, value_base, -1, 0, MAX_KEYS, MAX_KEY_LENGTH, VALUE_BLOCK_SIZE,
                       VALUE_BLOCKS, NULL /* mf_ctx */);
    assert(kv);

    printf("TEST TRANSPORT: Running transport layer permission tests...\n");
    test_kv_transport_permissions(kv);
    test_kv_transport_drop_behavior(kv);
    test_kv_transport_param_validation(kv);
    test_kv_transport_alloc_token_add_fail(kv);
    test_kv_transport_pin_on_seal(kv);
    test_kv_transport_pin_and_unpin(kv);
    test_kv_transport_unpin_no_such_key(kv);
    printf("TEST TRANSPORT: All tests passed!\n");

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);

    return 0;
}
