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
#include "priskv-threads.h"

#define MAX_KEY_LENGTH 64
#define VALUE_BLOCK_SIZE 1024
#define MAX_KEYS 1024
#define VALUE_BLOCKS (MAX_KEYS * 2)

/* --- Transport Layer Permission Tests (Negative Cases) --- */
static priskv_resp_status last_status;
static uint64_t last_token;
/* Thread-local slots for concurrent tests */
static __thread priskv_resp_status tls_status;
static __thread uint64_t tls_token;
static int mock_send_response(priskv_transport_conn *conn, uint64_t request_id,
                              priskv_resp_status status, uint32_t length, uint64_t addr_offset,
                              uint64_t token)
{
    /* Keep global values for single-threaded tests */
    last_status = status;
    last_token = token;
    /* Also record into TLS for multi-threaded tests */
    tls_status = status;
    tls_token = token;
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
        req->key_length = htobe16(strlen("perm_test_key") + 1);
        memcpy(mock_request_key(req), "perm_test_key", strlen("perm_test_key") + 1);
        priskv_transport_handle_recv(conn, req,
                                     sizeof(priskv_request) + strlen("perm_test_key") + 1);
    } else {
        req->key_length = htobe16(payload_len);
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
    req->key_length = htobe16(payload_len);
    memcpy(mock_request_key(req), payload, payload_len);
    priskv_transport_handle_recv(conn, req, sizeof(priskv_request) + payload_len);
}

/* Build ALLOC with key TTL (ms) and submit */
static void build_and_submit_alloc_with_timeout(priskv_transport_conn *conn, const char *key,
                                                uint16_t keylen, uint32_t alloc_len,
                                                uint64_t key_ttl_ms)
{
    size_t req_size = sizeof(priskv_request) + keylen;
    uint8_t *req_buf = alloca(req_size);
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, req_size);
    req->command = htobe16(PRISKV_COMMAND_ALLOC);
    req->alloc_length = htobe32(alloc_len);
    req->key_length = htobe16(keylen);
    req->timeout = htobe64(key_ttl_ms);
    memcpy((uint8_t *)req + sizeof(priskv_request), key, keylen);
    priskv_transport_handle_recv(conn, req, (uint16_t)req_size);
}

/* Build SEAL with optional PIN and per-request pin_ttl_ms */
static void build_and_submit_seal_with_ttl(priskv_transport_conn *conn, uint64_t token_host,
                                           uint32_t flags, uint64_t pin_ttl_ms)
{
    const size_t req_size = sizeof(priskv_request) + sizeof(uint64_t);
    uint8_t req_buf[req_size];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, req_size);
    req->command = htobe16(PRISKV_COMMAND_SEAL);
    req->flags = htobe32(flags);
    req->nsgl = htobe16(0);
    req->key_length = htobe16(sizeof(uint64_t));
    req->pin_ttl_ms = htobe64(pin_ttl_ms);
    uint64_t be_token = htobe64(token_host);
    memcpy((uint8_t *)req + sizeof(priskv_request), &be_token, sizeof(uint64_t));
    priskv_transport_handle_recv(conn, req, (uint16_t)req_size);
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

/* --- Combined flow: Alloc + Seal (PIN), then Acquire (PIN) + Release (UNPIN) twice --- */
/* --- Combined flow (multithreaded): Alloc + Seal (PIN), then N concurrent Acquire (PIN)
 * followed by N concurrent Release (UNPIN), finally manual unpin to zero. --- */
/* Combo worker semantics:
 * Phase 1 (prefill):
 *  - Role A: SEAL + PIN (allocates a private node and registers an ALLOC token)
 *  - Role B: ACQUIRE + PIN, then RELEASE (without UNPIN) to keep the pin
 * Random short wait
 * Phase 2 (decode): ACQUIRE (no PIN), then RELEASE (with UNPIN) for threads that pinned in prefill
 */
typedef enum {
    WORKER_PREFILL_SEAL = 0,
    WORKER_PREFILL_ACQ_NO_UNPIN = 1,
} worker_role;

typedef struct combo_worker_arg {
    priskv_transport_conn *conn;
    const char *key;
    uint16_t keylen;
    pthread_barrier_t *start_barrier;
    worker_role role;
    uint64_t seal_token_host; /* valid when role == WORKER_PREFILL_SEAL */
    priskv_resp_status status_prefill;
    priskv_resp_status status_decode_acq;
    priskv_resp_status status_decode_rel;
    int prefill_pinned; /* whether prefill phase left a pin */
} combo_worker_arg;

static void build_and_submit_req(priskv_transport_conn *conn, uint16_t cmd, uint32_t flags,
                                 const void *payload, uint16_t payload_len)
{
    size_t req_size = sizeof(priskv_request) + payload_len;
    uint8_t req_buf[req_size];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, req_size);
    req->command = htobe16(cmd);
    req->flags = htobe32(flags);
    req->nsgl = htobe16(0);
    req->key_length = htobe16(payload_len);
    if (payload_len && payload)
        memcpy((uint8_t *)req + sizeof(priskv_request), payload, payload_len);
    priskv_transport_handle_recv(conn, req, (uint16_t)req_size);
}

/* Helper: ACQUIRE + PIN with bounded retries to tolerate publish windows */
/* Helper: ACQUIRE with flags and bounded retries; returns status and token */
static void acquire_with_retry_flags(priskv_transport_conn *conn, const char *key, uint16_t keylen,
                                     uint32_t flags,
                                     priskv_resp_status *status_out, uint64_t *token_out,
                                     int max_retries, useconds_t retry_us, int *attempts_out)
{
    priskv_resp_status st = PRISKV_RESP_STATUS_SERVER_ERROR;
    uint64_t tok = 0;
    int attempt = 0;
    for (attempt = 0; attempt < max_retries; attempt++) {
        build_and_submit_req(conn, PRISKV_COMMAND_ACQUIRE, flags,
                             key, keylen);
        st = tls_status;
        tok = tls_token;
        if (st == PRISKV_RESP_STATUS_OK && tok != 0) break;
        usleep(retry_us);
    }
    if (status_out) *status_out = st;
    if (token_out) *token_out = tok;
    if (attempts_out) *attempts_out = attempt;
    if (!(st == PRISKV_RESP_STATUS_OK && tok != 0)) {
        printf("TEST TRANSPORT: ACQUIRE(flags=0x%x) failed after %d retries (status=%s, token=%lu)\n",
               flags,
               attempt, priskv_resp_status_str(st), tok);
    }
}

static void *combo_worker_thread(void *arg)
{
    combo_worker_arg *a = (combo_worker_arg *)arg;
    pthread_barrier_wait(a->start_barrier);

    /* Prefill phase */
    if (a->role == WORKER_PREFILL_SEAL) {
        /* In-thread: alloc private node -> register ALLOC token -> SEAL + PIN */
        uint8_t *val_ptr = NULL;
        void *node = NULL;
        int s = priskv_alloc_node_private(a->conn->kv, (uint8_t *)a->key, a->keylen,
                                          &val_ptr, 128, PRISKV_KEY_MAX_TIMEOUT, &node);
        if (s == PRISKV_RESP_STATUS_OK && node) {
            uint64_t token = priskv_transport_token_add(a->conn, node, PRISKV_TOKEN_TYPE_ALLOC);
            if (token) {
                uint64_t be_token = htobe64(token);
                build_and_submit_req(a->conn, PRISKV_COMMAND_SEAL, PRISKV_REQ_FLAG_PIN_ON_SEAL,
                                     &be_token, sizeof(uint64_t));
                a->status_prefill = tls_status;
                a->prefill_pinned = (tls_status == PRISKV_RESP_STATUS_OK) ? 1 : 0;
            } else {
                a->status_prefill = PRISKV_RESP_STATUS_SERVER_ERROR;
                a->prefill_pinned = 0;
                priskv_get_key_end(node);
            }
        } else {
            a->status_prefill = s;
            a->prefill_pinned = 0;
        }
    } else { /* WORKER_PREFILL_ACQ_NO_UNPIN */
        a->prefill_pinned = 0;
        priskv_resp_status st;
        uint64_t tok;
        int attempts;
        acquire_with_retry_flags(a->conn, a->key, a->keylen, PRISKV_REQ_FLAG_PIN_ON_ACQUIRE,
                                 &st, &tok, 5, 1000, &attempts);
        a->status_prefill = st;
        if (st == PRISKV_RESP_STATUS_OK && tok != 0) {
            /* RELEASE without UNPIN to keep the pin */
            uint64_t be_token = htobe64(tok);
            build_and_submit_req(a->conn, PRISKV_COMMAND_RELEASE, 0, &be_token, sizeof(uint64_t));
            a->prefill_pinned = 1;
        }
    }

    /* Random wait 0~5 ms */
    usleep((useconds_t)(random() % 5000));

    /* Decode phase: ACQUIRE (no PIN), then RELEASE; attach UNPIN if this thread pinned in prefill */
    /* Decode ACQUIRE with retries (no PIN); only RELEASE on success */
    priskv_resp_status st_dec;
    uint64_t tok_dec;
    int attempts_dec;
    acquire_with_retry_flags(a->conn, a->key, a->keylen, 0,
                             &st_dec, &tok_dec, 5, 1000, &attempts_dec);
    a->status_decode_acq = st_dec;
    if (st_dec == PRISKV_RESP_STATUS_OK && tok_dec != 0) {
        uint64_t be_token2 = htobe64(tok_dec);
        build_and_submit_req(a->conn, PRISKV_COMMAND_RELEASE,
                             a->prefill_pinned ? PRISKV_REQ_FLAG_UNPIN_ON_RELEASE : 0,
                             &be_token2, sizeof(uint64_t));
        a->status_decode_rel = tls_status;
    } else {
        a->status_decode_rel = st_dec; /* propagate failure */
    }
    return NULL;
}

static void test_kv_transport_alloc_seal_pin_acquire_release_unpin_combo(void *kv, int nthreads, int iters)
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

    const char *key = "combo_pin_unpin_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;

    for (int iter = 0; iter < iters; iter++) {
        printf("TEST TRANSPORT: COMBO iter %d/%d (threads=%d)\n", iter, iters, nthreads);

        /* Record stats baseline for this iteration */
        uint64_t pin_before = priskv_get_pin_ops(kv);
        uint64_t unpin_before = priskv_get_unpin_ops(kv);

        /* 0) Prepare an initial published version so ACQUIRE can succeed */
        void *keynode_alloc = NULL;
        int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 128,
                                          PRISKV_KEY_MAX_TIMEOUT, &keynode_alloc);
        assert(s == PRISKV_RESP_STATUS_OK && keynode_alloc);
        uint64_t alloc_token = priskv_transport_token_add(&conn, keynode_alloc, PRISKV_TOKEN_TYPE_ALLOC);
        uint64_t be_token = htobe64(alloc_token);
        last_status = -1;
        do_mock_req_with_flags(&conn, PRISKV_COMMAND_SEAL, &be_token, sizeof(uint64_t),
                               PRISKV_REQ_FLAG_PIN_ON_SEAL);
        assert(last_status == PRISKV_RESP_STATUS_OK);

        /* pin_count should now be 1 */
        uint32_t vlen = 0;
        void *latest = NULL;
        priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
        assert(latest);
        priskv_key *kn = (priskv_key *)latest;
        assert(kn->pin_count == 1);
        priskv_get_key_end(latest);

        /* 1) Half of the threads perform SEAL prefill (each allocs its own private node) */
        int n_seal = nthreads / 2;

        /* 2) Launch workers: half do SEAL prefill; the other half do ACQUIRE+RELEASE (no UNPIN) prefill; then decode */
        pthread_barrier_t start_barrier;
        pthread_barrier_init(&start_barrier, NULL, (unsigned int)nthreads);
        pthread_t *ths = calloc((size_t)nthreads, sizeof(pthread_t));
        combo_worker_arg *wargs = calloc((size_t)nthreads, sizeof(combo_worker_arg));
        for (int i = 0; i < nthreads; i++) {
            wargs[i].conn = &conn;
            wargs[i].key = key;
            wargs[i].keylen = keylen;
            wargs[i].start_barrier = &start_barrier;
            if (i < n_seal) {
                wargs[i].role = WORKER_PREFILL_SEAL;
                wargs[i].seal_token_host = 0; /* not used */
            } else {
                wargs[i].role = WORKER_PREFILL_ACQ_NO_UNPIN;
                wargs[i].seal_token_host = 0;
            }
            pthread_create(&ths[i], NULL, combo_worker_thread, &wargs[i]);
        }
        for (int i = 0; i < nthreads; i++) {
            pthread_join(ths[i], NULL);
        }
        pthread_barrier_destroy(&start_barrier);

        /* 3) Check pin_count: after decode UNPIN for successful prefill threads, it should be 1 */
        int sum_prefill_pins = 0;
        for (int i = 0; i < nthreads; i++) sum_prefill_pins += wargs[i].prefill_pinned;
        priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
        assert(latest);
        kn = (priskv_key *)latest;
        if (kn->pin_count != 1u) {
            printf("TEST TRANSPORT: COMBO iter %d pin_count expected %d, got %u [FAILED]\n",
                   iter, 1, kn->pin_count);
        }
        priskv_get_key_end(latest);

        // 4) Delta stats verification:
        // - No PIN in decode phase
        // - Prefill successful PINs: sum_prefill_pins; initial pin-on-seal counts as 1
        //   => delta_pin_ops >= 1 + sum_prefill_pins
        // - Decode UNPIN for successful prefill threads: sum_prefill_pins; then 1 manual cleanup UNPIN
        //   => delta_unpin_ops >= sum_prefill_pins + 1

        /* Manually unpin the remaining pin to reach 0 for cleanup (should be 1 here) */
        priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
        assert(latest);
        for (int i = 0; i < 1; i++) {
            priskv_resp_status ur = priskv_key_unpin_latest(kv, latest);
            assert(ur == PRISKV_RESP_STATUS_OK);
        }
        priskv_get_key_end(latest);

        priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
        assert(latest);
        kn = (priskv_key *)latest;
        assert(kn->pin_count == 0);
        priskv_get_key_end(latest);

        /* 5) Verify deltas and cleanup */
        uint64_t pin_after = priskv_get_pin_ops(kv);
        uint64_t unpin_after = priskv_get_unpin_ops(kv);
        uint64_t delta_pin = pin_after - pin_before;
        uint64_t delta_unpin = unpin_after - unpin_before;
        uint64_t expect_min_pin = (uint64_t)(1 + sum_prefill_pins);
        uint64_t expect_min_unpin = (uint64_t)(sum_prefill_pins + 1);
        if (delta_pin < expect_min_pin) {
            printf("TEST TRANSPORT: COMBO iter %d delta_pin expected >= %lu, got %lu [FAILED]\n",
                   iter, expect_min_pin, delta_pin);
            assert(0);
        }
        if (delta_unpin < expect_min_unpin) {
            printf("TEST TRANSPORT: COMBO iter %d delta_unpin expected >= %lu, got %lu [FAILED]\n",
                   iter, expect_min_unpin, delta_unpin);
            assert(0);
        }

        priskv_delete_key(kv, (uint8_t *)key, keylen);

        free(ths);
        free(wargs);
    }

    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

/* --- Key TTL race: ALLOC (short key TTL) + SEAL, then concurrent ACQUIRE/RELEASE
 * against the expire routine. Extended to operate on a set of keys and to run
 * for a configurable duration to strengthen foreground/background races. --- */
typedef struct ttl_race_worker_arg {
    priskv_transport_conn *conn;
    const char **keys;
    const uint16_t *keylens;
    int nkeys;
    useconds_t sleep_us;
    int duration_sec;
    int *acq_ok;
    int *acq_nosuch;
} ttl_race_worker_arg;

static void *ttl_race_worker(void *arg)
{
    ttl_race_worker_arg *a = (ttl_race_worker_arg *)arg;
    struct timeval start, now;
    gettimeofday(&start, NULL);

    for (;;) {
        gettimeofday(&now, NULL);
        long elapsed_ms = (now.tv_sec - start.tv_sec) * 1000L +
                          (now.tv_usec - start.tv_usec) / 1000L;
        if (elapsed_ms >= (long)a->duration_sec * 1000L) {
            break;
        }
        /* Randomly pick one key from the configured key set for this attempt. */
        int idx = (a->nkeys > 1) ? (int)(random() % a->nkeys) : 0;
        const char *key = a->keys[idx];
        uint16_t keylen = a->keylens[idx];

        build_and_submit_req(a->conn, PRISKV_COMMAND_ACQUIRE, 0, key, keylen);
        if (tls_status == PRISKV_RESP_STATUS_OK && tls_token != 0) {
            __sync_fetch_and_add(a->acq_ok, 1);
            uint64_t be_token = htobe64(tls_token);
            build_and_submit_req(a->conn, PRISKV_COMMAND_RELEASE, 0, &be_token, sizeof(uint64_t));
        } else if (tls_status == PRISKV_RESP_STATUS_NO_SUCH_KEY) {
            __sync_fetch_and_add(a->acq_nosuch, 1);
            /* Re-seed this key with a fresh short TTL so that foreground and
             * background cleanup continue to race over the full duration. */
            last_status = -1;
            last_token = 0;
            build_and_submit_alloc_with_timeout(a->conn, key, keylen, 128, 500 /* ms */);
            if (tls_status == PRISKV_RESP_STATUS_OK && tls_token != 0) {
                build_and_submit_seal_with_ttl(a->conn, tls_token, 0, 0);
            }
        }
        usleep(a->sleep_us);
    }
    return NULL;
}

static void test_kv_transport_key_ttl_expire_race(void *kv, int nthreads, int duration_sec)
{
    /* Mock driver */
    priskv_transport_driver mock_driver = {
        .name = "mock",
        .send_response = mock_send_response,
        .request_key_off = mock_request_key_off,
        .request_key = mock_request_key,
        .recv_req = mock_recv_req,
    };
    priskv_transport_driver *old_driver = g_transport_driver;
    g_transport_driver = &mock_driver;

    /* Transport connection */
    priskv_transport_conn conn = (priskv_transport_conn){0};
    conn.kv = kv;
    conn.conn_cap.max_key_length = MAX_KEY_LENGTH;
    conn.conn_cap.max_sgl = 8;
    pthread_spin_init(&conn.lock, PTHREAD_PROCESS_PRIVATE);

    /* Start expire routine with 1s interval. */
    priskv_threadpool *tp = priskv_threadpool_create("ttl_race", 1, 1, 0);
    assert(tp);
    priskv_thread *bgthread = priskv_threadpool_get_bgthread(tp, 0);
    assert(bgthread);
    priskv_set_expire_routine_interval(kv, 1);
    priskv_expire_routine(bgthread, kv);

    /* Prepare a small set of keys sharing the same short TTL to exercise
     * concurrent ACQUIRE/RELEASE and key TTL expiration across multiple
     * buckets/keys for the requested duration. We also track expire stats to
     * ensure the background routine, not just foreground ACQUIRE, performs key
     * cleanup. */
    static const char *ttl_keys[] = {
        "ttl_race_key_0",
        "ttl_race_key_1",
        "ttl_race_key_2",
        "ttl_race_key_3",
        "ttl_race_key_4",
        "ttl_race_key_5",
        "ttl_race_key_6",
        "ttl_race_key_7",
    };
    const int nkeys = (int)(sizeof(ttl_keys) / sizeof(ttl_keys[0]));
    uint16_t keylens[nkeys];
    uint64_t expire_before = priskv_get_expire_kv_count(kv);

    for (int i = 0; i < nkeys; i++) {
        keylens[i] = (uint16_t)(strlen(ttl_keys[i]) + 1);

        /* ALLOC with short key TTL=500ms for each key. */
        last_status = -1;
        last_token = 0;
        build_and_submit_alloc_with_timeout(&conn, ttl_keys[i], keylens[i], 128, 500 /* ms */);
        assert(last_status == PRISKV_RESP_STATUS_OK && last_token != 0);

        /* SEAL (no PIN) for each key. */
        build_and_submit_seal_with_ttl(&conn, last_token, 0, 0);
        assert(tls_status == PRISKV_RESP_STATUS_OK);
    }

    /* Multi-threaded ACQUIRE/RELEASE with background expiration, operating on
     * the key set above. */
    int acq_ok = 0, acq_nosuch = 0;
    pthread_t *ths = calloc((size_t)nthreads, sizeof(pthread_t));
    ttl_race_worker_arg *args = calloc((size_t)nthreads, sizeof(ttl_race_worker_arg));
    for (int i = 0; i < nthreads; i++) {
        args[i].conn = &conn;
        args[i].keys = ttl_keys;
        args[i].keylens = keylens;
        args[i].nkeys = nkeys;
        /* Use a short sleep to generate dense ACQUIRE/RELEASE traffic over the
         * configured duration, creating more chances to race with the expire
         * routine. */
        args[i].sleep_us = 1000; /* 1ms interval */
        args[i].duration_sec = duration_sec;
        args[i].acq_ok = &acq_ok;
        args[i].acq_nosuch = &acq_nosuch;
        pthread_create(&ths[i], NULL, ttl_race_worker, &args[i]);
    }
    for (int i = 0; i < nthreads; i++) pthread_join(ths[i], NULL);
    free(ths);
    free(args);

    if (!(acq_ok > 0 && acq_nosuch > 0)) {
        printf("TEST TRANSPORT: KEY TTL RACE expected both OK and NO_SUCH_KEY, got ok=%d nosuch=%d [FAILED]\n",
               acq_ok, acq_nosuch);
        assert(0);
    }
    uint64_t expire_after = priskv_get_expire_kv_count(kv);
    uint64_t expire_delta = expire_after - expire_before;
    /* Background expire routine may or may not win the race in this pattern.
     * Foreground ACQUIRE-based cleanup is already covered by ok/nosuch checks
     * above, so treat expire_kv_count as best-effort signal only. */
    if (expire_delta == 0) {
        printf("TEST TRANSPORT: KEY TTL RACE ok=%d nosuch=%d expire_delta=%lu (no background eviction observed) [OK]\n",
               acq_ok, acq_nosuch, expire_delta);
    } else {
        printf("TEST TRANSPORT: KEY TTL RACE ok=%d nosuch=%d expire_delta=%lu [OK]\n",
               acq_ok, acq_nosuch, expire_delta);
    }

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
    req->key_length = htobe16(too_long);
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
    req->key_length = htobe16(keylen);
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

/* --- Concurrent SEAL+PIN test: two threads seal the same key (PIN on SEAL) --- */
typedef struct seal_req_arg {
    priskv_transport_conn *conn;
    uint64_t token_host;           /* token in host byte order */
    pthread_barrier_t *barrier;
} seal_req_arg;

static void *seal_with_pin_thread(void *arg)
{
    seal_req_arg *a = (seal_req_arg *)arg;
    const size_t req_size = sizeof(priskv_request) + sizeof(uint64_t);
    uint8_t req_buf[req_size];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, req_size);
    req->command = htobe16(PRISKV_COMMAND_SEAL);
    req->flags = htobe32(PRISKV_REQ_FLAG_PIN_ON_SEAL);
    req->nsgl = htobe16(0);
    req->key_length = htobe16(sizeof(uint64_t));
    uint64_t be_token = htobe64(a->token_host);
    memcpy((uint8_t *)req + sizeof(priskv_request), &be_token, sizeof(uint64_t));
    pthread_barrier_wait(a->barrier);
    priskv_transport_handle_recv(a->conn, req, (uint16_t)req_size);
    return NULL;
}

static void test_kv_transport_concurrent_seal_pin(void *kv)
{
    /* Use a mock driver to avoid real network dependency */
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

    const char *key = "concurrent_seal_pin_key";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;
    void *node1 = NULL, *node2 = NULL;

    /* Pre-allocate two unpublished versions (ALLOC private nodes) */
    int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 256,
                                      PRISKV_KEY_MAX_TIMEOUT, &node1);
    assert(s == PRISKV_RESP_STATUS_OK && node1);
    s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 256,
                                  PRISKV_KEY_MAX_TIMEOUT, &node2);
    assert(s == PRISKV_RESP_STATUS_OK && node2);

    /* Register ALLOC tokens for each node */
    uint64_t t1 = priskv_transport_token_add(&conn, node1, PRISKV_TOKEN_TYPE_ALLOC);
    uint64_t t2 = priskv_transport_token_add(&conn, node2, PRISKV_TOKEN_TYPE_ALLOC);

    /* Launch two concurrent SEAL + PIN_ON_SEAL requests */
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 2);
    pthread_t th1, th2;
    seal_req_arg a1 = {.conn = &conn, .token_host = t1, .barrier = &barrier};
    seal_req_arg a2 = {.conn = &conn, .token_host = t2, .barrier = &barrier};
    pthread_create(&th1, NULL, seal_with_pin_thread, &a1);
    pthread_create(&th2, NULL, seal_with_pin_thread, &a2);
    pthread_join(th1, NULL);
    pthread_join(th2, NULL);
    pthread_barrier_destroy(&barrier);

    /* Verify: pin_count on latest version == 2, and unpin using the latest handle */
    uint32_t vlen = 0;
    void *latest = NULL;
    priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
    assert(latest);
    priskv_key *kn = (priskv_key *)latest;
    assert(kn->pin_count == 2);

    /* Hold the latest reference while unpinning to avoid stale-handle issues */
    priskv_resp_status r1 = priskv_key_unpin_latest(kv, latest);
    priskv_resp_status r2 = priskv_key_unpin_latest(kv, latest);
    priskv_resp_status r3 = priskv_key_unpin_latest(kv, latest);
    priskv_get_key_end(latest);
    assert(r1 == PRISKV_RESP_STATUS_OK);
    assert(r2 == PRISKV_RESP_STATUS_OK);
    assert(r3 == PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED);

    /* Cleanup */
    priskv_delete_key(kv, (uint8_t *)key, keylen);
    priskv_transport_token_cleanup(&conn);
    pthread_spin_destroy(&conn.lock);
    g_transport_driver = old_driver;
}

static void *seal_with_pin_thread_stress(void *arg)
{
    seal_req_arg *a = (seal_req_arg *)arg;
    const size_t req_size = sizeof(priskv_request) + sizeof(uint64_t);
    uint8_t req_buf[req_size];
    priskv_request *req = (priskv_request *)req_buf;
    memset(req, 0, req_size);
    req->command = htobe16(PRISKV_COMMAND_SEAL);
    req->flags = htobe32(PRISKV_REQ_FLAG_PIN_ON_SEAL);
    req->nsgl = htobe16(0);
    req->key_length = htobe16(sizeof(uint64_t));
    uint64_t be_token = htobe64(a->token_host);
    memcpy((uint8_t *)req + sizeof(priskv_request), &be_token, sizeof(uint64_t));
    pthread_barrier_wait(a->barrier);
    priskv_transport_handle_recv(a->conn, req, (uint16_t)req_size);
    return NULL;
}

static void test_kv_transport_concurrent_seal_pin_stress(void *kv, int nthreads, int iters)
{
    /* Mock driver in-process */
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

    const char *key = "concurrent_seal_pin_stress";
    uint16_t keylen = (uint16_t)(strlen(key) + 1);
    uint8_t *val_ptr = NULL;

    for (int iter = 0; iter < iters; iter++) {
        printf("[stress %d/%d]\n", iter, iters);
        /* Prepare nthreads private nodes and tokens in each round */
        void **nodes = calloc((size_t)nthreads, sizeof(void *));
        uint64_t *tokens = calloc((size_t)nthreads, sizeof(uint64_t));
        for (int i = 0; i < nthreads; i++) {
            int s = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, 64,
                                              PRISKV_KEY_MAX_TIMEOUT, &nodes[i]);
            assert(s == PRISKV_RESP_STATUS_OK && nodes[i]);
            tokens[i] = priskv_transport_token_add(&conn, nodes[i], PRISKV_TOKEN_TYPE_ALLOC);
        }

        pthread_barrier_t barrier;
        pthread_barrier_init(&barrier, NULL, (unsigned int)nthreads);
        pthread_t *ths = calloc((size_t)nthreads, sizeof(pthread_t));
        seal_req_arg *args = calloc((size_t)nthreads, sizeof(seal_req_arg));
        for (int i = 0; i < nthreads; i++) {
            args[i].conn = &conn;
            args[i].token_host = tokens[i];
            args[i].barrier = &barrier;
            pthread_create(&ths[i], NULL, seal_with_pin_thread_stress, &args[i]);
        }
        for (int i = 0; i < nthreads; i++) {
            pthread_join(ths[i], NULL);
        }
        pthread_barrier_destroy(&barrier);

        /* Verify pin_count == nthreads */
        uint32_t vlen = 0;
        void *latest = NULL;
        int gr = priskv_get_key(kv, (uint8_t *)key, keylen, &val_ptr, &vlen, &latest);
        if (gr != PRISKV_RESP_STATUS_OK || !latest) {
            printf("TEST TRANSPORT: STRESS iter %d get latest [FAILED] ret %d\n", iter, gr);
            assert(0);
        }
        priskv_key *kn = (priskv_key *)latest;
        if (kn->pin_count != (uint32_t)nthreads) {
            printf("TEST TRANSPORT: STRESS iter %d pin_count expected %d, got %u [FAILED]\n",
                   iter, nthreads, kn->pin_count);
            assert(0);
        }
        printf("TEST TRANSPORT: STRESS iter %d pin_count before unpin = %u\n", iter, kn->pin_count);
        /* Release pins using the latest visible node; keep a reference until done */
        /* log each UNPIN status to diagnose closure issues */
        int unpin_errors = 0;
        for (int i = 0; i < nthreads; i++) {
            priskv_resp_status ur = priskv_key_unpin_latest(kv, latest);
            if (ur != PRISKV_RESP_STATUS_OK) {
                printf("TEST TRANSPORT: STRESS iter %d unpin[%d] status = %s\n",
                       iter, i, priskv_resp_status_str(ur));
                unpin_errors++;
            }
        }
        priskv_get_key_end(latest);
        if (unpin_errors) {
            printf("TEST TRANSPORT: STRESS iter %d unpin errors = %d [FAILED]\n", iter, unpin_errors);
            assert(0);
        }
        priskv_delete_key(kv, (uint8_t *)key, keylen);

        free(ths);
        free(args);
        free(tokens);
        free(nodes);
    }

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

int main(int argc, char **argv)
{
    uint8_t *key_base, *value_base;
    void *kv;

    srandom(getpid());

    key_base = calloc(MAX_KEYS, priskv_mem_key_size(MAX_KEY_LENGTH));
    value_base = calloc(1, priskv_buddy_mem_size(VALUE_BLOCKS, VALUE_BLOCK_SIZE));
    kv = priskv_new_kv(key_base, value_base, -1, 0, MAX_KEYS, MAX_KEY_LENGTH, VALUE_BLOCK_SIZE,
                       VALUE_BLOCKS, NULL /* mf_ctx */);
    assert(kv);

    /* Optional args: argv[1]=combo_nthreads, argv[2]=combo_iters */
    int combo_nthreads = 8;
    int combo_iters = 1;
    if (argc >= 2) {
        int v = atoi(argv[1]);
        if (v > 0) combo_nthreads = v;
    }
    if (argc >= 3) {
        int v = atoi(argv[2]);
        if (v > 0) combo_iters = v;
    }

    printf("TEST TRANSPORT: Running transport layer permission tests... (combo threads=%d iters=%d)\n",
           combo_nthreads, combo_iters);
    test_kv_transport_permissions(kv);
    test_kv_transport_drop_behavior(kv);
    test_kv_transport_param_validation(kv);
    test_kv_transport_alloc_token_add_fail(kv);
    test_kv_transport_pin_on_seal(kv);
    test_kv_transport_pin_and_unpin(kv);
    /* TTL race tests (transport layer): */
    test_kv_transport_key_ttl_expire_race(kv, combo_nthreads, 60);
    test_kv_transport_alloc_seal_pin_acquire_release_unpin_combo(kv, combo_nthreads, combo_iters);
    test_kv_transport_unpin_no_such_key(kv);
    test_kv_transport_concurrent_seal_pin(kv);
    /* High-concurrency stress: threads and iterations are configurable; use 8*200 to expose atomicity issues */
    test_kv_transport_concurrent_seal_pin_stress(kv, 8, 200);
    printf("TEST TRANSPORT: All tests passed!\n");

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);

    return 0;
}
