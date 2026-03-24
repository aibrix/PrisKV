// Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
// Licensed under the Apache License, Version 2.0

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>

#include "kv.h"
#include "priskv-threads.h"
#include "priskv-protocol.h"
#include "memory.h"
#include "buddy.h"

/* Create a small KV for tests; caller frees key_base/value_base and destroys kv */
static void *create_kv(uint32_t max_keys, uint16_t max_key_length, uint32_t value_block_size,
                       uint64_t value_blocks, uint8_t **key_base_out, uint8_t **value_base_out)
{
    uint8_t *kbase = calloc(max_keys, priskv_mem_key_size(max_key_length));
    uint8_t *vbase = calloc(1, priskv_buddy_mem_size(value_blocks, value_block_size));
    void *kv = priskv_new_kv(kbase, vbase, -1, 0, max_keys, max_key_length, value_block_size,
                             value_blocks, NULL /* mf_ctx */);
    if (!kv) {
        free(kbase);
        free(vbase);
        *key_base_out = NULL;
        *value_base_out = NULL;
        return NULL;
    }
    *key_base_out = kbase;
    *value_base_out = vbase;
    return kv;
}

/* Run TTL scenarios (immediate unpin, TTL expiry, default TTL) on a fresh KV */
static int test_ttl_suite(void)
{
    /* thread pool for expire routine */
    uint8_t iothreads = 1, bgthreads = 1;
    priskv_threadpool *tp = priskv_threadpool_create("test", iothreads, bgthreads, 0);
    assert(tp);
    priskv_thread *bgthread = priskv_threadpool_get_bgthread(tp, 0);
    assert(bgthread);

    /* KV capacity */
    uint8_t *key_base = NULL, *value_base = NULL, *val_ptr = NULL;
    void *kv = create_kv(1024, 128, 4096, 1024 * 4ULL, &key_base, &value_base);
    if (!kv) {
        printf("TEST KV PIN TTL: create kv [FAILED]\n");
        return 1;
    }

    /* run expire routine in bg thread every 1s for TTL cleanup */
    priskv_set_expire_routine_interval(kv, 1 /* seconds */);
    priskv_expire_routine(bgthread, kv);

    const uint8_t *key = (const uint8_t *)"kpin";
    const uint16_t keylen = 4;
    const uint32_t alloc_len = 64;
    void *keynode = NULL;
    int ret;

    /* Scenario A: publish with short TTL and immediately unpin succeeds */
    ret = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, alloc_len,
                                    PRISKV_KEY_MAX_TIMEOUT, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: alloc private node [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_publish_node_with_pin(kv, keynode, true /* pin_on_publish */, 300 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: publish with pin [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_key_unpin_latest(kv, keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: unpin after publish [FAILED], ret=%d\n", ret);
        goto fail;
    }
    if (priskv_get_unpin_not_closed(kv) != 0) {
        printf("TEST KV PIN TTL: unpin_not_closed changed unexpectedly [FAILED]\n");
        goto fail;
    }

    /* Scenario B: TTL expiry cleans pinops, later UNPIN reports NOT_CLOSED */
    ret = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, alloc_len,
                                    PRISKV_KEY_MAX_TIMEOUT, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: alloc private node (B) [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_publish_node_with_pin(kv, keynode, true /* pin_on_publish */, 300 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: publish with pin (B) [FAILED], ret=%d\n", ret);
        goto fail;
    }
    /* Wait for TTL (300ms) + expire tick (1s) */
    usleep(1600 * 1000);
    ret = priskv_key_unpin_latest(kv, keynode);
    if (ret != PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED) {
        printf("TEST KV PIN TTL: unpin after TTL expiry should be NOT_CLOSED [FAILED], ret=%d\n",
               ret);
        goto fail;
    }

    /* Scenario C: ttl_ms == 0 uses default TTL path; just sanity-pin then unpin */
    ret = priskv_key_pin_latest(kv, keynode, 0 /* default TTL */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: pin with default TTL [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_key_unpin_latest(kv, keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: unpin after default TTL pin [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* Scenario D: re-pin with shorter TTL should NOT shorten pin_ttl (use max) */
    ret = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, alloc_len,
                                    PRISKV_KEY_MAX_TIMEOUT, &keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: alloc private node (D) [FAILED], ret=%d\n", ret);
        goto fail;
    }
    /* Publish with long TTL = 5000ms */
    ret = priskv_publish_node_with_pin(kv, keynode, true /* pin_on_publish */, 5000 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: publish with long pin (D) [FAILED], ret=%d\n", ret);
        goto fail;
    }
    /* Re-pin with short TTL = 300ms; pin_ttl should remain at max(5000ms, now+300ms) */
    ret = priskv_key_pin_latest(kv, keynode, 300 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: re-pin with short TTL (D) [FAILED], ret=%d\n", ret);
        goto fail;
    }
    /* Wait for expire tick (1s) so that if pin_ttl were shortened to 300ms, cleanup would reset */
    usleep(1600 * 1000);
    /* Expect UNPIN still succeeds because original longer TTL keeps pin active */
    ret = priskv_key_unpin_latest(kv, keynode);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN TTL: unpin after re-pin short (should be OK) [FAILED], ret=%d\n", ret);
        goto fail;
    }

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    printf("TEST KV PIN TTL: [OK]\n");
    return 0;

fail:
    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    return 1;
}

/* Run pin inherit semantics on a fresh KV */
static int test_inherit_suite(void)
{
    uint8_t *key_base = NULL, *value_base = NULL, *val_ptr = NULL;
    void *kv = create_kv(256, 64, 4096, 256 * 2ULL, &key_base, &value_base);
    if (!kv) {
        printf("TEST KV PIN INHERIT: create kv [FAILED]\n");
        return 1;
    }

    int ret;
    void *kn1 = NULL, *kn2 = NULL;
    const uint8_t *key = (const uint8_t *)"kver";
    const uint16_t keylen = 4;
    const uint32_t alloc_len = 32;

    /* v1: ALLOC + PUBLISH(pin) */
    ret = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, alloc_len,
                                    PRISKV_KEY_MAX_TIMEOUT, &kn1);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: alloc v1 [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_publish_node_with_pin(kv, kn1, true /* pin_on_publish */, 10000 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: publish v1 with pin [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* v2: ALLOC + PUBLISH(pin) */
    ret = priskv_alloc_node_private(kv, (uint8_t *)key, keylen, &val_ptr, alloc_len,
                                    PRISKV_KEY_MAX_TIMEOUT, &kn2);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: alloc v2 [FAILED], ret=%d\n", ret);
        goto fail;
    }
    ret = priskv_publish_node_with_pin(kv, kn2, true /* pin_on_publish */, 10000 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: publish v2 with pin [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* UNPIN #1 */
    ret = priskv_key_unpin_latest(kv, kn2);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: first unpin [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* UNPIN #2 */
    ret = priskv_key_unpin_latest(kv, kn2);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PIN INHERIT: second unpin [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* Extra UNPIN should report NOT_CLOSED */
    ret = priskv_key_unpin_latest(kv, kn2);
    if (ret != PRISKV_RESP_STATUS_UNPIN_NOT_CLOSED) {
        printf("TEST KV PIN INHERIT: extra unpin should be NOT_CLOSED [FAILED], ret=%d\n", ret);
        goto fail;
    }

    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    printf("TEST KV PIN INHERIT: [OK]\n");
    return 0;

fail:
    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    return 1;
}

/* Run pinned-eviction semantics on a fresh KV */
static int test_pinned_eviction_suite(void)
{
    /* Tiny KV to force pressure */
    uint8_t *key_base = NULL, *value_base = NULL, *val_ptr = NULL;
    void *kv = create_kv(8 /* max_keys */, 32 /* max_key_len */, 1024 /* vblk_size */,
                         8 /* vblks */, &key_base, &value_base);
    if (!kv) {
        printf("TEST KV PINNED EVICTION: create kv [FAILED]\n");
        return 1;
    }

    int ret;
    void *kn_pinned = NULL, *kn_tmp = NULL, *kn_check = NULL;
    const uint8_t *kpin = (const uint8_t *)"kpin";
    const uint8_t *ktemp = (const uint8_t *)"ktemp";
    const uint8_t *kbig = (const uint8_t *)"kbig";
    uint8_t *val_check = NULL;
    uint32_t vlen_check = 0;

    /* Insert pinned key with a small value, then pin it */
    ret = priskv_set_key(kv, (uint8_t *)kpin, 4, &val_ptr, 512, PRISKV_KEY_MAX_TIMEOUT, &kn_pinned);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PINNED EVICTION: set pinned key [FAILED], ret=%d\n", ret);
        goto fail;
    }
    priskv_set_key_end(kn_pinned);
    ret = priskv_key_pin_latest(kv, kn_pinned, 10000 /* ms */);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PINNED EVICTION: pin pinned key [FAILED], ret=%d\n", ret);
        goto fail;
    }

    /* Insert a temporary unpinned key to be evicted */
    ret = priskv_set_key(kv, (uint8_t *)ktemp, 5, &val_ptr, 1024, PRISKV_KEY_MAX_TIMEOUT, &kn_tmp);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PINNED EVICTION: set temp key [FAILED], ret=%d\n", ret);
        goto fail;
    }
    priskv_set_key_end(kn_tmp);

    /* Now try to insert a big value that forces eviction */
    (void)priskv_set_key(kv, (uint8_t *)kbig, 4, &val_ptr, 1024 * 6, PRISKV_KEY_MAX_TIMEOUT,
                         &kn_tmp);

    /* Verify pinned key still exists */
    ret = priskv_get_key(kv, (uint8_t *)kpin, 4, &val_check, &vlen_check, &kn_check);
    if (ret != PRISKV_RESP_STATUS_OK) {
        printf("TEST KV PINNED EVICTION: pinned key missing after pressure [FAILED], ret=%d\n",
               ret);
        goto fail;
    }
    priskv_get_key_end(kn_check);

    /* Cleanup */
    (void)priskv_key_unpin_latest(kv, kn_pinned);
    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    printf("TEST KV PINNED EVICTION: [OK]\n");
    return 0;

fail:
    (void)priskv_key_unpin_latest(kv, kn_pinned);
    priskv_destroy_kv(kv);
    free(key_base);
    free(value_base);
    return 1;
}

int main()
{
    if (test_ttl_suite() != 0)
        return 1;
    if (test_inherit_suite() != 0)
        return 1;
    if (test_pinned_eviction_suite() != 0)
        return 1;
    printf("TEST KV PIN TTL + INHERIT: [OK]\n");
    return 0;
}
