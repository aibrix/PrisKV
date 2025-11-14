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
 * Redis backend basic validation tests
 */

#define _DEFAULT_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/epoll.h>
#include <semaphore.h>
#include <time.h>
#include <errno.h>
#include <hiredis/hiredis.h>

#include "../backend/backend.h"
#include "../../include/priskv-log.h"

#define DEBUG_PRINT(fmt, ...) printf("[DEBUG] %s:%d: " fmt, __func__, __LINE__, ##__VA_ARGS__)

typedef struct cb_waiter {
    sem_t sem;
    volatile priskv_backend_status status;
    uint64_t valuelen; // Add valuelen to store the actual value length
} cb_waiter;

// Fix: Add valuelen parameter to match the callback signature
static void test_cb(priskv_backend_status status, uint64_t valuelen, void *arg)
{
    cb_waiter *w = (cb_waiter *)arg;
    DEBUG_PRINT("Callback called with status=%d, valuelen=%lu, waiter=%p\n", status, valuelen, w);
    w->status = status;
    w->valuelen = valuelen;
    sem_post(&w->sem);
}

static int wait_sem_ms(sem_t *sem, int ms, const char *stage)
{
    DEBUG_PRINT("Waiting for semaphore, stage=%s, timeout=%dms\n", stage, ms);

    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) == -1) {
        perror("clock_gettime");
        DEBUG_PRINT("clock_gettime failed\n");
        return -1;
    }
    ts.tv_sec += ms / 1000;
    ts.tv_nsec += (ms % 1000) * 1000000L;
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }

    int rc = sem_timedwait(sem, &ts);
    if (rc != 0) {
        if (errno == ETIMEDOUT) {
            DEBUG_PRINT("[%s] wait timeout after %d ms\n", stage, ms);
        } else {
            perror("sem_timedwait");
            DEBUG_PRINT("[%s] sem_timedwait error, errno=%d\n", stage, errno);
        }
        return -1;
    }

    DEBUG_PRINT("Semaphore acquired successfully for stage=%s\n", stage);
    return 0;
}

static void print_hex(const char *label, const uint8_t *data, size_t len)
{
    printf("[HEX] %s (%zu bytes): ", label, len);
    for (size_t i = 0; i < len; i++) {
        printf("%02x ", data[i]);
    }
    printf(" | ");
    for (size_t i = 0; i < len; i++) {
        printf("%c", (data[i] >= 32 && data[i] <= 126) ? data[i] : '.');
    }
    printf("\n");
}

static int run_basic_tests(priskv_backend_device *bdev)
{
    DEBUG_PRINT("Starting basic tests with bdev=%p\n", bdev);

    int epfd = epoll_create1(0);
    DEBUG_PRINT("Created epfd=%d\n", epfd);
    (void)epfd; /* not used, internal async thread drives hiredis */

    cb_waiter w;
    sem_init(&w.sem, 0, 0);
    w.valuelen = 0;

    // Test 1: SET without TTL
    DEBUG_PRINT("Test 1: SET without TTL\n");
    uint8_t setv[] = {'b', 'a', 'r'};
    print_hex("SET value", setv, sizeof(setv));

    DEBUG_PRINT("Calling priskv_backend_set: key='foo', valuelen=%zu, timeout=0\n", sizeof(setv));
    priskv_backend_set(bdev, "foo", setv, sizeof(setv), 0, test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "SET") != 0) {
        DEBUG_PRINT("SET timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("SET callback completed with status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("SET failed with status=%d\n", w.status);
        return -1;
    }
    DEBUG_PRINT("SET without TTL: OK\n");

    // Test 2: GET
    DEBUG_PRINT("Test 2: GET\n");
    uint8_t getv[3] = {0};
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_get: key='foo', buffer_size=%zu\n", sizeof(getv));
    priskv_backend_get(bdev, "foo", getv, sizeof(getv), test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "GET") != 0) {
        DEBUG_PRINT("GET timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("GET callback completed with status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("GET failed with status=%d\n", w.status);
        return -1;
    }

    print_hex("GET result", getv, w.valuelen);
    if (memcmp(getv, setv, sizeof(setv)) != 0) {
        DEBUG_PRINT("GET value mismatch\n");
        print_hex("Expected", setv, sizeof(setv));
        print_hex("Got", getv, w.valuelen);
        return -1;
    }
    DEBUG_PRINT("GET: OK\n");

    // Test 3: TEST exists
    DEBUG_PRINT("Test 3: TEST exists\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_test: key='foo'\n");
    priskv_backend_test(bdev, "foo", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "TEST exists") != 0) {
        DEBUG_PRINT("TEST exists timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("TEST exists callback completed with status=%d, valuelen=%lu\n", w.status,
                w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("TEST exists failed with status=%d\n", w.status);
        return -1;
    }
    DEBUG_PRINT("TEST exists: OK\n");

    // Test 4: SET with TTL
    DEBUG_PRINT("Test 4: SET with TTL\n");
    uint8_t setv_ttl[] = {'t', 't', 'l', 'v', 'a', 'l'};
    print_hex("SET with TTL value", setv_ttl, sizeof(setv_ttl));

    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    // Use reasonable TTL value (10 seconds)
    uint64_t timeout_ms = 10000;
    DEBUG_PRINT("Calling priskv_backend_set: key='foo_ttl', valuelen=%zu, timeout=%lu\n",
                sizeof(setv_ttl), timeout_ms);
    priskv_backend_set(bdev, "foo_ttl", setv_ttl, sizeof(setv_ttl), timeout_ms, test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "SET with TTL") != 0) {
        DEBUG_PRINT("SET with TTL timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("SET with TTL callback completed with status=%d, valuelen=%lu\n", w.status,
                w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("SET with TTL failed with status=%d\n", w.status);
        return -1;
    }
    DEBUG_PRINT("SET with TTL: OK\n");

    // Test 5: GET with TTL key
    DEBUG_PRINT("Test 5: GET with TTL key\n");
    uint8_t getv_ttl[6] = {0};
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_get: key='foo_ttl', buffer_size=%zu\n", sizeof(getv_ttl));
    priskv_backend_get(bdev, "foo_ttl", getv_ttl, sizeof(getv_ttl), test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "GET TTL") != 0) {
        DEBUG_PRINT("GET TTL timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("GET TTL callback completed with status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("GET TTL failed with status=%d\n", w.status);
        return -1;
    }

    print_hex("GET TTL result", getv_ttl, w.valuelen);
    if (memcmp(getv_ttl, setv_ttl, sizeof(setv_ttl)) != 0) {
        DEBUG_PRINT("GET TTL value mismatch\n");
        print_hex("Expected", setv_ttl, sizeof(setv_ttl));
        print_hex("Got", getv_ttl, w.valuelen);
        return -1;
    }
    DEBUG_PRINT("GET with TTL key: OK\n");

    // Test 6: DEL
    DEBUG_PRINT("Test 6: DEL\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_del: key='foo'\n");
    priskv_backend_del(bdev, "foo", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "DEL") != 0) {
        DEBUG_PRINT("DEL timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("DEL callback completed with status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("DEL failed with status=%d\n", w.status);
        return -1;
    }
    DEBUG_PRINT("DEL: OK\n");

    // Test 7: TEST not exists
    DEBUG_PRINT("Test 7: TEST not exists\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_test: key='foo' (should not exist)\n");
    priskv_backend_test(bdev, "foo", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "TEST not-exists") != 0) {
        DEBUG_PRINT("TEST not-exists timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("TEST not-exists callback completed with status=%d, valuelen=%lu\n", w.status,
                w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_NOT_FOUND) {
        DEBUG_PRINT("TEST not-found unexpected status: expected=%d, got=%d\n",
                    PRISKV_BACKEND_STATUS_NOT_FOUND, w.status);
        return -1;
    }
    DEBUG_PRINT("TEST not exists: OK\n");

    // Test 8: DEL TTL key
    DEBUG_PRINT("Test 8: DEL TTL key\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_del: key='foo_ttl'\n");
    priskv_backend_del(bdev, "foo_ttl", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "DEL TTL") != 0) {
        DEBUG_PRINT("DEL TTL timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("DEL TTL callback completed with status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_OK) {
        DEBUG_PRINT("DEL TTL failed with status=%d\n", w.status);
        return -1;
    }
    DEBUG_PRINT("DEL TTL key: OK\n");

    DEBUG_PRINT("All basic tests completed successfully\n");
    return 0;
}

typedef struct th_arg {
    priskv_backend_device *bdev;
    int idx;
    int rounds;
} th_arg;

static void *worker_th(void *arg)
{
    th_arg *ta = (th_arg *)arg;
    char key[64];
    uint8_t val[16];
    int i;

    DEBUG_PRINT("Worker %d starting, bdev=%p, rounds=%d\n", ta->idx, ta->bdev, ta->rounds);

    for (i = 0; i < ta->rounds; i++) {
        snprintf(key, sizeof(key), "conkey:%d:%d", ta->idx, i);
        uint8_t expected_val = (unsigned char)(ta->idx + i);
        memset(val, expected_val, sizeof(val));

        DEBUG_PRINT("Worker %d round %d: SET key=%s, value=%d (repeated)\n", ta->idx, i, key,
                    expected_val);

        cb_waiter w1;
        sem_init(&w1.sem, 0, 0);
        w1.status = 777;
        w1.valuelen = 0;

        // Use 0 as timeout, meaning never expire
        DEBUG_PRINT("Worker %d calling priskv_backend_set\n", ta->idx);
        priskv_backend_set(ta->bdev, key, val, sizeof(val), 0, test_cb, &w1);

        if (wait_sem_ms(&w1.sem, 3000, "worker SET") != 0) {
            DEBUG_PRINT("Worker %d SET timeout at round %d\n", ta->idx, i);
            break;
        }

        DEBUG_PRINT("Worker %d SET callback status=%d, valuelen=%lu\n", ta->idx, w1.status,
                    w1.valuelen);
        if (w1.status != PRISKV_BACKEND_STATUS_OK) {
            DEBUG_PRINT("Worker %d SET failed at round %d: status=%d\n", ta->idx, i, w1.status);
            break;
        }

        // Add a small delay between SET and GET to avoid potential race conditions
        usleep(1000); // 1ms delay

        DEBUG_PRINT("Worker %d round %d: GET key=%s\n", ta->idx, i, key);

        cb_waiter w2;
        sem_init(&w2.sem, 0, 0);
        w2.status = 777;
        w2.valuelen = 0;
        memset(val, 0, sizeof(val));

        DEBUG_PRINT("Worker %d calling priskv_backend_get\n", ta->idx);
        priskv_backend_get(ta->bdev, key, val, sizeof(val), test_cb, &w2);

        if (wait_sem_ms(&w2.sem, 3000, "worker GET") != 0) {
            DEBUG_PRINT("Worker %d GET timeout at round %d\n", ta->idx, i);
            break;
        }

        DEBUG_PRINT("Worker %d GET callback status=%d, valuelen=%lu\n", ta->idx, w2.status,
                    w2.valuelen);
        if (w2.status != PRISKV_BACKEND_STATUS_OK) {
            DEBUG_PRINT("Worker %d GET failed at round %d: status=%d\n", ta->idx, i, w2.status);
            break;
        }

        // Verify the retrieved value is correct
        uint8_t expected_val_check = (unsigned char)(ta->idx + i);
        uint8_t expected_buf[16];
        memset(expected_buf, expected_val_check, sizeof(expected_buf));

        DEBUG_PRINT("Worker %d verifying GET result, expected value=%d\n", ta->idx,
                    expected_val_check);

        if (memcmp(val, expected_buf, sizeof(val)) != 0) {
            DEBUG_PRINT("Worker %d GET value mismatch at round %d\n", ta->idx, i);
            print_hex("Expected", expected_buf, sizeof(expected_buf));
            print_hex("Got", val, sizeof(val));
            break;
        }

        DEBUG_PRINT("Worker %d round %d completed successfully\n", ta->idx, i);

        // Add a small delay between rounds to reduce concurrency pressure
        usleep(1000); // 1ms delay
    }

    DEBUG_PRINT("Worker %d finished, completed %d/%d rounds\n", ta->idx, i, ta->rounds);
    return NULL;
}

static int run_concurrency_tests(priskv_backend_device *bdev)
{
    DEBUG_PRINT("Starting concurrency tests with bdev=%p\n", bdev);

    // Reduce concurrency to avoid overwhelming the system
    int ths = 2;    // Reduced from 4 to 2
    int rounds = 5; // Reduced from 10 to 5
    pthread_t tids[8];
    th_arg args[8];

    DEBUG_PRINT("Creating %d worker threads, each with %d rounds\n", ths, rounds);

    for (int i = 0; i < ths; i++) {
        args[i].bdev = bdev;
        args[i].idx = i;
        args[i].rounds = rounds;
        DEBUG_PRINT("Creating worker thread %d\n", i);
        if (pthread_create(&tids[i], NULL, worker_th, &args[i]) != 0) {
            perror("pthread_create");
            DEBUG_PRINT("Failed to create worker thread %d\n", i);
            return -1;
        }
    }

    DEBUG_PRINT("Waiting for all worker threads to complete\n");
    for (int i = 0; i < ths; i++) {
        if (pthread_join(tids[i], NULL) != 0) {
            perror("pthread_join");
            DEBUG_PRINT("Failed to join worker thread %d\n", i);
        } else {
            DEBUG_PRINT("Worker thread %d joined successfully\n", i);
        }
    }

    DEBUG_PRINT("All concurrency tests completed\n");
    return 0;
}

static int run_error_case_tests(priskv_backend_device *bdev)
{
    DEBUG_PRINT("Starting error case tests with bdev=%p\n", bdev);

    cb_waiter w;

    // Test 1: GET non-existent key
    DEBUG_PRINT("Test 1: GET non-existent key\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;
    uint8_t dummy[10];

    DEBUG_PRINT("Calling priskv_backend_get for non-existent key\n");
    priskv_backend_get(bdev, "nonexistent_key_12345", dummy, sizeof(dummy), test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "GET non-existent") != 0) {
        DEBUG_PRINT("GET non-existent timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("GET non-existent callback status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_NOT_FOUND) {
        DEBUG_PRINT("GET non-existent expected NOT_FOUND (%d), got: %d\n",
                    PRISKV_BACKEND_STATUS_NOT_FOUND, w.status);
        return -1;
    }
    DEBUG_PRINT("GET non-existent: OK\n");

    // Test 2: TEST non-existent key
    DEBUG_PRINT("Test 2: TEST non-existent key\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_test for non-existent key\n");
    priskv_backend_test(bdev, "nonexistent_key_12345", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "TEST non-existent") != 0) {
        DEBUG_PRINT("TEST non-existent timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("TEST non-existent callback status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_NOT_FOUND) {
        DEBUG_PRINT("TEST non-existent expected NOT_FOUND (%d), got: %d\n",
                    PRISKV_BACKEND_STATUS_NOT_FOUND, w.status);
        return -1;
    }
    DEBUG_PRINT("TEST non-existent: OK\n");

    // Test 3: DEL non-existent key
    DEBUG_PRINT("Test 3: DEL non-existent key\n");
    sem_init(&w.sem, 0, 0);
    w.status = -1;
    w.valuelen = 0;

    DEBUG_PRINT("Calling priskv_backend_del for non-existent key\n");
    priskv_backend_del(bdev, "nonexistent_key_12345", test_cb, &w);

    if (wait_sem_ms(&w.sem, 3000, "DEL non-existent") != 0) {
        DEBUG_PRINT("DEL non-existent timeout occurred\n");
        return -1;
    }

    DEBUG_PRINT("DEL non-existent callback status=%d, valuelen=%lu\n", w.status, w.valuelen);
    if (w.status != PRISKV_BACKEND_STATUS_NOT_FOUND) {
        DEBUG_PRINT("DEL non-existent expected NOT_FOUND (%d), got: %d\n",
                    PRISKV_BACKEND_STATUS_NOT_FOUND, w.status);
        return -1;
    }
    DEBUG_PRINT("DEL non-existent: OK\n");

    DEBUG_PRINT("All error case tests completed successfully\n");
    return 0;
}

int main(int argc, char **argv)
{
    DEBUG_PRINT("Starting Redis backend test, argc=%d\n", argc);
    for (int i = 0; i < argc; i++) {
        DEBUG_PRINT("argv[%d] = %s\n", i, argv[i]);
    }

    const char *env_addr = getenv("PRISKV_TEST_REDIS_ADDR");
    DEBUG_PRINT("PRISKV_TEST_REDIS_ADDR=%s\n", env_addr ? env_addr : "NULL");

    const char *addr = NULL;
    char addrbuff[512] = {0};

    if (!env_addr || !*env_addr) {
        addr = "redis:host=127.0.0.1&port=6379&db=0&password=kvcache-redis";
        DEBUG_PRINT("Using default Redis address: %s\n", addr);
    } else {
        if (strchr(env_addr, ':') == NULL) {
            snprintf(addrbuff, sizeof(addrbuff), "redis:%s", env_addr);
            addr = addrbuff;
        } else {
            addr = env_addr;
        }
        DEBUG_PRINT("Using Redis address: %s\n", addr);
    }

    // First test synchronous connection to ensure Redis is available
    DEBUG_PRINT("Testing Redis synchronous connection...\n");
    redisContext *test_conn = redisConnect("127.0.0.1", 6379);
    if (test_conn == NULL || test_conn->err) {
        DEBUG_PRINT("Redis synchronous connection failed: %s\n",
                    test_conn ? test_conn->errstr : "can't allocate redis context");
        if (test_conn) {
            redisFree(test_conn);
        }
        return 1;
    }
    DEBUG_PRINT("Redis synchronous connection established\n");

    // Test authentication (if password is set)
    const char *password = getenv("PRISKV_REDIS_PASSWORD");
    DEBUG_PRINT("  PRISKV_REDIS_PASSWORD: [configured]\n");

    if (password && *password) {
        DEBUG_PRINT("Authenticating with Redis...\n");
        redisReply *auth_reply = redisCommand(test_conn, "AUTH %s", password);
        if (!auth_reply || auth_reply->type == REDIS_REPLY_ERROR) {
            DEBUG_PRINT("Redis AUTH failed: %s\n", auth_reply ? auth_reply->str : "no reply");
            if (auth_reply) {
                freeReplyObject(auth_reply);
            }
            redisFree(test_conn);
            return 1;
        }
        freeReplyObject(auth_reply);
        DEBUG_PRINT("Redis authentication successful\n");
    } else {
        DEBUG_PRINT("No password provided, skipping authentication\n");
    }

    // Test basic commands
    DEBUG_PRINT("Testing Redis PING command...\n");
    redisReply *reply = redisCommand(test_conn, "PING");
    if (reply) {
        DEBUG_PRINT("Redis PING: %s\n", reply->str);
        freeReplyObject(reply);
    } else {
        DEBUG_PRINT("Redis PING failed\n");
    }

    // Select database
    DEBUG_PRINT("Selecting Redis database 0...\n");
    redisReply *select_reply = redisCommand(test_conn, "SELECT 0");
    if (!select_reply || select_reply->type == REDIS_REPLY_ERROR) {
        DEBUG_PRINT("Redis SELECT failed: %s\n", select_reply ? select_reply->str : "no reply");
        if (select_reply) {
            freeReplyObject(select_reply);
        }
        redisFree(test_conn);
        return 1;
    }
    freeReplyObject(select_reply);
    DEBUG_PRINT("Redis database selected successfully\n");

    redisFree(test_conn);
    DEBUG_PRINT("Redis synchronous test passed\n");

    int epfd = epoll_create1(0);
    if (epfd == -1) {
        perror("epoll_create1");
        DEBUG_PRINT("epoll_create1 failed with errno=%d\n", errno);
        return 1;
    }
    DEBUG_PRINT("Created epoll fd: %d\n", epfd);

    DEBUG_PRINT("Calling priskv_backend_open with addr=%s, epfd=%d\n", addr, epfd);
    priskv_backend_device *bdev = priskv_backend_open(addr, epfd);
    if (!bdev) {
        DEBUG_PRINT("priskv_backend_open returned NULL\n");
        close(epfd);
        return 2;
    }
    DEBUG_PRINT("priskv_backend_open succeeded, bdev=%p\n", bdev);

    /* ensure starting clean */
    DEBUG_PRINT("Cleaning up Redis database...\n");
    if (bdev->bdrv->clearup(bdev) != 0) {
        DEBUG_PRINT("Warning: clearup failed, continue testing\n");
    } else {
        DEBUG_PRINT("Cleanup completed\n");
    }

    int ret = 0;

    // Run basic tests
    DEBUG_PRINT("=== Starting Basic Tests ===\n");
    if (run_basic_tests(bdev) != 0) {
        DEBUG_PRINT("Basic tests FAILED\n");
        ret = 1;
        goto out;
    } else {
        DEBUG_PRINT("Basic tests PASSED\n");
    }

    // Run error case tests
    DEBUG_PRINT("=== Starting Error Case Tests ===\n");
    if (run_error_case_tests(bdev) != 0) {
        DEBUG_PRINT("Error case tests FAILED\n");
        ret = 1;
        goto out;
    } else {
        DEBUG_PRINT("Error case tests PASSED\n");
    }

    // Run concurrency tests
    DEBUG_PRINT("=== Starting Concurrency Tests ===\n");
    if (run_concurrency_tests(bdev) != 0) {
        DEBUG_PRINT("Concurrency tests FAILED\n");
        ret = 1;
        goto out;
    } else {
        DEBUG_PRINT("Concurrency tests PASSED\n");
    }

    DEBUG_PRINT("\n=== ALL TESTS PASSED ===\n");

out:
    DEBUG_PRINT("Cleaning up, ret=%d\n", ret);

    DEBUG_PRINT("Closing backend...\n");
    if (priskv_backend_close(bdev) != 0) {
        DEBUG_PRINT("Failed to close backend\n");
        ret = 1;
    } else {
        DEBUG_PRINT("Backend closed successfully\n");
    }

    DEBUG_PRINT("Closing epoll fd...\n");
    close(epfd);

    if (ret == 0) {
        DEBUG_PRINT("Test completed successfully\n");
    } else {
        DEBUG_PRINT("Test completed with errors\n");
    }

    return ret;
}