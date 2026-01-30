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

#include <unistd.h>
#include <getopt.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include "priskv-protocol.h"
#include "priskv-utils.h"
#include "priskv-log.h"
#include "priskv-logo.h"

#include "transport/transport.h"
#include "memory.h"
#include "kv.h"
#include "priskv-threads.h"
#include "http.h"
#include "acl.h"
#include "backend/backend.h"

/* arguments of command line */
static int naddr = 0;
static char *addresses[PRISKV_TRANSPORT_MAX_BIND_ADDR];
static int port = PRISKV_TRANSPORT_DEFAULT_PORT;
static uint32_t max_key = PRISKV_TRANSPORT_DEFAULT_KEY;
static uint32_t value_block_size = PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK_SIZE;
static uint64_t value_block = PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK;
static uint8_t threads = 1;
static uint32_t thread_flags;
static uint32_t expire_routine_interval = PRISKV_KV_DEFAULT_EXPIRE_ROUTINE_INTERVAL;
static const char *memfile;
static priskv_log_level log_level = priskv_log_level_max;
static const char *g_log_file = NULL;
static priskv_logger *g_logger = NULL;
static priskv_transport_conn_cap conn_cap = {.max_sgl = PRISKV_TRANSPORT_DEFAULT_SGL,
                                             .max_key_length = PRISKV_TRANSPORT_DEFAULT_KEY_LENGTH,
                                             .max_inflight_command =
                                                 PRISKV_TRANSPORT_DEFAULT_INFLIGHT_COMMAND};

static priskv_http_config http_config = {
    .addr = NULL,
    .port = PRISKV_HTTP_DEFAULT_PORT,
    .verify_client = "off",
};

extern char *tiering_backend_address;
extern bool tiering_enabled;

extern priskv_threadpool *g_threadpool;
extern void *g_kv;

static void priskv_showhelp(void)
{
    printf("\nUsage:\n");
    printf("  -a/--addr ADDR\n\tbind to ADDR, support as max as %d addresses. Ex, -a xx.xx.xx.xx "
           "-a yy.yy.yy.yy\n",
           PRISKV_TRANSPORT_MAX_BIND_ADDR);
    printf("  -p/--port PORT\n\tlisten to PORT, default %d\n", PRISKV_TRANSPORT_DEFAULT_PORT);
    printf("  -f/--memfile PATH\n\tload memory file from tmpfs/hugetlbfs\n");
    printf("  -c/--max-inflight-command COMMANDS\n\tthe maxium count of inflight command, default "
           "%d, max %d\n",
           PRISKV_TRANSPORT_DEFAULT_INFLIGHT_COMMAND, PRISKV_TRANSPORT_MAX_INFLIGHT_COMMAND);
    printf("  -s/--max-sgl SGLS\n\tthe maxium count of scatter gather list, default %d, max %d\n",
           PRISKV_TRANSPORT_DEFAULT_SGL, PRISKV_TRANSPORT_MAX_SGL);
    printf("  -k/--max-keys KEYS\n\tthe maxium count of KV, default %d, max %d\n",
           PRISKV_TRANSPORT_DEFAULT_KEY, PRISKV_TRANSPORT_MAX_KEY);
    printf("  -K/--max-key-length BYTES\n\tthe maxium bytes of a key, default %d, max %d\n",
           PRISKV_TRANSPORT_DEFAULT_KEY_LENGTH, PRISKV_TRANSPORT_MAX_KEY_LENGTH);
    printf("  -v/--value-block-size BYTES\n\tthe block size of minimal value in bytes, "
           "default %d, max %d\n",
           PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK_SIZE, PRISKV_TRANSPORT_MAX_VALUE_BLOCK_SIZE);
    printf("  -b/--value-blocks BLOCKS\n\tthe count of value blocks, must be power of 2, "
           "default %ld, max %ld\n",
           PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK, PRISKV_TRANSPORT_MAX_VALUE_BLOCK);
    printf("  -t/--threads THREADS\n\tthe number of worker threads, default 1\n");
    printf("  -e/--expire-routine-interval INTERVAL\n\tthe interval to auto-clean expired kv in "
           "second, default 600\n");
    printf("  -B/--busy\n\tthe worker threads run in busy-poll mode, default event-based\n");
    printf("  -l/--log-level LEVEL\n\terror, warn, notice[default], info or debug\n");
    printf("  -L/--log-file FILEPATH\n\tlog to FILEPATH \n%s", PRISKV_LOGGER_HELP("\t"));
    printf("  -A/--http-addr ADDR\n\tlisten to ADDR, support IPv4 and IPv6\n");
    printf("  -P/--http-port PORT\n\tlisten to PORT, default %d\n", PRISKV_HTTP_DEFAULT_PORT);
    printf("  --http-cert PATH\n\tthe path of the certificate file\n");
    printf("  --http-key PATH\n\tthe path of the key file\n");
    printf("  --http-ca PATH\n\tthe path of the CA file\n");
    printf(
        "  --http-verify-client [off/optional/on]\n\tthe client certificate verification mode\n");
    printf("  -u/--slow-query-threshold-latency-us\n\tthe slow query threshold latency us, "
           "default %d, max %d\n",
           SLOW_QUERY_THRESHOLD_LATENCY_US, UINT32_MAX / 2);
    printf("  --backend ADDRESS\n\tbackend storage address (e.g., "
           "localfs:/data/priskv&size=100GB;s3:bucket1)\n");
    exit(0);
}

typedef enum priskv_short_arg {
    OPTARG_CERT = 1000,
    OPTARG_KEY,
    OPTARG_CA,
    OPTARG_VERIFY_CLIENT,
    OPTARG_ACL,
    OPTARG_BACKEND,
} priskv_short_arg;

static const char *priskv_short_opts = "a:p:A:P:f:c:s:K:k:v:b:t:Bl:L:e:u:h";
static struct option priskv_long_opts[] = {
    {"addr", required_argument, 0, 'a'},
    {"port", required_argument, 0, 'p'},
    {"http-addr", required_argument, 0, 'A'},
    {"http-port", required_argument, 0, 'P'},
    {"http-cert", required_argument, 0, OPTARG_CERT},
    {"http-key", required_argument, 0, OPTARG_KEY},
    {"http-ca", required_argument, 0, OPTARG_CA},
    {"http-verify-client", required_argument, 0, OPTARG_VERIFY_CLIENT},
    {"acl", required_argument, 0, OPTARG_ACL},
    {"backend", required_argument, 0, OPTARG_BACKEND},
    {"file", required_argument, 0, 'f'},
    {"max-inflight-command", required_argument, 0, 'c'},
    {"max-sgls", required_argument, 0, 's'},
    {"max-keys", required_argument, 0, 'k'},
    {"max-key-length", required_argument, 0, 'K'},
    {"value-block-size", required_argument, 0, 'v'},
    {"value-blocks", required_argument, 0, 'b'},
    {"threads", required_argument, 0, 't'},
    {"expire-routine-interval", required_argument, 0, 'e'},
    {"busy", required_argument, 0, 'B'},
    {"log-level", required_argument, 0, 'l'},
    {"log-file", required_argument, 0, 'L'},
    {"slow-query-threshold-latency-us", required_argument, 0, 'u'},
    {"help", no_argument, 0, 'h'},
};

static void priskv_parsr_arg(int argc, char *argv[])
{
    int args, ch;
    int64_t max_key_length = 0, _value_block_size = 0, _slow_query_threshold_latency_us = 0;

    while (1) {
        ch = getopt_long(argc, argv, priskv_short_opts, priskv_long_opts, &args);
        if (ch == -1) {
            break;
        }

        switch (ch) {
        case 'a':
            addresses[naddr++] = optarg;
            break;

        case 'p':
            if (port != PRISKV_TRANSPORT_DEFAULT_PORT) {
                printf("A single port is supported\n");
                priskv_showhelp();
            }

            port = atoi(optarg);
            if (!port || (port > 65535)) {
                printf("Invalid port (0, 65536)\n");
                priskv_showhelp();
            }
            break;

        case 'A':
            http_config.addr = optarg;
            break;

        case 'P':
            http_config.port = atoi(optarg);
            if (!http_config.port || (http_config.port > 65535)) {
                printf("Invalid http_port (0, 65536)\n");
                priskv_showhelp();
            }
            break;

        case 'f':
            memfile = optarg;
            break;

        case 'c':
            conn_cap.max_inflight_command = atoi(optarg);
            if (!conn_cap.max_inflight_command ||
                (conn_cap.max_inflight_command > PRISKV_TRANSPORT_MAX_INFLIGHT_COMMAND)) {
                printf("Invalid -c/--max-inflight-command\n");
                priskv_showhelp();
            }
            break;

        case 's':
            conn_cap.max_sgl = atoi(optarg);
            if (!conn_cap.max_sgl || (conn_cap.max_sgl > PRISKV_TRANSPORT_MAX_SGL)) {
                printf("Invalid -s/--max-sgl\n");
                priskv_showhelp();
            }
            break;

        case 'k':
            max_key = atoi(optarg);
            if (max_key > PRISKV_TRANSPORT_MAX_KEY) {
                printf("Invalid -k/--max-keys\n");
                priskv_showhelp();
            }
            break;

        case 'K':
            if (priskv_str2num(optarg, &max_key_length) || !max_key_length ||
                max_key_length > PRISKV_TRANSPORT_MAX_KEY_LENGTH) {
                printf("Invalid -K/--max-key-length\n");
                priskv_showhelp();
            }
            conn_cap.max_key_length = (uint16_t)max_key_length;
            break;

        case 'v':
            if (priskv_str2num(optarg, &_value_block_size) || !_value_block_size ||
                _value_block_size > PRISKV_TRANSPORT_MAX_VALUE_BLOCK_SIZE) {
                printf("Invalid -v/--value-block-size\n");
                priskv_showhelp();
            }

            value_block_size = (uint32_t)_value_block_size;
            break;

        case 'b':
            value_block = atoll(optarg);
            if (!value_block || (value_block > PRISKV_TRANSPORT_MAX_VALUE_BLOCK)) {
                priskv_showhelp();
            }

            if (!IS_POWER_OF_2(value_block)) {
                printf("-b/--value-blocks must be power of 2\n");
                priskv_showhelp();
            }
            break;

        case 't':
            threads = atoi(optarg);
            break;

        case 'e':
            expire_routine_interval = atoi(optarg);
            break;

        case 'B':
            thread_flags |= PRISKV_THREAD_BUSY_POLL;
            break;

        case 'l':
            if (!strcmp(optarg, "error")) {
                log_level = priskv_log_error;
            } else if (!strcmp(optarg, "warn")) {
                log_level = priskv_log_warn;
            } else if (!strcmp(optarg, "notice")) {
                log_level = priskv_log_notice;
            } else if (!strcmp(optarg, "info")) {
                log_level = priskv_log_info;
            } else if (!strcmp(optarg, "debug")) {
                log_level = priskv_log_debug;
            } else {
                priskv_showhelp();
            }
            break;

        case 'L':
            g_log_file = optarg;
            break;

        case 'u':
            if (priskv_str2num(optarg, &_slow_query_threshold_latency_us) ||
                _slow_query_threshold_latency_us <= 0 ||
                _slow_query_threshold_latency_us > UINT32_MAX / 2) {
                printf("Invalid -u/--slow-query-threshold-latency-us\n");
                priskv_showhelp();
            }

            g_slow_query_threshold_latency_us = (uint32_t)_slow_query_threshold_latency_us;
            break;

        case OPTARG_CERT:
            http_config.cert = optarg;
            break;

        case OPTARG_KEY:
            http_config.key = optarg;
            break;

        case OPTARG_CA:
            http_config.ca = optarg;
            break;

        case OPTARG_VERIFY_CLIENT:
            http_config.verify_client = optarg;
            break;

        case OPTARG_ACL:
            if (priskv_acl_add(optarg)) {
                printf("Failed to add ACL %s\n", optarg);
                priskv_showhelp();
            }
            break;

        case OPTARG_BACKEND:
            tiering_backend_address = optarg;
            tiering_enabled = true;
            break;

        case 'h':
        default:
            priskv_showhelp();
        }
    }
}

static void *priskv_server_create_kv()
{
    void *kv;
    void *mf_ctx;

    if (memfile) {
        mf_ctx = priskv_mem_load(memfile);
    } else {
        mf_ctx =
            priskv_mem_anon(conn_cap.max_key_length, max_key, value_block_size, value_block, threads);
    }

    if (!mf_ctx) {
        printf("Failed to load memory file. -l/--log-level debug for more information\n");
        return NULL;
    }

    uint8_t *key_base = priskv_mem_key_addr(mf_ctx);
    uint8_t *value_base = priskv_mem_value_addr(mf_ctx);

    if (memfile) {
        priskv_mem_header *hdr = (priskv_mem_header *)priskv_mem_header_addr(mf_ctx);
        kv = priskv_new_kv(key_base, value_base, hdr->max_keys, hdr->max_key_length,
                         hdr->value_block_size, hdr->value_blocks);

        /* try to recver key-value from memory file */
        if (priskv_recover(kv)) {
            return NULL;
        }
    } else {
        kv = priskv_new_kv(key_base, value_base, max_key, conn_cap.max_key_length, value_block_size,
                         value_block);
    }

    return kv;
}

extern priskv_transport_driver *g_transport_driver;

static void __priskv_transport_process(evutil_socket_t fd, short events, void *arg)
{
    priskv_transport_process();
}

static int priskv_server_start(struct event_base *evbase)
{
    struct event *ev;
    priskv_thread *bgthread;
    struct priskv_thread_hooks *backend_hooks = NULL;

    if (tiering_enabled && tiering_backend_address) {
        backend_hooks = priskv_get_thread_backend_hooks();
    }
    g_threadpool =
        priskv_threadpool_create_with_hooks("priskv", threads, 1, thread_flags, backend_hooks);

    if (!g_threadpool) {
        return -1;
    }

    g_kv = priskv_server_create_kv();
    if (!g_kv) {
        return -1;
    }

    bgthread = priskv_threadpool_find_bgthread(g_threadpool);
    priskv_set_expire_routine_interval(g_kv, expire_routine_interval);
    priskv_expire_routine(bgthread, g_kv);

    if (priskv_transport_listen(addresses, naddr, port, g_kv, &conn_cap)) {
        return -1;
    }

    ev = event_new(evbase, priskv_transport_get_fd(), EV_READ | EV_PERSIST, __priskv_transport_process, NULL);

    return event_add(ev, NULL);
};

static void priskv_server_log_fn(priskv_log_level level, const char *msg)
{
    priskv_logger_printf(g_logger, level, "%s", msg);
}

int main(int argc, char *argv[])
{
    priskv_show_logo();
    priskv_show_license();

    priskv_parsr_arg(argc, argv);
    priskv_set_log_level(log_level);
    if (g_log_file) {
        g_logger = priskv_logger_new(g_log_file);
        if (!g_logger) {
            printf("Failed to open logger %s, exit...\n", g_log_file);
            return -1;
        }
        priskv_set_log_fn(priskv_server_log_fn);
    }

    struct event_base *evbase = event_base_new();

    if (priskv_server_start(evbase)) {
        return -1;
    }

    if (http_config.addr) {
        if (priskv_http_start(evbase, &http_config)) {
            return -1;
        }
    }

    event_base_dispatch(evbase);

    priskv_logger_free(g_logger);
    event_base_free(evbase);

    return 0;
}
