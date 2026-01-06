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
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>

#include "priskv.h"
#include "priskv-log.h"
#include "priskv-logo.h"
#include "linenoise.h"

#define VALUE_MAX_LEN (64 * 1024)

/* arguments of command line */
static const char *raddr;
static int rport = ('H' << 8 | 'P');
static const char *laddr;
static int lport;
static priskv_log_level log_level = priskv_log_notice;
static priskv_logger *g_logger;
static const char *log_file = NULL;
static int g_exit = 0;
static const char *invalid_args_msg = "Invalid arguments, please enter 'help' to get usage";

static void priskv_showhelp(void)
{
    printf("Usage:\n");
    printf("  -h/--help\n\tprint this help message\n");
    printf("  -a/--raddr ADDR\n\tconnect remote ADDR, a single address is supported.\n");
    printf("  -p/--rport PORT\n\tconnect remote PORT, default %d\n", rport);
    printf("  -A/--laddr ADDR\n\tbind to local ADDR, a single address is supported.\n");
    printf("  -P/--lport PORT\n\tbind to PORT, default %d\n", lport);
    printf("  -l/--log-level LEVEL\n\terror, warn, notice[default], info or debug\n");
    printf("  -L/--log-file FILEPATH\n\tlog to FILEPATH \n%s", PRISKV_LOGGER_HELP("\t"));

    exit(0);
}

static const char *priskv_short_opts = "a:p:A:P:l:L:h";
static struct option priskv_long_opts[] = {
    {"raddr", required_argument, 0, 'a'},
    {"rport", required_argument, 0, 'p'},
    {"laddr", required_argument, 0, 'A'},
    {"lport", required_argument, 0, 'P'},
    {"log-level", required_argument, 0, 'l'},
    {"log-file", required_argument, 0, 'L'},
    {"help", no_argument, 0, 'h'},
};

static void priskv_parsr_arg(int argc, char *argv[])
{
    int args, ch;

    while (1) {
        ch = getopt_long(argc, argv, priskv_short_opts, priskv_long_opts, &args);
        if (ch == -1) {
            break;
        }

        switch (ch) {
        case 'a':
            raddr = optarg;
            break;

        case 'p':
            rport = atoi(optarg);
            break;

        case 'A':
            laddr = optarg;
            break;

        case 'P':
            lport = atoi(optarg);
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
            log_file = optarg;
            break;

        case 'h':
        default:
            priskv_showhelp();
        }
    }
}

typedef struct {
    priskv_client *client;
    void *buf;
    priskv_memory *priskvmem;
} client_context;

typedef struct {
    const char *name;
    void (*handler)(client_context *ctx, char *args);
    const char *help;
} priskv_command;

static void print_cmd_help();
static void help_handler(client_context *ctx, char *args)
{
    print_cmd_help();
}

static void set_handler(client_context *ctx, char *args)
{
    char *key, *value, *opt, *opt_val, *str_end;
    uint64_t expire_time_ms = 0;
    size_t valuelen;
    priskv_sgl sgl;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    value = strtok_r(args, " ", &args);
    if (!value) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    while (strlen(args) > 0) {
        opt = strtok_r(args, " ", &args);
        if (!strcmp(opt, "EX") || !strcmp(opt, "PX")) {
            if (expire_time_ms > 0) {
                printf("%s\n", invalid_args_msg);
                return;
            }

            opt_val = strtok_r(args, " ", &args);
            if (!strlen(opt_val) || opt_val[0] == '-') {
                printf("%s\n", invalid_args_msg);
                return;
            }

            errno = 0;
            expire_time_ms = strtoull(opt_val, &str_end, 10);
            if (errno > 0 || str_end == opt_val || *str_end != '\0' || expire_time_ms == 0) {
                printf("%s\n", invalid_args_msg);
                return;
            }

            if (!strcmp(opt, "EX")) {
                expire_time_ms *= 1000;
            }
        } else {
            printf("%s\n", invalid_args_msg);
            return;
        }
    }

    if (expire_time_ms == 0) {
        /* timeout not passed, considering as no timeout */
        expire_time_ms = PRISKV_KEY_MAX_TIMEOUT;
    }

    valuelen = strlen(value) + 1;

    memcpy(ctx->buf, value, valuelen);

    sgl.iova = (uint64_t)ctx->buf;
    sgl.length = (uint32_t)valuelen;
    sgl.mem = ctx->priskvmem;

    printf("SET key=%s, value[%ld]=%s, expire_time_ms=%lu\n", key, valuelen, value, expire_time_ms);
    status = priskv_set(ctx->client, key, &sgl, 1, expire_time_ms);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to SET, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("SET status(%d): %s\n", status, priskv_status_str(status));
}

static void get_handler(client_context *ctx, char *args)
{
    char *key;
    uint32_t valuelen;
    priskv_sgl sgl;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    memset(ctx->buf, 0x00, VALUE_MAX_LEN);

    sgl.iova = (uint64_t)ctx->buf;
    sgl.length = VALUE_MAX_LEN;
    sgl.mem = ctx->priskvmem;

    printf("GET key=%s\n", key);
    status = priskv_get(ctx->client, key, &sgl, 1, &valuelen);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to GET, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }

    ((char *)ctx->buf)[valuelen] = '\0';
    printf("GET status(%d): %s\n", status, priskv_status_str(status));
    printf("GET value[%u]=%s\n", valuelen, (char *)ctx->buf);
}

static void get_and_pin_handler(client_context *ctx, char *args)
{
    char *key;
    uint32_t valuelen;
    uint64_t pin_token = 7; /* for debug */
    priskv_sgl sgl;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    memset(ctx->buf, 0x00, VALUE_MAX_LEN);

    sgl.iova = (uint64_t)ctx->buf;
    sgl.length = VALUE_MAX_LEN;
    sgl.mem = ctx->priskvmem;

    printf("GET key=%s\n", key);
    status = priskv_get_and_pin(ctx->client, key, &sgl, 1, &pin_token, &valuelen);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to GET, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }

    ((char *)ctx->buf)[valuelen] = '\0';
    printf("GET status(%d): %s\n", status, priskv_status_str(status));
    printf("GET value[%u]=%s\n", valuelen, (char *)ctx->buf);
    printf("PIN token[%lu]\n", pin_token);
}

static void test_handler(client_context *ctx, char *args)
{
    char *key;
    uint32_t valuelen;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("TEST key=%s\n", key);
    status = priskv_test(ctx->client, key, &valuelen);
    if (status != PRISKV_STATUS_OK && status != PRISKV_STATUS_NO_SUCH_KEY) {
        printf("Failed to TEST, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("TEST status(%d): %s\n", status, priskv_status_str(status));
    if (status == PRISKV_STATUS_OK) {
        printf("TEST value[%u]\n", valuelen);
    }
}

static void delete_handler(client_context *ctx, char *args)
{
    char *key;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("DELETE key=%s\n", key);
    status = priskv_delete(ctx->client, key);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to DELETE, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("DELETE status(%d): %s\n", status, priskv_status_str(status));
}

static void expire_handler(client_context *ctx, char *args)
{
    char *key, *value, *str_end;
    uint64_t expire_time = 0;
    priskv_status status;

    key = strtok_r(args, " ", &args);
    if (!key) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    value = strtok_r(args, " ", &args);
    if (!value) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    errno = 0;
    expire_time = strtoll(value, &str_end, 10);
    if (errno > 0 || str_end == value || *str_end != '\0') {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("EXPIRE key=%s, expire_time_ms=%ld\n", key, expire_time * 1000);
    if (expire_time > 0) {
        status = priskv_expire(ctx->client, key, expire_time * 1000);
    } else {
        status = priskv_delete(ctx->client, key);
    }

    if (status != PRISKV_STATUS_OK) {
        printf("Failed to EXPIRE, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("EXPIRE status(%d): %s\n", status, priskv_status_str(status));
}

static void keys_handler(client_context *ctx, char *args)
{
    char *regex;
    priskv_status status;
    priskv_keyset *keyset;

    regex = strtok_r(args, " ", &args);
    if (!regex) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("KEYS regex=%s\n", regex);
    status = priskv_keys(ctx->client, regex, &keyset);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to KEYS, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("KEYS status(%d): %s\n", status, priskv_status_str(status));

    for (uint32_t i = 0; i < keyset->nkey; i++) {
        printf("\t%d) Key[%s] Valuelen[%d]\n", i, keyset->keys[i].key, keyset->keys[i].valuelen);
    }

    priskv_keyset_free(keyset);
}

static void nrkeys_handler(client_context *ctx, char *args)
{
    char *regex;
    priskv_status status;
    uint32_t nkey;

    regex = strtok_r(args, " ", &args);
    if (!regex) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("NRKEYS regex=%s\n", regex);
    status = priskv_nrkeys(ctx->client, regex, &nkey);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to KEYS, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("NRKEYS status(%d): %s. NRKEYS %d\n", status, priskv_status_str(status), nkey);
}

static void flush_handler(client_context *ctx, char *args)
{
    char *regex;
    priskv_status status;
    uint32_t nkey;

    regex = strtok_r(args, " ", &args);
    if (!regex) {
        printf("%s\n", invalid_args_msg);
        return;
    }

    printf("FLUSH regex=%s\n", regex);
    status = priskv_flush(ctx->client, regex, &nkey);
    if (status != PRISKV_STATUS_OK) {
        printf("Failed to KEYS, status(%d): %s\n", status, priskv_status_str(status));
        return;
    }
    printf("FLUSH status(%d): %s. NRKEYS %d\n", status, priskv_status_str(status), nkey);
}

static void capacity_handler(client_context *ctx, char *args)
{
    uint64_t capacity = priskv_capacity(ctx->client);

    printf("Capacity %ld\n", capacity);
}

static void exit_handler(client_context *ctx, char *args)
{
    g_exit = 1;
}

static priskv_command commands[] = {
    {"help", help_handler, "help\t\t\t\t\t\tprint this help\n"},
    {"set", set_handler,
     "set KEY VALUE [ EX seconds | PX milliseconds ]\tset key:value to priskv\n"},
    {"get", get_handler, "get KEY\t\t\t\t\t\tget key:value from priskv\n"},
    {"pget", get_and_pin_handler, "get and pin KEY\t\t\t\t\t\tget key:value from priskv\n"},
    {"test", test_handler, "test KEY\t\t\t\t\t\ttest if the key exists in priskv\n"},
    {"delete", delete_handler, "delete KEY\t\t\t\t\t\tdelete the key from priskv\n"},
    {"expire", expire_handler, "expire KEY seconds\t\t\t\t\tset expire time for key\n"},
    {"keys", keys_handler, "keys REGEX\t\t\t\t\t\tget keys matched with regex from priskv\n"},
    {"nrkeys", nrkeys_handler,
     "nrkeys REGEX\t\t\t\t\t\tget the number of keys matched with regex from priskv\n"},
    {"flush", flush_handler, "flush REGEX\t\t\t\t\t\tflush keys matched with regex from priskv\n"},
    {"capacity", capacity_handler, "capacity\t\t\t\t\t\tget capacity in bytes from priskv\n"},
    {"exit", exit_handler, "exit\t\t\t\t\t\texit the client\n"}};

static void print_cmd_help()
{
    printf("Usage:\n");
    for (int i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        printf("    %s", commands[i].help);
    }
}

static void completion(const char *buf, linenoiseCompletions *lc)
{
    int i;
    size_t buflen = strlen(buf);

    if (!buflen) {
        return;
    }

    for (i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        if (buflen == strlen(commands[i].name)) {
            continue;
        }

        if (!strncmp(commands[i].name, buf, buflen)) {
            linenoiseAddCompletion(lc, commands[i].name);
        }
    }
}

static const char *hints(const char *buf, int *color, int *bold)
{
    int i;
    size_t buflen = strlen(buf);

    if (!buflen) {
        return NULL;
    }

    *color = 33; /* Yellow */
    *bold = 0;

    for (i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
        if (buflen == strlen(commands[i].name)) {
            continue;
        }

        if (!strncmp(commands[i].name, buf, buflen)) {
            return commands[i].name + buflen;
        }
    }

    return NULL;
}

static void str_replace_char(char *str, char old, char new)
{
    char *c = str;

    while ((c = strchr(c, old)) != NULL) {
        *c = new;
        c++;
    }
}

static void priskv_client_log_fn(priskv_log_level level, const char *msg)
{
    priskv_logger_printf(g_logger, level, "%s", msg);
}

int main(int argc, char *argv[])
{
    char *line, *tmpline, *cmd, *args;
    client_context ctx;
    int i;
    int unknown = 1;

    priskv_show_logo();
    priskv_show_license();

    priskv_parsr_arg(argc, argv);
    priskv_set_log_level(log_level);
    if (log_file) {
        g_logger = priskv_logger_new(log_file);
        if (!g_logger) {
            printf("Failed to open logger %s, exit...\n", log_file);
            return -1;
        }
        priskv_set_log_fn(priskv_client_log_fn);
    }

    if (!raddr) {
        priskv_showhelp();
    }

    ctx.client = priskv_connect(raddr, rport, laddr, lport, 0);
    if (!ctx.client) {
        printf("Failed to connect, exit ... \n");
        return -1;
    }

    ctx.buf = mmap(NULL, VALUE_MAX_LEN, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (!ctx.buf) {
        printf("Failed to mmap, exit ... \n");
        return -1;
    }

    ctx.priskvmem =
        priskv_reg_memory(ctx.client, (uint64_t)ctx.buf, VALUE_MAX_LEN, (uint64_t)ctx.buf, -1);
    if (!ctx.priskvmem) {
        printf("Failed to register memory, exit ... \n");
        return -1;
    }

    linenoiseSetCompletionCallback(completion);
    linenoiseSetHintsCallback(hints);

    while (!g_exit) {
        line = linenoise("PrisKV> ");
        if (line == NULL) {
            break;
        }

        str_replace_char(line, '\t', ' ');
        tmpline = strdup(line);
        cmd = strtok_r(tmpline, " ", &args);

        if (cmd) {
            unknown = 1;
            for (i = 0; i < sizeof(commands) / sizeof(commands[0]); i++) {
                if (!strcmp(cmd, commands[i].name)) {
                    commands[i].handler(&ctx, args);
                    linenoiseHistoryAdd(line);
                    unknown = 0;
                    break;
                }
            }
            if (unknown) {
                printf("Unknown command: %s, please enter 'help' to get usage\n", cmd);
            }
        }

        free(tmpline);
        free(line);
    }

    priskv_dereg_memory(ctx.priskvmem);
    priskv_close(ctx.client);
    priskv_logger_free(g_logger);

    return 0;
}
