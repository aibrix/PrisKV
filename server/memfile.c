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

#include "priskv-utils.h"
#include "priskv-log.h"
#include "priskv-logo.h"

#include "transport/transport.h"
#include "memory.h"
#include "priskv-threads.h"

/* arguments of command line */
static uint32_t max_key_length = PRISKV_TRANSPORT_DEFAULT_KEY_LENGTH;
static uint32_t max_key = PRISKV_TRANSPORT_DEFAULT_KEY;
static uint32_t value_block_size = PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK_SIZE;
static uint64_t value_block = PRISKV_TRANSPORT_DEFAULT_VALUE_BLOCK;
static uint8_t threads = 1;
static priskv_log_level log_level = priskv_log_level_max;
static char *memfile;
static const char *operation = "info";

static void priskv_showhelp(void)
{
    printf("\nUsage:\n");
    printf("  -o/--op OPERATION\n\tsupport operations: create|info[default]\n");
    printf("  -f/--memfile PATH\n\tmemory file from tmpfs/hugetlbfs\n");
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
    printf("  -t/--threads THREADS\n\tthe number of worker threads to clean memory, default 0\n");
    printf("  -l/--log-level LEVEL\n\terror, warn, notice[default], info or debug\n");

    exit(0);
}

static const char *priskv_short_opts = "o:f:K:k:v:b:t:l:h";
static struct option priskv_long_opts[] = {
    {"op", required_argument, 0, 'o'},
    {"memfile", required_argument, 0, 'f'},
    {"max-keys", required_argument, 0, 'k'},
    {"max-key-length", required_argument, 0, 'K'},
    {"value-block-size", required_argument, 0, 'v'},
    {"value-blocks", required_argument, 0, 'b'},
    {"threads", required_argument, 0, 't'},
    {"log-level", required_argument, 0, 'l'},
    {"help", no_argument, 0, 'h'},
};

static void priskv_memfile_info(const char *file)
{
    void *mf_ctx;

    mf_ctx = priskv_mem_load(file);
    if (!mf_ctx) {
        printf("Failed to load memory file. -l/--log-level debug for more information\n");
        return;
    }

    priskv_mem_header *hdr = (priskv_mem_header *)priskv_mem_header_addr(mf_ctx);
    assert(hdr);

    printf("File name: %s\n", file);
    printf("\rMagic: 0x%x\n", hdr->magic);
    printf("\rFeature: 0x%lx\n", hdr->feature0);
    printf("\rMax key length: %d\n", hdr->max_key_length);
    printf("\rMax keys: %d\n", hdr->max_keys);
    printf("\rValue block size: %d\n", hdr->value_block_size);
    printf("\rValue blocks: %ld\n", hdr->value_blocks);

    uint16_t keysize = priskv_mem_key_size(hdr->max_key_length);
    uint8_t *key_base = priskv_mem_key_addr(mf_ctx);
    uint8_t *safekey = malloc(hdr->max_key_length + 1);

    for (uint32_t i = 0; i < hdr->max_keys; i++) {
        priskv_key *keynode = (priskv_key *)(key_base + keysize * i);

        if (!keynode->keylen) {
            continue;
        }

        if (priskv_mem_key_size(keynode->keylen) > hdr->max_key_length) {
            priskv_log_error("MEMFILE: memory file corrupted. keylen %d, exceed %d\n",
                           keynode->keylen, hdr->max_key_length);
            exit(-1);
        }

        memcpy(safekey, keynode->key, keynode->keylen);
        safekey[keynode->keylen] = '\0';

        if (keynode->inprocess) {
            priskv_log_notice("MEMFILE: key [%s] (%d bytes) corrupted\n", keynode->key,
                            keynode->keylen);
            continue;
        }

        priskv_log_info("MEMFILE: key [%s] (%d bytes) with value %ld bytes\n", keynode->key,
                      keynode->keylen, keynode->valuelen);
    }

    free(safekey);
}

static void priskv_memfile_create(char *file)
{
    int ret;

    ret = priskv_mem_create(memfile, max_key_length, max_key, value_block_size, value_block, threads);
    if (ret) {
        printf("Failed to create memory file. -l/--log-level debug for more information\n");
    } else {
        printf("Done\n");
    }
}

static void priskv_parsr_arg(int argc, char *argv[])
{
    int args, ch;
    int64_t key_length = 0, block_size = 0;

    while (1) {
        ch = getopt_long(argc, argv, priskv_short_opts, priskv_long_opts, &args);
        if (ch == -1) {
            break;
        }

        switch (ch) {
        case 'o':
            operation = optarg;
            break;

        case 'f':
            memfile = optarg;
            break;

        case 'k':
            max_key = atoi(optarg);
            if (!max_key || (max_key > PRISKV_TRANSPORT_MAX_KEY)) {
                printf("Invalid -k/--max-keys\n");
                priskv_showhelp();
            }
            break;

        case 'K':
            if (priskv_str2num(optarg, &key_length) < 0 || !key_length ||
                key_length > PRISKV_TRANSPORT_MAX_KEY_LENGTH) {
                printf("Invalid -K/--max-key-length\n");
                priskv_showhelp();
            }
            max_key_length = (uint32_t)key_length;
            break;

        case 'v':
            if (priskv_str2num(optarg, &block_size) < 0 || !block_size ||
                block_size > PRISKV_TRANSPORT_MAX_VALUE_BLOCK_SIZE) {
                printf("Invalid -v/--value-block-size\n");
                priskv_showhelp();
            }

            value_block_size = (uint32_t)block_size;
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

        case 'h':
        default:
            priskv_showhelp();
        }
    }

    if (!memfile) {
        printf("Missing memory file\n");
        priskv_showhelp();
    }
}

int main(int argc, char *argv[])
{
    priskv_show_logo();
    priskv_show_license();

    priskv_parsr_arg(argc, argv);
    priskv_set_log_level(log_level);

    if (!strcmp(operation, "info")) {
        priskv_memfile_info(memfile);
    } else if (!strcmp(operation, "create")) {
        priskv_memfile_create(memfile);
    }

    return 0;
}
