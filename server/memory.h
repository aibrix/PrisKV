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

#ifndef __PRISKV_SERVER_MEMORY__
#define __PRISKV_SERVER_MEMORY__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdint.h>
#include <time.h>

#include "list.h"

typedef struct priskv_key {
    void *kv;
    struct list_node entry;
    struct list_node lru_entry;
    struct timeval expire_time;
    pthread_spinlock_t lock;
    uint32_t refcnt;
    bool inprocess;
    bool reserved[2];
    uint16_t keylen;
    uint32_t valuelen;
    uint64_t value_off; /* offset from value blocks. [0, blocks * block size) */
    uint8_t key[0];     /* pointer to a slab element */
} priskv_key;

#define PRISKV_MEM_MAGIC ('H' << 24 | 'P' << 16 | 'K' << 8 | 'V')
#define PRISKV_MEM_ALIGN_UP 4096
#define PRISKV_MEM_HEADER_SIZE 4096

typedef struct priskv_mem_header {
    uint32_t magic;
    uint16_t reserved4;
    uint16_t max_key_length;
    uint32_t max_keys;
    uint32_t value_block_size;
    uint64_t value_blocks;
    uint64_t feature0;
    uint8_t reserved[PRISKV_MEM_HEADER_SIZE - 32];
} priskv_mem_header;

typedef struct priskv_mem_info {
    const char *type;

    union {
        /* for memory mapped mapping */
        struct {
            const char *path;
            uint64_t filesize;
            uint64_t pagesize;
            uint64_t feature0;
        };

        /* for anonymous mapping */
        struct {};
    };
} priskv_mem_info;

/*
 * +--------+-------------+----------------+
 * | header | keys (slab) | values (buddy) |
 * +--------+-------------+----------------+
 */

static inline uint16_t priskv_mem_key_size(uint16_t max_key_length)
{
    return sizeof(priskv_key) + max_key_length;
}

int priskv_mem_create(const char *path, uint16_t max_key_length, uint32_t max_keys,
                    uint32_t value_block_size, uint64_t value_blocks, uint8_t nthreads);

void *priskv_mem_load(const char *path);

void *priskv_mem_anon(uint16_t max_key_length, uint32_t max_keys, uint32_t value_block_size,
                    uint64_t value_blocks, uint8_t threads);

void priskv_mem_close(void *ctx);

void *priskv_mem_malloc(size_t size, bool guard);

void priskv_mem_free(void *ptr, size_t size, bool guard);

uint8_t *priskv_mem_header_addr(void *ctx);

uint8_t *priskv_mem_key_addr(void *ctx);

uint8_t *priskv_mem_value_addr(void *ctx);

priskv_mem_info *priskv_mem_info_get(void);

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_SERVER_MEMORY__ */
