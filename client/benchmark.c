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
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/mman.h>
#include <errno.h>
#include <stdlib.h>
#include <getopt.h>
#include <time.h>
#include <sys/time.h>
#include <inttypes.h>
#include <stddef.h>
#include <sys/eventfd.h>
#include <sys/timerfd.h>
#include <signal.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>
#include <ncurses.h>
#include <sys/ioctl.h>

#include "priskv.h"
#include "priskv-log.h"
#include "priskv-logo.h"
#include "priskv-utils.h"
#include "priskv-event.h"
#include "priskv-threads.h"
#include "priskv-protocol-helper.h"

#define STRING_MAX_LEN 128

#define DEFAULT_MIN_KEY_LEN 1
#define DEFAULT_MAX_KEY_LEN 128
#define DEFAULT_ALIGN_VALUE_LEN 4096UL                               /* 4KB */
#define DEFAULT_MIN_VALUE_LEN DEFAULT_ALIGN_VALUE_LEN                /* 4KB */
#define DEFAULT_MAX_VALUE_LEN (DEFAULT_ALIGN_VALUE_LEN * 1024 * 256) /* 1GB */

static int g_scroll;

static atomic_uint g_exiting;

static char *raddr;
static int rport = -1;
static char *laddr;
static int lport;
static int g_iodepth = 1;
static uint64_t g_runtime = 0;
static uint64_t g_interval = 1; /* secend */
static uint8_t g_threads = 1;
static const char *g_driver = "priskv";
static uint32_t g_priskv_value_block_size = 0;
static const char *g_kvm = "share";
static int g_device_id = 0;
static uint16_t g_max_sgl = 1;
static bool g_transfer = false;
static bool g_temp_reg = false;

static int64_t g_key_min_len = DEFAULT_MIN_KEY_LEN;
static int64_t g_key_max_len = DEFAULT_MAX_KEY_LEN;
static int64_t g_value_min_len = DEFAULT_MIN_VALUE_LEN;
static int64_t g_value_max_len = DEFAULT_MAX_VALUE_LEN;
static uint32_t g_value_aligned_len = DEFAULT_ALIGN_VALUE_LEN;
static uint64_t g_total_value_size = 0;

static int g_rows, g_cols;

static priskv_log_level g_log_level = priskv_log_level_max;
static const char *g_log_file = NULL;
static priskv_logger *g_logger;

static priskv_threadpool *g_threadpool;
static int g_nqueue = 0;

typedef struct job_context job_context;
typedef struct job_sem job_sem;

static void job_fill(job_context *job);
static void job_process(int fd, void *opaque, uint32_t events);
static void job_sig_clear(int fd, void *opaque, uint32_t events);

typedef struct {
    const char *name;
    void *(*alloc)(size_t size);
    void (*free)(void *ptr);
    void (*memset)(void *ptr, int value, size_t size);
    void (*memcpy)(void *dst, const void *src, size_t size);
} memory_manager;

typedef struct {
    const char *name;
    void (*init)(job_context *job, void **ctx);
    void (*deinit)(job_context *job, void *ctx);
    void (*alloc_kv)(void *ctx, char **key, void **value, uint32_t *vlen);
    void (*free_kv)(void *ctx, char *key, void *value);
    int (*alloc_done)(void *ctx);
    void (*reset)(void *ctx);
    int (*verify)(void *ctx, job_context *job);
    void *(*get_mem_pool)(void *ctx, uint64_t *len);
} kv_manager;

typedef struct {
    const char *name;
    bool transfer;
    int (*init)(job_context *job, void **ctx);
    void (*deinit)(job_context *job, void *ctx);
    int (*get_fd)(void *ctx);
    void (*handler)(int fd, void *opaque, uint32_t events);
    const char *(*status_str)(int status);
    bool (*is_error)(int status);
    void (*get)(void *ctx, const char *key, void *value, uint32_t value_len,
                void (*cb)(int, void *), void *cbarg);
    void (*set)(void *ctx, const char *key, void *value, uint32_t value_len,
                void (*cb)(int, void *), void *cbarg);
    void (*del)(void *ctx, const char *key, void (*cb)(int, void *), void *cbarg);
    void (*test)(void *ctx, const char *key, void (*cb)(int, void *), void *cbarg);
} kv_driver;

typedef int (*action_handler)(job_context *job);

typedef enum {
    JOB_STATUS_INIT = 0,
    JOB_STATUS_SETTING,
    JOB_STATUS_SET_ENDING,
    JOB_STATUS_GETTING,
    JOB_STATUS_GET_ENDING,
    JOB_STATUS_VERIFYING,
    JOB_STATUS_CLEARING,
    JOB_STATUS_CLEAR_ENDING,
    JOB_STATUS_FINISHED,
} job_status;

const char *job_status_str[] = {
    [JOB_STATUS_INIT] = "start to test",
    [JOB_STATUS_SETTING] = "start to test SETTING",
    [JOB_STATUS_SET_ENDING] = "wait for all SET requests to complete",
    [JOB_STATUS_GETTING] = "start to test GETTING",
    [JOB_STATUS_GET_ENDING] = "wait for all GET requests to complete",
    [JOB_STATUS_VERIFYING] = "start to VERIFY",
    [JOB_STATUS_CLEARING] = "start to CLEAR",
    [JOB_STATUS_CLEAR_ENDING] = "wait for CLEAR to complete",
    [JOB_STATUS_FINISHED] = "finish to test",
};

typedef enum {
    JOB_STATE_INIT = 0,
    JOB_STATE_RUNNING,
    JOB_STATE_ERROR,
    JOB_STATE_EXITING,
    JOB_STATE_EXIT,
    JOB_STATE_EXITED,
} job_state;

typedef struct {
    job_state state;
    double qps;
    double latency;
    char msg[STRING_MAX_LEN];
} job_info;

const char *job_state_str[] = {
    [JOB_STATE_INIT] = "init",       [JOB_STATE_RUNNING] = "running", [JOB_STATE_ERROR] = "error",
    [JOB_STATE_EXITING] = "exiting", [JOB_STATE_EXIT] = "exit",       [JOB_STATE_EXITED] = "exited",
};

struct job_context {
    uint8_t threadid;
    int epollfd;
    int eventfd;
    int timerfd;
    uint64_t req_count;
    uint64_t last_req_count;
    int inflight;

    job_state state;
    job_status status;

    void *kvm_ctx;
    void *drv_ctx;

    const char *op_name;
    action_handler op_action;

    const memory_manager *mm;
    const kv_manager *kvm;
    const kv_driver *kv_drv;

    uint64_t first_ns;
    uint64_t last_ns;
    uint64_t interval_ns;
    uint64_t print_count;

    job_info info;
    pthread_spinlock_t lock;
    char err_msg[STRING_MAX_LEN];
};

struct job_sem {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    uint64_t count;
};

#ifdef PRISKV_USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>

static bool gpu_device_is_available(int cudaDev)
{
    return cudaSetDevice(cudaDev) == cudaSuccess;
}

static bool gdr_is_support(void)
{
    CUdevice currentDev;
    int cudaDev;
    int support_gdr = 0;

    cudaError_t err;

    err = cudaGetDevice(&cudaDev);
    assert(err == cudaSuccess);
    err = cuDeviceGet(&currentDev, cudaDev);
    assert(err == cudaSuccess);
    err = cuDeviceGetAttribute(&support_gdr, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_SUPPORTED,
                               currentDev);
    assert(err == cudaSuccess);

    priskv_log_debug("Cuda(%d) support GDR: %s\n", (int)currentDev, support_gdr ? "yes" : "no");

    return support_gdr;
}
#else
static bool gpu_device_is_available(int cudaDev)
{
    priskv_log_error("Please rebuild in Cuda environment with PRISKV_USE_CUDA=1\n");
    return false;
}

static bool gdr_is_support(void)
{
    priskv_log_error("Please rebuild in Cuda environment with PRISKV_USE_CUDA=1\n");
    return false;
}
#endif

#ifdef PRISKV_USE_ACL
#include <acl/acl.h>

static bool npu_device_is_available(int device_id)
{
    return aclrtSetDevice(device_id) == ACL_SUCCESS;
}
#else
static bool npu_device_is_available(int cudaDev)
{
    priskv_log_error("Please rebuild in Ascend environment with PRISKV_USE_ACL=1\n");
    return false;
}
#endif

static void *cmem_alloc(size_t size)
{
    return malloc(size);
}
static void cmem_free(void *ptr)
{
    free(ptr);
}
static void cmem_memset(void *ptr, int value, size_t count)
{
    memset(ptr, value, count);
}
static void cmem_memcpy(void *dst, const void *src, size_t size)
{
    memcpy(dst, src, size);
}

static const memory_manager cpu_mm = {
    .name = "cpu",
    .alloc = cmem_alloc,
    .free = cmem_free,
    .memset = cmem_memset,
    .memcpy = cmem_memcpy,
};

#ifdef PRISKV_USE_CUDA
static void *gmem_alloc(size_t size)
{
    void *ptr;
    cudaError_t err;

    err = cudaMalloc(&ptr, size);
    if (err != cudaSuccess) {
        return NULL;
    }

    return ptr;
}
static void gmem_free(void *ptr)
{
    assert(cudaFree(ptr) == cudaSuccess);
}
static void gmem_memset(void *ptr, int value, size_t count)
{
    assert(cudaMemset(ptr, value, count) == cudaSuccess);
}
static void gmem_memcpy(void *dst, const void *src, size_t size)
{
    assert(cudaMemcpy(dst, src, size, cudaMemcpyDefault) == cudaSuccess);
}

static const memory_manager gpu_mm = {
    .name = "gpu",
    .alloc = gmem_alloc,
    .free = gmem_free,
    .memset = gmem_memset,
    .memcpy = gmem_memcpy,
};
#else
static const memory_manager gpu_mm;
#endif

#ifdef PRISKV_USE_ACL
static void *npu_mem_alloc(size_t size)
{
    void *ptr;
    aclError err;

    err = aclrtMalloc(&ptr, size, ACL_MEM_MALLOC_HUGE_FIRST);
    if (err != ACL_SUCCESS) {
        return NULL;
    }

    return ptr;
}
static void npu_mem_free(void *ptr)
{
    assert(aclrtFree(ptr) == ACL_SUCCESS);
}
static void npu_mem_memset(void *ptr, int value, size_t count)
{
    assert(aclrtMemset(ptr, count, value, count) == ACL_SUCCESS);
}
static void npu_mem_memcpy(void *dst, const void *src, size_t size)
{
    assert(aclrtMemcpy(dst, size, src, size, 0) == ACL_SUCCESS);
}

static const memory_manager npu_mm = {
    .name = "npu",
    .alloc = npu_mem_alloc,
    .free = npu_mem_free,
    .memset = npu_mem_memset,
    .memcpy = npu_mem_memcpy,
};
#else
static const memory_manager npu_mm;
#endif

typedef enum {
    PRISKV_MEM_TYPE_CPU = 0,
    PRISKV_MEM_TYPE_GPU,
    PRISKV_MEM_TYPE_NPU,
    PRISKV_MEM_TYPE_MAX,
} priskv_mem_type;

typedef struct {
    const char *name;
    const memory_manager *mm;
} priskv_mem;

priskv_mem_type g_mem_type = PRISKV_MEM_TYPE_CPU;
const priskv_mem mm_list[] = {[PRISKV_MEM_TYPE_CPU] = {"cpu", &cpu_mm},
                            [PRISKV_MEM_TYPE_GPU] = {"gpu", &gpu_mm},
                            [PRISKV_MEM_TYPE_NPU] = {"npu", &npu_mm},
                            [PRISKV_MEM_TYPE_MAX] = {"unknown", NULL}};
static priskv_mem_type priskv_get_mem_type(const char *mem_type_str)
{
    int i;
    for (i = 0; i < PRISKV_MEM_TYPE_MAX; i++) {
        if (!strcmp(mem_type_str, mm_list[i].name)) {
            return i;
        }
    }
    return PRISKV_MEM_TYPE_MAX;
}

static const char *priskv_get_mem_type_str(priskv_mem_type mem_type)
{
    if (mem_type < 0 || mem_type > PRISKV_MEM_TYPE_MAX) {
        mem_type = PRISKV_MEM_TYPE_MAX;
    }
    return mm_list[mem_type].name;
}

static const memory_manager *priskv_get_mm(priskv_mem_type mem_type)
{
    if (mem_type < 0 || mem_type > PRISKV_MEM_TYPE_MAX) {
        mem_type = PRISKV_MEM_TYPE_MAX;
    }

    return mm_list[mem_type].mm;
}

typedef struct {
    char *key;
    void *value;
    uint32_t value_len;
    int done;
} share_context;

static void share_init(job_context *job, void **ctx)
{
    share_context **share_ctx = (share_context **)ctx;
    size_t key_len;

    *share_ctx = malloc(sizeof(share_context));

    if (g_key_max_len == g_key_min_len) {
        key_len = g_key_max_len;
    } else {
        key_len = rand() % (g_key_max_len - g_key_min_len) + g_key_min_len;
    }
    (*share_ctx)->key = malloc(key_len + 1);
    assert((*share_ctx)->key);

    /* key: "AAAAA..." */
    memset((*share_ctx)->key, 'A', key_len);
    ((*share_ctx)->key)[key_len] = '\0';

    if (g_value_max_len == g_value_min_len) {
        (*share_ctx)->value_len = g_value_max_len;
    } else {
        (*share_ctx)->value_len = rand() % (g_value_max_len - g_value_min_len) + g_value_min_len;
    }
    (*share_ctx)->value_len = ALIGN_DOWN((*share_ctx)->value_len, g_value_aligned_len);
    if ((*share_ctx)->value_len == 0) {
        (*share_ctx)->value_len = g_value_aligned_len;
    }
    (*share_ctx)->value = job->mm->alloc((*share_ctx)->value_len);
    assert((*share_ctx)->value);

    /* value: "aaaaaaaaaaaaaa..." */
    job->mm->memset((*share_ctx)->value, 0, (*share_ctx)->value_len);
    job->mm->memset((*share_ctx)->value, 'a', (*share_ctx)->value_len - 1);

    (*share_ctx)->done = 0;
}

static void share_deinit(job_context *job, void *ctx)
{
    share_context *share_ctx = ctx;
    free(share_ctx->key);
    job->mm->free(share_ctx->value);
    free(share_ctx);
}

static void share_alloc_kv(void *ctx, char **key, void **value, uint32_t *vlen)
{
    share_context *share_ctx = ctx;

    *key = share_ctx->key;

    *vlen = share_ctx->value_len;
    *value = share_ctx->value;

    share_ctx->done = 1;
}

static void share_free_kv(void *ctx, char *key, void *value)
{
}

static int share_alloc_done(void *ctx)
{
    share_context *share_ctx = ctx;
    return share_ctx->done;
}

static void share_reset(void *ctx)
{
    share_context *share_ctx = ctx;
    share_ctx->done = 0;
}

static int share_verify(void *ctx, job_context *job)
{
    share_context *share_ctx = ctx;
    void *tmp1 = malloc(share_ctx->value_len);
    void *tmp2 = malloc(share_ctx->value_len);
    int ret;

    memset(tmp1, 0, share_ctx->value_len);
    memset(tmp1, 'a', share_ctx->value_len - 1);
    job->mm->memcpy(tmp2, share_ctx->value, share_ctx->value_len);

    ret = memcmp(tmp1, tmp2, share_ctx->value_len);

    if (ret) {
        printf("verify failed!\n");
        printf("key: %s\n", share_ctx->key);
        printf("remote value[%u]: %s\n", share_ctx->value_len, (char *)tmp2);
        printf("local value[%u]: %s\n", share_ctx->value_len, (char *)tmp1);
    }

    free(tmp1);
    free(tmp2);

    return ret;
}

static void *share_get_mem_pool(void *ctx, uint64_t *len)
{
    share_context *share_ctx = ctx;
    *len = share_ctx->value_len;

    return share_ctx->value;
}

static const kv_manager share_kvm = {
    .name = "share",
    .init = share_init,
    .deinit = share_deinit,
    .alloc_kv = share_alloc_kv,
    .free_kv = share_free_kv,
    .alloc_done = share_alloc_done,
    .reset = share_reset,
    .verify = share_verify,
    .get_mem_pool = share_get_mem_pool,
};

typedef struct {
    char *key;
    uint32_t value_off;
    uint32_t value_len;
} random_kv_meta;

typedef struct {
    void *data;
    void *ptr;
    random_kv_meta *meta;
    int count;

    int alloced_count;
    int alloc_done;

    uint32_t key_min_len;
    uint32_t key_max_len;

    uint32_t value_min_len;
    uint32_t value_max_len;
    uint32_t value_aligned_len;
    uint64_t total_value_size;
    uint64_t used_value_size;
} random_context;

static char *random_gen_key(random_context *ctx)
{
    char *key;
    size_t key_len;

    if (ctx->key_max_len == ctx->key_min_len) {
        key_len = ctx->key_max_len;
    } else {
        key_len = rand() % (ctx->key_max_len - ctx->key_min_len) + ctx->key_min_len;
    }

    key = malloc(key_len + 1);

    priskv_uuid((uint8_t *)key, key_len + 1);

    return key;
}

static int random_gen_value(random_context *ctx, uint64_t *value_off, uint32_t *value_len)
{
    if (ctx->used_value_size > ctx->total_value_size - ctx->value_min_len) {
        return -1;
    }

    if (ctx->value_max_len == ctx->value_min_len) {
        *value_len = ctx->value_max_len;
    } else {
        *value_len = rand() % (ctx->value_max_len - ctx->value_min_len) + ctx->value_min_len;
    }
    if (*value_len > ctx->total_value_size - ctx->used_value_size) {
        *value_len = ctx->total_value_size - ctx->used_value_size;
    }
    *value_len = ALIGN_DOWN(*value_len, ctx->value_aligned_len);
    if (*value_len == 0) {
        *value_len = ctx->value_aligned_len;
    }

    *value_off = ctx->used_value_size;

    priskv_random_string(ctx->data + *value_off, *value_len);

    ctx->used_value_size += *value_len;

    return 0;
}

static void random_add_kv(random_context *ctx, char *key, uint64_t value_off, uint32_t value_len)
{
    ctx->count++;
    ctx->meta = realloc(ctx->meta, sizeof(random_kv_meta) * ctx->count);
    ctx->meta[ctx->count - 1].key = key;
    ctx->meta[ctx->count - 1].value_off = value_off;
    ctx->meta[ctx->count - 1].value_len = value_len;
}

static void random_print_kv(random_context *ctx)
{
    int i;
    int short_len = 32;
    char value_short[short_len];

    for (i = 0; i < ctx->count; i++) {
        priskv_string_shorten((char *)(ctx->data + ctx->meta[i].value_off), ctx->meta[i].value_len,
                            value_short, short_len);
        printf("(%d) key: %s\n", i, ctx->meta[i].key);
        printf("(%d) value[%u]: %s\n", i, ctx->meta[i].value_len, value_short);
    }
}

static void random_init(job_context *job, void **ctx)
{
    random_context **random_ctx = (random_context **)ctx;
    uint64_t value_off = 0;
    uint32_t value_len = 0;
    char *key = NULL;

    *random_ctx = malloc(sizeof(random_context));

    memset(*random_ctx, 0, sizeof(random_context));

    (*random_ctx)->key_min_len = g_key_min_len;
    (*random_ctx)->key_max_len = g_key_max_len;
    (*random_ctx)->value_min_len = g_value_min_len;
    (*random_ctx)->value_max_len = g_value_max_len;
    (*random_ctx)->total_value_size = g_total_value_size;
    (*random_ctx)->value_aligned_len = g_value_aligned_len;

    (*random_ctx)->data = malloc((*random_ctx)->total_value_size);

    while (random_gen_value(*random_ctx, &value_off, &value_len) == 0) {
        key = random_gen_key(*random_ctx);
        random_add_kv(*random_ctx, key, value_off, value_len);
    }

    if (g_log_level == priskv_log_debug) {
        random_print_kv(*random_ctx);
    }

    (*random_ctx)->ptr = job->mm->alloc((*random_ctx)->total_value_size);
    job->mm->memset((*random_ctx)->ptr, 0, (*random_ctx)->total_value_size);
    job->mm->memcpy((*random_ctx)->ptr, (*random_ctx)->data, (*random_ctx)->total_value_size);
}

static void random_deinit(job_context *job, void *ctx)
{
    random_context *random_ctx = ctx;
    int i;

    for (i = 0; i < random_ctx->count; i++) {
        free(random_ctx->meta[i].key);
    }
    free(random_ctx->meta);

    free(random_ctx->data);
    job->mm->free(random_ctx->ptr);

    free(random_ctx);
}

static void random_alloc_kv(void *ctx, char **key, void **value, uint32_t *vlen)
{
    random_context *random_ctx = ctx;

    *key = random_ctx->meta[random_ctx->alloced_count].key;
    *value = random_ctx->ptr + random_ctx->meta[random_ctx->alloced_count].value_off;
    *vlen = random_ctx->meta[random_ctx->alloced_count].value_len;

    random_ctx->alloced_count++;
    if (random_ctx->alloced_count == random_ctx->count) {
        random_ctx->alloc_done = 1;
        random_ctx->alloced_count = 0;
    } else {
        random_ctx->alloc_done = 0;
    }
}

static void random_free_kv(void *ctx, char *key, void *value)
{
}

static int random_alloc_done(void *ctx)
{
    random_context *random_ctx = ctx;
    return random_ctx->alloc_done;
}

static void random_reset(void *ctx)
{
    random_context *random_ctx = ctx;
    random_ctx->alloc_done = 0;
    random_ctx->alloced_count = 0;
}

static int random_verify(void *ctx, job_context *job)
{
    random_context *random_ctx = ctx;
    uint32_t value_off, value_len;
    int ret = 0;
    int i;
    void *tmp = malloc(random_ctx->total_value_size);

    job->mm->memcpy(tmp, random_ctx->ptr, random_ctx->total_value_size);

    for (i = 0; i < random_ctx->count; i++) {
        value_off = random_ctx->meta[i].value_off;
        value_len = random_ctx->meta[i].value_len;
        ret = memcmp(tmp + value_off, random_ctx->data + value_off, value_len);
        if (ret) {
            printf("verify failed!\n");
            printf("(%d) key: %s\n", i, random_ctx->meta[i].key);
            printf("remote value[%u]: %s\n", value_len, (char *)tmp + value_off);
            printf("local value[%u]: %s\n", value_len, (char *)random_ctx->data + value_off);
            break;
        }
    }

    free(tmp);

    return ret;
}

static void *random_get_mem_pool(void *ctx, uint64_t *len)
{
    random_context *random_ctx = ctx;

    *len = random_ctx->total_value_size;
    return random_ctx->ptr;
}

static const kv_manager random_kvm = {
    .name = "random",
    .init = random_init,
    .deinit = random_deinit,
    .alloc_kv = random_alloc_kv,
    .free_kv = random_free_kv,
    .alloc_done = random_alloc_done,
    .reset = random_reset,
    .verify = random_verify,
    .get_mem_pool = random_get_mem_pool,
};

const kv_manager *kvms[] = {&share_kvm, &random_kvm};

static const kv_manager *get_kvm(const char *name)
{
    int i;
    int count = sizeof(kvms) / sizeof(kv_manager *);

    for (i = 0; i < count; i++) {
        if (!strcmp(name, kvms[i]->name)) {
            return kvms[i];
        }
    }

    return NULL;
}

typedef struct {
    job_context *job;
    priskv_client *client;
    priskv_memory *priskvmem;
} priskv_context;

static int priskv_drv_init(job_context *job, void **ctx)
{
    priskv_context **priskv_ctx = (priskv_context **)ctx;
    void *mem_pool;
    uint64_t len = 0;

    *priskv_ctx = malloc(sizeof(priskv_context));

    (*priskv_ctx)->job = job;

    (*priskv_ctx)->client = priskv_connect(raddr, rport, laddr, lport, g_nqueue);
    if (!(*priskv_ctx)->client) {
        printf("Failed to connect, exit ... \n");
        return -1;
    }

    mem_pool = job->kvm->get_mem_pool(job->kvm_ctx, &len);

    if (!g_transfer && !g_temp_reg) {
        (*priskv_ctx)->priskvmem =
            priskv_reg_memory((*priskv_ctx)->client, (uint64_t)mem_pool, len, (uint64_t)mem_pool, -1);
        if ((*priskv_ctx)->priskvmem == NULL) {
            return -1;
        }
    } else {
        (*priskv_ctx)->priskvmem = NULL;
    }

    return 0;
}

static void priskv_drv_deinit(job_context *job, void *ctx)
{
    priskv_context *priskv_ctx = ctx;

    priskv_close(priskv_ctx->client);
    if (!g_transfer && !g_temp_reg) {
        priskv_dereg_memory(priskv_ctx->priskvmem);
    }
    free(priskv_ctx);
}

static int priskv_drv_get_fd(void *ctx)
{
    priskv_context *priskv_ctx = ctx;
    return priskv_get_fd(priskv_ctx->client);
}

static void priskv_drv_handler(int fd, void *opaque, uint32_t events)
{
    priskv_context *priskv_ctx = opaque;
    priskv_process(priskv_ctx->client, EPOLLIN);
}

static const char *priskv_drv_status_str(int status)
{
    return priskv_status_str(status);
}

typedef struct {
    void (*cb)(int, void *);
    void *cbarg;
    int ignore_no_such_key;
} priskv_req_context;

static void priskv_req_cb(uint64_t request_id, priskv_status status, void *result)
{
    priskv_req_context *ctx = (priskv_req_context *)request_id;
    int ret = 0;

    if (status != PRISKV_STATUS_OK) {
        if (!ctx->ignore_no_such_key || status != PRISKV_STATUS_NO_SUCH_KEY) {
            ret = status;
        }
    }

    ctx->cb(ret, ctx->cbarg);
    free(ctx);
}

static void priskv_drv_get(void *ctx, const char *key, void *value, uint32_t value_len,
                         void (*cb)(int, void *), void *cbarg)
{
    priskv_context *priskv_ctx = ctx;
    priskv_sgl sgl;
    priskv_sgl *sgls;
    priskv_req_context *priskv_req_ctx;
    uint16_t nsgl;

    if (g_priskv_value_block_size && g_priskv_value_block_size < value_len) {
        nsgl = DIV_ROUND_UP(value_len, g_priskv_value_block_size);
        sgls = calloc(nsgl, sizeof(priskv_sgl));
        uint64_t offset = 0;
        for (int i = 0; i < nsgl; i++) {
            sgls[i].iova = (uint64_t)value + offset;
            sgls[i].length = priskv_min_u32(g_priskv_value_block_size, value_len - offset);
            sgls[i].mem = priskv_ctx->priskvmem;
            offset += g_priskv_value_block_size;
        }
    } else {
        nsgl = 1;
        sgl.iova = (uint64_t)value;
        sgl.length = value_len;
        sgl.mem = priskv_ctx->priskvmem;
        sgls = &sgl;
    }

    priskv_req_ctx = malloc(sizeof(priskv_req_context));
    priskv_req_ctx->cb = cb;
    priskv_req_ctx->cbarg = cbarg;
    priskv_req_ctx->ignore_no_such_key = 0;

    priskv_get_async(priskv_ctx->client, key, sgls, nsgl, (uint64_t)priskv_req_ctx, priskv_req_cb);

    if (g_priskv_value_block_size && g_priskv_value_block_size < value_len) {
        free(sgls);
    }
}

typedef struct {
    priskv_context *priskv_ctx;
    void (*cb)(int, void *);
    void *cbarg;
    void *transfer_value;
    void *value;
    uint32_t value_len;
} priskv_req_transfer_context;

static void priskv_get_transfer_cb(int status, void *arg)
{
    priskv_req_transfer_context *ctx = arg;
    job_context *job = ctx->priskv_ctx->job;

    job->mm->memcpy(ctx->value, ctx->transfer_value, ctx->value_len);

    ctx->cb(status, ctx->cbarg);

    free(ctx->transfer_value);
    free(ctx);
}

static void priskv_drv_get_transfer(void *ctx, const char *key, void *value, uint32_t value_len,
                                  void (*cb)(int, void *), void *cbarg)
{
    priskv_req_transfer_context *req_ctx = malloc(sizeof(priskv_req_transfer_context));
    priskv_context *priskv_ctx = ctx;

    req_ctx->priskv_ctx = priskv_ctx;
    req_ctx->cb = cb;
    req_ctx->cbarg = cbarg;
    req_ctx->transfer_value = malloc(value_len);
    req_ctx->value = value;
    req_ctx->value_len = value_len;

    priskv_drv_get(priskv_ctx, key, req_ctx->transfer_value, value_len, priskv_get_transfer_cb, req_ctx);
}

static void priskv_drv_set(void *ctx, const char *key, void *value, uint32_t value_len,
                         void (*cb)(int, void *), void *cbarg)
{
    priskv_context *priskv_ctx = ctx;
    priskv_sgl sgl;
    priskv_sgl *sgls;
    uint16_t nsgl;
    priskv_req_context *priskv_req_ctx;

    if (g_priskv_value_block_size && g_priskv_value_block_size < value_len) {
        nsgl = DIV_ROUND_UP(value_len, g_priskv_value_block_size);
        sgls = calloc(nsgl, sizeof(priskv_sgl));
        uint64_t offset = 0;
        for (int i = 0; i < nsgl; i++) {
            sgls[i].iova = (uint64_t)value + offset;
            sgls[i].length = priskv_min_u32(g_priskv_value_block_size, value_len - offset);
            sgls[i].mem = priskv_ctx->priskvmem;
            offset += g_priskv_value_block_size;
        }
    } else {
        nsgl = 1;
        sgl.iova = (uint64_t)value;
        sgl.length = value_len;
        sgl.mem = priskv_ctx->priskvmem;
        sgls = &sgl;
    }

    priskv_req_ctx = malloc(sizeof(priskv_req_context));
    priskv_req_ctx->cb = cb;
    priskv_req_ctx->cbarg = cbarg;
    priskv_req_ctx->ignore_no_such_key = 0;

    priskv_set_async(priskv_ctx->client, key, sgls, nsgl, PRISKV_KEY_MAX_TIMEOUT, (uint64_t)priskv_req_ctx,
                   priskv_req_cb);

    if (g_priskv_value_block_size && g_priskv_value_block_size < value_len) {
        free(sgls);
    }
}

static void priskv_set_transfer_cb(int status, void *arg)
{
    priskv_req_transfer_context *ctx = arg;

    ctx->cb(status, ctx->cbarg);

    free(ctx->transfer_value);
    free(ctx);
}

static void priskv_drv_set_transfer(void *ctx, const char *key, void *value, uint32_t value_len,
                                  void (*cb)(int, void *), void *cbarg)
{
    priskv_context *priskv_ctx = ctx;
    job_context *job = priskv_ctx->job;
    priskv_req_transfer_context *req_ctx = malloc(sizeof(priskv_req_transfer_context));

    req_ctx->priskv_ctx = ctx;
    req_ctx->cb = cb;
    req_ctx->cbarg = cbarg;
    req_ctx->transfer_value = malloc(value_len);
    req_ctx->value_len = value_len;

    job->mm->memcpy(req_ctx->transfer_value, value, value_len);

    priskv_drv_set(ctx, key, req_ctx->transfer_value, value_len, priskv_set_transfer_cb, req_ctx);
}

static void priskv_drv_del(void *ctx, const char *key, void (*cb)(int, void *), void *cbarg)
{
    priskv_context *priskv_ctx = ctx;
    priskv_req_context *priskv_req_ctx = malloc(sizeof(priskv_req_context));
    priskv_req_ctx->cb = cb;
    priskv_req_ctx->cbarg = cbarg;
    priskv_req_ctx->ignore_no_such_key = 1;

    priskv_delete_async(priskv_ctx->client, key, (uint64_t)priskv_req_ctx, priskv_req_cb);
}

static void priskv_drv_test(void *ctx, const char *key, void (*cb)(int, void *), void *cbarg)
{
    priskv_context *priskv_ctx = ctx;
    priskv_req_context *priskv_req_ctx = malloc(sizeof(priskv_req_context));
    priskv_req_ctx->cb = cb;
    priskv_req_ctx->cbarg = cbarg;
    priskv_req_ctx->ignore_no_such_key = 1;

    priskv_test_async(priskv_ctx->client, key, (uint64_t)priskv_req_ctx, priskv_req_cb);
}

static bool priskv_drv_is_error(int status)
{
    return status != PRISKV_STATUS_OK && status != PRISKV_STATUS_NO_SUCH_KEY &&
           status != PRISKV_STATUS_KEY_UPDATING;
}

static const kv_driver priskv_drv = {
    .name = "priskv",
    .transfer = false,
    .init = priskv_drv_init,
    .deinit = priskv_drv_deinit,
    .get_fd = priskv_drv_get_fd,
    .handler = priskv_drv_handler,
    .status_str = priskv_drv_status_str,
    .is_error = priskv_drv_is_error,
    .get = priskv_drv_get,
    .set = priskv_drv_set,
    .del = priskv_drv_del,
    .test = priskv_drv_test,
};

static const kv_driver priskv_drv_transfer = {
    .name = "priskv",
    .transfer = true,
    .init = priskv_drv_init,
    .deinit = priskv_drv_deinit,
    .get_fd = priskv_drv_get_fd,
    .handler = priskv_drv_handler,
    .status_str = priskv_drv_status_str,
    .is_error = priskv_drv_is_error,
    .get = priskv_drv_get_transfer,
    .set = priskv_drv_set_transfer,
    .del = priskv_drv_del,
    .test = priskv_drv_test,
};

const kv_driver *kv_drivers[] = {&priskv_drv, &priskv_drv_transfer};

static const kv_driver *get_driver(const char *name, bool transfer)
{
    int i;
    int count = sizeof(kv_drivers) / sizeof(kv_driver *);

    for (i = 0; i < count; i++) {
        if (!strcmp(name, kv_drivers[i]->name) && kv_drivers[i]->transfer == transfer) {
            return kv_drivers[i];
        }
    }

    return NULL;
}

static inline bool job_is_running(job_context *job)
{
    return job->state == JOB_STATE_RUNNING;
}

static inline bool job_is_error(job_context *job)
{
    return job->state == JOB_STATE_ERROR;
}

static inline bool job_is_exiting(job_context *job)
{
    return job->state == JOB_STATE_EXITING;
}

static inline bool job_is_exit(job_context *job)
{
    return job->state == JOB_STATE_EXIT;
}

static inline bool job_is_exited(job_context *job)
{
    return job->state == JOB_STATE_EXITED;
}

static inline bool jobs_are_exited(job_context *jobs, int count, int *errjob)
{
    int err = 0;

    for (int i = 0; i < count; i++) {
        if (!job_is_exited(&jobs[i])) {
            if (!job_is_error(&jobs[i])) {
                return false;
            } else {
                err++;
            }
        }
    }

    *errjob = err;

    return true;
}

static void job_status_change(job_context *job, job_status status)
{
    job->status = status;
}

static void job_set_error(job_context *job, const char *fmt, ...)
{
    va_list ap;
    char errstr[STRING_MAX_LEN] = {0};

    va_start(ap, fmt);
    assert(vsnprintf(errstr, STRING_MAX_LEN, fmt, ap) >= 0);
    va_end(ap);

    strncpy(job->err_msg, errstr, STRING_MAX_LEN);
    job->err_msg[STRING_MAX_LEN - 1] = '\0';

    job->state = JOB_STATE_ERROR;
}

static void job_cb(int status, void *arg)
{
    job_context *job = arg;

    if (job->kv_drv->is_error(status)) {
        job_set_error(job, "resp status[%d]: %s", status, job->kv_drv->status_str(status));
    }

    if (job_is_error(job)) {
        /* stopping sending requests */
        return;
    }

    job->req_count++;
    job->inflight--;
    job_fill(job);
}

static int job_action_get(job_context *job)
{
    char *key = NULL;
    void *value = NULL;
    uint32_t value_len = 0;
    int stop = 0;

    switch (job->status) {
    case JOB_STATUS_INIT:
        job->kvm->reset(job->kvm_ctx);
        job_status_change(job, JOB_STATUS_SETTING);
        /* no break and go on */

    case JOB_STATUS_SETTING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->set(job->drv_ctx, key, value, value_len, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx)) {
            job_status_change(job, JOB_STATUS_SET_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_SET_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job->kvm->reset(job->kvm_ctx);
        job_status_change(job, JOB_STATUS_GETTING);
        /* no break and go on */

    case JOB_STATUS_GETTING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->get(job->drv_ctx, key, value, value_len, job_cb, job);
        if (job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_GET_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_GET_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job->kvm->reset(job->kvm_ctx);
        job_status_change(job, JOB_STATUS_CLEARING);
        /* no break and go on */

    case JOB_STATUS_CLEARING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->del(job->drv_ctx, key, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx)) {
            job_status_change(job, JOB_STATUS_CLEAR_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_CLEAR_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job_status_change(job, JOB_STATUS_FINISHED);
        /* no break and go on */

    case JOB_STATUS_FINISHED:
        job->state = JOB_STATE_EXIT;
        stop = 1;
        break;

    default:
        assert(0);
        break;
    }

    return stop;
}

static int job_action_set(job_context *job)
{
    char *key = NULL;
    void *value = NULL;
    uint32_t value_len = 0;
    int stop = 0;

    switch (job->status) {
    case JOB_STATUS_INIT:
        job->kvm->reset(job->kvm_ctx);
        job_status_change(job, JOB_STATUS_SETTING);
        /* no break and go on */

    case JOB_STATUS_SETTING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->set(job->drv_ctx, key, value, value_len, job_cb, job);
        if (job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_SET_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_SET_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job->kvm->reset(job->kvm_ctx);
        job_status_change(job, JOB_STATUS_CLEARING);
        /* no break and go on */

    case JOB_STATUS_CLEARING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->del(job->drv_ctx, key, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx)) {
            job_status_change(job, JOB_STATUS_CLEAR_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_CLEAR_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job_status_change(job, JOB_STATUS_FINISHED);
        /* no break and go on */

    case JOB_STATUS_FINISHED:
        job->state = JOB_STATE_EXIT;
        stop = 1;
        break;

    default:
        assert(0);
        break;
    }

    return stop;
}

static int job_action_verify(job_context *job)
{
    char *key = NULL;
    void *value = NULL;
    uint32_t value_len = 0;
    int stop = 0;

    switch (job->status) {
    again:
    case JOB_STATUS_INIT:
        job_status_change(job, JOB_STATUS_SETTING);
        job->kvm->reset(job->kvm_ctx);
        /* no break and go on */

    case JOB_STATUS_SETTING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->set(job->drv_ctx, key, value, value_len, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx) || job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_SET_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_SET_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job->kvm->reset(job->kvm_ctx);
        if (job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_CLEARING);
            goto clear;
        } else {
            job_status_change(job, JOB_STATUS_GETTING);
        }
        /* no break and go on */

    case JOB_STATUS_GETTING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->get(job->drv_ctx, key, value, value_len, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx) || job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_GET_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_GET_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        job->kvm->reset(job->kvm_ctx);
        if (job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_CLEARING);
            goto clear;
        } else {
            job_status_change(job, JOB_STATUS_VERIFYING);
        }
        /* no break and go on */

    case JOB_STATUS_VERIFYING:
        if (job->kvm->verify(job->kvm_ctx, job)) {
            job_set_error(job, "verify failed!");
            job->state = JOB_STATE_EXIT;
            stop = 1;
            break;
        } else {
            job_status_change(job, JOB_STATUS_CLEARING);
            /* no break and go on */
        }

    clear:
    case JOB_STATUS_CLEARING:
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->del(job->drv_ctx, key, job_cb, job);
        if (job->kvm->alloc_done(job->kvm_ctx)) {
            job_status_change(job, JOB_STATUS_CLEAR_ENDING);
        }
        stop = 0;
        /* must return after set/get/del/test */
        break;

    case JOB_STATUS_CLEAR_ENDING:
        /* wait for all requests to return */
        if (job->inflight) {
            stop = 1;
            break;
        }

        if (job_is_exiting(job)) {
            job_status_change(job, JOB_STATUS_FINISHED);
            goto exit;
        } else {
            job_status_change(job, JOB_STATUS_INIT);
            goto again;
        }

    exit:
    case JOB_STATUS_FINISHED:
        job->state = JOB_STATE_EXIT;
        stop = 1;
        break;

    default:
        assert(0);
        break;
    }

    return stop;
}

static int job_action_del(job_context *job)
{
    char *key = NULL;
    void *value = NULL;
    uint32_t value_len = 0;
    int stop = 0;

    if (!job_is_exiting(job)) {
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->del(job->drv_ctx, key, job_cb, job);
    } else {
        if (!job->inflight) {
            job->state = JOB_STATE_EXIT;
        }
        stop = 1;
    }

    return stop;
}

static int job_action_test(job_context *job)
{
    char *key = NULL;
    void *value = NULL;
    uint32_t value_len = 0;
    int stop = 0;

    if (!job_is_exiting(job)) {
        job->kvm->alloc_kv(job->kvm_ctx, &key, &value, &value_len);
        job->kv_drv->test(job->drv_ctx, key, job_cb, job);
    } else {
        if (!job->inflight) {
            job->state = JOB_STATE_EXIT;
        }
        stop = 1;
    }

    return stop;
}

typedef enum {
    JOB_OP_GET = 0,
    JOB_OP_SET,
    JOB_OP_DEL,
    JOB_OP_VERIFY,
    JOB_OP_TEST,
    JOB_OP_MAX,
} job_op;

typedef struct {
    const char *name;
    action_handler action;
} job_op_action;

job_op g_op = JOB_OP_GET;
const job_op_action op_action[] = {
    [JOB_OP_GET] = {"get", job_action_get},    [JOB_OP_SET] = {"set", job_action_set},
    [JOB_OP_DEL] = {"del", job_action_del},    [JOB_OP_VERIFY] = {"verify", job_action_verify},
    [JOB_OP_TEST] = {"test", job_action_test}, [JOB_OP_MAX] = {"unknown", NULL}};
static job_op job_get_op(const char *op_str)
{
    int i;
    for (i = 0; i < JOB_OP_MAX; i++) {
        if (!strcmp(op_str, op_action[i].name)) {
            return i;
        }
    }
    return JOB_OP_MAX;
}

static const char *job_get_op_str(job_op op)
{
    if (op < 0 || op > JOB_OP_MAX) {
        op = JOB_OP_MAX;
    }

    return op_action[op].name;
}

static action_handler job_get_op_action(job_op op)
{
    if (op < 0 || op > JOB_OP_MAX) {
        op = JOB_OP_MAX;
    }

    return op_action[op].action;
}

static uint64_t get_clock_ns(void)
{
    int res;
    uint64_t ns;
    struct timeval tv;

    res = gettimeofday(&tv, NULL);
    ns = tv.tv_sec * 1000000000ULL + tv.tv_usec * 1000;
    if (res == -1) {
        fprintf(stderr, "could not get requested clock\n");
        exit(10);
    }

    return ns;
}

static void job_info_update(job_context *job, job_info *info)
{
    pthread_spin_lock(&job->lock);
    memcpy(&job->info, info, sizeof(job_info));
    pthread_spin_unlock(&job->lock);
}

static void job_info_get(job_context *job, job_info *info)
{
    pthread_spin_lock(&job->lock);
    memcpy(info, &job->info, sizeof(job_info));
    pthread_spin_unlock(&job->lock);
}

static void job_debug(int fd, void *opaque, uint32_t events)
{
    job_context *job = opaque;
    uint64_t count;

    read(fd, &count, sizeof(uint64_t));

    priskv_log_debug("job[%d]: inflight=%d\n", job->threadid, job->inflight);
}

static int job_init(job_context *job, int threadid)
{
    int drv_fd = 0;

    printf("job[%d] init...\n", threadid);

    memset(job, 0, sizeof(job_context));

    job->epollfd = epoll_create1(0);
    if (job->epollfd < 0) {
        priskv_log_error("Failed to epoll_create1, exit ... \n");
        return -1;
    }

    job->mm = priskv_get_mm(g_mem_type);
    if (!job->mm) {
        return -1;
    }

    job->kvm = get_kvm(g_kvm);
    if (!job->kvm) {
        return -1;
    }

    printf("job[%d] prepare testing data...\n", threadid);
    job->kvm->init(job, &job->kvm_ctx);

    job->kv_drv = get_driver(g_driver, g_transfer);
    if (!job->kv_drv) {
        return -1;
    }

    printf("job[%d] connecting to server...\n", threadid);
    if (job->kv_drv->init(job, &job->drv_ctx)) {
        return -1;
    }

    job->op_name = job_get_op_str(g_op);
    job->op_action = job_get_op_action(g_op);
    job->inflight = 0;
    job->req_count = 0;

    job->state = JOB_STATE_INIT;
    job->status = JOB_STATUS_INIT;
    job->interval_ns = g_interval * 1000000000UL;

    pthread_spin_init(&job->lock, 0);

    drv_fd = job->kv_drv->get_fd(job->drv_ctx);
    priskv_set_fd_handler(drv_fd, job->kv_drv->handler, NULL, job->drv_ctx);
    priskv_add_event_fd(job->epollfd, drv_fd);

    job->eventfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    assert(job->eventfd);
    priskv_set_fd_handler(job->eventfd, job_sig_clear, NULL, job);
    priskv_add_event_fd(job->epollfd, job->eventfd);

    if (g_log_level == priskv_log_debug) {
        job->timerfd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
        struct itimerspec timerspec;

        memset(&timerspec, 0, sizeof(struct itimerspec));
        timerspec.it_value.tv_sec = 1;
        timerspec.it_value.tv_nsec = 0;
        timerspec.it_interval.tv_sec = 1;
        timerspec.it_interval.tv_nsec = 0;
        timerfd_settime(job->timerfd, 0, &timerspec, NULL);
        priskv_set_fd_handler(job->timerfd, job_debug, NULL, job);
        priskv_add_event_fd(job->epollfd, job->timerfd);
    }

    job->threadid = threadid;

    printf("job[%d] finish to init\n", threadid);
    return 0;
}

static void job_deinit(job_context *job)
{
    if (!job) {
        return;
    }

    job->kv_drv->deinit(job, job->drv_ctx);
    job->kvm->deinit(job, job->kvm_ctx);

    if (job->timerfd) {
        close(job->timerfd);
    }
    if (job->eventfd) {
        close(job->eventfd);
    }
    close(job->epollfd);
}

static void job_kick(job_context *job)
{
    uint64_t u = 1;

    priskv_set_fd_handler(job->epollfd, job_process, NULL, job);
    priskv_thread_add_event_handler(priskv_threadpool_get_iothread(g_threadpool, job->threadid),
                                    job->epollfd);
    write(job->eventfd, &u, sizeof(u));
}

static void job_sig_clear(int fd, void *opaque, uint32_t events)
{
    job_context *job = opaque;
    uint64_t u = 1;

    read(job->eventfd, &u, sizeof(u));
}

static void job_fill(job_context *job)
{
    while (job->inflight < g_iodepth) {
        if (job->op_action(job)) {
            return;
        }

        job->inflight++;
    }
}

static void job_update(job_context *job)
{
    uint64_t now = get_clock_ns();
    uint64_t interval = now - job->last_ns;
    uint32_t exit;
    job_info info;

    if (interval < job->interval_ns && !job_is_error(job) && job_is_running(job)) {
        return;
    }

    exit = atomic_load(&g_exiting);

    if (job_is_running(job) && ((g_runtime && now - job->first_ns >= g_runtime) || exit)) {
        job->state = JOB_STATE_EXITING;
    }

    info.state = job->state;
    if (interval) {
        info.qps = 1000000000.0 * (job->req_count - job->last_req_count) / interval;
        info.latency = 1000000.0 / info.qps;
    }

    if (job_is_error(job)) {
        atomic_fetch_add(&g_exiting, 1);
        strcpy(info.msg, job->err_msg);
    } else {
        strcpy(info.msg, job_status_str[job->status]);
    }

    job->last_ns = now;
    job->last_req_count = job->req_count;

    job_info_update(job, &info);
}

static void job_wait(job_context *job)
{
    while (!job_is_exit(job) && !job_is_error(job)) {
        priskv_events_process(job->epollfd, 1000);

        job_update(job);
    }

    if (!job_is_error(job)) {
        job->state = JOB_STATE_EXITED;
    }

    job_update(job);

    job_deinit(job);
}

static void job_process(int fd, void *opaque, uint32_t events)
{
    job_context *job = opaque;

    assert(job->epollfd == fd);

    job->state = JOB_STATE_RUNNING;
    job->last_ns = job->first_ns = get_clock_ns();

#ifdef PRISKV_USE_ACL
    assert(aclrtSetDevice(g_device_id) == ACL_SUCCESS);
#endif

    job_fill(job);
    job_wait(job);
}

typedef enum priskv_short_arg {
    OPTARG_DEVICE_ID = 1000,
    OPTARG_PRISKV_VALUE_BLOCK_SIZE,
    OPTARG_TRANSFER,
    OPTARG_TEMP_REG,
} priskv_short_arg;

static void priskv_showhelp(void)
{
    printf("Usage:\n");
    printf("  -h/--help\n\tprint this help, then exit\n");
    printf("  -p/--rport PORT\n\tremote port\n");
    printf("  -a/--raddr ADDR\n\tremote address\n");
    printf("  -P/--lport PORT\n\tlocal port\n");
    printf("  -A/--laddr ADDR\n\tlocal address\n");
    printf("  -o/--operator [set/get/verify]\n");
    printf(
        "  -k/--key-length MIN:MAX\n\tKEY length range (in bytes), the maximum range is [%d, %d]\n",
        DEFAULT_MIN_KEY_LEN, DEFAULT_MAX_KEY_LEN);
    printf("  -v/--value-length MIN:MAX\n\tVALUE length range (in bytes), the maximum range is "
           "[%lu, %lu]\n",
           DEFAULT_MIN_VALUE_LEN, DEFAULT_MAX_VALUE_LEN);
    printf("  -d/--iodepth DEPTH\n\tthe count of concurrent requests\n");
    printf("  -m/--mem-type [gpu/cpu/npu]\n\tthe position of buffer\n");
    printf("  --device-id DEVICE_ID\n\tthe number of GPU/NPU device, default 0\n");
    printf("  -t/--runtime SECENDS\n\truntime in secends\n");
    printf("  -q/--queue QUEUES\n\tthe number of queue, default to 0\n");
    printf("  -T/--threads THREADS\n\tthe number of threads, default to 1\n");
    printf("  -D/--driver [priskv]\n\tbackend driver, default priskv\n");
    printf("  -G/--kv-gen [share/random]\n\tthe method of generating KV, default share\n"
           "\t\t[share]: all KV requests share one KV\n"
           "\t\t[random]: each KV request has a randomly generated KV\n");
    printf("  -M/--total-size BYTES\n\tthe total size of random values generated by "
           "each job, only for `-G random`\n");
    printf("  -S/--value-align-size BYTES\n\tthe alignment size of the generated VALUE, "
           "defalut %lu\n",
           DEFAULT_ALIGN_VALUE_LEN);
    printf("  -l/--log-level LEVEL\n\terror, warn, notice[default], info or debug\n");
    printf("  -L/--log-file FILEPATH\n\tlog to FILEPATH \n%s", PRISKV_LOGGER_HELP("\t"));
    printf("  -i/--interval SECENDS\n\tthe interval for data statistics and printing, default 1\n");
    printf("  --priskv-value-block-size BYTES\n\tfor priskv, split VALUE into blocks according to "
           "BYTES, no splitting by default\n");
    printf("  --transfer\n\tWhether to use the CPU memory as an intermediate for data transfer, "
           "default false\n");
    printf("  --temp-reg\n\tWhether to temporarily register memory to RDMA when making a request, "
           "default false\n");
    exit(0);
}

static const char *priskv_short_opts = "hp:a:P:A:o:k:v:d:m:t:q:T:D:G:M:S:l:L:i:";
static struct option priskv_long_opts[] = {
    {"rport", required_argument, 0, 'p'},
    {"raddr", required_argument, 0, 'a'},
    {"lport", required_argument, 0, 'P'},
    {"laddr", required_argument, 0, 'A'},
    {"operator", required_argument, 0, 'o'},
    {"key-length", required_argument, 0, 'k'},
    {"value-length", required_argument, 0, 'v'},
    {"iodepth", required_argument, 0, 'd'},
    {"mem-type", required_argument, 0, 'm'},
    {"runtime", required_argument, 0, 't'},
    {"queue", required_argument, 0, 'q'},
    {"threads", required_argument, 0, 'T'},
    {"help", no_argument, 0, 'h'},
    {"driver", required_argument, 0, 'D'},
    {"kv-gen", required_argument, 0, 'G'},
    {"total-size", required_argument, 0, 'T'},
    {"value-align-size", required_argument, 0, 'S'},
    {"log-level", required_argument, 0, 'l'},
    {"log-file", required_argument, 0, 'L'},
    {"interval", required_argument, 0, 'i'},
    {"device-id", required_argument, 0, OPTARG_DEVICE_ID},
    {"priskv-value-block-size", required_argument, 0, OPTARG_PRISKV_VALUE_BLOCK_SIZE},
    {"transfer", no_argument, 0, OPTARG_TRANSFER},
    {"temp-reg", no_argument, 0, OPTARG_TEMP_REG},
};

static int parse_range_arg(char *optarg, int64_t *min, int64_t *max)
{
    char *p = optarg, *n;

    if (!optarg) {
        return -1;
    }

    n = strchr(optarg, ':');
    if (!n) {
        if (priskv_str2num(p, min)) {
            return -1;
        }
        *max = *min;
    } else {
        *n = '\0';
        if (priskv_str2num(p, min)) {
            return -1;
        }
        if (priskv_str2num(n + 1, max)) {
            return -1;
        }
    }

    return 0;
}

static int parse_arg(int argc, char *argv[])
{
    int args, ch;
    int64_t total_value_size = 0, value_aligned_len = 0, priskv_value_block_size = 0;
    int ret = 0;

    while (1) {
        ch = getopt_long(argc, argv, priskv_short_opts, priskv_long_opts, &args);
        if (ch == -1) {
            break;
        }

        switch (ch) {
        case 'h':
            priskv_showhelp();
            return 0;

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

        case 'o':
            g_op = job_get_op(optarg);
            if (g_op == JOB_OP_MAX) {
                printf("unknown operator\n");
                priskv_showhelp();
                ret = -1;
            }
            break;

        case 'k':
            if (parse_range_arg(optarg, &g_key_min_len, &g_key_max_len)) {
                printf("invalid paremeter: `-k/--key-length MIN:MAX`\n");
                priskv_showhelp();
                ret = -1;
            }
            break;

        case 'v':
            if (parse_range_arg(optarg, &g_value_min_len, &g_value_max_len)) {
                printf("invalid paremeter: `-v/--value-length MIN:MAX`\n");
                priskv_showhelp();
                ret = -1;
            }
            break;

        case 'd':
            g_iodepth = atoi(optarg);
            break;

        case 'm':
            g_mem_type = priskv_get_mem_type(optarg);
            if (g_mem_type == PRISKV_MEM_TYPE_MAX) {
                printf("unknown memory type\n");
                priskv_showhelp();
                ret = -1;
            }
            break;

        case 't':
            g_runtime = atoll(optarg) * 1000000000UL;
            break;

        case 'q':
            g_nqueue = atoi(optarg);
            break;

        case 'T':
            g_threads = atoi(optarg);
            break;

        case 'D':
            g_driver = optarg;
            break;

        case 'G':
            g_kvm = optarg;
            break;

        case 'M':
            if (priskv_str2num(optarg, &total_value_size)) {
                printf("invalid paremeter: `-M/--total-size BYTES`\n");
                priskv_showhelp();
                ret = -1;
            }
            g_total_value_size = (uint64_t)total_value_size;
            break;

        case 'S':
            if (priskv_str2num(optarg, &value_aligned_len)) {
                printf("invalid paremeter: `-S/--value-align-size BYTES`\n");
                priskv_showhelp();
                ret = -1;
            }
            g_value_aligned_len = (uint64_t)value_aligned_len;
            break;

        case 'l':
            if (!strcmp(optarg, "error")) {
                g_log_level = priskv_log_error;
            } else if (!strcmp(optarg, "warn")) {
                g_log_level = priskv_log_warn;
            } else if (!strcmp(optarg, "notice")) {
                g_log_level = priskv_log_notice;
            } else if (!strcmp(optarg, "info")) {
                g_log_level = priskv_log_info;
            } else if (!strcmp(optarg, "debug")) {
                g_log_level = priskv_log_debug;
            } else {
                priskv_showhelp();
                ret = -1;
            }
            break;

        case 'L':
            g_log_file = optarg;
            break;

        case 'i':
            g_interval = atoll(optarg);
            break;

        case OPTARG_DEVICE_ID:
            g_device_id = atoi(optarg);
            break;

        case OPTARG_PRISKV_VALUE_BLOCK_SIZE:
            if (priskv_str2num(optarg, &priskv_value_block_size)) {
                printf("invalid paremeter: `-priskv-value-block-size BYTES`\n");
                priskv_showhelp();
                ret = -1;
            }
            g_priskv_value_block_size = (uint64_t)priskv_value_block_size;
            break;

        case OPTARG_TRANSFER:
            g_transfer = true;
            break;

        case OPTARG_TEMP_REG:
            g_temp_reg = true;
            break;

        default:
            priskv_showhelp();
            ret = -1;
        }
    }

    return ret;
}

static int check_arg()
{
    if (!get_kvm(g_kvm)) {
        priskv_log_error("Invaild parameter: `-G/--kv-gen %s`\n", g_kvm);
        return -1;
    }

    if (!get_driver(g_driver, g_transfer)) {
        priskv_log_error("Invaild parameter: `-D/--driver %s`\n", g_driver);
        return -1;
    }

    if (g_mem_type == PRISKV_MEM_TYPE_GPU) {
        if (!gpu_device_is_available(g_device_id)) {
            priskv_log_error("GPU device %d is unavailable\n", g_device_id);
            return -1;
        }

        if (!gdr_is_support()) {
            priskv_log_error("The device does NOT support GPUDirect RDMA\n");
            return -1;
        }
    } else if (g_mem_type == PRISKV_MEM_TYPE_NPU) {
        if (!npu_device_is_available(g_device_id)) {
            priskv_log_error("NPU device %d is unavailable\n", g_device_id);
            return -1;
        }

        if (!g_transfer) {
            priskv_log_notice("NPU device have to use '--transfer' and '--transfer' have been "
                            "automatically set\n");
            g_transfer = true;
        }
    }

    if (g_key_min_len < DEFAULT_MIN_KEY_LEN) {
        priskv_log_error("Invaild parameter: The minimum value of KEY length should "
                       "be greater than %d\n",
                       DEFAULT_MIN_KEY_LEN);
        return -1;
    }

    if (g_key_max_len > DEFAULT_MAX_KEY_LEN) {
        priskv_log_error("Invaild parameter: The maximum value of KEY length should "
                       "be less than %d\n",
                       DEFAULT_MAX_KEY_LEN);
        return -1;
    }

    if (g_key_min_len > g_key_max_len) {
        priskv_log_error("Invaild parameter: The maximum value of KEY length should "
                       "be greater than the minimum value\n");
        return -1;
    }

    if (g_value_min_len < DEFAULT_MIN_VALUE_LEN) {
        priskv_log_error("Invaild parameter: The minimum value of VALUE length should "
                       "be greater than %d\n",
                       DEFAULT_MIN_VALUE_LEN);
        return -1;
    }

    if (g_value_max_len > DEFAULT_MAX_VALUE_LEN) {
        priskv_log_error("Invaild parameter: The maximum value of VALUE length should "
                       "be less than %d\n",
                       DEFAULT_MAX_VALUE_LEN);
        return -1;
    }

    if (g_value_min_len > g_value_max_len) {
        priskv_log_error("Invaild parameter: The maximum value of VALUE length should "
                       "be greater than the minimum value\n");
        return -1;
    }

    if (g_value_min_len < g_value_aligned_len) {
        priskv_log_error("Invaild parameter: The minimum value of VALUE length should "
                       "be greater than the aligned length\n");
        return -1;
    }

    if (g_priskv_value_block_size) {
        if (DIV_ROUND_UP(g_value_max_len, g_priskv_value_block_size) > UINT16_MAX) {
            priskv_log_error("Invaild parameter: The --priskv-value-block-size is too small that "
                           "block count exceeds UINT16_MAX(%d)\n",
                           UINT16_MAX);
            return -1;
        }

        g_max_sgl = DIV_ROUND_UP(g_value_max_len, g_priskv_value_block_size);
        priskv_log_debug("The maximum SGL count is %d\n", g_max_sgl);
    }

    if (!strcmp(g_kvm, "random")) {
        if (!g_total_value_size) {
            priskv_log_error("Invaild parameter: Must set `-M/--total-size` in random mode\n");
            return -1;
        }

        if (g_value_max_len > g_total_value_size) {
            priskv_log_error("Invaild parameter: The maximum value of VALUE length should "
                           "be less than the total size\n");
            return -1;
        }
    }

    if ((g_threads > 1) && lport) {
        priskv_log_error("Invaild parameter: -P/--lport is only support by single thread\n");
        return -1;
    }

    return 0;
}

static void sigint_handler(int sig)
{
    atomic_fetch_add(&g_exiting, 1);
    if (g_exiting >= 5) {
        exit(-1);
    }
    printf("Benchmark: received signal %d, force the exit after %u more times!\n", sig,
           5 - g_exiting);
}

static int benchmark_print_info(bool summary)
{
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char now_str[64];
    static int lines = 4;

    strftime(now_str, sizeof(now_str), "%Y-%m-%d %H:%M:%S", t);

    if (summary) {
        printf("PrisKV Benchmark <%s>\n", now_str);
        printf("server=[%s %d] jobs=%d op=%s\nkey-len=[%ld,%ld] value-len=[%ld,%ld] "
               "iodepth=%d\ninterval=%lds driver=%s device=%s\n",
               raddr, rport, g_threads, job_get_op_str(g_op), g_key_min_len, g_key_max_len,
               g_value_min_len, g_value_max_len, g_iodepth, g_interval, g_driver,
               priskv_get_mem_type_str(g_mem_type));
    } else {
        printw("PrisKV Benchmark <%s>\n", now_str);
        printw("server=[%s %d] jobs=%d op=%s\nkey-len=[%ld,%ld] value-len=[%ld,%ld] "
               "iodepth=%d\ninterval=%lds driver=%s device=%s\n",
               raddr, rport, g_threads, job_get_op_str(g_op), g_key_min_len, g_key_max_len,
               g_value_min_len, g_value_max_len, g_iodepth, g_interval, g_driver,
               priskv_get_mem_type_str(g_mem_type));
    }

    return lines;
}

static void term_resize(int unusused __attribute__((__unused__)))
{
    struct winsize ws;

    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) != -1) {
        g_cols = ws.ws_col;
        g_rows = ws.ws_row;
    } else {
        g_cols = 80;
        g_rows = 24;
    }
}

static void frame_init(void)
{
    signal(SIGWINCH, term_resize);
    term_resize(0);
    initscr();
    cbreak();
    noecho();
    resizeterm(g_rows, g_cols);
}

static void frame_clear(void)
{
    resizeterm(g_rows, g_cols);
    erase();
}

static void frame_refresh(void)
{
    refresh();
}

static void frame_deinit(void)
{
    endwin();
}

static int jobs_print_headings(bool summary)
{
    static int lines = 1;

    attron(A_BOLD | A_REVERSE);
    if (summary) {
        printf("%-10s%-4s%-8s%-8s %11s %11s  %-25s\n", "JOB", "ID", "TYPE", "STATUS", "QPS",
               "LAT(us)", "DETAILS");
    } else {
        printw("%-10s%-4s%-8s%-8s %11s %11s  %-25s\n", "JOB", "ID", "TYPE", "STATUS", "QPS",
               "LAT(us)", "DETAILS");
    }
    attroff(A_BOLD | A_REVERSE);

    return lines;
}

static int win_scroll(int start_row, int rows, int njobs)
{
    if (g_scroll > 0 && start_row + rows < njobs) {
        start_row++;
    } else if (g_scroll < 0 && start_row > 0) {
        start_row--;
    }

    return start_row;
}

static void jobs_print_info(job_context *jobs, int njobs)
{
    int i;
    job_info info;
    int lines = 0;
    int rows;
    static int start_row = 0;
    static const char *dots[3] = {".", "..", "..."};

    lines += benchmark_print_info(false);
    lines += jobs_print_headings(false);

    if (lines + njobs > g_rows) {
        rows = g_rows - lines;
        start_row = win_scroll(start_row, rows, njobs);
    } else {
        rows = njobs;
        start_row = 0;
    }

    for (i = start_row; i < start_row + rows; i++) {
        job_info_get(jobs + i, &info);
        printw("%-10s%-4d%-8s%-8s %11.2lf %11.2lf  %s%s\n", "current", i, jobs[i].op_name,
               job_state_str[info.state], info.qps, info.latency, info.msg,
               dots[jobs[i].print_count++ % 3]);
    }
}

static void job_print_summary(job_context *job)
{
    double qps = 0, latency = 0;
    uint64_t interval = job->last_ns - job->first_ns;

    if (interval) {
        qps = 1000000000.0 * job->req_count / interval;
        latency = 1000000.0 / qps;
    }

    printf("%-10s%-4d%-8s%-8s %11.2lf %11.2lf  %s\n", "average", job->threadid, job->op_name,
           job_state_str[job->state], qps, latency, strlen(job->err_msg) ? job->err_msg : "GOOD!");
}

static int jobs_success_count(job_context *jobs, int njob)
{
    int i;
    int count = 0;

    for (i = 0; i < njob; i++) {
        if (!job_is_error(&jobs[i])) {
            count++;
        }
    }

    return count;
}

static void jobs_print_summary(job_context *jobs, int njob)
{
    int i;

    benchmark_print_info(true);
    printf("Summary: success/all=%d/%d\n", jobs_success_count(jobs, njob), njob);
    jobs_print_headings(true);
    for (i = 0; i < njob; i++) {
        job_print_summary(jobs + i);
    }
}

static int wait_for_user(int timeout_sec)
{
    struct timeval tv = {.tv_sec = timeout_sec, .tv_usec = 0};
    fd_set readfds;
    char c;

    g_scroll = 0;

    FD_ZERO(&readfds);
    FD_SET(STDIN_FILENO, &readfds);
    if (select(STDOUT_FILENO, &readfds, NULL, NULL, &tv) > 0) {
        if (read(STDIN_FILENO, &c, 1) != 1) {
            return -1;
        }

        switch (c) {
        case 'j':
            g_scroll = 1;
            break;

        case 'k':
            g_scroll = -1;
            refresh();
            break;

        default:
            break;
        }
    }

    return 0;
}

static void priskv_benchmark_log_fn(priskv_log_level level, const char *msg)
{
    priskv_logger_printf(g_logger, level, "%s", msg);
}

int main(int argc, char *argv[])
{
    int i;
    int errjobs = 0;

    priskv_show_logo();

    if (parse_arg(argc, argv)) {
        return -1;
    }

    priskv_set_log_level(g_log_level);
    if (g_log_file) {
        g_logger = priskv_logger_new(g_log_file);
        if (!g_logger) {
            printf("Failed to open logger %s, exit...\n", g_log_file);
            return -1;
        }
        priskv_set_log_fn(priskv_benchmark_log_fn);
    }

    if (check_arg()) {
        return -1;
    }

    benchmark_print_info(true);

    atomic_init(&g_exiting, 0);
    srand((uint32_t)time(NULL));

    job_context jobs[g_threads];
    g_threadpool = priskv_threadpool_create("job", g_threads, 0, 0);
    if (!g_threadpool) {
        priskv_log_error("Failed to create thread pool\n");
        return -1;
    }

    for (i = 0; i < g_threads; i++) {
        if (job_init(jobs + i, i)) {
            return -1;
        }
    }

    for (i = 0; i < g_threads; i++) {
        job_kick(jobs + i);
    }

    signal(SIGINT, sigint_handler);

    assert(atexit(frame_deinit) == 0);
    frame_init();
    while (!jobs_are_exited(jobs, g_threads, &errjobs)) {
        wait_for_user(g_interval);
        frame_clear();
        jobs_print_info(jobs, g_threads);
        frame_refresh();
    }
    frame_deinit();

    jobs_print_summary(jobs, g_threads);

    priskv_logger_free(g_logger);

    return errjobs ? -1 : 0;
}
