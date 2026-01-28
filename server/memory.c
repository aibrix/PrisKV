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

#include <assert.h>
#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <libmount/libmount.h>

#include "priskv-config.h"
#include "priskv-cuda.h"
#include "priskv-utils.h"
#include "priskv-log.h"

#include "buddy.h"
#include "memory.h"

typedef struct priskv_mem_file {
#define PRISKV_MEM_INVALID_FD -1
    int fd;

    union {
        /* for memory mapped mapping */
        struct {
            uint8_t *memfile_addr;
            uint64_t length;
        };

        /* for anonymous mapping */
        struct {
            uint8_t *key_addr;
            uint64_t key_length;
            uint8_t *value_addr;
            int value_fd;
            uint64_t value_length;
        };
    };
} priskv_mem_file;

typedef struct priskv_memfd_create_compat {
    void* handle;
    int (*memfd_create_fn)(const char* name, unsigned int flags);
} priskv_memfd_create_compat;

static priskv_mem_info g_memory_info;
static priskv_memfd_create_compat g_memfd_create_compat;

/**
 * Notes on [memfd_create vs. mkstemp] for shared memory allocation
 *
 * We prefer memfd_create(2) on Linux when available because:
 * 1. Memory accounting: With memfd_create, shared memory usage is correctly
 *    attributed to the process that creates the file descriptor (i.e., the process
 *    that performs the actual anonymous physical memory allocation), providing
 *    more accurate resource tracking.
 * 2. SIGBUS prevention: memfd_create eliminates SIGBUS errors that can occur
 *    when using file-backed shared memory (via mkstemp) if the underlying
 *    filesystem runs out of space or encounters other I/O issues.
 * 3. Performance: Anonymous memory mappings via memfd_create avoid filesystem
 *    overhead and provide better performance for purely in-memory operations.
 *
 * Fallback to mkstemp(3) if memfd_create is unavailable
 *
 * Reference: https://dvdhrm.wordpress.com/2014/06/10/memfd_create2/
 */
static void __attribute__((constructor)) priskv_memfd_create_compat_init(void)
{
    // ensure config is initialized
    priskv_config_init();

    g_memfd_create_compat.handle = dlopen(NULL, RTLD_NOW);
    g_memfd_create_compat.memfd_create_fn = NULL;
    
    if (g_memfd_create_compat.handle) {
        g_memfd_create_compat.memfd_create_fn = dlsym(g_memfd_create_compat.handle, "memfd_create");
    }
    
    if (g_memfd_create_compat.memfd_create_fn) {
        priskv_log_info("MEM: Use memfd_create(2) for shared memory\n");
    } else {
        priskv_log_info("MEM: Use mkstemp(3) for shared memory\n");
    }
}

static int priskv_memfd_create(size_t size, bool guard) {
    int fd = -1;
    const char* file_template = "/dev/shm/priskv-anon-XXXXXX";
    char file_name[strlen(file_template) + 1];
    strcpy(file_name, file_template);
    size_t page_size = getpagesize();

    size = ALIGN_UP(size, page_size);
    if (guard) {
        size += 2 * page_size;
    }

    if (g_memfd_create_compat.memfd_create_fn) {
        fd = g_memfd_create_compat.memfd_create_fn(file_name, 0);
        if (fd < 0 && errno == ENOSYS) {
            priskv_log_warn("MEM: memfd_create(2) failed %m, fallback to mkstemp(3)\n");
            // disable memfd_create, and fall through
            g_memfd_create_compat.memfd_create_fn = NULL;
        } else if (fd < 0) {
            return fd;  // propagate the error
        } else {
            goto truncate;
        }
    }
    
    fd = mkstemp(file_name);
    if (fd < 0) {
        priskv_log_error("MEM: failed to create memory file %s, %m\n", file_name);
        return fd;
    }
    
    if (unlink(file_name) != 0) {
        priskv_log_error("MEM: failed to unlink file %s, %m\n", file_name);
        close(fd);
        return -1;
    }

truncate:
    if (ftruncate(fd, size) != 0) {
        priskv_log_error("MEM: failed to truncate fd %d to %ld, %m\n", fd, size);
        close(fd);
        return -1;
    }
    
    return fd;
}

static inline void priskv_mem_build_check()
{
    PRISKV_BUILD_BUG_ON(sizeof(priskv_mem_header) != PRISKV_MEM_HEADER_SIZE);
}

static int priskv_mem_file_page_size(const char *path)
{
    struct libmnt_table *tb = NULL;
    struct libmnt_fs *fs = NULL;
    char *mp = NULL, *fstype = NULL, *pagesize = NULL;
    int ret;

    mp = mnt_get_mountpoint(path);
    if (!mp) {
        priskv_log_error("MEM: Failed to get mountpoint for %s\n", path);
        ret = -errno;
        goto out;
    }

    tb = mnt_new_table_from_file("/proc/self/mountinfo");
    if (!tb) {
        priskv_log_error("MEM: faile to open /proc/self/mountinfo\n");
        ret = -errno;
        goto out;
    }

    fs = mnt_table_find_mountpoint(tb, mp, 0);
    if (!fs) {
        priskv_log_error("MEM: Failed to get fs table mountpoint for %s\n", path);
        ret = -errno;
        goto out;
    }

    fstype = (char *)mnt_fs_get_fstype(fs);
    if (!fstype) {
        priskv_log_error("MEM: Failed to get fs type for %s\n", path);
        ret = -errno;
        goto out;
    }

    if (!strcmp(fstype, "hugetlbfs")) {
        pagesize = (char *)mnt_fs_get_fs_options(fs);
        if (strstr(pagesize, "pagesize=2M")) {
            ret = 1024 * 1024 * 2;
        } else if (strstr(pagesize, "pagesize=1G")) {
            ret = 1024 * 1024 * 1024;
        } else {
            priskv_log_error("MEM: unsupported hugetlbfs page size: %s\n", pagesize);
            ret = -ENOTSUP;
            goto out;
        }
    } else if (!strcmp(fstype, "tmpfs")) {
        ret = getpagesize();
    } else {
        priskv_log_notice("MEM: support hugetlbfs/tmpfs only(against %s @ %s)\n", path, fstype);
        ret = -ENODEV;
        goto out;
    }

out:
    mnt_unref_fs(fs);
    mnt_unref_table(tb);
    free(mp);

    return ret;
}

typedef struct priskv_mem_clear_thd {
    pthread_t thread;
    uint8_t idx;
    uint8_t *addr;
    uint64_t size;
} priskv_mem_clear_thd;

static void *priskv_mem_clear_routine(void *arg)
{
    priskv_mem_clear_thd *thd = arg;

    priskv_log_debug("MEM: thread[%d] starts to clear memory from %p # %ld\n", thd->idx, thd->addr,
                   thd->size);
    memset(thd->addr, 0x00, thd->size);

    return NULL;
}

static void priskv_mem_clear(uint8_t *addr, uint64_t size, uint8_t nthreads)
{
    /* if no threads is specified, or a single thread, just clear it in main thread */
    if (!nthreads || (nthreads == 1)) {
        memset(addr, 0x00, size);
        return;
    }

    uint64_t batch_size = size / nthreads;
    priskv_mem_clear_thd thds[nthreads];
    memset(thds, 0x00, sizeof(thds));

    for (uint8_t i = 0; i < nthreads; i++) {
        priskv_mem_clear_thd *thd = &thds[i];

        thd->idx = i;
        thd->addr = addr + batch_size * i;
        thd->size = batch_size;
        assert(!pthread_create(&thd->thread, NULL, priskv_mem_clear_routine, thd));
    }

    for (uint8_t i = 0; i < nthreads; i++) {
        priskv_mem_clear_thd *thd = &thds[i];
        pthread_join(thd->thread, NULL);
    }
}

int priskv_mem_create(const char *path, uint16_t max_key_length, uint32_t max_keys,
                    uint32_t value_block_size, uint64_t value_blocks, uint8_t nthreads)
{
    struct stat statbuf;
    mode_t mode = S_IRUSR | S_IWUSR;
    uint8_t *addr = NULL;
    int fd = -1;
    int ret;

    /* these should be already checked strictly */
    assert(path);
    assert(max_key_length);
    assert(max_keys);
    assert(value_block_size);
    assert(value_blocks);
    assert(IS_POWER_OF_2(value_blocks));

    ret = stat(path, &statbuf);
    if (!ret) {
        priskv_log_notice("MEM: file %s already exist\n", path);
        return -EEXIST;
    } else {
        if (errno != ENOENT) {
            priskv_log_error("MEM: file %s should invalid file path, however failed: %m\n", path);
            return -errno;
        }
    }

    fd = open(path, O_CREAT | O_RDWR, mode);
    if (fd == -1) {
        priskv_log_error("MEM: failed to create file %s: %m\n", path);
        return -errno;
    }

    int pagesize = priskv_mem_file_page_size(path);
    if (pagesize < 0) {
        ret = pagesize;
        goto error;
    }

    uint16_t hdr_size = PRISKV_MEM_HEADER_SIZE;
    uint64_t key_size = (sizeof(priskv_key) + max_key_length) * max_keys;
    uint64_t aligned_key_size = ALIGN_UP(key_size, PRISKV_MEM_ALIGN_UP);
    uint64_t value_size = priskv_buddy_mem_size(value_blocks, value_block_size);
    uint64_t file_size = ALIGN_UP(hdr_size + aligned_key_size + value_size, pagesize);

    ret = ftruncate(fd, file_size);
    if (ret == -1) {
        priskv_log_error("MEM: failed to truncate memory file %s to %ld, %m\n", path, file_size);
        goto error;
    }

    int flags = pagesize == getpagesize() ? 0 : MAP_HUGETLB;
    addr = (uint8_t *)mmap(NULL, file_size, PROT_WRITE, MAP_SHARED | flags, fd, 0);
    if (addr == MAP_FAILED) {
        priskv_log_error("MEM: failed to map memory file %s, %m\n", path);
        ret = -errno;
        goto error;
    }

    if (madvise(addr, file_size, MADV_DONTDUMP)) {
        priskv_log_warn("MEM: failed to set memory dont dump for %s on create, ignore %m\n", path);
    }

    priskv_mem_clear(addr, file_size, nthreads);

    priskv_mem_header *hdr = (priskv_mem_header *)addr;
    hdr->magic = PRISKV_MEM_MAGIC;
    hdr->reserved4 = 0;
    hdr->max_key_length = max_key_length;
    hdr->max_keys = max_keys;
    hdr->value_block_size = value_block_size;
    hdr->value_blocks = value_blocks;
    hdr->feature0 = 0;

    priskv_log_notice("MEM: create a memory file %s with size %ld\n", path, file_size);
    priskv_log_debug("MEM: create hdr-size %d, key-size %d, value-size %ld, page-size %d\n", hdr_size,
                   key_size, value_size, pagesize);

    munmap(addr, file_size);
    close(fd);
    return 0;

error:
    if (addr) {
        munmap(addr, file_size);
    }

    if (fd != PRISKV_MEM_INVALID_FD) {
        close(fd);
    }
    unlink(path);

    return ret;
}

void *priskv_mem_malloc(size_t size, int mmap_flags, int mmap_fd, bool guard)
{
    size_t page_size = getpagesize(), real_size;
    uint8_t *ptr;

    real_size = size = ALIGN_UP(size, page_size);
    if (guard) {
        real_size += 2 * page_size;
    }

    ptr = mmap(NULL, real_size, PROT_READ | PROT_WRITE, mmap_flags, mmap_fd, 0);
    if (ptr == MAP_FAILED) {
        priskv_log_error("MEM: failed to allocate memory: %m\n");
        return NULL;
    }

    if (madvise(ptr, real_size, MADV_DONTDUMP)) {
        priskv_log_warn("MEM: failed to set anonymous memory dont dump, ignore %m\n");
    }

    if (guard) {
        assert(!mprotect(ptr, page_size, PROT_NONE));
        assert(!mprotect(ptr + size + page_size, page_size, PROT_NONE));
        ptr += page_size;
    }

    return (void *)ptr;
}

void priskv_mem_free(void *ptr, size_t size, bool guard)
{
    uint32_t page_size = getpagesize();

    if (!ptr) {
        return;
    }

    if ((unsigned long)ptr & (page_size - 1)) {
        priskv_log_warn("MEM: try to unmap unaligned memory\n");
    }

    size = ALIGN_UP(size, page_size);
    if (guard) {
        size += 2 * page_size;
        ptr = (void *)((uint8_t *)ptr - page_size);
    }

    munmap(ptr, size);
}

void *priskv_mem_anon(uint16_t max_key_length, uint32_t max_keys, uint32_t value_block_size,
                    uint64_t value_blocks, uint8_t threads)
{
    uint64_t key_size = (sizeof(priskv_key) + max_key_length) * max_keys;
    uint64_t aligned_key_size = ALIGN_UP(key_size, PRISKV_MEM_ALIGN_UP);
    uint64_t value_size = priskv_buddy_mem_size(value_blocks, value_block_size);
    int key_mmap_flags = MAP_PRIVATE | MAP_ANONYMOUS;
    int value_mmap_flags = 0;
    int value_mmap_fd = PRISKV_MEM_INVALID_FD;
    if (g_config.mem.use_shm) {
        value_mmap_flags |= MAP_SHARED;
        value_mmap_fd = priskv_memfd_create(value_size, true);
        assert(value_mmap_fd >= 0);
    } else {
        value_mmap_flags |= MAP_PRIVATE | MAP_ANONYMOUS;
    }

    priskv_mem_file *memfile = calloc(1, sizeof(*memfile));
    assert(memfile);

    memfile->fd = PRISKV_MEM_INVALID_FD;
    memfile->key_addr = priskv_mem_malloc(aligned_key_size, key_mmap_flags, -1, true);
    assert(memfile->key_addr);
    memfile->key_length = aligned_key_size;
    memfile->value_addr = priskv_mem_malloc(value_size, value_mmap_flags, value_mmap_fd, true);
    memfile->value_fd = value_mmap_fd;
    assert(memfile->value_addr);
    memfile->value_length = value_size;

    priskv_mem_clear(memfile->key_addr, key_size, threads);
    priskv_mem_clear(memfile->value_addr, value_size, threads);

    if (g_config.mem.use_cuda) {
        assert(!priskv_cuda_host_register(memfile->value_addr, value_size));
    }

    g_memory_info.type = "anonymous";

    return memfile;
}

void *priskv_mem_load(const char *path)
{
    struct stat statbuf;
    int ret, fd;
    uint8_t *addr = NULL;
    priskv_mem_file *memfile;

    if (!path) {
        priskv_log_error("MEM: invalid path\n");
        errno = EINVAL;
        return NULL;
    }

    ret = stat(path, &statbuf);
    if (ret) {
        priskv_log_error("MEM: failed to stat memory file %s\n", path);
        return NULL;
    }

    fd = open(path, O_RDWR);
    if (fd == -1) {
        priskv_log_error("MEM: failed to open file %s\n", path);
        return NULL;
    }

    int pagesize = priskv_mem_file_page_size(path);
    if (pagesize < 0) {
        errno = -pagesize;
        goto error;
    }

    int flags = pagesize == getpagesize() ? 0 : MAP_HUGETLB;
    addr = (uint8_t *)mmap(NULL, statbuf.st_size, PROT_WRITE, MAP_SHARED | flags, fd, 0);
    if (addr == MAP_FAILED) {
        priskv_log_error("MEM: failed to map memory file %s, %m\n", path);
        goto error;
    }

    if (madvise(addr, statbuf.st_size, MADV_DONTDUMP)) {
        priskv_log_warn("MEM: failed to set memory dont dump for %s on load, ignore %m\n", path);
    }

    errno = -EIO;
    priskv_mem_header *hdr = (priskv_mem_header *)addr;
    if (hdr->magic != PRISKV_MEM_MAGIC) {
        priskv_log_error("MEM: mismatched magic 0x%x (expected 0x%x) map memory file %s\n",
                       hdr->magic, PRISKV_MEM_MAGIC, path);
        goto error;
    }

    if (hdr->feature0 != 0) {
        priskv_log_error("MEM: unsupported feature0 0x%lx (expected 0x%lx) map memory file %s\n",
                       hdr->feature0, 0, path);
        goto error;
    }

    if (!IS_POWER_OF_2(hdr->value_blocks)) {
        priskv_log_error("MEM: unsupported value-blocks %ld from map memory file %s\n",
                       hdr->value_blocks, path);
        goto error;
    }

    if (!IS_POWER_OF_2(hdr->value_block_size)) {
        priskv_log_error("MEM: unsupported value-block-size %ld from map memory file %s\n",
                       hdr->value_blocks, path);
        goto error;
    }

    uint16_t hdr_size = PRISKV_MEM_HEADER_SIZE;
    uint64_t key_size = (sizeof(priskv_key) + hdr->max_key_length) * hdr->max_keys;
    uint64_t aligned_key_size = ALIGN_UP(key_size, PRISKV_MEM_ALIGN_UP);
    uint64_t value_size = priskv_buddy_mem_size(hdr->value_blocks, hdr->value_block_size);
    uint64_t file_size = ALIGN_UP(hdr_size + aligned_key_size + value_size, pagesize);
    if (file_size != statbuf.st_size) {
        priskv_log_error("MEM: mismatched file size %ld (expected %ld) from map memory file %s\n",
                       statbuf.st_size, file_size, path);
        goto error;
    }

    priskv_log_debug("MEM: load feature0 0x%lx, max-key-length %d, max-key %d, value-block-size %ld, "
                   "value-blocks %ld, page-size %d. [%p, %p]\n",
                   hdr->feature0, hdr->max_key_length, hdr->max_keys, hdr->value_block_size,
                   hdr->value_blocks, pagesize, addr, addr + statbuf.st_size);

    memfile = calloc(1, sizeof(*memfile));
    assert(memfile);
    memfile->fd = fd;
    memfile->memfile_addr = addr;
    memfile->length = statbuf.st_size;

    if (g_config.mem.use_cuda) {
        assert(!priskv_cuda_host_register(memfile->memfile_addr, memfile->length));
    }

    g_memory_info.type = "memfile";
    g_memory_info.path = path;
    g_memory_info.filesize = statbuf.st_size;
    g_memory_info.pagesize = pagesize;
    g_memory_info.feature0 = hdr->feature0;

    return memfile;

error:
    if (addr) {
        munmap(addr, statbuf.st_size);
    }

    close(fd);

    return NULL;
}

void priskv_mem_close(void *ctx)
{
    priskv_mem_file *memfile = ctx;

    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
#ifdef PRISKV_USE_CUDA
        if (g_config.mem.use_cuda) {
            cudaHostUnregister(memfile->memfile_addr);
        }
#endif
        close(memfile->fd);
        munmap(memfile->memfile_addr, memfile->length);
    } else {
#ifdef PRISKV_USE_CUDA
        if (g_config.mem.use_cuda) {
            cudaHostUnregister(memfile->value_addr);
        }
#endif
        priskv_mem_free(memfile->key_addr, memfile->key_length, true);
        priskv_mem_free(memfile->value_addr, memfile->value_length, true);
        if (memfile->value_fd != PRISKV_MEM_INVALID_FD) {
            close(memfile->value_fd);
        }
    }

    free(ctx);
}

uint8_t *priskv_mem_header_addr(void *ctx)
{
    priskv_mem_file *memfile = ctx;

    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        return memfile->memfile_addr;
    } else {
        return NULL;
    }
}

uint8_t *priskv_mem_key_addr(void *ctx)
{
    priskv_mem_file *memfile = ctx;

    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        return memfile->memfile_addr + PRISKV_MEM_HEADER_SIZE;
    } else {
        return memfile->key_addr;
    }
}

uint8_t *priskv_mem_value_addr(void *ctx)
{
    priskv_mem_file *memfile = ctx;

    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        priskv_mem_header *hdr = (priskv_mem_header *)(memfile->memfile_addr);
        uint64_t key_size = (sizeof(priskv_key) + hdr->max_key_length) * hdr->max_keys;
        uint64_t aligned_key_size = ALIGN_UP(key_size, PRISKV_MEM_ALIGN_UP);
        return memfile->memfile_addr + PRISKV_MEM_HEADER_SIZE + aligned_key_size;
    } else {
        return memfile->value_addr;
    }
}

int priskv_mem_shm_fd(void *ctx)
{
    priskv_mem_file *memfile = ctx;
    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        return memfile->fd;
    } else {
        return memfile->value_fd;
    }
}

uint64_t priskv_mem_shm_length(void *ctx)
{
    priskv_mem_file *memfile = ctx;
    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        return memfile->length;
    } else {
        return memfile->value_length;
    }
}

ptrdiff_t priskv_mem_value_offset(void *ctx, void *ptr)
{
    priskv_mem_file *memfile = ctx;
    uint8_t *addr = ptr;
    if (memfile->fd != PRISKV_MEM_INVALID_FD) {
        assert(addr >= priskv_mem_value_addr(ctx));
        return addr - memfile->memfile_addr;
    } else {
        assert(addr >= memfile->value_addr);
        return addr - memfile->value_addr;
    }
}

priskv_mem_info *priskv_mem_info_get(void)
{
    return &g_memory_info;
}
