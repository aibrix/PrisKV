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

#ifndef __PRISKV_UTILS__
#define __PRISKV_UTILS__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <unistd.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <errno.h>
#include <uuid/uuid.h>
#include <stdlib.h>
#include <netdb.h>
#include <sys/poll.h>

#ifndef offsetof
#define offsetof(TYPE, MEMBER) ((size_t)&((TYPE *)0)->MEMBER)
#endif

#ifndef container_of
#define container_of(ptr, type, member)                                                            \
    ({                                                                                             \
        const typeof(((type *)0)->member) *__mptr = (ptr);                                         \
        (type *)((char *)__mptr - offsetof(type, member));                                         \
    })
#endif

#define ALIGN_UP(x, ALIGNMENT) (((x) + (ALIGNMENT - 1)) & ~(ALIGNMENT - 1))

#define ALIGN_DOWN(x, ALIGNMENT) ((x) & ~(ALIGNMENT - 1))

#ifndef DIV_ROUND_UP
#define DIV_ROUND_UP(n, d) (((n) + (d) - 1) / (d))
#endif

#define IS_POWER_OF_2(x) (!((x) & ((x) - 1)))

#define PRISKV_BUILD_BUG_ON(cond) ((void)sizeof(char[1 - 2 * !!(cond)]))

static inline int priskv_set_nonblock(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    return fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

static inline int priskv_set_block(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);

    return fcntl(fd, F_SETFL, flags & ~O_NONBLOCK);
}

static inline int priskv_add_event_fd(int epollfd, int fd)
{
    struct epoll_event event = {0};

    /* don't add EPOLLOUT by default */
    event.events = EPOLLIN | EPOLLET;
    event.data.fd = fd;

    return epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
}

static inline int priskv_del_event(int epollfd, int fd)
{
    struct epoll_event event = {0};

    return epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, &event);
}

static inline uint32_t priskv_min_u16(uint16_t a, uint16_t b)
{
    return a < b ? a : b;
}

static inline uint32_t priskv_min_u32(uint32_t a, uint32_t b)
{
    return a < b ? a : b;
}

static inline int priskv_atomic_inc(int *ptr)
{
    return __atomic_add_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

static inline int priskv_atomic_dec(int *ptr)
{
    return __atomic_sub_fetch(ptr, 1, __ATOMIC_SEQ_CST);
}

static inline int priskv_atomic_get(int *ptr)
{
    return __atomic_load_n(ptr, __ATOMIC_SEQ_CST);
}

#define PRISKV_ADDR_LEN (INET6_ADDRSTRLEN + 7)
static inline void priskv_inet_ntop(struct sockaddr *addr, char *dst)
{
    char port[7] = {0}; /* ":65536" */

    if (addr->sa_family == AF_INET) {
        struct sockaddr_in *sa4 = (struct sockaddr_in *)addr;
        inet_ntop(AF_INET, &sa4->sin_addr, dst, PRISKV_ADDR_LEN);
        sprintf(port, ":%d", ntohs(sa4->sin_port));
        strcat(dst, port);
    } else if (addr->sa_family == AF_INET6) {
        struct sockaddr_in6 *sa6 = (struct sockaddr_in6 *)addr;
        inet_ntop(AF_INET6, &sa6->sin6_addr, dst, PRISKV_ADDR_LEN);
        sprintf(port, ":%d", ntohs(sa6->sin6_port));
        strcat(dst, port);
    } else {
        sprintf(dst, "%s", "Unknown address");
    }
}

static inline int priskv_sock_io(int sock, ssize_t (*sock_call)(int, void *, size_t, int),
                                 int poll_events, void *data, size_t size,
                                 void (*progress)(void *arg), void *arg, const char *name)
{
    size_t total = 0;
    struct pollfd pfd;
    int ret;

    while (total < size) {
        pfd.fd = sock;
        pfd.events = poll_events;
        pfd.revents = 0;

        ret = poll(&pfd, 1, 1); /* poll for 1ms */
        if (ret > 0) {
            ret = sock_call(sock, (char *)data + total, size - total, 0);
            if ((ret == 0) && (poll_events & POLLIN)) {
                return -1;
            }
            if (ret < 0) {
                return -1;
            }
            total += ret;
        } else if ((ret < 0) && (errno != EINTR)) {
            return -1;
        }

        /* progress user context */
        if (progress != NULL) {
            progress(arg);
        }
    }
    return 0;
}

static inline int priskv_safe_send(int sock, void *data, size_t size, void (*progress)(void *arg),
                                   void *arg)
{
    typedef ssize_t (*sock_call)(int, void *, size_t, int);

    return priskv_sock_io(sock, (sock_call)send, POLLOUT, data, size, progress, arg, "send");
}

static inline int priskv_safe_recv(int sock, void *data, size_t size, void (*progress)(void *arg),
                                   void *arg)
{
    return priskv_sock_io(sock, recv, POLLIN, data, size, progress, arg, "recv");
}

static inline unsigned long priskv_rdtsc(void)
{
    unsigned long low, high;

    asm volatile("rdtsc" : "=a"(low), "=d"(high));

    return ((low) | (high) << 32);
}

static inline unsigned long priskv_random(void)
{
    unsigned long r0, r1;

    r0 = priskv_rdtsc() * 2147483647 + 2147483593;
    r1 = priskv_rdtsc() * 2147483593 + 2147483647;

    return r0 ^ r1;
}

static inline void priskv_random_string(uint8_t *ptr, uint32_t size)
{
    static const uint8_t charset[] =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    int cs_size = sizeof(charset) - 1;
    unsigned long r;
    uint32_t i;

    ptr[size - 1] = '\0';
    size--;

    for (i = 0; i < (size / 8) * 8; i += 8) {
        r = priskv_random();

        ptr[i] = charset[(uint8_t)r % cs_size];
        ptr[i + 1] = charset[((uint8_t)(r >> 8)) % cs_size];
        ptr[i + 2] = charset[((uint8_t)(r >> 16)) % cs_size];
        ptr[i + 3] = charset[((uint8_t)(r >> 24)) % cs_size];

        r = ~r;
        ptr[i + 4] = charset[(uint8_t)r % cs_size];
        ptr[i + 5] = charset[(uint8_t)(r >> 8) % cs_size];
        ptr[i + 6] = charset[(uint8_t)(r >> 16) % cs_size];
        ptr[i + 7] = charset[(uint8_t)(r >> 24) % cs_size];
    }

    for (; i < size; i++) {
        ptr[i] = charset[rand() % cs_size];
    }
}

static inline void priskv_uuid(uint8_t *ptr, uint32_t size)
{
    uuid_t uuid;
    char uuid_str[37];

    uuid_generate_random(uuid);

    uuid_unparse(uuid, uuid_str);

    if (size > 37) {
        memcpy(ptr, uuid_str, 36);
        memset(ptr + 36, 'A', size - 37);
    } else {
        memcpy(ptr, uuid_str, size - 1);
    }
    ptr[size - 1] = '\0';
}

static inline void priskv_string_shorten(const char *src, int src_len, char *dst, int dst_len)
{
    if (src_len >= dst_len) {
        strncpy(dst, src, dst_len - 1);
        dst[dst_len - 1] = '\0';
        if (dst_len > 6) {
            strncpy(dst + dst_len - 4, "...", 4);
        }
    } else {
        strncpy(dst, src, src_len);
        dst[src_len] = '\0';
    }
}

static inline long priskv_time_elapsed_us(const struct timeval *start, const struct timeval *end)
{
    return (end->tv_sec - start->tv_sec) * 1000000L + (end->tv_usec - start->tv_usec);
}

static inline long priskv_time_elapsed_ms(struct timeval start, struct timeval end)
{
    return (end.tv_sec - start.tv_sec) * 1000 + (end.tv_usec - start.tv_usec) / 1000;
}

static inline void priskv_time_add_ms(struct timeval *timeval, uint64_t milliseconds)
{
    long usec = timeval->tv_usec + (milliseconds % 1000) * 1000;
    timeval->tv_sec += milliseconds / 1000 + usec / 1000000;
    timeval->tv_usec = usec % 1000000;
}

static inline int64_t priskv_memcmp64(uint8_t *s, uint8_t val, uint64_t size)
{
    uint64_t aligned_size = size / 8 * 8, i;
    uint64_t val64;

    memset(&val64, val, sizeof(val64));

    for (i = 0; i < aligned_size; i += 8) {
        uint64_t *s64 = (uint64_t *)(s + i);
        if (*s64 != val64) {
            return *s64 - val64;
        }
    }

    for (; i < size; i++) {
        if (*(s + i) != val) {
            return *(s + i) - val;
        }
    }

    return 0;
}

static inline int __priskv_get_mult_bytes(const char *p, int64_t *result)
{
    int64_t mult = 1024;
    int i, j = 0, pow = 0;
    char *c;
    int ret = 0;
    size_t len = strlen(p);

    *result = 1;

    if (!p || !len) {
        return ret;
    }

    c = strdup(p);
    for (i = 0; i < len; i++) {
        if (c[i] != ' ') {
            c[j++] = tolower((unsigned char)c[i]);
        }
    }
    c[j] = '\0';
    len = j;

    if ((!strncmp("p", c, 1) && len == 1) || (!strncmp("pb", c, 2) && len == 2) ||
        (!strncmp("pib", c, 3) && len == 3)) {
        pow = 5;
    } else if ((!strncmp("t", c, 1) && len == 1) || (!strncmp("tb", c, 2) && len == 2) ||
               (!strncmp("tib", c, 3) && len == 3)) {
        pow = 4;
    } else if ((!strncmp("g", c, 1) && len == 1) || (!strncmp("gb", c, 2) && len == 2) ||
               (!strncmp("gib", c, 3) && len == 3)) {
        pow = 3;
    } else if ((!strncmp("m", c, 1) && len == 1) || (!strncmp("mb", c, 2) && len == 2) ||
               (!strncmp("mib", c, 3) && len == 3)) {
        pow = 2;
    } else if ((!strncmp("k", c, 1) && len == 1) || (!strncmp("kb", c, 2) && len == 2) ||
               (!strncmp("kib", c, 3) && len == 3)) {
        pow = 1;
    } else {
        printf("invalid unit %s\n", c);
        ret = -1;
        goto out;
    }

    while (pow--) {
        *result *= mult;
    }

out:
    free(c);
    return ret;
}

static inline int priskv_str2num(const char *str, int64_t *result)
{
    char *unit = NULL;
    int64_t mult = 1;

    *result = strtoll(str, &unit, 10);
    if (*result == 0 && unit == str) {
        return -1;
    }
    if (errno == ERANGE) {
        return -1;
    }

    if (__priskv_get_mult_bytes(unit, &mult) < 0) {
        return -1;
    }

    *result *= mult;

    return 0;
}

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_UTILS__ */
