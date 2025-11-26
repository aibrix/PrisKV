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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "priskv-utils.h"
#include "priskv-log.h"

#include "acl.h"

struct list_head acl_list = LIST_HEAD_INIT(acl_list);
pthread_rwlock_t acl_mutex = PTHREAD_RWLOCK_INITIALIZER;
static const char *acl_any = "any";

static int priskv_acl_addr(const char *addr, struct sockaddr *saddr)
{
    struct addrinfo hints = {0}, *res = NULL;
    const char *_port = "0";
    int ret;

    hints.ai_flags = AI_PASSIVE;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    ret = getaddrinfo(addr, _port, &hints, &res);
    if (ret) {
        priskv_log_error("ACL: getaddrinfo %s failed: %d", addr, ret);
        return -EINVAL;
    } else if (!res) {
        priskv_log_error("ACL: getaddrinfo %s: no availabe address", addr);
        return -EINVAL;
    }

    memcpy(saddr, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);

    return 0;
}

static bool __priskv_acl_verify_addr(const uint8_t *saddr, const uint8_t *daddr, const int mask_bits)
{
    int bytes = mask_bits / 8;
    int bits = mask_bits % 8;

    for (int i = 0; i < bytes; i++) {
        if (saddr[i] != daddr[i]) {
            return false;
        }
    }

    if (bits) {
        uint8_t m = 0;
        for (int k = 0; k < bits; k++) {
            m |= (1 << k);
        }

        m = ~m;
        return (saddr[bytes] & m) == (daddr[bytes] & m);
    }

    return true;
}

static bool priskv_acl_verify_addr(const struct sockaddr *saddr, const struct sockaddr *daddr,
                                 const int mask_bits)
{
    if (saddr->sa_family != daddr->sa_family) {
        return false;
    }

    if (saddr->sa_family == AF_INET) {
        struct sockaddr_in *ssa4 = (struct sockaddr_in *)saddr;
        struct sockaddr_in *dsa4 = (struct sockaddr_in *)daddr;
        return __priskv_acl_verify_addr((uint8_t *)&ssa4->sin_addr, (uint8_t *)&dsa4->sin_addr,
                                      mask_bits);
    } else if (saddr->sa_family == AF_INET6) {
        struct sockaddr_in6 *ssa6 = (struct sockaddr_in6 *)saddr;
        struct sockaddr_in6 *dsa6 = (struct sockaddr_in6 *)daddr;
        return __priskv_acl_verify_addr(ssa6->sin6_addr.__in6_u.__u6_addr8,
                                      dsa6->sin6_addr.__in6_u.__u6_addr8, mask_bits);
    } else {
        return false;
    }

    return true;
}

int priskv_acl_add(const char *rule)
{
    priskv_acl *acl;
    char *tmp, *addr = NULL;
    int ret;

    acl = calloc(1, sizeof(priskv_acl));
    assert(acl);

    if (!strcasecmp(rule, acl_any)) {
        goto insert;
    }

    tmp = strchr(rule, '/');
    if (tmp) {
        addr = strndup(rule, tmp - rule);
        acl->mask_bits = atoi(tmp + 1);
    } else {
        addr = strdup(rule);
        acl->mask_bits = 0;
    }

    ret = priskv_acl_addr(addr, &acl->addr);
    if (ret) {
        goto error;
    }

    if (acl->addr.sa_family == AF_INET) {
        if (acl->mask_bits) {
            if (acl->mask_bits > 32) {
                priskv_log_error("ACL: mask must be <= 32 for IPv4\n");
                ret = -EINVAL;
                goto error;
            }
        } else {
            acl->mask_bits = 32;
        }
    } else if (acl->addr.sa_family == AF_INET6) {
        if (acl->mask_bits) {
            if (acl->mask_bits > 128) {
                priskv_log_error("ACL: mask must be <= 128 for IPv6\n");
                ret = -EINVAL;
                goto error;
            }
        } else {
            acl->mask_bits = 128;
        }
    } else {
        priskv_log_error("ACL: IPv4/IPv6 supported only\n");
        goto error;
    }

insert:
    acl->rule = strdup(rule);
    list_node_init(&acl->entry);

    pthread_rwlock_wrlock(&acl_mutex);
    list_add_tail(&acl_list, &acl->entry);
    pthread_rwlock_unlock(&acl_mutex);
    priskv_log_notice("ACL: add %s\n", acl->rule);

    ret = 0;
    goto out;

error:
    free(acl->rule);
    free(acl);

out:
    free(addr);

    return ret;
}

int priskv_acl_del(const char *rule)
{
    priskv_acl *acl;

    pthread_rwlock_wrlock(&acl_mutex);
    list_for_each (&acl_list, acl, entry) {
        if (!strcasecmp(acl->rule, rule)) {
            list_del(&acl->entry);
            pthread_rwlock_unlock(&acl_mutex);
            priskv_log_notice("ACL: delete %s\n", acl->rule);
            free(acl->rule);
            free(acl);
            return 0;
        }
    }
    pthread_rwlock_unlock(&acl_mutex);

    return -ENOENT;
}

int priskv_acl_verify(const struct sockaddr *addr)
{
    priskv_acl *acl;

    pthread_rwlock_rdlock(&acl_mutex);
    list_for_each (&acl_list, acl, entry) {
        if (!strcasecmp(acl->rule, acl_any)) {
            pthread_rwlock_unlock(&acl_mutex);
            priskv_log_info("ACL: verify success on %s\n", acl->rule);
            return 0;
        }

        if (priskv_acl_verify_addr(&acl->addr, addr, acl->mask_bits)) {
            pthread_rwlock_unlock(&acl_mutex);
            priskv_log_info("ACL: verify success on %s\n", acl->rule);
            return 0;
        }
    }
    pthread_rwlock_unlock(&acl_mutex);

    return -EACCES;
}

/* used by test program */
int __priskv_acl_verify(const char *addr)
{
    struct sockaddr_in6 saddr;
    int ret;

    ret = priskv_acl_addr(addr, (struct sockaddr *)&saddr);
    if (ret) {
        return ret;
    }

    return priskv_acl_verify((const struct sockaddr *)&saddr);
}

char **priskv_acl_get_rules(int *nrules)
{
    priskv_acl *acl;
    int i = 0;
    char **rules = NULL;

    if (!nrules) {
        return NULL;
    }

    *nrules = 0;

    list_for_each (&acl_list, acl, entry) {
        (*nrules)++;
    }

    rules = realloc(rules, sizeof(char *) * (*nrules));
    list_for_each (&acl_list, acl, entry) {
        rules[i++] = strdup(acl->rule);
    }

    return rules;
}

void priskv_acl_free_rules(char **rules, int nrules)
{
    for (int i = 0; i < nrules; i++) {
        free(rules[i]);
    }
    free(rules);
}
