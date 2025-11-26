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

#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/eventfd.h>
#include <errno.h>
#include <limits.h>
#include <sys/time.h>
#include <signal.h>

#include "priskv-utils.h"
#include "priskv-log.h"
#include "list.h"

#include "priskv-threads.h"
#include "priskv-event.h"
#include "priskv-workqueue.h"

#define PRISKV_THREAD_INVALID_THREAD 0xff
#define PRISKV_THREAD_MAX_THREAD (PRISKV_THREAD_INVALID_THREAD - 1)

/* Thread-local storage for priskv_thread pointer */
static __thread priskv_thread *thread_self_ptr = NULL;

struct priskv_thread {
    pthread_t thread;
    uint8_t index;
    uint32_t flags;

    int nevent; /* atomic */
    int epollfd;

    priskv_threadpool *pool;
    priskv_workqueue *wq;

    void *user_data; /* for thread-specific contexts like tiering */
} __attribute__((aligned(64)));

struct priskv_threadpool {
    uint32_t thread_flags;
    priskv_thread *iothreads;
    int niothread;
    priskv_thread *bgthreads;
    int nbgthread;
    int cur_bgthread;

    pthread_barrier_t barrier;

    struct priskv_thread_hooks *hooks;
};

static void priskv_thread_setname(priskv_thread *thd, const char *prefix, const char *infix, int index)
{
    char name[64] = {0};

    snprintf(name, sizeof(name) - 1, "%s-%s-%d", prefix, infix, index);
    pthread_setname_np(thd->thread, name);
}

static void priskv_thread_signal_handler(int signum)
{
    char name[16] = {0};
    priskv_thread *thd = thread_self_ptr;

    pthread_getname_np(pthread_self(), name, sizeof(name));
    priskv_log_notice("thread [%s] is exiting\n", name);

    if (thd && thd->pool && thd->pool->hooks && thd->pool->hooks->cleanup) {
        priskv_log_debug("Calling cleanup hook for thread [%s]\n", name);
        thd->pool->hooks->cleanup(thd, thd->pool->hooks->arg);
    }

    pthread_exit(NULL);
}

static void *priskv_thread_routine(void *arg)
{
    priskv_thread *thd = arg;
    int timeout = -1;

    thread_self_ptr = thd;

    signal(SIGUSR1, priskv_thread_signal_handler);

    thd->nevent = 0;

    thd->epollfd = epoll_create(1024);
    assert(thd->epollfd >= 0);

    thd->wq = priskv_workqueue_create(thd->epollfd);
    assert(thd->wq);

    if (thd->flags & PRISKV_THREAD_BUSY_POLL) {
        timeout = 0;
    }

    /* Call init hook if provided */
    if (thd->pool->hooks && thd->pool->hooks->init) {
        thd->pool->hooks->init(thd, thd->pool->hooks->arg);
    }

    pthread_barrier_wait(&thd->pool->barrier);

    while (1) {
        priskv_events_process(thd->epollfd, timeout);
    };

    return NULL;
}

static void priskv_thread_kill(priskv_thread *thd)
{
    pthread_kill(thd->thread, SIGUSR1);
    pthread_join(thd->thread, NULL);
}

typedef struct priskv_thread_event_handler {
    priskv_thread *thd;
    int fd;
    int val;
} priskv_thread_event_handler;

/* called by priskv_thread_call_function in work thread context */
static int __priskv_thread_mod_event_handler(void *opaque)
{
    priskv_thread_event_handler *eh = opaque;
    priskv_thread *thd = eh->thd;
    const char *op;
    int nevent;

    assert(pthread_self() == thd->thread);

    if (eh->val > 0) {
        priskv_add_event_fd(thd->epollfd, eh->fd);
        nevent = priskv_atomic_inc(&thd->nevent);
        assert(nevent <= INT_MAX);
        op = "Add";
    } else {
        priskv_del_event(thd->epollfd, eh->fd);
        nevent = priskv_atomic_dec(&thd->nevent);
        assert(nevent >= 0);
        op = "Del";
    }

    priskv_log_debug("Thread[%d]: %s fd[%d], nevent %d\n", thd->index, op, eh->fd, nevent);

    return 0;
}

static int priskv_thread_mod_event_handler(priskv_thread *thread, int fd, int val)
{
    priskv_thread_event_handler eh;

    eh.thd = thread;
    eh.fd = fd;
    eh.val = val;

    return priskv_thread_call_function(thread, __priskv_thread_mod_event_handler, &eh);
}

int priskv_thread_add_event_handler(priskv_thread *thread, int fd)
{
    return priskv_thread_mod_event_handler(thread, fd, 1);
}

int priskv_thread_del_event_handler(priskv_thread *thread, int fd)
{
    return priskv_thread_mod_event_handler(thread, fd, -1);
}

int priskv_thread_call_function(priskv_thread *thread, int (*func)(void *arg), void *arg)
{
    struct timeval start, end;

    gettimeofday(&start, NULL);

    int ret = priskv_workqueue_call(thread->wq, func, arg);

    gettimeofday(&end, NULL);
    priskv_log_debug("Thread[%d]: call function delay %d us\n", thread->index,
                   priskv_time_elapsed_us(&start, &end));

    return ret;
}

void priskv_thread_submit_function(priskv_thread *thread, int (*func)(void *arg), void *arg)
{
    priskv_workqueue_submit(thread->wq, func, arg);
}

void priskv_thread_set_user_data(priskv_thread *thread, void *user_data)
{
    thread->user_data = user_data;
}

void *priskv_thread_get_user_data(priskv_thread *thread)
{
    return thread->user_data;
}

int priskv_thread_get_epollfd(priskv_thread *thread)
{
    return thread->epollfd;
}

priskv_threadpool *priskv_threadpool_create(const char *prefix, int niothread, int nbgthread, int flags)
{
    return priskv_threadpool_create_with_hooks(prefix, niothread, nbgthread, flags, NULL);
}

priskv_threadpool *priskv_threadpool_create_with_hooks(const char *prefix, int niothread, int nbgthread,
                                                   int flags, const struct priskv_thread_hooks *hooks)
{
    priskv_threadpool *pool;

    if (!niothread || niothread > PRISKV_THREAD_MAX_THREAD) {
        priskv_log_error("Thread: invalid niothread = %d, should be [1, %d]\n", niothread,
                       PRISKV_THREAD_MAX_THREAD);
        return NULL;
    }

    if (nbgthread > PRISKV_THREAD_MAX_THREAD) {
        priskv_log_error("Thread: invalid nbgthread = %d, should be [0, %d]\n", nbgthread,
                       PRISKV_THREAD_MAX_THREAD);
        return NULL;
    }

    pool = calloc(1, sizeof(priskv_threadpool));
    assert(pool);

    pool->thread_flags = flags;
    pool->niothread = niothread;
    pool->iothreads = calloc(niothread, sizeof(priskv_thread));
    assert(pool->iothreads);

    pool->nbgthread = nbgthread;
    pool->bgthreads = calloc(nbgthread, sizeof(priskv_thread));
    assert(pool->bgthreads);
    pool->cur_bgthread = 0;

    pthread_barrier_init(&pool->barrier, NULL, niothread + nbgthread + 1);
    pool->hooks = hooks;

    for (int i = 0; i < niothread; i++) {
        pool->iothreads[i].index = i;
        pool->iothreads[i].flags = flags;
        pool->iothreads[i].pool = pool;
        assert(!pthread_create(&pool->iothreads[i].thread, NULL, priskv_thread_routine,
                               &pool->iothreads[i]));

        priskv_thread_setname(&pool->iothreads[i], prefix, "work", i);
    }

    for (int i = 0; i < nbgthread; i++) {
        pool->bgthreads[i].index = i;
        pool->bgthreads[i].flags = flags;
        pool->bgthreads[i].pool = pool;
        assert(!pthread_create(&pool->bgthreads[i].thread, NULL, priskv_thread_routine,
                               &pool->bgthreads[i]));

        priskv_thread_setname(&pool->bgthreads[i], prefix, "bg", i);
    }

    pthread_barrier_wait(&pool->barrier);

    return pool;
}

void priskv_threadpool_destroy(priskv_threadpool *pool)
{
    if (!pool) {
        return;
    }

    for (int i = 0; i < pool->niothread; i++) {
        priskv_thread_kill(&pool->iothreads[i]);
    }

    for (int i = 0; i < pool->nbgthread; i++) {
        priskv_thread_kill(&pool->bgthreads[i]);
    }

    pthread_barrier_destroy(&pool->barrier);

    free(pool->iothreads);
    free(pool->bgthreads);
    free(pool);
}

priskv_thread *priskv_threadpool_get_iothread(priskv_threadpool *pool, int index)
{
    if (index < 0 || index >= pool->niothread) {
        return NULL;
    }

    return pool->iothreads + index;
}

priskv_thread *priskv_threadpool_get_bgthread(priskv_threadpool *pool, int index)
{
    if (index < 0 || index >= pool->nbgthread) {
        return NULL;
    }

    return pool->bgthreads + index;
}

int priskv_threadpool_for_each_iothread(priskv_threadpool *pool, priskv_threadpool_iter_cb cb, void *arg)
{
    int ret = 0;

    for (int i = 0; i < pool->niothread; i++) {
        priskv_thread *thread = &pool->iothreads[i];
        ret = cb(thread, arg);
        if (ret != 0) {
            break;
        }
    }

    return ret;
}

/* find the idlest iothread */
priskv_thread *priskv_threadpool_find_iothread(priskv_threadpool *pool)
{
    int min_event = INT_MAX;
    priskv_thread *thread = NULL;

    for (uint8_t i = 0; i < pool->niothread; i++) {
        priskv_thread *thd = pool->iothreads + i;

        int thd_nevent = priskv_atomic_get(&thd->nevent);
        if (thd_nevent < min_event) {
            min_event = thd_nevent;
            thread = thd;
        }
    }

    return thread;
}

/* return background threads in a round-robin manner */
priskv_thread *priskv_threadpool_find_bgthread(priskv_threadpool *pool)
{
    int index = (pool->cur_bgthread++) % pool->nbgthread;

    return pool->bgthreads + index;
}
