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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "priskv-log.h"

static priskv_log_level _log_level = priskv_log_warn;
static priskv_log_fn _log_fn = priskv_log_default_fn;
priskv_logger *g_default_logger = NULL;

void priskv_set_log_level(priskv_log_level level)
{
    if (level >= priskv_log_level_max) {
        return;
    }

    _log_level = level;
}

priskv_log_level priskv_get_log_level(void)
{
    return _log_level;
}

void priskv_set_log_fn(priskv_log_fn fn)
{
    _log_fn = fn;
}

const char *priskv_log_level_str[] = {
    [priskv_log_error] = "error", [priskv_log_warn] = "warn",   [priskv_log_notice] = "notice",
    [priskv_log_info] = "info",   [priskv_log_debug] = "debug", [priskv_log_level_max] = NULL,
};

static inline const char *priskv_log_level_to_str(priskv_log_level level)
{
    if (level >= priskv_log_level_max) {
        return "unknown";
    }
    return priskv_log_level_str[level];
}

void priskv_log(priskv_log_level level, const char *fmt, ...)
{
    if (level > _log_level) {
        return;
    }

    va_list ap;
    char msg[1024];
    time_t now = time(NULL);
    char nowstr[27] = {0};
    int off;

    ctime_r(&now, nowstr);
    nowstr[24] = '\0';

    /* print header part: [TIMESTAMP]LOG_LEVEL */
    off = snprintf(msg, sizeof(msg), "[%s %6s] ", nowstr, priskv_log_level_to_str(level));

    /* print real log */
    va_start(ap, fmt);
    vsnprintf(&msg[off], sizeof(msg) - off, fmt, ap);
    va_end(ap);

    _log_fn(level, msg);
}

typedef struct priskv_logger_driver {
    const char *protocol;
    int (*open)(priskv_logger *logger);
    void (*close)(priskv_logger *logger);
    void (*vsprintf)(priskv_logger *logger, priskv_log_level level, const char *fmt, va_list ap);
} priskv_logger_driver;

struct priskv_logger {
    char *filepath;
    char *protocol;
    char *path;
    void *opaque;
    priskv_logger_driver *drv;
};

static int priskv_log_stdout_open(priskv_logger *logger)
{
    return 0;
}
static void priskv_log_stdout_close(priskv_logger *logger)
{
}
static void priskv_log_stdout_vsprintf(priskv_logger *logger, priskv_log_level level, const char *fmt,
                                     va_list ap)
{
    vfprintf(stdout, fmt, ap);
    fflush(stdout);
}

static int priskv_log_stderr_open(priskv_logger *logger)
{
    return 0;
}
static void priskv_log_stderr_close(priskv_logger *logger)
{
}
static void priskv_log_stderr_vsprintf(priskv_logger *logger, priskv_log_level level, const char *fmt,
                                     va_list ap)
{
    vfprintf(stderr, fmt, ap);
    fflush(stderr);
}

static int priskv_log_null_open(priskv_logger *logger)
{
    return 0;
}
static void priskv_log_null_close(priskv_logger *logger)
{
}
static void priskv_log_null_vsprintf(priskv_logger *logger, priskv_log_level level, const char *fmt,
                                   va_list ap)
{
}

static int priskv_log_file_open(priskv_logger *logger)
{
    FILE *f = fopen(logger->path, "a");
    if (!f) {
        return -1;
    }
    logger->opaque = f;
    return 0;
}
static void priskv_log_file_close(priskv_logger *logger)
{
    FILE *f = logger->opaque;
    fclose(f);
}
static void priskv_log_file_vsprintf(priskv_logger *logger, priskv_log_level level, const char *fmt,
                                   va_list ap)
{
    FILE *f = logger->opaque;
    vfprintf(f, fmt, ap);
    fflush(f);
}

typedef struct priskv_log_multifile {
    FILE *files[priskv_log_level_max];
} priskv_log_multifile;

static int priskv_log_multifile_open(priskv_logger *logger)
{
    int i;
    priskv_log_multifile *multifile = calloc(1, sizeof(priskv_log_multifile));
    if (!multifile) {
        return -1;
    }

    char filepath[1024] = {0};
    for (i = 0; i < priskv_log_level_max; i++) {
        snprintf(filepath, sizeof(filepath), "%s.%s", logger->path, priskv_log_level_to_str(i));
        multifile->files[i] = fopen(filepath, "a");
        if (!multifile->files[i]) {
            goto err;
        }
    }

    logger->opaque = multifile;
    return 0;

err:
    for (; i >= 0; i--) {
        fclose(multifile->files[i]);
    }
    free(multifile);
    return -1;
}
static void priskv_log_multifile_close(priskv_logger *logger)
{
    priskv_log_multifile *multifile = logger->opaque;
    for (int i = 0; i < priskv_log_level_max; i++) {
        fclose(multifile->files[i]);
    }
    free(multifile);
}
static void priskv_log_multifile_vsprintf(priskv_logger *logger, priskv_log_level level, const char *fmt,
                                        va_list ap)
{
    priskv_log_multifile *multifile = logger->opaque;
    vfprintf(multifile->files[level], fmt, ap);
    fflush(multifile->files[level]);
    return;
}

typedef struct priskv_log_unix_socket {
    int fd;
    FILE *f;
} priskv_log_unix_socket;
static int priskv_log_unix_socket_open(priskv_logger *logger)
{
    priskv_log_unix_socket *socket_ctx = NULL;
    struct sockaddr_un addr;
    int fd;

    fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, logger->path, sizeof(addr.sun_path) - 1);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }

    socket_ctx = calloc(1, sizeof(priskv_log_unix_socket));
    if (!socket_ctx) {
        close(fd);
        return -1;
    }
    socket_ctx->fd = fd;
    socket_ctx->f = fdopen(fd, "w");
    if (!socket_ctx->f) {
        close(fd);
        free(socket_ctx);
        return -1;
    }
    logger->opaque = socket_ctx;

    return 0;
}
static void priskv_log_unix_socket_close(priskv_logger *logger)
{
    priskv_log_unix_socket *socket_ctx = logger->opaque;
    fclose(socket_ctx->f);
    close(socket_ctx->fd);
    free(socket_ctx);
}
static void priskv_log_unix_socket_vsprintf(priskv_logger *logger, priskv_log_level level,
                                          const char *fmt, va_list ap)
{
    priskv_log_unix_socket *socket_ctx = logger->opaque;
    vfprintf(socket_ctx->f, fmt, ap);
    fflush(socket_ctx->f);
    return;
}

static priskv_logger_driver g_log_drivers[] = {
    {"stdout", priskv_log_stdout_open, priskv_log_stdout_close, priskv_log_stdout_vsprintf},
    {"stderr", priskv_log_stderr_open, priskv_log_stderr_close, priskv_log_stderr_vsprintf},
    {"null", priskv_log_null_open, priskv_log_null_close, priskv_log_null_vsprintf},
    {"file", priskv_log_file_open, priskv_log_file_close, priskv_log_file_vsprintf},
    {"multifile", priskv_log_multifile_open, priskv_log_multifile_close, priskv_log_multifile_vsprintf},
    {"unix", priskv_log_unix_socket_open, priskv_log_unix_socket_close, priskv_log_unix_socket_vsprintf},
};

/* filepath: protocol[:path] */
static void priskv_logger_parse_filepath(priskv_logger *logger, const char *filepath)
{
    char *p = NULL;

    logger->filepath = strdup(filepath);
    p = strchr(filepath, ':');
    if (p) {
        logger->protocol = strndup(filepath, p - filepath);
        logger->path = strdup(p + 1);
    } else {
        logger->protocol = strdup(filepath);
        logger->path = NULL;
    }
}

priskv_logger *priskv_logger_new(const char *filepath)
{
    priskv_logger *logger = calloc(1, sizeof(priskv_logger));
    if (!logger) {
        return NULL;
    }

    priskv_logger_parse_filepath(logger, filepath);

    for (size_t i = 0; i < sizeof(g_log_drivers) / sizeof(priskv_logger_driver); i++) {
        if (strcmp(g_log_drivers[i].protocol, logger->protocol) == 0) {
            logger->drv = &g_log_drivers[i];
            break;
        }
    }

    if (!logger->drv || logger->drv->open(logger)) {
        free(logger);
        return NULL;
    }

    return logger;
}

void priskv_logger_free(priskv_logger *logger)
{
    if (!logger) {
        return;
    }

    if (logger->drv) {
        logger->drv->close(logger);
    }

    if (logger->filepath) {
        free(logger->filepath);
    }

    if (logger->protocol) {
        free(logger->protocol);
    }

    if (logger->path) {
        free(logger->path);
    }

    free(logger);
}

void priskv_logger_printf(priskv_logger *logger, priskv_log_level level, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    logger->drv->vsprintf(logger, level, fmt, ap);
    va_end(ap);
}
