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

#ifndef __PRISKV_LOG__
#define __PRISKV_LOG__

#include <time.h>
#include <stdio.h>
#include <stdarg.h>

#if defined(__cplusplus)
extern "C"
{
#endif

#include <stdio.h>

typedef enum priskv_log_level {
    priskv_log_error,
    priskv_log_warn,
    priskv_log_notice,
    priskv_log_info,
    priskv_log_debug,
    priskv_log_level_max,
} priskv_log_level;

typedef void (*priskv_log_fn)(priskv_log_level level, const char *msg);
void priskv_set_log_fn(priskv_log_fn fn);
void priskv_set_log_level(priskv_log_level level);
priskv_log_level priskv_get_log_level(void);
void priskv_log(priskv_log_level level, const char *fmt, ...);

static inline void priskv_log_default_fn(priskv_log_level level, const char *msg)
{
    fprintf(stdout, "%s", msg);
}

#define priskv_log_error(fmt, ...) priskv_log(priskv_log_error, fmt, ##__VA_ARGS__)

#define priskv_log_warn(fmt, ...) priskv_log(priskv_log_warn, fmt, ##__VA_ARGS__)

#define priskv_log_notice(fmt, ...) priskv_log(priskv_log_notice, fmt, ##__VA_ARGS__)

#define priskv_log_info(fmt, ...) priskv_log(priskv_log_info, fmt, ##__VA_ARGS__)

#define priskv_log_debug(fmt, ...) priskv_log(priskv_log_debug, fmt, ##__VA_ARGS__)

typedef struct priskv_logger priskv_logger;
priskv_logger *priskv_logger_new(const char *filename);
void priskv_logger_free(priskv_logger *logger);
void priskv_logger_printf(priskv_logger *logger, priskv_log_level level, const char *fmt, ...);
#define PRISKV_LOGGER_HELP(__prefix)                                                                 \
    __prefix "log FILEPATH options:\n" __prefix "    (stdout)\t\tprint to stdout\n" __prefix       \
             "    (stderr)\t\tprint to stderr\n" __prefix                                          \
             "    (null)\t\tdiscard all logs\n" __prefix                                           \
             "    (file:PATH)\t\tprint to file PATH\n" __prefix                                    \
             "    (multifile:PATH)\tprint to multiple files based on log level\n" __prefix         \
             "    (unix:PATH)\t\tprint to unix socket\n"

extern const char *priskv_log_level_str[];
extern priskv_logger *g_default_logger;

static inline void priskv_logger_default_fn(priskv_log_level level, const char *msg)
{
    priskv_logger_printf(g_default_logger, level, "%s", msg);
}

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_LOG__ */
