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

#ifndef __PRISKV_CONFIG__
#define __PRISKV_CONFIG__

#if defined(__cplusplus)
extern "C"
{
#endif

#include <ucs/config/parser.h>
#include <ucs/config/types.h>
#include <ucs/sys/compiler_def.h>
#include <ucs/sys/preprocessor.h>

/**
 * @brief Declare config table.
 * 
 * @param TABLE Config table.
 * @param NAME Name of config table.
 * @param PREFIX Prefix of config table.
 * @param TYPE Type of config table.
 */
#define PRISKV_CONFIG_DECLARE_TABLE(TABLE, NAME, PREFIX, TYPE)                                     \
    static ucs_config_global_list_entry_t g_##TABLE##_config_entry = {.name = NAME,                \
                                                                      .prefix = PREFIX,            \
                                                                      .table = TABLE,              \
                                                                      .size = sizeof(TYPE),        \
                                                                      .list = {NULL, NULL},        \
                                                                      .flags = 0};

#define PRISKV_CONFIG_GET_TABLE(TABLE) &g_##TABLE##_config_entry
#define PRISKV_ENV_PREFIX "PRISKV_"

/**
 * @brief Initialize config.
 */
void priskv_config_init(void);

/**
 * @brief Fill options from config entry.
 * 
 * @param opts Options to fill.
 * @param entry Config entry.
 * @param env_prefix Prefix of environment variables.
 * @param ignore_errors Ignore errors if set to 1.
 * @return ucs_status_t UCS_OK on success, otherwise error.
 */
ucs_status_t priskv_config_parser_fill_opts(void *opts, ucs_config_global_list_entry_t *entry,
                                            const char *env_prefix, int ignore_errors);

/**
 * @brief Release options.
 * 
 * @param opts Options to release.
 * @param fields Config fields.
 */
void priskv_config_parser_release_opts(void *opts, ucs_config_field_t *fields);

/**
 * @brief Set value for option.
 * 
 * @param opts Options to set value.
 * @param fields Config fields.
 * @param prefix Prefix of option.
 * @param name Name of option.
 * @param value Value of option.
 * @return ucs_status_t UCS_OK on success, otherwise error.
 */
ucs_status_t priskv_config_parser_set_value(void *opts, ucs_config_field_t *fields,
                                            const char *prefix, const char *name,
                                            const char *value);

typedef enum priskv_transport_backend {
    PRISKV_TRANSPORT_BACKEND_RDMA,
    PRISKV_TRANSPORT_BACKEND_UCX,
    PRISKV_TRANSPORT_BACKEND_MAX,
} priskv_transport_backend;

typedef struct priskv_client_config {
    int nqueue;
    int metadata_update_interval_sec;
    char *metadata_key;
    int meta_server_connect_timeout_sec;
} priskv_client_config_t;

typedef struct priskv_server_config {

} priskv_server_config_t;

typedef struct priskv_config {
    priskv_transport_backend transport;
    priskv_client_config_t client;
    priskv_server_config_t server;
} priskv_config_t;

extern priskv_config_t g_config;

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_CONFIG__ */
