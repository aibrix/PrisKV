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

#include "priskv-config.h"
#include "priskv-log.h"
#include <pthread.h>

static const char *priskv_transport_backend_names[] = {[PRISKV_TRANSPORT_BACKEND_RDMA] = "RDMA",
                                                       [PRISKV_TRANSPORT_BACKEND_UCX] = "UCX",
                                                       [PRISKV_TRANSPORT_BACKEND_MAX] = NULL};

static ucs_config_field_t priskv_client_config_table[] = {
    {"NQUEUE", "0", "Number of worker threads for client.",
     ucs_offsetof(priskv_client_config_t, nqueue),
     UCS_CONFIG_TYPE_INT},
    {"METADATA_KEY", "priskv_cluster_metadata", "Key to store cluster metadata.",
     ucs_offsetof(priskv_client_config_t, metadata_key),
     UCS_CONFIG_TYPE_STRING},
    {"METADATA_UPDATE_INTERVAL_SEC", "5", "Interval to update metadata in seconds.",
     ucs_offsetof(priskv_client_config_t, metadata_update_interval_sec),
     UCS_CONFIG_TYPE_INT},
    {"META_SERVER_CONNECT_TIMEOUT_SEC", "5", "Timeout to connect to meta server in seconds.",
     ucs_offsetof(priskv_client_config_t, meta_server_connect_timeout_sec),
     UCS_CONFIG_TYPE_INT},

    {NULL}
};

static ucs_config_field_t priskv_server_config_table[] = {
    {NULL}
};

static ucs_config_field_t priskv_config_table[] = {
    {"TRANSPORT", "RDMA", "PrisKV Transport. Supported transports are [RDMA, UCX].",
     ucs_offsetof(priskv_config_t, transport),
     UCS_CONFIG_TYPE_ENUM(priskv_transport_backend_names)},

    {"", "", NULL, ucs_offsetof(priskv_config_t, client),
     UCS_CONFIG_TYPE_TABLE(priskv_client_config_table)},

    {"", "", NULL, ucs_offsetof(priskv_config_t, server),
     UCS_CONFIG_TYPE_TABLE(priskv_server_config_table)},

    {NULL}
};

PRISKV_CONFIG_DECLARE_TABLE(priskv_config_table, "PrisKV Config", NULL, priskv_config_t);

static pthread_once_t g_priskv_config_once = PTHREAD_ONCE_INIT;
priskv_config_t g_config = {0};

static void priskv_config_init_impl(void)
{
    ucs_status_t status = priskv_config_parser_fill_opts(
        &g_config, PRISKV_CONFIG_GET_TABLE(priskv_config_table), PRISKV_ENV_PREFIX, 1);
    if (status != UCS_OK) {
        priskv_log_error("Failed to initialize config: %s\n", ucs_status_string(status));
    } else {
        ucs_config_parser_print_opts(
            stdout, "PrisKV Environment Variables", &g_config, priskv_config_table, NULL,
            PRISKV_ENV_PREFIX,
            UCS_CONFIG_PRINT_CONFIG | UCS_CONFIG_PRINT_HEADER | UCS_CONFIG_PRINT_DOC, NULL);
    }
}

void priskv_config_init(void)
{
    pthread_once(&g_priskv_config_once, priskv_config_init_impl);
}

ucs_status_t priskv_config_parser_fill_opts(void *opts, ucs_config_global_list_entry_t *entry,
                                            const char *env_prefix, int ignore_errors)
{
    return ucs_config_parser_fill_opts(opts, entry, env_prefix, ignore_errors);
}

void priskv_config_parser_release_opts(void *opts, ucs_config_field_t *fields)
{
    ucs_config_parser_release_opts(opts, fields);
}

ucs_status_t priskv_config_parser_set_value(void *opts, ucs_config_field_t *fields,
                                            const char *prefix, const char *name, const char *value)
{

    return ucs_config_parser_set_value(opts, fields, prefix, name, value);
}
