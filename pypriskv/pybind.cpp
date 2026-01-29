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

#include <string>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>

extern "C" {
    #include "../cluster/client/client.h"
}

namespace py = pybind11;

typedef struct priskv_sgl_wrapper {
    uint64_t iova;
    uint32_t length;
    uintptr_t mem;
} priskv_sgl_wrapper;


uintptr_t priskv_connect_wrapper(std::string raddr, int rport, std::string password)
{
    return (uintptr_t)priskvClusterConnect(raddr.c_str(), rport, password.c_str());
}

void priskv_close_wrapper(uintptr_t client)
{
    priskvClusterClose((priskvClusterClient *)client);
}

uintptr_t priskv_reg_memory_wrapper(uintptr_t client, uint64_t offset, size_t length, uint64_t iova,
                                  int fd)
{
    return (uintptr_t)priskvClusterRegMemory((priskvClusterClient *)client, offset, length, iova, fd);
}

void priskv_dereg_memory_wrapper(uintptr_t mem)
{
    priskvClusterDeregMemory((priskvClusterMemory *)mem);
}

int priskv_set_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint64_t timeout)
{
    priskvClusterSGL sgl;

    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskvClusterMemory *)sgl_wrapper->mem;
    return priskvClusterSet((priskvClusterClient *)client, key.c_str(), &sgl, nsgl, timeout);
}

int priskv_setstr_wrapper(uintptr_t client, std::string key, std::string value, uint64_t timeout)
{
    int ret;
    priskvClusterSGL sgl;
    uint32_t length = value.length();
    void *ptr = (void *)value.c_str();
    priskvClusterMemory *reg_mr = priskvClusterRegMemory((priskvClusterClient *)client, (uint64_t)ptr, length, (uint64_t)ptr, -1);

    sgl.iova = (uint64_t)ptr;
    sgl.length = value.length();
    sgl.mem = reg_mr;
    ret = priskvClusterSet((priskvClusterClient *)client, key.c_str(), &sgl, 1, timeout);

    priskvClusterDeregMemory(reg_mr);
    return ret;
}

int priskv_mset_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                      const std::vector<priskv_sgl_wrapper> &sgl_wrappers, uint64_t timeout, std::vector<uint32_t> &mset_status)
{
    priskvClusterSGL sgl;
    int ret = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i >= sgl_wrappers.size())
            break;

        const auto &key = keys[i];
        const auto &wrapper = sgl_wrappers[i];

        sgl.iova = wrapper.iova;
        sgl.length = wrapper.length;
        sgl.mem = (priskvClusterMemory *)wrapper.mem;

        int res = priskvClusterSet((priskvClusterClient *)client, key.c_str(), &sgl, 1, timeout);
        if (res != 0) {
            ret = res;
            mset_status[i] = res;
        }
    }

    return ret;
}

std::tuple<int, uint64_t>
priskv_mset_and_pin_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                            const std::vector<priskv_sgl_wrapper> &sgl_wrappers, uint64_t timeout,
                            std::vector<uint32_t> &mset_status)
{
    priskvClusterSGL sgl;
    int ret = 0;
    uint64_t pin_token = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        if (i >= sgl_wrappers.size())
            break;

        const auto &key = keys[i];
        const auto &wrapper = sgl_wrappers[i];

        sgl.iova = wrapper.iova;
        sgl.length = wrapper.length;
        sgl.mem = (priskvClusterMemory *)wrapper.mem;

        int res = priskvClusterSetAndPin((priskvClusterClient *)client, key.c_str(), &sgl, 1,
                                         timeout, &pin_token);
        if (res != 0) {
            ret = res;
            mset_status[i] = res;
        }
    }

    return std::make_tuple(ret, pin_token);
}

int priskv_get_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint32_t *valuelen)
{
    priskvClusterSGL sgl;

    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskvClusterMemory *)sgl_wrapper->mem;
    return priskvClusterGet((priskvClusterClient *)client, key.c_str(), &sgl, nsgl, valuelen);
}

std::string priskv_getstr_wrapper(uintptr_t client, std::string key)
{
    priskvClusterSGL sgl;
    int ret;
    uint32_t valuelen;
    priskvClusterMemory *reg_mr;
    ret = priskvClusterTest((priskvClusterClient *)client, key.c_str(), &valuelen);
    if (ret != 0 || valuelen == 0) {
        return std::string();
    }

    std::string value = std::string(valuelen, '\0');
    reg_mr = priskvClusterRegMemory((priskvClusterClient *)client, (uint64_t)value.c_str(), valuelen, (uint64_t)value.c_str(), -1);
    sgl.iova = (uint64_t)value.c_str();
    sgl.length = valuelen;
    sgl.mem = reg_mr;
    ret = priskvClusterGet((priskvClusterClient *)client, key.c_str(), &sgl, 1, &valuelen);
    priskvClusterDeregMemory(reg_mr);

    if (ret == 0) {
        return value;
    } else {
        return std::string();
    }
}

int priskv_mget_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                      const std::vector<priskv_sgl_wrapper> &sgl_wrappers,
                      std::vector<uint32_t> &value_lengths, std::vector<uint32_t> &mget_status)
{
    priskvClusterSGL sgl;
    int ret = 0;
    value_lengths.resize(keys.size(), 0);

    for (size_t i = 0; i < keys.size(); ++i) {
        if (i >= sgl_wrappers.size())
            break;

        const auto &key = keys[i];
        const auto &wrapper = sgl_wrappers[i];

        sgl.iova = wrapper.iova;
        sgl.length = wrapper.length;
        sgl.mem = (priskvClusterMemory *)wrapper.mem;

        int res =
            priskvClusterGet((priskvClusterClient *)client, key.c_str(), &sgl, 1, &value_lengths[i]);

        if (res != 0) {
            mget_status[i] = res;
            ret =  res;
        }
    }

    return ret;
}

enum PIN_MOD { PIN = 0, UNPIN = 1 };

int priskv_mget_base(uintptr_t client, const std::vector<std::string> &keys,
                     const std::vector<priskv_sgl_wrapper> &sgl_wrappers,
                     std::vector<uint32_t> &value_lengths, std::vector<uint32_t> &mget_status,
                     uint64_t &token, PIN_MOD pin_mod)
{
    priskvClusterSGL sgl;
    int ret = 0;
    value_lengths.resize(keys.size(), 0);

    for (size_t i = 0; i < keys.size(); ++i) {
        if (i >= sgl_wrappers.size())
            break;

        const auto &key = keys[i];
        const auto &wrapper = sgl_wrappers[i];

        sgl.iova = wrapper.iova;
        sgl.length = wrapper.length;
        sgl.mem = (priskvClusterMemory *)wrapper.mem;
        int res = -1;
        switch (pin_mod) {
        case PIN:
            res = priskvClusterGetAndPin((priskvClusterClient *)client, key.c_str(), &sgl, 1,
                                         &value_lengths[i], &token);
            break;
        case UNPIN:
            res = priskvClusterGetAndUnPin((priskvClusterClient *)client, key.c_str(), &sgl, 1,
                                           &value_lengths[i], &token);
            break;
        default:
            break;
        }
        if (res != 0) {
            mget_status[i] = res;
            ret = res;
        }
    }

    return ret;
}

int priskv_mget_and_pin_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                                const std::vector<priskv_sgl_wrapper> &sgl_wrappers,
                                std::vector<uint32_t> &value_lengths,
                                std::vector<uint32_t> &mget_status, uint64_t &token)
{
    return priskv_mget_base(client, keys, sgl_wrappers, value_lengths, mget_status, token,
                            PIN_MOD::PIN);
}

int priskv_mget_and_unpin_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                                  const std::vector<priskv_sgl_wrapper> &sgl_wrappers,
                                  std::vector<uint32_t> &value_lengths,
                                  std::vector<uint32_t> &mget_status, uint64_t &token)
{
    return priskv_mget_base(client, keys, sgl_wrappers, value_lengths, mget_status, token,
                            PIN_MOD::UNPIN);
}

int priskv_test_wrapper(uintptr_t client, std::string key)
{
    uint32_t valuelen;
    return priskvClusterTest((priskvClusterClient *)client, key.c_str(), &valuelen);
}

int priskv_mtest_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                       std::vector<uint32_t> &mtest_status)
{
    int ret = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];
        uint32_t valuelen;
        int res = priskvClusterTest((priskvClusterClient *)client, key.c_str(), &valuelen);

        if (res != 0) {
            mtest_status[i] = res;
            ret = res;
        }
    }

    return ret;
}

int priskv_delete_wrapper(uintptr_t client, std::string key)
{
    return priskvClusterDelete((priskvClusterClient *)client, key.c_str());
}

int priskv_mdelete_wrapper(uintptr_t client, const std::vector<std::string> &keys,
                         std::vector<uint32_t> &mdelete_status)
{
    int ret = 0;

    for (size_t i = 0; i < keys.size(); ++i) {
        const auto &key = keys[i];
        int res = priskvClusterDelete((priskvClusterClient *)client, key.c_str());
        if (res != 0) {
            mdelete_status[i] = res;
            ret = res;
        }
    }

    return ret;
}

std::vector<std::string> priskv_keys_wrapper(uintptr_t client, std::string regex) {
    priskv_keyset *keyset;
    priskvClusterKeys((priskvClusterClient *)client, regex.c_str(), &keyset);
    std::vector<std::string> keys_vec(keyset->nkey);

    for (uint32_t i = 0; i < keyset->nkey; i++) {
        keys_vec[i] = std::string(keyset->keys[i].key);
    }

    priskv_keyset_free(keyset);

    return keys_vec;
}

PYBIND11_MODULE(_priskv_client, m)
{
    m.attr("PRISKV_KEY_MAX_TIMEOUT") = PRISKV_KEY_MAX_TIMEOUT;

    pybind11::class_<priskv_sgl_wrapper>(m, "SGL", py::module_local())
        .def(pybind11::init<>())
        .def(pybind11::init<uint64_t, uint32_t, uintptr_t>())
        .def_readwrite("iova", &priskv_sgl_wrapper::iova)
        .def_readwrite("length", &priskv_sgl_wrapper::length)
        .def_readwrite("mem", &priskv_sgl_wrapper::mem);

    m.def("connect", &priskv_connect_wrapper, "A function to connect to Priskv meta server.");
    m.def("close", &priskv_close_wrapper, "A function to close connection from Priskv server.");
    m.def("reg_memory", &priskv_reg_memory_wrapper, "A function to register memory.");
    m.def("dereg_memory", &priskv_dereg_memory_wrapper, "A function to dereg memory.");
    m.def("set", &priskv_set_wrapper, "A function to set key-val.");
    m.def("setstr", &priskv_setstr_wrapper, "A function to set key-strval.");
    m.def("getstr", &priskv_getstr_wrapper, "A function to get key-strval.");
    m.def("get", &priskv_get_wrapper, "A function to get key-val.");
    m.def("exists", &priskv_test_wrapper, "A function to exists key-val.");
    m.def("delete", &priskv_delete_wrapper, "A function to delete key-val.");
    m.def("mset", &priskv_mset_wrapper, "A function to mset key-val.");
    m.def("mget", &priskv_mget_wrapper, "A function to mget key-val.");
    m.def("mexists", &priskv_mtest_wrapper, "A function to mtest key-val.");
    m.def("mdel", &priskv_mdelete_wrapper, "A function to mdelete key-val.");
    m.def("keys", &priskv_keys_wrapper, "A function to get keys.");
    m.def("mget_and_pin", &priskv_mget_and_pin_wrapper, "A function to get and pin key-val.");
    m.def("mget_and_unpin", &priskv_mget_and_unpin_wrapper, "A function to get and unpin key-val.");
    m.def("mget_and_pin", &priskv_mset_and_pin_wrapper, "A function to mset and pin key-val.");
}
