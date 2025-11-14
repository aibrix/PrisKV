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

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/functional.h>
#include <pybind11/pytypes.h>

#include <thread>
#include <string>
#include <vector>
#include <mutex>
#include <atomic>

#include "priskv.h"

namespace py = pybind11;

typedef struct priskv_sgl_wrapper {
    uint64_t iova;
    uint32_t length;
    uintptr_t mem;
} priskv_sgl_wrapper;

uintptr_t priskv_connect_wrapper(std::string raddr, int rport, pybind11::object laddr, int lport, int nqueue)
{
    std::string laddr_str = laddr.is_none() ? std::string() : laddr.cast<std::string>();

    return (uintptr_t)priskv_connect(raddr.c_str(), rport, laddr_str.empty()? NULL : laddr_str.c_str(), lport, nqueue);
}

void priskv_close_wrapper(uintptr_t client)
{
    return priskv_close((priskv_client *)client);
}

uintptr_t priskv_reg_memory_wrapper(uintptr_t client, uint64_t offset, size_t length, uint64_t iova,
                                  int fd)
{
    return (uintptr_t)priskv_reg_memory((priskv_client *)client, offset, length, iova, fd);
}

std::vector<uintptr_t> priskv_batch_reg_memory_wrapper(uintptr_t client, std::vector<uint64_t> offset, std::vector<size_t> length, std::vector<uint64_t> iova,
                                    std::vector<int> fd, int workers)
{
    py::gil_scoped_release release;
    std::vector<uintptr_t> mems(offset.size());
    std::vector<std::thread> threads;
    if (workers <=0 ) {
        workers = 4;
    }

    size_t batch_size = (offset.size() + workers - 1) / workers;
    for (int i = 0; i < workers; i++) {
        size_t start = batch_size * i;
        size_t end = start + batch_size > offset.size() ? offset.size() : start + batch_size;
        threads.emplace_back([&, start, end]() {
            for (auto j = start; j < end; j++) {
                mems[j] = (uintptr_t)priskv_reg_memory((priskv_client *)client, offset[j], length[j], iova[j], fd[j]);
            }
        });
    }

    for (auto &t : threads) {
        t.join();
    }
    
    return mems;
}

void priskv_dereg_memory_wrapper(uintptr_t mem)
{
    return priskv_dereg_memory((priskv_memory *)mem);
}

int priskv_set_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint64_t timeout)
{
    priskv_sgl sgl;

    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskv_memory *)sgl_wrapper->mem;
    return priskv_set((priskv_client *)client, key.c_str(), &sgl, nsgl, timeout);
}

int priskv_setstr_wrapper(uintptr_t client, std::string key, std::string value, uint64_t timeout)
{
    int ret;
    priskv_sgl sgl;
    uint32_t length = value.length();
    void *ptr = (void *)value.c_str();
    priskv_memory *reg_mr = priskv_reg_memory((priskv_client *)client, (uint64_t)ptr, length, (uint64_t)ptr, -1);

    sgl.iova = (uint64_t)ptr;
    sgl.length = value.length();
    sgl.mem = reg_mr;
    ret = priskv_set((priskv_client *)client, key.c_str(), &sgl, 1, timeout);

    priskv_dereg_memory(reg_mr);
    return ret;
}

int priskv_get_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint32_t *valuelen)
{
    priskv_sgl sgl;

    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskv_memory *)sgl_wrapper->mem;
    return priskv_get((priskv_client *)client, key.c_str(), &sgl, nsgl, valuelen);
}

std::string priskv_getstr_wrapper(uintptr_t client, std::string key)
{
    priskv_sgl sgl;
    int ret;
    uint32_t valuelen;
    priskv_memory *reg_mr;
    ret = priskv_test((priskv_client *)client, key.c_str(), &valuelen);
    if (ret != 0 || valuelen == 0) {
        return std::string();
    }

    std::string value = std::string(valuelen, '\0');
    reg_mr = priskv_reg_memory((priskv_client *)client, (uint64_t)value.c_str(), valuelen, (uint64_t)value.c_str(), -1);
    sgl.iova = (uint64_t)value.c_str();
    sgl.length = valuelen;
    sgl.mem = reg_mr;
    ret = priskv_get((priskv_client *)client, key.c_str(), &sgl, 1, &valuelen);
    priskv_dereg_memory(reg_mr);

    if (ret == 0) {
        return value;
    } else {
        return std::string();
    }
}

int priskv_test_wrapper(uintptr_t client, std::string key)
{
    uint32_t valuelen;
    return priskv_test((priskv_client *)client, key.c_str(), &valuelen);
}

int priskv_expire_wrapper(uintptr_t client, std::string key, uint64_t timeout)
{
    if (timeout > 0) {
        return priskv_expire((priskv_client *)client, key.c_str(), timeout);
    } else {
        return priskv_delete((priskv_client *)client, key.c_str());
    }
}

std::vector<std::string> priskv_keys_wrapper(uintptr_t client, std::string regex) {
    priskv_keyset *keyset;
    priskv_keys((priskv_client *)client, regex.c_str(), &keyset);
    std::vector<std::string> keys_vec(keyset->nkey);

    for (uint32_t i = 0; i < keyset->nkey; i++) {
        keys_vec[i] = std::string(keyset->keys[i].key);
    }

    priskv_keyset_free(keyset);

    return keys_vec;
}

int priskv_delete_wrapper(uintptr_t client, std::string key)
{
    return priskv_delete((priskv_client *)client, key.c_str());
}

int64_t priskv_nrkeys_wrapper(uintptr_t client, std::string regex)
{
    uint32_t nkey = 0;
    int ret = 0;

    ret = priskv_nrkeys((priskv_client *)client, regex.c_str(), &nkey);
    if (ret != 0) {
        return -ret;
    }

    return nkey;
}

int64_t priskv_flush_wrapper(uintptr_t client, std::string regex)
{
    uint32_t nkey = 0;
    int ret = 0;

    ret = priskv_flush((priskv_client *)client, regex.c_str(), &nkey);
    if (ret != 0) {
        return -ret;
    }

    return nkey;
}

uint64_t priskv_capacity_wrapper(uintptr_t client)
{
    return priskv_capacity((priskv_client *)client);
}

// python callback definition
// https://pybind11.readthedocs.io/en/stable/advanced/misc.html
using PythonCallback = std::function<void(int)>;

struct AsyncRequestContext {
    uint64_t request_id;
    PythonCallback callback;

    AsyncRequestContext(uint64_t id, PythonCallback cb)
        : request_id(id), callback(std::move(cb)) {
    }
};

namespace {
    std::unordered_map<uint64_t, std::unique_ptr<AsyncRequestContext>> pending_requests;
    std::mutex requests_mutex;

    /*
     * uint64_t has a very huge value range, which is almost impossible to reach in practical applications. 
     * Moreover, according to the C++ standard, for atomic operations on unsigned integers, overflow is a 
     * well-defined behavior. It will perform modulo arithmetic and wrap around to 0.
     */
    std::atomic<uint64_t> next_request_id{1};

    uint64_t priskv_build_req_context(PythonCallback callback) {
        uint64_t request_id = next_request_id.fetch_add(1);
        auto ctx = std::make_unique<AsyncRequestContext>(request_id, std::move(callback));
        
        {
            std::lock_guard<std::mutex> lock(requests_mutex);
            pending_requests[request_id] = std::move(ctx);            
        }
        
        return request_id;
    }

    void priskv_handle_completion(uint64_t request_id, int status, void *result) {
        std::unique_ptr<AsyncRequestContext> ctx;
        
        {
            std::lock_guard<std::mutex> lock(requests_mutex);
            auto it = pending_requests.find(request_id);
            if (it != pending_requests.end()) {
                ctx = std::move(it->second);
                pending_requests.erase(it);
            }
        }
        
        if (ctx && ctx->callback) {
            try {
                // Call the Python callback
                ctx->callback(status);
            } catch (const std::exception& e) {
                fprintf(stderr, "Error in Python callback: %s\n", e.what());
            }
        }
    }
}

extern "C" void priskv_common_async_cb(uint64_t request_id, priskv_status status, void* result) {
    priskv_handle_completion(request_id, status, result);
}

int priskv_get_fd_wrapper(uintptr_t client)
{
    return priskv_get_fd((priskv_client *)client);
}

int priskv_process_wrapper(uintptr_t client)
{
    return priskv_process((priskv_client *)client, 0);
}

int priskv_set_async_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint64_t timeout, PythonCallback callback)
{
    priskv_sgl sgl;
    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskv_memory *)sgl_wrapper->mem;    

    uint64_t request_id = priskv_build_req_context(std::move(callback));

    priskv_set_async((priskv_client *)client, key.c_str(), &sgl, nsgl, timeout, request_id, priskv_common_async_cb);
    
    return request_id;
}

int priskv_get_async_wrapper(uintptr_t client, std::string key,
                     priskv_sgl_wrapper *sgl_wrapper, uint16_t nsgl, uint32_t *valuelen, PythonCallback callback)
{
    priskv_sgl sgl;
    sgl.iova = sgl_wrapper->iova;
    sgl.length = sgl_wrapper->length;
    sgl.mem = (priskv_memory *)sgl_wrapper->mem;    

    uint64_t request_id = priskv_build_req_context(std::move(callback));

    priskv_get_async((priskv_client *)client, key.c_str(), &sgl, nsgl, request_id, priskv_common_async_cb);
    
    return request_id;
}

int priskv_test_async_wrapper(uintptr_t client, std::string key, PythonCallback callback)
{
    uint64_t request_id = priskv_build_req_context(std::move(callback));

    priskv_test_async((priskv_client *)client, key.c_str(), request_id, priskv_common_async_cb);

    return request_id;
}

int priskv_delete_async_wrapper(uintptr_t client, std::string key, PythonCallback callback)
{
    uint64_t request_id = priskv_build_req_context(std::move(callback));

    priskv_delete_async((priskv_client *)client, key.c_str(), request_id, priskv_common_async_cb);

    return request_id;
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

    m.def("connect", &priskv_connect_wrapper, "A function to connect to PrisKV server.");
    m.def("close", &priskv_close_wrapper, "A function to close connection from PrisKV server.");
    m.def("reg_memory", &priskv_reg_memory_wrapper, "A function to register memory.");
    m.def("batch_reg_memory", &priskv_batch_reg_memory_wrapper, "A function to batch register memory.");
    m.def("dereg_memory", &priskv_dereg_memory_wrapper, "A function to dereg memory.");
    m.def("set", &priskv_set_wrapper, "A function to set key-val.");
    m.def("setstr", &priskv_setstr_wrapper, "A function to set key-strval.");
    m.def("get", &priskv_get_wrapper, "A function to get key-val.");
    m.def("getstr", &priskv_getstr_wrapper, "A function to get key-strval.");
    m.def("test", &priskv_test_wrapper, "A function to test key-val.");
    m.def("expire", &priskv_expire_wrapper, "A function to set timeout for key-val.");
    m.def("delete", &priskv_delete_wrapper, "A function to delete key-val.");
    m.def("keys", &priskv_keys_wrapper, "A function to get all keys.");
    m.def("nrkeys", &priskv_nrkeys_wrapper, "A function to get nrkeys.");
    m.def("flush", &priskv_flush_wrapper, "A function to flush keys.");
    m.def("capacity", &priskv_capacity_wrapper, "A function to get capacity.");
    m.def("get_epoll_fd", &priskv_get_fd_wrapper, "A function to get client epoll fd.");
    m.def("handle_epoll_events", &priskv_process_wrapper, "A function to process client event.");
    m.def("set_async", &priskv_set_async_wrapper, "A function to set key-val asynchronously.");
    m.def("get_async", &priskv_get_async_wrapper, "A function to get key-val asynchronously.");
    m.def("test_async", &priskv_test_async_wrapper, "A function to test key-val asynchronously.");
    m.def("delete_async", &priskv_delete_async_wrapper, "A function to delete key-val asynchronously.");
}
