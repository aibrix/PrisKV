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

#ifndef __PRISKV_CUDA__
#define __PRISKV_CUDA__

#if defined(__cplusplus)
extern "C"
{
#endif

#ifdef PRISKV_USE_CUDA
#include <cuda_runtime_api.h>
#endif

#include "priskv-log.h"

#ifdef PRISKV_USE_CUDA
static inline int priskv_cuda_host_register(void *addr, size_t size)
{
    // pages are already pre-faulted at the server end, register them directly
    cudaError_t ret = cudaHostRegister(addr, size, cudaHostRegisterDefault);
    if (ret != cudaSuccess) {
        priskv_log_error("Cuda: failed to register host memory %p, %s\n", addr,
                         cudaGetErrorString(ret));
        return -1;
    }
    return 0;
}
#else
static inline int priskv_cuda_host_register(void *addr, size_t size)
{
    priskv_log_error("Cuda: PrisKV is not compiled with CUDA support\n");
    return -1;
}
#endif

#if defined(__cplusplus)
}
#endif

#endif /* __PRISKV_CUDA__ */
