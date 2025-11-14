# Copyright (c) 2025 ByteDance Ltd. and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Authors:
#   Jinlong Xuan <15563983051@163.com>
#   Xu Ji <sov.matrixac@gmail.com>
#   Yu Wang <wangyu.steph@bytedance.com>
#   Bo Liu <liubo.2024@bytedance.com>
#   Zhenwei Pi <pizhenwei@bytedance.com>
#   Rui Zhang <zhangrui.1203@bytedance.com>
#   Changqi Lu <luchangqi.123@bytedance.com>
#   Enhua Zhou <zhouenhua@bytedance.com>

import priskv
import time
import argparse


def align_down(x, size):
    return x & ~(size - 1)


class PriskvBenchmark:

    def __init__(self, args):
        self.op_name = args.operation
        self.mem_type = args.mem_type
        self.torch = args.torch

        self.key_len = args.key_len
        self.value_len = align_down(args.value_len, args.aligned_len)

        self.req_count = 0
        self.last_req_count = 0

        self.interval_ns = args.interval
        self.runtime = args.runtime

        self.client = priskv.PriskvClient(args.raddr, args.rport, args.laddr,
                                      args.lport, 1)
        assert self.client != 0, "Failed to connect to PrisKV server."

        self.shared_key = "1" * self.key_len

        if self.op_name == "set":
            self.exec_op = self.op_set
        elif self.op_name == "get":
            self.exec_op = self.op_get
        else:
            raise ValueError("Invalid operator. Must be 'set' or 'get'.")

    def check_args(self):
        if self.op_name not in ["set", "get"]:
            raise ValueError("Invalid operator. Must be 'set' or 'get'.")

        if self.mem_type not in ["gpu", "cpu", "npu"]:
            raise ValueError(
                "Invalid memory type. Must be 'gpu', 'cpu' or 'npu'.")

    def op_set(self):
        return self.client.set(self.shared_key, self.shared_val_sgl)

    def op_get(self):
        return self.client.get(self.shared_key, self.shared_val_sgl,
                               self.value_len)

    def disconnect(self):
        self.client.close()

    def prepare_env(self):
        if self.op_name == "get":
            self.op_set()

    def run(self):
        self.check_args()
        self.prepare_env()

        self.first_ns = time.time_ns()
        self.last_ns = self.first_ns

        while True:
            ret = self.exec_op()
            if ret != 0:
                print("PrisKV benchmark failed: %d", ret)
                return

            self.req_count += 1

            now_ns = time.time_ns()
            if (self.runtime != 0) and (now_ns - self.first_ns >=
                                        self.runtime * 1e9):
                break

            if now_ns - self.last_ns >= self.interval_ns * 1e9:
                qps = (self.req_count -
                       self.last_req_count) / (now_ns - self.last_ns) * 1e9
                latency = (qps != 0 and (now_ns - self.last_ns) / (qps * 1e3)
                           or 0)
                print(
                    f"current qps {self.op_name} operation: {qps:.2f} qps, latency: {latency:.2f} us."
                )
                self.last_req_count = self.req_count
                self.last_ns = now_ns

        self.disconnect()


class PriskvBenchmarkTorch(PriskvBenchmark):

    def __init__(self, args):
        import torch

        if args.mem_type == "gpu":
            self.device = "cuda"
        elif args.mem_type == "npu":
            import torch_npu
            torch.npu.set_device(0)
            self.device = "npu"
        elif args.mem_type == "cpu":
            self.device = "cpu"
        else:
            raise ValueError(
                "Invalid memory type. Must be 'gpu', 'cpu' or 'npu'.")

        if args.transfer or self.device == "npu":
            self.need_transfer = True
        else:
            self.need_transfer = False

        if self.need_transfer:
            self.tensor_client = priskv.PriskvTensorClient(args.raddr, args.rport,
                                                       args.laddr, args.lport,
                                                       1)
            self.op_get = self.op_transfer_get
            self.op_set = self.op_transfer_set

        super().__init__(args)

        self.dtype_map = {
            "int8": [torch.int8, 1],
            "int16": [torch.int16, 2],
            "float32": [torch.float32, 4],
            "float64": [torch.float64, 8],
        }

        if args.dtype not in self.dtype_map:
            raise ValueError(
                "Invalid tensor type. Must be 'int8', 'int16', 'float32', or 'float64'."
            )

        self.tensor_size = self.value_len / self.dtype_map[args.dtype][1]
        self.dtype = self.dtype_map[args.dtype][0]

        self.device_tensor = torch.rand(int(self.tensor_size),
                                        dtype=self.dtype,
                                        device=self.device)
        if self.need_transfer:
            self.cpu_tensor = self.device_tensor.to("cpu")
        else:
            self.reg_handler = self.client.reg_memory(
                self.device_tensor.data_ptr(), self.value_len)
            self.shared_val_sgl = priskv.SGL(self.device_tensor.data_ptr(),
                                           self.value_len, self.reg_handler)

    def op_transfer_get(self):
        ret = self.tensor_client.get(self.shared_key, self.cpu_tensor)
        if ret != 0:
            return ret

        self.cpu_tensor.to(self.device)
        return 0

    def op_transfer_set(self):
        t = self.device_tensor.to("cpu")
        return self.tensor_client.set(self.shared_key, t)

    def disconnect(self):
        super().disconnect()
        if self.need_transfer:
            self.tensor_client.close()
        else:
            self.client.dereg_memory(self.reg_handler)


class PRISKVBenchmarkNumpy(PriskvBenchmark):

    def __init__(self, args):
        import numpy as np

        super().__init__(args)

        self.dtype_map = {
            "int8": [np.int8, 1],
            "int16": [np.int16, 2],
            "float32": [np.float32, 4],
            "float64": [np.float64, 8],
        }

        if args.dtype not in self.dtype_map:
            raise ValueError(
                "Invalid tensor type. Must be 'int8', 'int16', 'float32', or 'float64'."
            )

        self.tensor_size = self.value_len / self.dtype_map[args.dtype][1]
        self.dtype = self.dtype_map[args.dtype][0]

        if self.dtype == np.int8:
            self.reg_buf = np.random.randint(-128,
                                             127,
                                             int(self.tensor_size),
                                             dtype=self.dtype)
        elif self.dtype == np.int16:
            self.reg_buf = np.random.randint(-32768,
                                             32767,
                                             int(self.tensor_size),
                                             dtype=self.dtype)
        else:
            self.reg_buf = np.random.rand(int(self.tensor_size))

        if self.dtype == np.float32:
            self.reg_buf = self.reg_buf.astype(np.float32)

        self.reg_handler = self.client.reg_memory(self.reg_buf.ctypes.data,
                                                  self.value_len)

        self.shared_val_sgl = priskv.SGL(self.reg_buf.ctypes.data,
                                       self.value_len, self.reg_handler)

    def disconnect(self):
        self.client.dereg_memory(self.reg_handler)
        super().disconnect()


def main():
    parser = argparse.ArgumentParser(description='PrisKV Benchmark')

    parser.add_argument("--raddr",
                        type=str,
                        required=True,
                        help="remote address")

    parser.add_argument("--rport",
                        type=int,
                        default=18512,
                        help="remote port, default 18512")

    parser.add_argument(
        "--laddr",
        type=str,
        default=None,
        help="local address, default None (auto chosen by system)")

    parser.add_argument("--lport",
                        type=int,
                        default=0,
                        help="local port, default 0 (any)")

    parser.add_argument("--operation",
                        type=str,
                        default="get",
                        help="operator [set/get]")

    parser.add_argument("--key-len",
                        type=int,
                        default=10,
                        help="key-length in bytes")

    parser.add_argument("--value-len",
                        type=int,
                        default=4096,
                        help="value-length in bytes")

    parser.add_argument("--mem-type",
                        type=str,
                        default="cpu",
                        help="memory type [gpu/cpu/npu], default cpu")

    parser.add_argument("--runtime",
                        type=int,
                        default=0,
                        help="time to benchmark(s)")

    parser.add_argument("--dtype",
                        type=str,
                        default="float32",
                        help="type of tensor [int8/int16/float32/float64]")

    parser.add_argument("--torch",
                        action="store_true",
                        default=False,
                        help="whether to use torch tensor, default use numpy")

    parser.add_argument(
        "--interval",
        type=int,
        default=1,
        help="the interval for data statistics and printing, default 1")

    parser.add_argument(
        "--aligned-len",
        type=int,
        default=4096,
        help="the alignment size of the generated VALUE, default 4096")

    parser.add_argument(
        "--transfer",
        action="store_true",
        default=False,
        help="whether to use CPU memory for transfer, default false")

    args = parser.parse_args()
    print(args)

    if args.torch:
        benchmark = PriskvBenchmarkTorch(args)
    else:
        benchmark = PRISKVBenchmarkNumpy(args)

    benchmark.run()


if __name__ == "__main__":
    main()
