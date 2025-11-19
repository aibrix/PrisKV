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

import numpy as np
import priskv
import argparse


class PriskvClientTesting:

    def __init__(self, raddr: str, rport: int, password: str):
        self.client = priskv.PriskvClient(raddr, rport, password)
        self.key = "priskv-testing-key"

        # 常量定义
        NUM_VALUES = 3  # 需要存储/获取的值的数量
        VALUE_SIZE = 1024 * 4  # 每个值的元素数量

        # 初始化 sendbuf（发送多个值）
        self.sendbuf = np.zeros(NUM_VALUES * VALUE_SIZE, dtype=np.float32)
        self.sendmr = self.client.reg_memory(self.sendbuf.ctypes.data,
                                             self.sendbuf.nbytes)
        assert self.sendmr != 0, "Memory registration for sendbuf failed"

        # 初始化 recvbuf（接收多个值）
        self.recvbuf = np.zeros(NUM_VALUES * VALUE_SIZE, dtype=np.float32)
        self.recvmr = self.client.reg_memory(self.recvbuf.ctypes.data,
                                             self.recvbuf.nbytes)
        assert self.recvmr != 0, "Memory registration for recvbuf failed"

    def set(self):
        assert self.client.set(
            self.key,
            priskv.SGL(self.sendbuf.ctypes.data, self.sendbuf.nbytes,
                     self.sendmr), 1) == 0

    def get(self):
        assert self.client.get(
            self.key,
            priskv.SGL(self.recvbuf.ctypes.data, self.recvbuf.nbytes,
                     self.recvmr), 1) == 0

    def verify(self):
        import numpy as np
        assert np.array_equal(self.sendbuf, self.recvbuf)

    def exists(self) -> int:
        return self.client.exists(self.key)

    def delete(self):
        assert self.client.delete(self.key) == 0

    def cleanup(self):
        self.client.dereg_memory(self.sendmr)
        self.client.dereg_memory(self.recvmr)
        self.client.close()

    def test_mset(self):
        """Test mset: Set multiple keys at once."""
        keys = [f"mset_key_{i}" for i in range(3)]
        values = [
            np.random.rand(1024 * 4).astype(np.float32) for _ in range(3)
        ]

        # 将每个 value 拷贝到 sendbuf 的连续区域
        for i in range(len(values)):
            start_idx = i * (1024 * 4)
            end_idx = start_idx + (1024 * 4)
            self.sendbuf[start_idx:end_idx] = values[i]

        # 构造 SGL 列表（用于 mset）
        byte_len = (1024 * 4) * 4  # 每个值的字节数
        sgls = [
            priskv.SGL(self.sendbuf.ctypes.data + i * byte_len, byte_len,
                     self.sendmr) for i in range(len(values))
        ]

        # 执行 mset
        status, _ = self.client.mset(keys, sgls)
        print(f"[DEBUG] mset status: {status}")  # 打印 mset 的状态码

        if status != 0:
            print("[ERROR] mset failed. Check server logs for more info.")
            return

        # 执行 mexists
        status, _ = self.client.mexists(keys)

        assert status == 0, "mexists failed"

    def test_mget(self):
        """Test mget: Get multiple keys at once."""
        keys = [f"mget_key_{i}" for i in range(3)]
        values = [
            np.random.rand(1024 * 4).astype(np.float32) for _ in range(3)
        ]

        # 将每个 value 拷贝到 sendbuf 的连续区域（用于 mset）
        for i in range(len(values)):
            start_idx = i * (1024 * 4)
            end_idx = start_idx + (1024 * 4)
            self.sendbuf[start_idx:end_idx] = values[i]

        # 构造 SGL 列表（用于 mset）
        byte_len = (1024 * 4) * 4  # 每个值的字节数
        sgls = [
            priskv.SGL(self.sendbuf.ctypes.data + i * byte_len, byte_len,
                     self.sendmr) for i in range(len(values))
        ]

        # 执行 mset
        status, _ = self.client.mset(keys, sgls)
        assert status == 0, "mset failed before mget"

        # 构造 SGL 列表（用于 mget，复用 self.recvbuf）
        recv_sgls = [
            priskv.SGL(self.recvbuf.ctypes.data + i * byte_len, byte_len,
                     self.recvmr) for i in range(len(keys))
        ]

        # 执行 mget
        status, _ = self.client.mget(keys, recv_sgls, [0] * len(keys))
        assert status == 0, "mget failed"

        # 验证数据一致性
        for i in range(len(keys)):
            start_idx = i * (1024 * 4)
            end_idx = start_idx + (1024 * 4)
            retrieved_value = self.recvbuf[start_idx:end_idx]
            assert np.array_equal(retrieved_value,
                                  values[i]), f"Value {i} mismatch"

    def test_mexist(self):
        """Test mexists: Check existence of multiple keys."""
        keys = [f"mexist_key_{i}" for i in range(3)]
        values = [
            np.random.rand(1024 * 4).astype(np.float32) for _ in range(3)
        ]

        # 将每个 value 拷贝到 sendbuf 的连续区域
        for i in range(len(values)):
            start_idx = i * (1024 * 4)
            end_idx = start_idx + (1024 * 4)
            self.sendbuf[start_idx:end_idx] = values[i]

        # 构造 SGL 列表（用于 mset）
        byte_len = (1024 * 4) * 4  # 每个值的字节数
        sgls = [
            priskv.SGL(self.sendbuf.ctypes.data + i * byte_len, byte_len,
                     self.sendmr) for i in range(len(values))
        ]

        # 设置部分键
        status, _ = self.client.mset([keys[0], keys[1]], [sgls[0], sgls[1]])
        assert status == 0, "mset failed before mexists"

        # 执行 mexists
        status, _ = self.client.mexists(keys)
        assert status != 0, "mexists failed"  # 因为最后一个值不存在，所以返回的 status 不为 0，为no such key

    def test_mdel(self):
        """Test mdel: Delete multiple keys at once."""
        keys = [f"mdel_key_{i}" for i in range(3)]
        values = [
            np.random.rand(1024 * 4).astype(np.float32) for _ in range(3)
        ]

        # 将每个 value 拷贝到 sendbuf 的连续区域
        for i in range(len(values)):
            start_idx = i * (1024 * 4)
            end_idx = start_idx + (1024 * 4)
            self.sendbuf[start_idx:end_idx] = values[i]

        # 构造 SGL 列表（用于 mset）
        byte_len = (1024 * 4) * 4  # 每个值的字节数
        sgls = [
            priskv.SGL(self.sendbuf.ctypes.data + i * byte_len, byte_len,
                     self.sendmr) for i in range(len(values))
        ]

        # 设置键值
        status, _ = self.client.mset(keys, sgls)
        assert status == 0, "mset failed before mdel"

        # 执行 mdel
        status, _ = self.client.mdel(keys)
        assert status == 0, "mdel failed"

        # 验证键已删除
        for i, key in enumerate(keys):
            status, _ = self.client.mexists([key])
            assert status != 0, f"mexists for key '{key}' failed (index {i})"


def run_testing(testing):
    testing.set()
    testing.get()
    testing.verify()
    assert testing.exists() == 0
    # testing.keys()
    # testing.nrkeys()
    testing.delete()
    assert testing.exists() != 0

    testing.test_mset()
    testing.test_mget()
    testing.test_mexist()
    testing.test_mdel()

    testing.cleanup()

    print("test priskv success!")


def main():
    parser = argparse.ArgumentParser(description='Priskv Example')
    parser.add_argument("--raddr",
                        type=str,
                        required=True,
                        help="remote address")

    parser.add_argument("--rport",
                        type=int,
                        default=6379,
                        help="remote port, default 6379")

    parser.add_argument("--password",
                        type=str,
                        default="kvcache-redis",
                        help="password, default kvcache-redis")

    args = parser.parse_args()

    testing = PriskvClientTesting(args.raddr, args.rport, args.password)

    run_testing(testing)


if __name__ == "__main__":
    main()
