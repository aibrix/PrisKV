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
import ctypes

"""
Zero-Copy Transport Semantics Assertions (for tests):
- release(ALLOC token): expect PRISKV_STATUS_PERMISSION_DENIED
- seal(ACQUIRE token): expect PRISKV_STATUS_PERMISSION_DENIED
- release(ACQUIRE token): expect PRISKV_STATUS_OK
- acquire on unpublished key: expect PRISKV_STATUS_NO_SUCH_KEY
- drop on ALLOC token (unpublished): expect PRISKV_STATUS_OK; subsequent acquire: PRISKV_STATUS_NO_SUCH_KEY
- drop on ACQUIRE token (published): expect PRISKV_STATUS_PERMISSION_DENIED
- acquire after delete: expect PRISKV_STATUS_NO_SUCH_KEY
- seal/release after delete with empty token: expect PRISKV_STATUS_NO_SUCH_TOKEN
"""

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

    def test_memory_operations_full_flow(self):
        """Test full flow: alloc -> write data -> seal -> acquire -> verify data -> release"""
        TEST_KEY = "priskv_test_full_flow"
        ALLOC_SIZE = 4096  # 4KB memory size
        TIMEOUT = 3000

        # Construct test data, pad with \x00 to match allocation size and prevent out-of-bounds access
        test_data = b"Priskv_Memory_Data_Verify_2026\x00"
        write_data = test_data.ljust(ALLOC_SIZE, b"\x00")
        print(f"[DEBUG] Test data constructed, total length: {len(write_data)} bytes")

        # ========== Step1: Allocate memory ==========
        status, alloc_region = self.client.alloc(
            key=TEST_KEY,
            alloc_length=ALLOC_SIZE,
            timeout=TIMEOUT
        )
        assert status == 0, f"alloc failed, status code: {status}"
        assert alloc_region.addr != 0, "alloc returned invalid memory address (0x0)"
        assert alloc_region.length == ALLOC_SIZE, (
            f"Allocation size mismatch, expected: {ALLOC_SIZE}, actual: {alloc_region.length}"
        )
        mem_addr = alloc_region.addr
        print(f"[DEBUG] alloc succeeded, memory address: {hex(mem_addr)}, allocated size: {alloc_region.length}")

        # ========== Step2: Write data to the allocated physical memory ==========
        try:
            # Convert integer address to ctypes operable memory buffer
            mem_buffer = (ctypes.c_char * alloc_region.length).from_address(mem_addr)
            # Safe memory copy (equivalent to C memmove, no memory overlap risk)
            ctypes.memmove(ctypes.byref(mem_buffer), write_data, len(write_data))
            print(f"[DEBUG] Data write completed, target address: {hex(mem_addr)}")
        except Exception as e:
            raise RuntimeError(f"Memory write failed: {str(e)}") from e


        acq_status, _ = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status != 0, f"acquire a unseal key expect failed"

        # ========== Step3: Seal the memory region ==========
        seal_status = self.client.seal(TEST_KEY, alloc_region)
        assert seal_status == 0, f"seal failed, status code: {seal_status}"
        print("[DEBUG] seal succeeded, memory region unlocked")

        # ========== Step4: Acquire memory information ==========
        acq_status, acq_region = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status == 0, f"acquire failed, status code: {acq_status}"
        assert acq_region.addr == mem_addr, (
            f"Memory address mismatch, allocated: {hex(mem_addr)}, acquired: {hex(acq_region.addr)}"
        )
        assert acq_region.length == ALLOC_SIZE, (
            f"Memory length mismatch, expected: {ALLOC_SIZE}, actual: {acq_region.length}"
        )
        print(f"[DEBUG] acquire succeeded, acquired address: {hex(acq_region.addr)}, length: {acq_region.length}")

        # ========== Step5: Read memory data and verify consistency (Core Logic) ==========
        try:
            read_buffer = (ctypes.c_char * acq_region.length).from_address(acq_region.addr)
            read_data = bytes(read_buffer)
        except Exception as e:
            raise RuntimeError(f"Memory read failed: {str(e)}") from e

        # Core assertion: verify written data and read data are identical
        assert read_data == write_data, (
            f"Data verification failed!\nWritten snippet: {write_data[:50]}...\nRead snippet: {read_data[:50]}..."
        )
        print("[INFO] ✅ Memory data consistency verification passed!")

        # ========== Step6: Release memory resources ==========
        rel_status = self.client.release(TEST_KEY, acq_region)
        assert rel_status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK, \
            f"release failed, expected OK, got: {rel_status}"
        print("[DEBUG] release succeeded, memory region released")

        acq_status, acq_region2 = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK, \
            f"acquire after release expected OK, got: {acq_status}"
        print("[DEBUG] acquire after release succeeded, memory region acquired again")

        rel_status = self.client.release(TEST_KEY, acq_region2)
        assert rel_status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK, \
            f"release expected OK, got: {rel_status}"
        print("[DEBUG] release succeeded, memory region released")

        # delete
        assert self.client.delete(TEST_KEY) == 0
        # After delete: acquire should be NO_SUCH_KEY
        acq_status, _ = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status == priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_KEY, \
            f"Acquire deleted key expected NO_SUCH_KEY, got: {acq_status}"

        # After delete: seal with an empty/non-existent token should be NO_SUCH_TOKEN
        dummy_region = priskv.MemoryRegion()
        seal_status = self.client.seal(TEST_KEY, dummy_region)
        assert seal_status == priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_TOKEN, \
            f"seal non-existent token expected NO_SUCH_TOKEN, got: {seal_status}"

        # After delete: release with an empty/non-existent token should be NO_SUCH_TOKEN
        rel_status = self.client.release(TEST_KEY, dummy_region)
        assert rel_status == priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_TOKEN, \
            f"release non-existent token expected NO_SUCH_TOKEN, got: {rel_status}"




    def test_transport_permissions(self):
        """Transport permission semantics (precise expectations):
        - release(ALLOC token): expect PRISKV_STATUS_PERMISSION_DENIED
        - seal(ACQUIRE token): expect PRISKV_STATUS_PERMISSION_DENIED
        - release(ACQUIRE token): expect PRISKV_STATUS_OK
        """
        TEST_KEY = "priskv_transport_perm_py"
        ALLOC_SIZE = 4096
        TIMEOUT = 3000

        print("[INFO] [TPERM] Test start: Transport permission semantics")
        # Step1: alloc (unsealed)
        status, alloc_region = self.client.alloc(
            key=TEST_KEY,
            alloc_length=ALLOC_SIZE,
            timeout=TIMEOUT
        )
        assert status == 0, f"alloc failed, status code: {status}"
        assert alloc_region.addr != 0 and alloc_region.length == ALLOC_SIZE
        print(f"[DEBUG] [TPERM] alloc succeeded: addr={hex(alloc_region.addr)}, len={alloc_region.length}")

        # Optionally write a small payload to verify after sealing
        try:
            buf = (ctypes.c_char * alloc_region.length).from_address(alloc_region.addr)
            payload = b"transport_perm_py\x00"
            ctypes.memmove(ctypes.byref(buf), payload, len(payload))
            print(f"[DEBUG] [TPERM] payload write completed, length={len(payload)}")
        except Exception as e:
            raise RuntimeError(f"prepare alloc buffer failed: {str(e)}") from e

        # Step2: release on ALLOC token (should fail: Permission Denied)
        rel_status = self.client.release(TEST_KEY, alloc_region)
        assert rel_status == priskv.PRISKV_STATUS.PRISKV_STATUS_PERMISSION_DENIED, \
            f"release on alloc token should be PERMISSION_DENIED, got {rel_status}"
        print(f"[INFO] [TPERM] release(alloc token) denied: status={rel_status}")

        # Step3: seal (publish to make value visible)
        seal_status = self.client.seal(TEST_KEY, alloc_region)
        assert seal_status == 0, f"seal failed, status code: {seal_status}"
        print(f"[INFO] [TPERM] seal succeeded: status={seal_status}")

        # Step4: acquire (get read-only token)
        acq_status, acq_region = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status == 0 and acq_region.addr != 0
        print(f"[DEBUG] [TPERM] acquire succeeded: addr={hex(acq_region.addr)}, len={acq_region.length}")

        # Step5: seal on ACQUIRE token (should fail: Permission Denied)
        seal_status2 = self.client.seal(TEST_KEY, acq_region)
        assert seal_status2 == priskv.PRISKV_STATUS.PRISKV_STATUS_PERMISSION_DENIED, \
            f"seal on acquire token should be PERMISSION_DENIED, got {seal_status2}"
        print(f"[INFO] [TPERM] seal(acquire token) denied: status={seal_status2}")

        # Step6: release on ACQUIRE token (should succeed: OK)
        rel_status2 = self.client.release(TEST_KEY, acq_region)
        assert rel_status2 == priskv.PRISKV_STATUS.PRISKV_STATUS_OK, \
            f"release(acquire token) expected OK, got {rel_status2}"
        print(f"[INFO] [TPERM] release(acquire token) succeeded: status={rel_status2}")

        # Cleanup
        assert self.client.delete(TEST_KEY) == 0
        print("[INFO] [TPERM] cleanup complete and key deleted")
        print("[INFO] [TPERM] Test end: Transport permission semantics")

    def test_transport_drop_behavior(self):
        """Transport DROP semantics (precise expectations):
        - drop on ALLOC token (unpublished): expect PRISKV_STATUS_OK; subsequent acquire: PRISKV_STATUS_NO_SUCH_KEY
        - drop on ACQUIRE token (published): expect PRISKV_STATUS_PERMISSION_DENIED
        """
        TEST_KEY = "priskv_transport_drop_py"
        ALLOC_SIZE = 2048
        TIMEOUT = 3000

        print("[INFO] [TDROP] Test start: Transport DROP semantics")
        # Case A: Unpublished key (ALLOC only)
        status, alloc_region = self.client.alloc(TEST_KEY, ALLOC_SIZE, TIMEOUT)
        assert status == 0 and alloc_region.addr != 0
        print(f"[DEBUG] [TDROP] alloc (unpublished): addr={hex(alloc_region.addr)}, len={alloc_region.length}")

        # When unpublished, acquire should be invisible (NO_SUCH_KEY)
        acq_status, _ = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status == priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_KEY, \
            f"acquire unpublished key expected NO_SUCH_KEY, got {acq_status}"
        print(f"[INFO] [TDROP] acquire unpublished key denied: status={acq_status}")

        # Drop on ALLOC token should succeed (OK)
        drop_status = self.client.drop(TEST_KEY, alloc_region)
        assert drop_status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK, \
            f"drop unpublished (alloc token) expected OK, got {drop_status}"
        print(f"[INFO] [TDROP] drop(alloc token) succeeded: status={drop_status}")

        # After drop, acquire should still return NO_SUCH_KEY
        acq_status2, _ = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status2 == priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_KEY, \
            f"acquire after drop (unpublished) expected NO_SUCH_KEY, got {acq_status2}"
        print(f"[INFO] [TDROP] after drop, acquire still denied: status={acq_status2}")

        # Case B: Published key (after seal)
        status, alloc_region2 = self.client.alloc(TEST_KEY, ALLOC_SIZE, TIMEOUT)
        assert status == 0 and alloc_region2.addr != 0
        assert self.client.seal(TEST_KEY, alloc_region2) == 0
        print(f"[DEBUG] [TDROP] alloc+seal (published): addr={hex(alloc_region2.addr)}, len={alloc_region2.length}")

        acq_status3, acq_region = self.client.acquire(TEST_KEY, TIMEOUT)
        assert acq_status3 == 0 and acq_region.addr != 0
        print(f"[DEBUG] [TDROP] acquire succeeded: addr={hex(acq_region.addr)}, len={acq_region.length}")

        # Drop on ACQUIRE token should fail (PERMISSION_DENIED)
        drop_status2 = self.client.drop(TEST_KEY, acq_region)
        assert drop_status2 == priskv.PRISKV_STATUS.PRISKV_STATUS_PERMISSION_DENIED, \
            f"drop(acquire token) expected PERMISSION_DENIED, got {drop_status2}"
        print(f"[INFO] [TDROP] drop(acquire token) denied: status={drop_status2}")

        # Cleanup
        assert self.client.release(TEST_KEY, acq_region) == 0
        assert self.client.delete(TEST_KEY) == 0
        print("[INFO] [TDROP] cleanup complete and key deleted")
        print("[INFO] [TDROP] Test end: Transport DROP semantics")

    def test_pin_on_seal_and_inheritance(self):
        """Validate pin_on_seal and multi-version inheritance (feature path coverage)."""
        TEST_KEY = "py_pin_seal_key"
        SIZE = 256
        TIMEOUT = 3000

        # Version 1: alloc + seal(pin)
        status, region1 = self.client.alloc(TEST_KEY, SIZE, TIMEOUT)
        assert status == 0 and region1.length == SIZE
        status = self.client.seal(TEST_KEY, region1, pin_on_seal=True)
        assert status == 0

        # Version 2: alloc + seal(pin)
        status, region2 = self.client.alloc(TEST_KEY, SIZE // 2, TIMEOUT)
        assert status == 0 and region2.length == SIZE // 2
        status = self.client.seal(TEST_KEY, region2, pin_on_seal=True)
        assert status == 0

        # Two rounds of acquire/release(unpin)
        status, acq1 = self.client.acquire(TEST_KEY, TIMEOUT, pin_on_acquire=False)
        assert status == 0
        status = self.client.release(TEST_KEY, acq1, unpin_on_release=True)
        assert status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK

        status, acq2 = self.client.acquire(TEST_KEY, TIMEOUT, pin_on_acquire=False)
        assert status == 0
        status = self.client.release(TEST_KEY, acq2, unpin_on_release=True)
        assert status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK

        # Cleanup
        assert self.client.delete(TEST_KEY) == 0

    def test_pin_on_acquire_and_unpin_on_release(self):
        """Validate acquire(pin_on_acquire) and release(unpin_on_release)."""
        TEST_KEY = "py_pin_unpin_key"
        SIZE = 128
        TIMEOUT = 3000

        status, region = self.client.alloc(TEST_KEY, SIZE, TIMEOUT)
        assert status == 0
        status = self.client.seal(TEST_KEY, region)
        assert status == 0

        status, acq = self.client.acquire(TEST_KEY, TIMEOUT, pin_on_acquire=True)
        assert status == 0
        status = self.client.release(TEST_KEY, acq, unpin_on_release=True)
        assert status == priskv.PRISKV_STATUS.PRISKV_STATUS_OK

        assert self.client.delete(TEST_KEY) == 0

    def test_unpin_not_closed_on_release(self):
        """Unpin without prior pin should return UNPIN_NOT_CLOSED."""
        TEST_KEY = "py_unpin_not_closed"
        SIZE = 64
        TIMEOUT = 3000

        status, region = self.client.alloc(TEST_KEY, SIZE, TIMEOUT)
        assert status == 0
        status = self.client.seal(TEST_KEY, region)
        assert status == 0

        status, acq = self.client.acquire(TEST_KEY, TIMEOUT, pin_on_acquire=False)
        assert status == 0
        status = self.client.release(TEST_KEY, acq, unpin_on_release=True)
        assert status == priskv.PRISKV_STATUS.PRISKV_STATUS_UNPIN_NOT_CLOSED, \
            f"expected UNPIN_NOT_CLOSED, got {status}"

        assert self.client.delete(TEST_KEY) == 0

    def test_unpin_no_such_key(self):
        """When latest version does not exist, unpin returns NO_SUCH_KEY/NO_SUCH_TOKEN."""
        TEST_KEY = "py_unpin_no_such_key"
        SIZE = 96
        TIMEOUT = 3000

        status, region = self.client.alloc(TEST_KEY, SIZE, TIMEOUT)
        assert status == 0
        status = self.client.seal(TEST_KEY, region)
        assert status == 0

        status, acq = self.client.acquire(TEST_KEY, TIMEOUT, pin_on_acquire=True)
        assert status == 0

        # Release after delete
        assert self.client.delete(TEST_KEY) == 0
        status = self.client.release(TEST_KEY, acq, unpin_on_release=True)
        assert status in (priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_TOKEN,
                          priskv.PRISKV_STATUS.PRISKV_STATUS_NO_SUCH_KEY)

    # TODO(wangyi): Add PinTTL expiry tests
    # - Simulate pin-on-acquire/pin-on-seal with a short TTL and verify automatic cleanup
    #   decrements pin_count on the latest version after TTL.
    # - Cover cases where latest is deleted/expired to ensure cleanup counters record
    #   NO_SUCH_KEY scenarios without crashing.

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
    testing.test_memory_operations_full_flow()
    testing.test_transport_permissions()
    testing.test_transport_drop_behavior()
    testing.test_pin_on_seal_and_inheritance()
    testing.test_pin_on_acquire_and_unpin_on_release()
    testing.test_unpin_not_closed_on_release()
    testing.test_unpin_no_such_key()

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
