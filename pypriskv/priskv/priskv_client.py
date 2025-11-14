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

import priskv._priskv as client
from typing import Optional, Dict, Any, Callable

import asyncio


class PriskvClient:
    '''PriskvClient:

    Keep the same parameters with the C client.

    Args:
        raddr (str): remote address
        rport (int): remote port
        laddr (str): local address
        lport (int): local port
        nqueue (int): number of queues, default 0
    '''

    def __init__(self,
                 raddr: str,
                 rport: int,
                 laddr: str,
                 lport: int,
                 nqueue: int = 0):
        self.raddr = raddr
        self.rport = rport
        self.laddr = laddr
        self.laddr = laddr

        # only used for bacth operation
        self._event_loop: Optional[asyncio.AbstractEventLoop] = None

        self.conn = client.connect(raddr, rport, laddr, lport, nqueue)
        if self.conn == 0:
            raise RuntimeError("Failed to connect to PrisKV server.")

        self._conn_epoll_fd = client.get_epoll_fd(self.conn)
        if self._conn_epoll_fd < 0:
            raise RuntimeError("Failed to get epoll fd.")

    def reg_memory(
        self,
        iova: int,
        length: int,
        offset: int = 0,
        fd: int = -1,
    ) -> int:
        if offset == 0:
            offset = iova

        return client.reg_memory(self.conn, offset, length, iova, fd)

    def batch_reg_memory(self,
                         iova: list[int],
                         length: list[int],
                         offset: Optional[list[int]] = None,
                         fd: Optional[list[int]] = None,
                         **kwargs) -> list[int]:
        assert len(iova) == len(length)
        if offset is None:
            offset = iova

        if fd is None:
            fd = [-1] * len(iova)

        workers = kwargs.get("workers", 16)
        return client.batch_reg_memory(self.conn, offset, length, iova, fd,
                                       workers)

    def dereg_memory(self, mem: int):
        return client.dereg_memory(mem)

    def set(self,
            key: str,
            sgl: client.SGL,
            nsgl: int = 1,
            timeout: int = client.PRISKV_KEY_MAX_TIMEOUT) -> int:
        return client.set(self.conn, key, sgl, nsgl, timeout)

    def setstr(self,
               key: str,
               value: str,
               timeout: int = client.PRISKV_KEY_MAX_TIMEOUT) -> int:
        return client.setstr(self.conn, key, value, timeout)

    def batch_set(self,
                  keys: list[str],
                  sgls: list[client.SGL],
                  nsgls: Optional[list[int]] = None,
                  timeouts: Optional[list[int]] = None) -> list[int]:
        assert len(keys) == len(
            sgls), f"Keys length({len(keys)}) != sgls length({len(sgls)})"

        if nsgls is None:
            nsgls = [1] * len(keys)

        if timeouts is None:
            timeouts = [client.PRISKV_KEY_MAX_TIMEOUT] * len(keys)

        # Ensure to always use one event loop by using asyncio.Runner other than asyncio.run
        self._running_eventloop_check()
        with asyncio.Runner() as runner:
            self._event_loop = runner.get_loop()
            self._add_epollfd_to_eventloop()

            async_set_tasks = []
            for key, sgl, nsgl, timeout in zip(keys, sgls, nsgls, timeouts):
                async_set_tasks.append(self._set_async(key, sgl, nsgl,
                                                       timeout))

            results = runner.run(self._gather_async_task(async_set_tasks))

            self._remove_epollfd_from_eventloop()
            self._event_loop = None
            return results

    def get(self,
            key: str,
            sgl: client.SGL,
            value_len: int,
            nsgl: int = 1) -> int:
        return client.get(self.conn, key, sgl, nsgl, value_len)

    def getstr(self, key: str) -> Optional[str]:
        val = client.getstr(self.conn, key)
        return val if len(val) > 0 else None

    def batch_get(self,
                  keys: list[str],
                  sgls: list[client.SGL],
                  value_lens: list[int],
                  nsgls: Optional[list[int]] = None) -> list[int]:
        assert len(keys) == len(sgls) == len(value_lens), \
        f"Keys length({len(keys)}) != sgls length({len(sgls)}) != value_lens length({len(value_lens)})"

        if nsgls is None:
            nsgls = [1] * len(keys)

        self._running_eventloop_check()
        with asyncio.Runner() as runner:
            self._event_loop = runner.get_loop()
            self._add_epollfd_to_eventloop()

            async_get_tasks = []
            for key, sgl, value_len, nsgl in zip(keys, sgls, value_lens,
                                                 nsgls):
                async_get_tasks.append(
                    self._get_async(key, sgl, value_len, nsgl))

            results = runner.run(self._gather_async_task(async_get_tasks))

            self._remove_epollfd_from_eventloop()
            self._event_loop = None
            return results

    def delete(self, key: str) -> int:
        return client.delete(self.conn, key)

    def batch_delete(self, keys: list[str]) -> list[int]:
        self._running_eventloop_check()
        with asyncio.Runner() as runner:
            self._event_loop = runner.get_loop()
            self._add_epollfd_to_eventloop()

            async_delete_tasks = []
            for key in keys:
                async_delete_tasks.append(self._delete_async(key))

            results = runner.run(self._gather_async_task(async_delete_tasks))

            self._remove_epollfd_from_eventloop()
            self._event_loop = None
            return results

    def test(self, key: str) -> bool:
        return client.test(self.conn, key) == 0

    def batch_test(self, keys: list[str]) -> list[bool]:
        self._running_eventloop_check()
        with asyncio.Runner() as runner:
            self._event_loop = runner.get_loop()
            self._add_epollfd_to_eventloop()

            async_test_tasks = []
            for key in keys:
                async_test_tasks.append(self._test_async(key))

            results = runner.run(self._gather_async_task(async_test_tasks))

            self._remove_epollfd_from_eventloop()
            self._event_loop = None
            return results

    def keys(self, regex: str) -> list:
        return client.keys(self.conn, regex)

    def expire(self, key: str, timeout: int) -> int:
        return client.expire(self.conn, key, timeout)

    def nrkeys(self, regex: str) -> int:
        return client.nrkeys(self.conn, regex)

    def flush(self, regex: str) -> int:
        return client.flush(self.conn, regex)

    def capacity(self) -> int:
        return client.capacity(self.conn)

    def close(self):
        return client.close(self.conn)

    async def _set_async(self,
                         key: str,
                         sgl: client.SGL,
                         nsgl: int = 1,
                         timeout: int = client.PRISKV_KEY_MAX_TIMEOUT) -> int:
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")

        future = self._event_loop.create_future()
        callback = self._create_callback(future)

        try:
            client.set_async(self.conn, key, sgl, nsgl, timeout, callback)

            return await future
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise

    async def _get_async(self,
                         key: str,
                         sgl: client.SGL,
                         value_len: int,
                         nsgl: int = 1) -> int:
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")

        future = self._event_loop.create_future()
        callback = self._create_callback(future)

        try:
            client.get_async(self.conn, key, sgl, nsgl, value_len, callback)

            return await future
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise

    async def _test_async(self, key: str) -> bool:
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")

        future = self._event_loop.create_future()
        callback = self._create_callback(future)

        try:
            client.test_async(self.conn, key, callback)

            status = await future
            return status == 0
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise

    async def _delete_async(self, key: str) -> int:
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")

        future = self._event_loop.create_future()
        callback = self._create_callback(future)

        try:
            client.delete_async(self.conn, key, callback)

            return await future
        except Exception as e:
            if not future.done():
                future.set_exception(e)
            raise

    async def _gather_async_task(self, tasks: list[asyncio.Task]) -> list:
        return await asyncio.gather(*tasks, return_exceptions=True)

    def _add_epollfd_to_eventloop(self):
        # let asyncio eventloop handle epoll events
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")
        self._event_loop.add_reader(self._conn_epoll_fd,
                                    self._handle_epoll_events)

    def _remove_epollfd_from_eventloop(self):
        if not self._event_loop:
            raise RuntimeError("Asyncio event loop not initialized")
        self._event_loop.remove_reader(self._conn_epoll_fd)

    def _create_callback(self, future: asyncio.Future) -> Callable:
        """
        Create a callback to set the corresponding future when 
        the RDMA operation is completed and return the status.
        """

        def callback(status: int):
            if future.cancelled():
                return

            future.set_result(status)

        return callback

    def _handle_epoll_events(self):
        try:
            client.handle_epoll_events(self.conn)
        except Exception as e:
            print(f"Error handling epoll events: {e}")

    def _running_eventloop_check(self):
        try:
            asyncio.get_running_loop()
            # there is a event loop running, raise Execption which asyncio.run() will raise
            raise RuntimeError(
                "asyncio.Runner cannot be called from a running event loop")
        except RuntimeError:
            # RuntimeError Exception raise by get_running_loop means no running event loop,
            # that is expected
            pass
