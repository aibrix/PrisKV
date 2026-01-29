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
from typing import List, Tuple, Optional


class PriskvClient:
    '''PriskvClient:

    Keep the same parameters with the C client.

    Args:
        raddr (str): remote address (redis addr)
        rport (int): remote port (redis port)
    '''

    def __init__(self, raddr: str, rport: int, password: str):
        self.raddr = raddr
        self.rport = rport
        self.password = password

        self.conn = client.connect(raddr, rport, password)
        if self.conn == None:
            raise RuntimeError(
                f"Failed to connect to Priskv meta server at {self.raddr}:{self.rport}."
            )

    def reg_memory(self,
                   iova: int,
                   length: int,
                   offset: int = 0,
                   fd: int = -1) -> int:
        if offset == 0:
            offset = iova
        return client.reg_memory(self.conn, offset, length, iova, fd)

    def dereg_memory(self, mem: int) -> int:
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

    def get(self,
            key: str,
            sgl: client.SGL,
            value_len: int,
            nsgl: int = 1) -> int:
        return client.get(self.conn, key, sgl, nsgl, value_len)

    def getstr(self, key: str) -> Optional[str]:
        val = client.getstr(self.conn, key)
        return val if len(val) > 0 else None

    def delete(self, key: str) -> int:
        status = client.delete(self.conn, key)
        return status

    def exists(self, key: str) -> int:
        return client.exists(self.conn, key)

    def mset(
            self,
            keys: List[str],
            sgls: List[client.SGL],
            timeout: int = client.PRISKV_KEY_MAX_TIMEOUT
    ) -> Tuple[int, List[int]]:
        mset_status = [0] * len(keys)
        status = client.mset(self.conn, keys, sgls, timeout, mset_status)
        return (status, mset_status)
    
    def mset_and_pin(
            self,
            keys: List[str],
            sgls: List[client.SGL],
            timeout: int = client.PRISKV_KEY_MAX_TIMEOUT,
    ) -> Tuple[int, List[int], int]:
        mset_status = [0] * len(keys)
        status, token = client.mset_and_pin(self.conn, keys, sgls, timeout, mset_status)
        return (status, mset_status, token)


    def mget(self, keys: List[str], sgls: List[client.SGL],
             value_lens: List[int]) -> Tuple[int, List[int]]:
        mget_status = [0] * len(keys)
        status = client.mget(self.conn, keys, sgls, value_lens, mget_status)
        return (status, mget_status)
    
    def mget_and_pin(self, keys: List[str], sgls: List[client.SGL],
            value_lens: List[int]) -> Tuple[int, List[int], int]:
        mget_status = [0] * len(keys)
        status, token = client.mget_and_pin(self.conn, keys, sgls, value_lens, mget_status)
        return (status, mget_status, token)
    
    def mget_and_unpin(self, keys: List[str], sgls: List[client.SGL],
            value_lens: List[int], token : int) -> Tuple[int, List[int], int]:
        mget_status = [0] * len(keys)
        status = client.mget_and_pin(self.conn, keys, sgls, value_lens, mget_status, token)
        return (status, mget_status)

    def mset_and_pin(self, keys: List[str], sgls: List[client.SGL],
            
            ) -> Tuple[int, List[int], int]:
        mget_status = [0] * len(keys)
        status, token = client.mset_and_pin(self.conn, keys, sgls, value_lens, mget_status)
        return (status, mget_status, token)

    def mexists(self, keys: List[str]) -> Tuple[int, List[int]]:
        mexist_status = [0] * len(keys)
        status = client.mexists(self.conn, keys, mexist_status)
        return (status, mexist_status)

    def mdel(self, keys: List[str]) -> Tuple[int, List[int]]:
        mdel_status = [0] * len(keys)
        status = client.mdel(self.conn, keys, mdel_status)
        return (status, mdel_status)

    def keys(self, regax: str) -> List[str]:
        return client.keys(self.conn, regax)

    def close(self):
        return client.close(self.conn)
