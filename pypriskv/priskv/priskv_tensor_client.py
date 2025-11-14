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

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import torch

import priskv._priskv as client
from .priskv_client import PriskvClient


class PriskvTensorClient(PriskvClient):
    '''PriskvTensorClient:

    The client of PrisKV is specifically used for storing tensors.
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
        super().__init__(raddr, rport, laddr, lport, nqueue)

    def set(
        self,
        key: str,
        value: 'torch.Tensor',
        timeout: int = client.PRISKV_KEY_MAX_TIMEOUT,
    ) -> int:
        try:
            reg_buf = self.reg_memory(value.data_ptr(),
                                      value.element_size() * value.numel())
            if reg_buf == 0:
                return -1

            sgl = client.SGL(value.data_ptr(),
                             value.element_size() * value.numel(), reg_buf)
            return super().set(key, sgl, timeout=timeout)

        finally:
            self.dereg_memory(reg_buf)

    def get(self, key: str, value: 'torch.Tensor') -> int:
        try:
            reg_buf = self.reg_memory(value.data_ptr(),
                                      value.element_size() * value.numel())
            if reg_buf == 0:
                return -1

            sgl = client.SGL(value.data_ptr(),
                             value.element_size() * value.numel(), reg_buf)
            return super().get(key, sgl, value.element_size() * value.numel())
        finally:
            self.dereg_memory(reg_buf)

    def batch_set(self,
                  keys: list[str],
                  values: list['torch.Tensor'],
                  reg_threads: int = 4) -> list[int]:
        try:
            sgls = []
            data_ptrs = [value.data_ptr() for value in values]
            value_lens = [
                value.element_size() * value.numel() for value in values
            ]
            reg_bufs = self.batch_reg_memory(data_ptrs,
                                             value_lens,
                                             workers=reg_threads)

            for reg_buf, data_ptr, value_len in zip(reg_bufs, data_ptrs,
                                                    value_lens):
                sgls.append(client.SGL(data_ptr, value_len, reg_buf))

            return super().batch_set(keys, sgls)

        finally:
            for reg_buf in reg_bufs:
                self.dereg_memory(reg_buf)

    def batch_get(self,
                  keys: list[str],
                  values: list['torch.Tensor'],
                  reg_threads: int = 4) -> list[int]:
        try:
            sgls = []
            data_ptrs = [value.data_ptr() for value in values]
            value_lens = [
                value.element_size() * value.numel() for value in values
            ]
            reg_bufs = self.batch_reg_memory(data_ptrs,
                                             value_lens,
                                             workers=reg_threads)

            for reg_buf, data_ptr, value_len in zip(reg_bufs, data_ptrs,
                                                    value_lens):
                sgls.append(client.SGL(data_ptr, value_len, reg_buf))

            return super().batch_get(keys, sgls, value_lens)

        finally:
            for reg_buf in reg_bufs:
                self.dereg_memory(reg_buf)
