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
import argparse


class PriskvExample():

    def __init__(self, args):
        self.raddr = args.raddr
        self.rport = args.rport
        self.password = args.password

        ######## PriskvClient ########
        raw_client = priskv.PriskvClient(self.raddr, self.rport, self.password)
        assert (raw_client != 0)

        self.raw_client = raw_client

    def get_ptr(self):
        pass

    def run_example(self):
        # set sgl
        self.reg = self.raw_client.reg_memory(self.get_ptr(),
                                              self.reg_buf_bytes)
        self.sgl = priskv.SGL(self.get_ptr(), self.reg_buf_bytes, self.reg)

        # set key-value
        # device -> database
        ret = self.raw_client.set("key", self.sgl)
        assert (ret == 0)

        # exists key
        ret = self.raw_client.exists("key")  # self.reg_buf_bytes)
        assert (ret == 0)

    def clear(self):
        # delete key
        ret = self.raw_client.delete("key")
        if ret != 0:
            print("delete key failed")

        # dereg mr
        self.raw_client.dereg_memory(self.reg)

        # close conn
        self.raw_client.close()


class PriskvNumpyExample(PriskvExample):

    def __init__(self, args):
        import numpy as np

        super().__init__(args)

        # register mr
        self.reg_buf = np.random.rand((1024 * 4)).astype(np.float32)
        self.reg_buf_bytes = self.reg_buf.nbytes

    def get_ptr(self):
        super().get_ptr()
        return self.reg_buf.ctypes.data

    def run_example(self):
        import numpy as np
        super().run_example()

        new_reg_buf = np.zeros((1024 * 4)).astype(np.float32)
        new_reg_buf_bytes = new_reg_buf.nbytes
        assert (new_reg_buf_bytes == self.reg_buf_bytes)

        new_reg = self.raw_client.reg_memory(new_reg_buf.ctypes.data,
                                             new_reg_buf_bytes)
        new_sgl = priskv.SGL(new_reg_buf.ctypes.data, new_reg_buf_bytes, new_reg)

        # get key-value
        # database -> device
        ret = self.raw_client.get("key", new_sgl, new_reg_buf_bytes)
        assert (ret == 0)

        # data consistency verification
        assert (np.array_equal(self.reg_buf, new_reg_buf))

        self.raw_client.dereg_memory(new_reg)


def example(args):
    priskv_example = PriskvNumpyExample(args)

    priskv_example.run_example()
    priskv_example.clear()

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
    example(args)


if __name__ == "__main__":
    main()
