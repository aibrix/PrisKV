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

from setuptools import setup, find_packages
try:
    from pybind11.setup_helpers import Pybind11Extension
except ImportError:
    from setuptools import Extension as Pybind11Extension

# LIBS = ["rdmacm", "ibverbs"]
ext_modules = [
    Pybind11Extension(
        "priskv._priskv._priskv_client",
        ["pybind.cpp"],
        include_dirs=["../include", "../client"],
        extra_link_args=["../client/libpriskv.a", "-lrdmacm", "-libverbs"],
    ),
]

setup(
    name='priskv',
    version='0.0.2',
    description=
    '''This is PrisKV's client. PrisKV is specifically designed for modern high-performance '''
    '''computing (HPC) and artificial intelligence (AI) computing. It solely supports RDMA. '''
    '''PrisKV also supports GDR (GPU Direct RDMA), enabling the value of a key to be directly '''
    '''transferred between PrisKV and the GPU.''',
    # package_data={
    #     'priskv._priskv': ['*.pyi', '*.so'],
    # },
    author='Lu Changqi <luchangqi.123@bytedance.com>',
    author_email='luchangqi.123@bytedance.com',
    license='''
         _______  _______     _____   ______   ___  ____  ____   ____  
        |_   __ \|_   __ \   |_   _|.' ____ \ |_  ||_  _||_  _| |_  _| 
          | |__) | | |__) |    | |  | (___ \_|  | |_/ /    \ \   / /   
          |  ___/  |  __ /     | |   _.____`.   |  __'.     \ \ / /    
         _| |_    _| |  \ \_  _| |_ | \____) | _| |  \ \_    \ ' /     
        |_____|  |____| |___||_____| \______.'|____||____|    \_/      

	 - Copyright (c) 2025 ByteDance Ltd. and/or its affiliates''',
    ext_modules=ext_modules,
    install_requires=["pybind11"],
    python_requires='>=3.8',
    packages=find_packages(),
    include_package_data=True,
    options={
        'bdist_wheel': {
            "plat_name": "manylinux2014_x86_64"
        },
    })
