
# PrisKV

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

PrisKV is specifically designed for modern high-performance computing (HPC) and
artificial intelligence (AI) computing. It solely supports RDMA. PrisKV also
supports GDR (GPU Direct RDMA), enabling the value of a key to be directly
transferred between PrisKV and the GPU.

# How to Build

To prepare build environment:

  ```bash
  apt-get install -y git gcc make cmake librdmacm-dev rdma-core libibverbs-dev libncurses5-dev libmount-dev libevent-dev libssl-dev dpkg-dev debhelper python3-pybind11 python3-dev python3-pip libonig-dev libhiredis-dev liburing-dev
  pip3 install pybind11 yapf==0.32.0
  ```
  or
  ```bash
  yum install -y git gcc gcc-c++ make cmake librdmacm rdma-core-devel libibverbs ncurses-devel libmount-devel libevent-devel openssl-devel rpm-build rpmdevtools rpmlint python3-devel python3-pip hiredis-devel
  pip3 install pybind11 yapf==0.32.0
  ```

To build PrisKV:

  ```bash
  make

  make PRISKV_USE_CUDA=1 # benchmark with CUDA support

  make PRISKV_USE_ACL=1 # benchmark with NPU support
  ```

To rebuild PrisKV:

  ```bash
  make rebuild
  ```

After a successful build, launch the server with anonymous memory:

  ```bash
  ./server/priskv-server -a 192.168.122.1 -a fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4 --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4/120
  ```

# ACL

An access control list (ACL) is a list of rules that specifies which clients are granted
access to a PrisKV server.

  ```bash
  ./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4 # A single IPv6 address style
  ./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4/120 # IPv6 address with mask style
  ./server/priskv-server --acl 192.168.122.1 # A single IPv4 style
  ./server/priskv-server --acl 192.168.122.1 --acl 192.168.122.100 # Multiple IPv4 addresses style
  ./server/priskv-server --acl 192.168.122.1/24 # IPv4 address with mask style
  ./server/priskv-server --acl any # Any address is allowed
  ```

# Memory file

PrisKV supports file-mapping based memory. priskv-server tries to load Key-Value
from file on startup. Once priskv-server crashes or gets killed, it will recover
on restart.

Note that only hugetlbfs and tmpfs are supported, regular disk based file (Ex, ext4/xfs)
is not supported.

## step 1 - create a file

To create a file on tmpfs:

  ```bash
  ./priskv-memfile -o create -f /run/memfile --max-keys 1024 --max-key-length 128 --value-block-size 4096 --value-blocks 4096
  ```

To create a file on hugetlbfs:

  ```bash
  ./priskv-memfile -o create -f /dev/hugepages/memfile --max-keys 1024 --max-key-length 128 --value-block-size 4096 --value-blocks 4096
  ```

To query information of a priskv memory file:

  ```bash
  ./priskv-memfile -o info -f /dev/hugepages/memfile
  ```

## step 2 - launch priskv-server with priskv memory file

  ```bash
  ./priskv-server -a 192.168.122.1 -A localhost -f /run/memfile
  ```

If you want to use http server for getting information, please run

  ```bash
  # http server default port 18512
  ./priskv-server -a 192.168.122.1 -A localhost -f /run/memfile
  ```

, then http server will listen on localhost:18512. 

If you want to use https server, please refer to [https server](scripts/certificate/README.md).

# RXE

To setup a soft RDMA environment:

  ```bash
  rdma link add rxe_eth0 type rxe netdev eth0
  ```

# Valkey Benchmark

If you want to compare the performance between PrisKV and Valkey, just run the Valkey benchmark:

  ```bash
  git submodule init && git submodule update
  cd client && make valkey-benchmark
  ./valkey-benchmark -a 127.0.0.1 -p 6379 -o set -e 1 -k 16 -v 4096 -t 20
  ```

# Python Client(PyPRISKV)

PrisKV provides a Python client for easy access. To install the Python client:

If you install using source code, run:

  ```bash
  make all
  cd pypriskv && pip3 install -v -e .
  ```

If you install using binary, run:

  ```bash
  make all
  cd pypriskv && python3 setup.py build_ext bdist_wheel
  ```

Then a file will be generated(./dist/*.whl), install it by:

  ```bash
  pip3 install ./dist/*.whl
  ```

For usage, please refer to the code in `./pypriskv/example.py`.
For benchmark, please refer to the code in `./pypriskv/benchmark.py`.

# Build packages in different environment by docker

Build packages in different environment with:

  ```bash
  make pkg-<env name>
  ```

You could find `<env name>` in ./docker/Dockerfile_`<env name>`.

For example, if you want to build deb packages in Ubuntu 20.04, run:

  ```bash
  make pkg-ubuntu2004
  ```

and you could find the deb packages in ./output/ubuntu20.04.

If you want to build packages in other environment, you could add a new Dockerfile in ./docker. For example, if you want to build packages in Ubuntu 18.04, you could add a new Dockerfile in ./docker with name Dockerfile_ubuntu1804. Then run:

  ```bash
  make pkg-ubuntu1804
  ```

If you want to build packages for different environment at the same time, you could run:

  ```bash
  make pkg-ubuntu1804 pkg-ubuntu2004 -j
  ```

# Contributing

Contributions are welcome! Please feel free to submit issues or pull requests to help improve PrisKV.

# License

PrisKV is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
