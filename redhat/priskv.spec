Name:           priskv
Version:        20250327
Release:        1%{?dist}
Source0:        %{name}.tar
Summary:        RPM packages for PrisKV server, client and some utilities.
License:        Beijing Volcano Engine Technology Ltd - 2024. All rights reserved.
BuildRequires:  git gcc gcc-c++ make cmake librdmacm rdma-core-devel libibverbs ncurses-devel libmount-devel libevent-devel openssl-devel

%description
PrisKV is specifically designed for modern high-performance computing (HPC) \
and artificial intelligence (AI) computing. It solely supports RDMA. \
PrisKV also supports GDR (GPU Direct RDMA), enabling the value of a key to \
be directly transferred between PrisKV and the GPU.

%package server
Summary:        RPM packages for PrisKV server.

%description server
priskv-server is a server that provides a key-value store service.

%package client
Summary:        RPM packages for PrisKV client.

%description client
priskv-client is a client that provides a key-value store service.

%package pyclient
Summary:        RPM packages for PrisKV python client.
Requires:       python3

%description pyclient
priskv-pyclient is a python client that provides a key-value store service.

%prep
%setup -q -n %{name}

%build
make all -j

%install
make install-server PRISKV_DESTDIR=%{buildroot} PRISKV_PKG_VERSION=%{version}
make install-client PRISKV_DESTDIR=%{buildroot} PRISKV_PKG_VERSION=%{version}
make install-pypriskv PRISKV_DESTDIR=%{buildroot} PRISKV_PKG_VERSION=%{version} PYTHON3_SITEPACKAGES=%{python3_sitelib}

%files server
%{_bindir}/priskv-server
%{_bindir}/priskv-memfile
%{_mandir}/man1/priskv-server.1.gz

%files client
%{_bindir}/priskv-client
%{_bindir}/priskv-benchmark
%{_includedir}/priskv/priskv.h
%{_libdir}/libpriskv.a
%{_mandir}/man7/libpriskv.7.gz

%files pyclient
%{python3_sitelib}/priskv/__pycache__/__init__.cpython-39.opt-1.pyc
%{python3_sitelib}/priskv/__pycache__/__init__.cpython-39.pyc
%{python3_sitelib}/priskv/__pycache__/priskv_client.cpython-39.opt-1.pyc
%{python3_sitelib}/priskv/__pycache__/priskv_client.cpython-39.pyc
%{python3_sitelib}/priskv/__pycache__/priskv_tensor_client.cpython-39.opt-1.pyc
%{python3_sitelib}/priskv/__pycache__/priskv_tensor_client.cpython-39.pyc
%{python3_sitelib}/priskv/__init__.py
%{python3_sitelib}/priskv/_client.cpython-39-x86_64-linux-gnu.so
%{python3_sitelib}/priskv/example/__pycache__/benchmark.cpython-39.opt-1.pyc
%{python3_sitelib}/priskv/example/__pycache__/benchmark.cpython-39.pyc
%{python3_sitelib}/priskv/example/__pycache__/example.cpython-39.opt-1.pyc
%{python3_sitelib}/priskv/example/__pycache__/example.cpython-39.pyc
%{python3_sitelib}/priskv/example/benchmark.py
%{python3_sitelib}/priskv/example/example.py
%{python3_sitelib}/priskv/priskv_client.py
%{python3_sitelib}/priskv/priskv_tensor_client.py
%{_mandir}/man7/pypriskv.7.gz

%changelog
* Thu Mar 27 2025 Rui Zhang <zhangrui.1203@bytedance.com> - 20250327
  - Client: Support multiple queues.
  - Client: Support automatic registration memory.
  - Client: Support cluster mode.
  - Benchmark: Generate the random keys by UUID.
  - Benchmark: Support cluster mode.
  - Server: Support KV copy/move command in HTTP service.

* Thu Feb 27 2025 Rui Zhang <zhangrui.1203@bytedance.com> - 20250227
  - Optimize BYTES param with unit.
  - Benchmark supports to use CPU memory for transferring data.
  - Benchmark supports NPU device.
  - Speedup anonymous memory initialization.
  - Replace bytes `key` with string `key` in client API.
  - Remove connection params in client API.

* Thu Dec 26 2024 zhenwei pi <pizhenwei@bytedance.com> - 20241226
  - Support to detect capacity from server.
  - Support huge value size.
  - Support NRKEYS command.
  - Support FLUSH command.
  - Support to set logging file.
  - Support to get or set ACL rules by HTTP service.
  - Support to clean up expired KV in background.

* Fri Nov 22 2024 zhenwei pi <pizhenwei@bytedance.com> - 20241122
  - Support GDR (GPU Direct RDMA). The fastest in theory.
  - Support RDMA only, the server listens as more as 32 addresses.
  - Supports File-Mapping based memory.
  - Support anonymouse memory as KV store memory.
  - Zero memory copy for value part.
  - Support ACL.
  - Support multiple outstanding commands.
  - Support tmpfs as KV store memory backend.
  - Support hugetlbfs as KV store memory backend.
  - Support C/C++ client library.
  - Support Python client.
  - Support HTTP/HTTPS control console.
  - Support key expiration.
