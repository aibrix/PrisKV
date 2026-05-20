
# PrisKV

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

PrisKV is a key-value store designed for modern high-performance computing (HPC) and AI workloads. It supports RDMA, TCP, and shared-memory transports for efficient cross-host communication, and supports GPU Direct RDMA (GDR) so values can be transferred directly between server memory and GPU memory.

## Table of Contents

- [Quick Start](#quick-start)
- [Installation](#installation)
- [Running the Server](#running-the-server)
- [Clients](#clients)
- [Further Reading](#further-reading)
- [Contributing](#contributing)
- [License](#license)

## Quick Start

```bash
# 1. Build
make

# 2. Run the server, binding to one of your local addresses
./server/priskv-server -a 192.168.122.1

# 3. From another terminal, drive a quick smoke test
./client/priskv-benchmark -a 192.168.122.1 -p 18512 \
    -o set -e 1 -k 16 -v 4096 -t 5
```

No RDMA hardware? Set up a soft-RDMA device first — see [Setup RXE (Soft RDMA)](#setup-rxe-soft-rdma).

## Installation

### Dependencies

Debian/Ubuntu:

```bash
apt-get install -y git gcc make cmake \
    librdmacm-dev rdma-core libibverbs-dev \
    libncurses5-dev libmount-dev libevent-dev libssl-dev \
    dpkg-dev debhelper \
    python3-pybind11 python3-dev python3-pip \
    libonig-dev libhiredis-dev liburing-dev
pip3 install pybind11 yapf==0.32.0
```

RHEL/CentOS/Fedora:

```bash
yum install -y git gcc gcc-c++ make cmake \
    librdmacm rdma-core-devel libibverbs \
    ncurses-devel libmount-devel libevent-devel openssl-devel \
    rpm-build rpmdevtools rpmlint \
    python3-devel python3-pip hiredis-devel
pip3 install pybind11 yapf==0.32.0
```

### Setup RXE (Soft RDMA)

For development and testing without dedicated RDMA hardware, you can use RXE (RDMA over Converged Ethernet), a software RDMA implementation.

**1. Make sure the `rdma_rxe` kernel module is available.**

On many minimal cloud images (e.g. Ubuntu on AWS), `rdma_rxe` ships in the
`linux-modules-extra` package and is not installed by default:

```bash
# Check whether the module is already present
modinfo rdma_rxe >/dev/null 2>&1 && echo "rdma_rxe available" || echo "rdma_rxe missing"

# If missing on Debian/Ubuntu, install the extras package for the running kernel
sudo apt-get update
sudo apt-get install -y "linux-modules-extra-$(uname -r)"

# Load the module
sudo modprobe rdma_rxe
```

**2. Create the soft RDMA device on top of your Ethernet interface.**

Replace `<iface>` below with the name of your Ethernet interface (e.g. `eth0`, `ens5`, `enp0s3`). You can discover it with `ip -o link show` or with:

```bash
IFACE=$(ip route get 1 2>/dev/null | awk '{for (i=1;i<=NF;i++) if ($i=="dev") print $(i+1)}')
echo "Using $IFACE"
sudo rdma link add "rxe_${IFACE}" type rxe netdev "${IFACE}"
```

**3. Verify the link is up.**

```bash
rdma link
# Expected output (example):
# link rxe_ens5/1 state ACTIVE physical_state LINK_UP netdev ens5
```

If the link is `ACTIVE` and `LINK_UP`, PrisKV can use it as an RDMA device.

### Build

From source:

```bash
make                    # Standard build
make PRISKV_USE_CUDA=1  # With CUDA (GPU Direct RDMA, affects benchmark lib)
make PRISKV_USE_ACL=1   # With Ascend NPU support (affects benchmark lib)
make rebuild            # Clean rebuild
```

Build distribution packages with Docker:

```bash
make pkg-ubuntu2004                       # Single environment
make pkg-ubuntu1804 pkg-ubuntu2004 -j     # Multiple in parallel
```

Available environments correspond to `./docker/Dockerfile_<env_name>`. Output lands in `./output/<env_name>/`.

## Running the Server

The basics cover most setups. For the full option list, persistence, ACLs, and the HTTP management endpoint, see [docs/server-configuration.md](docs/server-configuration.md).

Bind to a local address and start serving:

```bash
./server/priskv-server -a 192.168.122.1
```

Common flags:

| Flag                       | Description |
|----------------------------|-------------|
| `-a, --addr ADDR`          | Bind to `ADDR`. Repeat for multiple addresses (IPv4 or IPv6). |
| `-p, --port PORT`          | Listen on `PORT` (default `18512`). |
| `-f, --memfile PATH`       | Load state from a memory file on `tmpfs`/`hugetlbfs` (persistence). |
| `--acl ADDRESS`            | Restrict access to `ADDRESS` (repeatable; `--acl any` to allow all). |
| `-l, --log-level LEVEL`    | `error`, `warn`, `notice` (default), `info`, or `debug`. |
| `-L, --log-file FILEPATH`  | Write logs to `FILEPATH` instead of stderr. |
| `-h, --help`               | Print the full help and exit. |

## Clients

PrisKV provides three client interfaces.

### C / C++

Native client with RDMA support. See [`client/example.c`](client/example.c) for a complete example covering connection setup, CUDA/GDR integration, and async operations.

```bash
# Client lib is built by 'make all'; link against libpriskv.a
gcc your_app.c -I./include -L./client -lpriskv -lrdmacm -libverbs -o your_app
```

### Cluster

Cluster mode adds automatic sharding and routing. See [`cluster/client/example.c`](cluster/client/example.c).

```c
client = priskvClusterConnect("127.0.0.1", 6379, "kvcache-redis");
priskvClusterSet(client, key, value, ...);
priskvClusterGet(client, key, ...);
```

### Python (pypriskv)

From source:

```bash
make all
cd pypriskv && pip3 install -v -e .
```

Or build a wheel:

```bash
make all
cd pypriskv && python3 setup.py build_ext bdist_wheel
pip3 install ./dist/*.whl
```

Examples:

- [`pypriskv/example.py`](pypriskv/example.py) — basic usage
- [`pypriskv/benchmark.py`](pypriskv/benchmark.py) — benchmarking

## Further Reading

- [Server configuration reference](docs/server-configuration.md) — all server flags, persistence (memfile), ACLs, HTTP/HTTPS endpoint.
- [Benchmarking](docs/benchmarking.md) — `priskv-benchmark`, CUDA/GDR, NPU, and Valkey comparison.
- [Samples](samples/) — end-to-end usage scenarios, including [`samples/kvcache-offloading/`](samples/kvcache-offloading/).
- [Cluster setup](samples/cluster/) — running PrisKV in cluster mode.

## Contributing

Contributions are welcome. Please open an issue or pull request. See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## License

PrisKV is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
