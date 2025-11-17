
# PrisKV

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)

PrisKV is specifically designed for modern high-performance computing (HPC) and
artificial intelligence (AI) computing. It solely supports RDMA. PrisKV also
supports GDR (GPU Direct RDMA), enabling the value of a key to be directly
transferred between PrisKV and the GPU.

## How to Build

### Prerequisites

#### Install Dependencies

For Debian/Ubuntu systems:

```bash
apt-get install -y git gcc make cmake librdmacm-dev rdma-core libibverbs-dev libncurses5-dev libmount-dev libevent-dev libssl-dev dpkg-dev debhelper python3-pybind11 python3-dev python3-pip libonig-dev libhiredis-dev liburing-dev
pip3 install pybind11 yapf==0.32.0
```

For RHEL/CentOS/Fedora systems:

```bash
yum install -y git gcc gcc-c++ make cmake librdmacm rdma-core-devel libibverbs ncurses-devel libmount-devel libevent-devel openssl-devel rpm-build rpmdevtools rpmlint python3-devel python3-pip hiredis-devel
pip3 install pybind11 yapf==0.32.0
```

#### Setup RXE (Soft RDMA)

For development and testing without dedicated RDMA hardware, you can use RXE (RDMA over Converged Ethernet), a software RDMA implementation:

```bash
rdma link add rxe_eth0 type rxe netdev eth0
```

This creates a soft RDMA device `rxe_eth0` on top of your Ethernet interface `eth0`.

### Build

**From source:**

```bash
make                      # Standard build

# CUDA or NPU build only affects benchmark lib, rest is same as standard build
make PRISKV_USE_CUDA=1    # Build with CUDA support (for GPU Direct RDMA)
make PRISKV_USE_ACL=1     # Build with NPU support (for Ascend NPU acceleration)
make rebuild              # Clean rebuild
```

**Build packages for different environments using Docker:**

```bash
make pkg-<env_name>       # Build for specific environment (e.g., make pkg-ubuntu2004)
make pkg-ubuntu1804 pkg-ubuntu2004 -j  # Build for multiple environments in parallel
```

Available environments: `./docker/Dockerfile_<env_name>`. Packages are generated in `./output/<env_name>/`.

## Usage

### Server Usage

#### Quick Start

Launch the server with anonymous memory:

```bash
./server/priskv-server -a 192.168.122.1
```

Launch with multiple addresses (IPv4 and IPv6):

```bash
./server/priskv-server -a 192.168.122.1 -a fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4
```

#### Server Command-Line Arguments

```
  -a, --addr ADDR
        Bind to ADDR (supports up to 16 addresses). Can be specified multiple times.
        Example: -a 192.168.1.1 -a 192.168.1.2

  -p, --port PORT
        Listen on PORT (default: 18512)

  -f, --memfile PATH
        Load memory file from tmpfs/hugetlbfs (enables persistence)

  -c, --max-inflight-command COMMANDS
        Maximum count of inflight commands (default: 128, max: 256)

  -s, --max-sgl SGLS
        Maximum count of scatter-gather list (default: 4, max: 16)

  -k, --max-keys KEYS
        Maximum count of key-value pairs (default: 65536, max: 16777216)

  -K, --max-key-length BYTES
        Maximum bytes of a key (default: 128, max: 4096)

  -v, --value-block-size BYTES
        Block size of minimal value in bytes (default: 4096, max: 1048576)

  -b, --value-blocks BLOCKS
        Count of value blocks, must be power of 2 (default: 65536, max: 16777216)

  -t, --threads THREADS
        Number of worker threads (default: 1)

  -e, --expire-routine-interval INTERVAL
        Interval to auto-clean expired KV in seconds (default: 600)

  -B, --busy
        Worker threads run in busy-poll mode (default: event-based)

  -l, --log-level LEVEL
        Log level: error, warn, notice (default), info, or debug

  -L, --log-file FILEPATH
        Log to FILEPATH

  -A, --http-addr ADDR
        HTTP server listen address (supports IPv4 and IPv6)

  -P, --http-port PORT
        HTTP server port (default: 18512)

  --http-cert PATH
        Path to certificate file (for HTTPS)

  --http-key PATH
        Path to key file (for HTTPS)

  --http-ca PATH
        Path to CA file (for HTTPS)

  --http-verify-client [off/optional/on]
        Client certificate verification mode (for HTTPS)

  --acl ADDRESS
        Access control list rule (can be specified multiple times)

  -u, --slow-query-threshold-latency-us
        Slow query threshold latency in microseconds (default: 1000)

  --backend ADDRESS
        Backend storage address (e.g., localfs:/data/priskv&size=100GB;s3:bucket1)

  -h, --help
        Show help message
```

### Memory File (Persistence)

PrisKV supports file-mapping based memory for persistence. The server loads key-value data from the file on startup and can recover after crashes or restarts.

**Note:** Only `hugetlbfs` and `tmpfs` are supported. Regular disk-based filesystems (ext4, xfs, etc.) are **not** supported.

#### Step 1: Create a Memory File

```bash
# Create on tmpfs:
./server/priskv-memfile -o create -f /run/memfile --max-keys 1024 --max-key-length 128 --value-block-size 4096 --value-blocks 4096

# Create on hugetlbfs
./server/priskv-memfile -o create -f /dev/hugepages/memfile --max-keys 1024 --max-key-length 128 --value-block-size 4096 --value-blocks 4096
```

**Query memfile information:**

```bash
./server/priskv-memfile -o info -f /dev/hugepages/memfile
```


#### Step 2: Launch Server with Memory File

```bash
./server/priskv-server -a 192.168.122.1 -f /run/memfile
```

### Access Control (ACL)

An access control list (ACL) specifies which clients are granted access to the PrisKV server.

```bash
./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4 # A single IPv6 address style
./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4/120 # IPv6 address with mask style
./server/priskv-server --acl 192.168.122.1 # A single IPv4 style
./server/priskv-server --acl 192.168.122.1 --acl 192.168.122.100 # Multiple IPv4 addresses style
./server/priskv-server --acl 192.168.122.1/24 # IPv4 address with mask style
./server/priskv-server --acl any # Any address is allowed
```

## Benchmarking

### PrisKV Benchmark

The client includes a benchmark tool for testing performance:

```bash
./client/priskv-benchmark -a <server-address> -p 18512 -o set -e 1 -k 16 -v 4096 -t 20
```

**With CUDA support** (requires building with `PRISKV_USE_CUDA=1`):

```bash
# Example usage for GPU Direct RDMA benchmarking
./client/priskv-example
```

See `client/example.c` for a complete CUDA/GDR usage example.

**With NPU support** (requires building with `PRISKV_USE_ACL=1`):

```bash
# NPU-accelerated operations
./client/priskv-benchmark <args>
```

### Valkey Comparison

To compare performance between PrisKV and Valkey:

```bash
git submodule init && git submodule update
cd client && make valkey-benchmark
./valkey-benchmark -a 127.0.0.1 -p 6379 -o set -e 1 -k 16 -v 4096 -t 20
```

## Client Usage

### C/C++ Client

PrisKV provides a native C/C++ client library with RDMA support.

**Basic example:** See `client/example.c` for a complete example demonstrating:
- Client initialization and connection
- CUDA integration for GPU Direct RDMA
- Memory management (device and host buffers)
- Asynchronous operations with epoll

**Build client applications:**

```bash
# Client library is built automatically with 'make all'
# Link against libpriskv.a
gcc your_app.c -I./include -L./client -lpriskv -lrdmacm -libverbs -o your_app
```

### Cluster Client

PrisKV supports cluster mode with automatic sharding and routing.

**Example:** See `cluster/client/example.c` for cluster client usage:

```c
// Connect to cluster
client = priskvClusterConnect("127.0.0.1", 6379, "kvcache-redis");

// Operations are automatically routed to the correct shard
priskvClusterSet(client, key, value, ...);
priskvClusterGet(client, key, ...);
```

### Python Client (PyPRISKV)

PrisKV provides a Python client for easy integration with Python applications.

#### Installation from Source

```bash
make all
cd pypriskv && pip3 install -v -e .
```

#### Installation from Wheel

```bash
make all
cd pypriskv && python3 setup.py build_ext bdist_wheel
pip3 install ./dist/*.whl
```

#### Usage

For usage examples, refer to:
- **Basic usage:** `./pypriskv/example.py`
- **Benchmarking:** `./pypriskv/benchmark.py`

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests to help improve PrisKV.

## License

PrisKV is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for details.
