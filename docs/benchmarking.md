# Benchmarking

Tools and recipes for measuring PrisKV performance. For a quick smoke test, see the [README](../README.md#quick-start). For server tuning, see [Server Configuration](server-configuration.md).

## Table of Contents

- [priskv-benchmark](#priskv-benchmark)
- [CUDA / GPU Direct RDMA](#cuda--gpu-direct-rdma)
- [Ascend NPU](#ascend-npu)
- [Comparison vs Valkey](#comparison-vs-valkey)

## priskv-benchmark

The client ships with a benchmark tool that drives load against a running server.

```bash
./client/priskv-benchmark -a <server-address> -p 18512 \
    -o set -e 1 -k 16 -v 4096 -t 20
```

Common flags:

| Flag           | Description |
|----------------|-------------|
| `-a ADDR`      | Server address. |
| `-p PORT`      | Server port (default `18512`). |
| `-o OP`        | Operation: `set`, `get`, etc. |
| `-k BYTES`     | Key length. |
| `-v BYTES`     | Value length. |
| `-t SECS`      | Test duration. |
| `-e N`         | Number of inflight requests per client. |

Run `./client/priskv-benchmark -h` for the full list.

## CUDA / GPU Direct RDMA

Requires building with `PRISKV_USE_CUDA=1`:

```bash
make PRISKV_USE_CUDA=1
```

The repo ships an end-to-end CUDA/GDR example. See [`client/example.c`](../client/example.c) for the full source covering:

- client initialization and connection
- CUDA integration for GPU Direct RDMA
- device and host buffer management
- asynchronous operations driven by `epoll`

Run it:

```bash
./client/priskv-example
```

## Ascend NPU

Requires building with `PRISKV_USE_ACL=1`:

```bash
make PRISKV_USE_ACL=1
```

Then run the standard benchmark binary; NPU-resident buffers are used when available:

```bash
./client/priskv-benchmark -a <server-address> -p 18512 \
    -o set -e 1 -k 16 -v 4096 -t 20
```

## Comparison vs Valkey

Build the bundled `valkey-benchmark` for a side-by-side comparison:

```bash
git submodule init && git submodule update
cd client && make valkey-benchmark

./valkey-benchmark -a 127.0.0.1 -p 6379 -o set -e 1 -k 16 -v 4096 -t 20
```

Match the flags (`-k`, `-v`, `-e`, `-t`) when comparing to `priskv-benchmark` so the two runs exercise comparable workloads.
