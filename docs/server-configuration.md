# Server Configuration

Reference for `priskv-server` command-line options, persistence via memory files, and access control. For a quick start, see the [README](../README.md#quick-start).

## Table of Contents

- [Command-Line Options](#command-line-options)
  - [Network](#network)
  - [Capacity and Sizing](#capacity-and-sizing)
  - [Workers and Expiry](#workers-and-expiry)
  - [Persistence](#persistence)
  - [HTTP / HTTPS Management Endpoint](#http--https-management-endpoint)
  - [Access Control](#access-control)
  - [Logging](#logging)
  - [Backend Storage](#backend-storage)
  - [Diagnostics](#diagnostics)
- [Persistence (Memory File)](#persistence-memory-file)
- [Access Control (ACL)](#access-control-acl)

## Command-Line Options

`priskv-server -h` prints the full list. The tables below group options by purpose; defaults reflect the source tree at the time of writing.

### Network

| Flag                         | Default | Description |
|------------------------------|---------|-------------|
| `-a, --addr ADDR`            | —       | Bind to `ADDR`. May be specified up to 16 times. IPv4 and IPv6 are both accepted. |
| `-p, --port PORT`            | `18512` | Listen on `PORT` for the transport (RDMA/TCP/UCX). |

Example:

```bash
./server/priskv-server -a 192.168.1.1 -a 192.168.1.2 -p 18512
```

### Capacity and Sizing

| Flag                              | Default | Max         | Description |
|-----------------------------------|---------|-------------|-------------|
| `-k, --max-keys KEYS`             | `65536` | `16777216`  | Maximum number of key/value pairs. |
| `-K, --max-key-length BYTES`      | `128`   | `4096`      | Maximum bytes per key. |
| `-v, --value-block-size BYTES`    | `4096`  | `1048576`   | Block size (in bytes) for the minimal value unit. |
| `-b, --value-blocks BLOCKS`       | `65536` | `16777216`  | Count of value blocks. Must be a power of 2. |
| `-c, --max-inflight-command CMDS` | `128`   | `256`       | Maximum count of in-flight commands. |
| `-s, --max-sgl SGLS`              | `4`     | `16`        | Maximum scatter-gather list count. |

> When using a memory file (`-f`), these sizing flags must match the values
> the file was created with. See [Persistence (Memory File)](#persistence-memory-file).

### Workers and Expiry

| Flag                                  | Default | Description |
|---------------------------------------|---------|-------------|
| `-t, --threads THREADS`               | `1`     | Number of worker threads. |
| `-B, --busy`                          | off     | Worker threads run in busy-poll mode (default is event-based). |
| `-e, --expire-routine-interval SECS`  | `600`   | Interval to scan for and clean expired keys, in seconds. |

### Persistence

| Flag                  | Description |
|-----------------------|-------------|
| `-f, --memfile PATH`  | Load key-value state from a memory file on `tmpfs` or `hugetlbfs`. Enables persistence across restarts. See [Persistence (Memory File)](#persistence-memory-file). |

### HTTP / HTTPS Management Endpoint

The HTTP endpoint is **disabled by default**. It starts only when
`-A/--http-addr` is provided.

| Flag                                       | Default | Description |
|--------------------------------------------|---------|-------------|
| `-A, --http-addr ADDR`                     | —       | HTTP server bind address (IPv4 or IPv6). Required to enable the endpoint. |
| `-P, --http-port PORT`                     | `18512` | HTTP server port. Shares its default with `-p`; pick a distinct value if you enable HTTP. |
| `--http-cert PATH`                         | —       | TLS certificate file (enables HTTPS). |
| `--http-key PATH`                          | —       | TLS private key file. |
| `--http-ca PATH`                           | —       | CA file for verifying client certificates. |
| `--http-verify-client off\|optional\|on`   | `off`   | Client certificate verification mode. |

### Access Control

| Flag             | Description |
|------------------|-------------|
| `--acl ADDRESS`  | Allow a client address (or CIDR). May be specified multiple times. Use `--acl any` to allow all. See [Access Control (ACL)](#access-control-acl). |

### Logging

| Flag                          | Default  | Description |
|-------------------------------|----------|-------------|
| `-l, --log-level LEVEL`       | `notice` | One of `error`, `warn`, `notice`, `info`, `debug`. |
| `-L, --log-file FILEPATH`     | stderr   | Write logs to `FILEPATH`. |

### Backend Storage

| Flag                  | Description |
|-----------------------|-------------|
| `--backend ADDRESS`   | Tiered/backing storage address. Supports multiple backends separated by `;`. |

Example (quote the argument so the shell does not interpret `&` or `;`):

```bash
./server/priskv-server -a 192.168.1.1 \
  --backend 'localfs:/data/priskv&size=100GB;s3:bucket1'
```

### Diagnostics

| Flag                                                 | Default | Description |
|------------------------------------------------------|---------|-------------|
| `-u, --slow-query-threshold-latency-us LATENCY`      | `1000`  | Log queries slower than `LATENCY` microseconds. |
| `-h, --help`                                         | —       | Print help and exit. |

## Persistence (Memory File)

PrisKV uses file-backed memory for persistence. The server loads key-value state from the file on startup and can recover after crashes or restarts.

> **Only `tmpfs` and `hugetlbfs` are supported.** Regular disk-based
> filesystems (ext4, xfs, etc.) are **not** supported.

### Create a memory file

```bash
# On tmpfs:
./server/priskv-memfile -o create -f /run/memfile \
    --max-keys 1024 --max-key-length 128 \
    --value-block-size 4096 --value-blocks 4096

# On hugetlbfs:
./server/priskv-memfile -o create -f /dev/hugepages/memfile \
    --max-keys 1024 --max-key-length 128 \
    --value-block-size 4096 --value-blocks 4096
```

Inspect an existing memfile:

```bash
./server/priskv-memfile -o info -f /dev/hugepages/memfile
```

### Launch with a memory file

```bash
./server/priskv-server -a 192.168.122.1 -f /run/memfile
```

The server's sizing flags (`--max-keys`, `--max-key-length`, `--value-block-size`, `--value-blocks`) must match the values the memfile was created with. Mismatches cause the server to refuse the file.

## Access Control (ACL)

An ACL specifies which client addresses are allowed to talk to the server.
Multiple `--acl` flags may be combined.

```bash
# Single IPv6 address
./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4

# IPv6 with prefix length
./server/priskv-server --acl fdbd:ff1:ce00:4c7:156a:a66b:b407:19c4/120

# Single IPv4 address
./server/priskv-server --acl 192.168.122.1

# Multiple IPv4 addresses
./server/priskv-server --acl 192.168.122.1 --acl 192.168.122.100

# IPv4 with mask
./server/priskv-server --acl 192.168.122.1/24

# Allow any address
./server/priskv-server --acl any
```
