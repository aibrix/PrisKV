# PrisKV: From Zero to Working (UCX TCP + Python client)

This guide targets developer environments without RDMA hardware and uses UCX over TCP for connectivity.
The goal is:
1. Verify `import priskv` works (Python client is installed)
2. Connect using `PriskvClient` to a running `priskv-server`
3. Optionally validate end-to-end `set/get`

> Note: PrisKV may require UCX-version-specific compatibility patches. If your system uses UCX 1.12, verify the items in **Section 7** exist in your source tree.

---

## 0. Prerequisites (Debian/Ubuntu)

### System dependencies

```bash
apt-get update
apt-get install -y \
  git gcc make cmake \
  librdmacm-dev rdma-core libibverbs-dev \
  libncurses5-dev libmount-dev libevent-dev libssl-dev \
  dpkg-dev debhelper \
  pkg-config \
  python3-pybind11 python3-dev python3-pip \
  libonig-dev libhiredis-dev liburing-dev
```

### Python build tooling

```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install pybind11 yapf==0.32.0
```

If you are running as `root` inside a container, `sudo` is usually not required.

---

## 1. Prepare the source tree

```bash
cd /PrisKV
```

If the repository uses submodules (e.g., `json-c`), initialize them:

```bash
git submodule update --init --recursive
```

---

## 2. Build `priskv-server` (C/C++)

Recommended: build everything once, then rebuild server if needed:

```bash
cd /PrisKV
make
```

Or build only the server (preferred if build without RDMA):

```bash
cd /PrisKV
make server
```

---

## 3. Build and install the Python client (`pypriskv`)

If you use a virtual environment (recommended), activate it first:

```bash
source .venv/bin/activate
pip install -U pip setuptools wheel
```

### Option A: Editable install (recommended for development)

```bash
cd /PrisKV
make all
cd pypriskv
pip install -v -e .
```

### Option B: Install from wheel

```bash
cd /PrisKV
make all
cd pypriskv
python3 setup.py build_ext bdist_wheel
pip install ./dist/*.whl
```

---

## 4. Configure runtime environment variables for UCX TCP

Make sure both server and client use the same transport configuration:

- `PRISKV_TRANSPORT=ucx`
- `UCX_TLS=tcp`
- If using direct mode (no Redis meta service): `PRISKV_CLIENT_DIRECT_MODE=y`

Example:

```bash
export PRISKV_TRANSPORT=ucx
export UCX_TLS=tcp
export PRISKV_CLIENT_DIRECT_MODE=y
```

Optional debugging:

```bash
export PRISKV_LOG_LEVEL=notice
```

---

## 5. Start `priskv-server` (UCX TCP)

```bash
cd /PrisKV
export PRISKV_TRANSPORT=ucx
export UCX_TLS=tcp
export PRISKV_CLIENT_DIRECT_MODE=y
export PRISKV_USE_SHM=n

./server/priskv-server -a 127.0.0.1 -p 6379 --acl any
```

Expected server log includes:
- `UCX: <...> ready`

---

## 6. Verify connectivity from Python

Activate your venv:

```bash
source .venv/bin/activate
```

Then run a minimal connectivity check:

```bash
export PRISKV_TRANSPORT=ucx
export UCX_TLS=tcp
export PRISKV_CLIENT_DIRECT_MODE=y

python - <<'PY'
import priskv

c = priskv.PriskvClient("127.0.0.1", 6379, "kvcache-redis")
print("connected")
c.close()
print("closed")
PY
```

Optional: validate `set/get`:

```bash
python - <<'PY'
import priskv

c = priskv.PriskvClient("127.0.0.1", 6379, "kvcache-redis")
print("setstr:", c.setstr("k1", "v1", 2000))
print("getstr:", c.getstr("k1"))
c.close()
PY
```

---

## 7. UCX 1.12 compatibility patches to verify

If your system UCX is 1.12, PrisKV may need compatibility adjustments to avoid build errors or protocol/ABI mismatches.
Verify the following items exist in your checkout.

### 7.1 `PrisKV/lib/config.c`: UCX config parser API compatibility

- Add `<string.h>` (avoid implicit `strcmp`)
- Fix `ucs_config_parser_print_opts` call signature (argument count)
- Fix wrapper signatures/arguments for `ucs_config_parser_fill_opts` and `ucs_config_parser_set_value`

### 7.2 `PrisKV/include/priskv-config.h`: config table struct compatibility

- In `PRISKV_CONFIG_DECLARE_TABLE`, remove `.flags = 0` if the field does not exist in UCX 1.12.

### 7.3 `PrisKV/lib/ucx.c`: UCX 1.12 API compatibility

- Replace packed RKEY release logic with UCX 1.12 behavior (`ucp_rkey_buffer_release()`)
- Adjust `ucp_worker_get_address()` length argument to `size_t*` and convert back to PrisKV types as needed

### 7.4 `PrisKV/lib/ucx.c` (client/server UCX wrapper): tag-recv completion info

- In `priskv_ucx_post_tag_recv()`, ensure `ucp_request_param_t.op_attr_mask` includes `UCP_OP_ATTR_FIELD_RECV_INFO`

If you still see messages like `UCX: recv <...>, expected 48` or endpoint timeouts, continue deeper protocol-level debugging (request/response completion info and response struct decoding).

---

## 8. Readiness checklist for developers

- Successful `import priskv` indicates the Python extension is built/installed correctly.
- Server logs show `UCX ... ready` indicates UCX transport is initialized.
- Client logs show `established` indicates handshake/connection succeeded.
- If `set/get` hangs, investigate the end-to-end request/response path:
  - server response generation
  - client response receive callback
  - protocol struct decoding and completion-info handling

