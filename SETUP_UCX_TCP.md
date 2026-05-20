# PrisKV: From Zero to Working (UCX TCP + Python client)

This guide targets developer environments without RDMA hardware and uses UCX over TCP for connectivity.
The goal is:
1. Verify `import priskv` works (Python client is installed)
2. Connect using `PriskvClient` to a running `priskv-server`
3. Optionally validate end-to-end `set/get`

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
  libonig-dev libhiredis-dev liburing-dev \
  libucx-dev libucx0
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
pip install --no-build-isolation -v -e .
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
export PRISKV_LOG_LEVEL=debug
```

---

## 5. Start `priskv-server` (UCX TCP)

```bash
source .venv/bin/activate
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

## 7. Readiness checklist for developers

- Successful `import priskv` indicates the Python extension is built/installed correctly.
- Server logs show `UCX ... ready` indicates UCX transport is initialized.
- Client logs show `established` indicates handshake/connection succeeded.
- If `set/get` hangs, investigate the end-to-end request/response path:
  - server response generation
  - client response receive callback
  - protocol struct decoding and completion-info handling

