## KVCache Offloading with PrisKV — Quickstart

This guide helps you launch a complete KVCache offloading setup with PrisKV, an inference engine (vLLM or SGLang), and a benchmark container using Docker Compose.

What you’ll do:
- Prepare the PrisKV server image (prebuilt or build from source)
- Prepare an inference engine image (vLLM or SGLang) that already has aibrix_kvcache
- Optionally install the PrisKV Python client SDK (if you build custom images)
- Launch everything with Docker Compose and verify

### Prerequisites
- A Linux host with Docker and Docker Compose installed 
- NVIDIA GPU drivers and runtime set up (required for vLLM/SGLang GPU inference)
- Privileged mode enabled for Docker (required for PrisKV server and engine containers)

## PrisKV Server Image
### Use prebuilt image
- kvcache-container-image-hb2-cn-beijing.cr.volces.com/aibrix/priskv:v0.0.2

### Build from source
```bash
# Clone the repo
git clone https://github.com/aibrix/PrisKV
cd PrisKV

# Build a server image from source (use Ubuntu 22.04 Dockerfile available in repo)
TAG="aibrix/priskv:v0.0.2"
docker build . -t ${TAG} --network=host -f ./docker/Dockerfile_ubuntu2204
```

## Inference Engine Image
You need an inference engine image with aibrix_kvcache and PrisKV (either vLLM or SGLang). You can use one of the prebuilt images below or build your own following the linked instructions.

Prebuilt images:
- vLLM + aibrix_kvcache + nixl + PrisKV: aibrix-container-registry-cn-beijing.cr.volces.com/aibrix/vllm-openai:v0.10.2-aibrix0.5.1-nixl0.7.1-priskv0.0.2-20251121
- SGLang + aibrix_kvcache + nixl + PrisKV: aibrix-container-registry-cn-beijing.cr.volces.com/aibrix/sglang:v0.5.5.post3-aibrix0.5.1-nixl0.7.1-priskv0.0.2-20251121

If you need to build the inference engine image yourself, please refer to https://github.com/vllm-project/aibrix/tree/main/python/aibrix_kvcache/integration/ for building base images with aibrix_kvcache and follow the following steps to install PrisKV Python client SDK.

### PrisKV Python Client Installation (optional)
If you’re building a custom engine image, you may need the PrisKV Python SDK.

#### Install dependencies
```bash
apt update && apt install -y \
  git gcc make cmake librdmacm-dev rdma-core libibverbs-dev \
  libncurses5-dev libmount-dev libevent-dev libssl-dev \
  libhiredis-dev liburing-dev
```

#### Option A: Install with pip (prebuilt wheel or from PyPI)
```bash
# From PyPI (simplest)
pip install pypriskv
```

#### Option B: Build the Python client from source
```bash
git clone https://github.com/aibrix/PrisKV
cd PrisKV
make pyclient
# Install the built wheel (adjust the version tag to the one you built)
pip install pypriskv/dist/priskv-0.0.2-cp312-cp312-manylinux2014_x86_64.whl
```

## Benchmark and Deployment with Docker Compose
- Use any deploy.yaml under kvcache-offloading/xxx/ (e.g., vLLM or SGLang directories). These compose files typically start: PrisKV server, the engine, and a benchmark container.

### Customize settings
#### PrisKV Configuration
Please refer to https://aibrix.readthedocs.io/latest/designs/aibrix-kvcache-offloading-framework.html#priskv-connector-configuration for connector configuration.

Modify `PRISKV_CLUSTER_META` to describe the consistent hashing topology for your PrisKV cluster. For a single-server setup:
```json
{
  "version": 1,
  "nodes": [
    {
      "name": "node0",
      "addr": "<REPLACE_WITH_SERVER_IP>",
      "port": 9000,
      "slots": [
        { "start": 0, "end": 4095 }
      ]
    }
  ]
}
```
Tips:
- Replace <REPLACE_WITH_SERVER_IP> with the reachable IP of your PrisKV server and update `-a` in the PrisKV server command.
- If port 9000 is not available, change it to a free port in `PRISKV_CLUSTER_META` and update `-p` in the PrisKV server command.

#### PrisKV Server Arguments
Please refer to https://github.com/aibrix/PrisKV/blob/main/README.md#server-command-line-arguments for all available server command line arguments.

#### Engine Configuration
  - `ENGINE_PORT`: port for engine to listen on (default: `18000`).
  - `MODEL`: folder name under `/data01/models` on host.
  - `TP`: tensor parallelism size; ensure it matches `CUDA_VISIBLE_DEVICES` count.
  - `VLLM_KV_CONFIG`: can use blank string to disable KV connector.
  - `SGLANG_HICACHE_STORAGE_BACKEND`: storage backend for SGLang HiCache.

### Deploy
- Run `docker compose -f deploy.yaml up -d` to start all services
- Run `docker compose -f deploy.yaml ps` to list running containers
- Run `docker compose -f deploy.yaml logs -f` to stream logs from all services
- Or, run `docker compose -f deploy.yaml logs -f engine` and `docker compose -f deploy.yaml logs -f bench` to view engine and benchmark logs separately
- Run `docker compose -f deploy.yaml stop` to stop all services

### Step-by-step: vLLM example
1) Navigate to the vLLM sample directory: `cd samples/kvcache-offloading/vllm`.
2) Ensure your model files exist on the host at `/data01/models/<MODEL>`. If not, download or place your model there. Alternatively, update the `volumes:` and `--model` path in the compose file to match your host path.
3) Open `deploy.yaml` and adjust:
   - `PRISKV_CLUSTER_META`: use the single-node example above and replace `<REPLACE_WITH_SERVER_IP>` with your PrisKV server IP.
   - `ENGINE_PORT`: set to the port you want the engine to listen on (default: `18000`).
   - `MODEL`: set to the folder name under `/data01/models` (e.g., `Qwen3-32B`).
   - `TP`: set to the number of GPUs you will use (e.g., `4`).
   - `CUDA_VISIBLE_DEVICES`: list the GPU IDs (e.g., `0,1,2,3`).
   - `priskv` service command: update `-a` to your host IP or `0.0.0.0` to bind all interfaces; keep `-p 9000` unless you need a different port.
   - Optionally change `AIBRIX_KV_CACHE_OL_PRISKV_PASSWORD` (remember to keep engine/redis consistent).
4) Start services: `docker compose -f deploy.yaml up -d`.
5) Verify services:
   - Verify Redis initialization: `docker compose -f deploy.yaml logs -f init`.
   - Check PrisKV logs: `docker compose -f deploy.yaml logs -f priskv` (ensure it reports listening on your chosen address/port).
   - Check engine logs: `docker compose -f deploy.yaml logs -f engine`
6) Benchmark: the `bench` container will automatically run two rounds. View output: `docker compose -f deploy.yaml logs -f bench`.
7) Stop services: `docker compose -f deploy.yaml stop`.
8) Cleanup (optional): `docker compose -f deploy.yaml down -v` to remove containers and Redis data.

### Common issues
- Engine startup errors about model path: ensure `/data01/models/<MODEL>` exists and is mounted; otherwise adjust `volumes:` and `--model` path.
- GPU visibility: ensure NVIDIA drivers and runtime are installed; test with `nvidia-smi` inside the engine container.
- PrisKV bind error: update `-a` to your host IP or `0.0.0.0`; ensure port `9000` is not in use.
- Redis authentication errors: confirm the password matches `AIBRIX_KV_CACHE_OL_PRISKV_PASSWORD` everywhere.