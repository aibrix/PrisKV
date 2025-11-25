## 使用 PrisKV 进行 KVCache 卸载 —— Quick Start

本指南将使用 Docker Compose 启动一个完整的 KVCache 卸载环境：包含 PrisKV Server、推理引擎（vLLM 或 SGLang）以及基准测试容器。

共包含以下步骤：
- 准备 PrisKV Server 镜像
- 准备带 aibrix_kvcache 的推理引擎镜像（vLLM 或 SGLang）
- 可选：安装 PrisKV Python client SDK（用于自定义镜像或开发客户端代码）
- 使用 Docker Compose 启动并验证各服务

### 环境准备
- 安装了 Docker 和 Docker Compose 的 Linux 环境
- 已安装 NVIDIA GPU 驱动与运行时（vLLM/SGLang 的 GPU 推理需要）
- Docker 以特权模式运行（PrisKV Server和引擎容器需要）

## PrisKV Server镜像
### 使用预构建镜像
- kvcache-container-image-hb2-cn-beijing.cr.volces.com/aibrix/priskv:v0.0.2

### 从源码构建
```bash
# 克隆仓库
git clone https://github.com/aibrix/PrisKV
cd PrisKV

# 使用仓库内的 Ubuntu 22.04 Dockerfile 构建Server镜像
TAG="aibrix/priskv:v0.0.2"
docker build . -t ${TAG} --network=host -f ./docker/Dockerfile_ubuntu2204
```

## 推理引擎镜像
使用的推理引擎镜像（vLLM 或 SGLang）需要集成 aibrix_kvcache 和 PrisKV。可以直接使用以下预先构建好的镜像，或者按照链接给出的说明自行构建。

预构建镜像：
- vLLM + aibrix_kvcache + nixl + PrisKV: aibrix-container-registry-cn-beijing.cr.volces.com/aibrix/vllm-openai:v0.10.2-aibrix0.5.1-nixl0.7.1-priskv0.0.2-20251121
- SGLang + aibrix_kvcache + nixl + PrisKV: aibrix-container-registry-cn-beijing.cr.volces.com/aibrix/sglang:v0.5.5.post3-aibrix0.5.1-nixl0.7.1-priskv0.0.2-20251121

如果无法使用预构建镜像，请参考：https://github.com/vllm-project/aibrix/tree/main/python/aibrix_kvcache/integration/ 自行构建集成了 aibrix_kvcache 的基础镜像并按照下列步骤安装 PrisKV Python client SDK。

### PrisKV Python 客户端安装（可选）
如果你要构建自定义引擎镜像，那么需要在基础镜像上安装 PrisKV Python SDK。

#### 安装依赖
```bash
apt update && apt install -y \
  git gcc make cmake librdmacm-dev rdma-core libibverbs-dev \
  libncurses5-dev libmount-dev libevent-dev libssl-dev \
  libhiredis-dev liburing-dev
```

#### 方案 A：通过 pip 安装
```bash
# 从 PyPI 安装（最简单）
pip install pypriskv
```

#### 方案 B：从源码编译并安装
```bash
git clone https://github.com/aibrix/PrisKV
cd PrisKV
make pyclient
# 安装编译好的 wheel（根据实际编译的版本调整文件名）
pip install pypriskv/dist/priskv-0.0.2-cp312-cp312-manylinux2014_x86_64.whl
```

## 基准测试与 Docker Compose 部署
- 使用 kvcache-offloading/xxx/ 路径下的任意 deploy.yaml（例如 vLLM 或 SGLang 目录下的 deploy.yaml），通过这些文件启动 docker compose 会拉起以下容器：PrisKV Server、引擎以及基准测试容器。

### 自定义设置
#### PrisKV 配置
Connector 配置请参考：https://aibrix.readthedocs.io/latest/designs/aibrix-kvcache-offloading-framework.html#priskv-connector-configuration

修改 `PRISKV_CLUSTER_META` 来描述 PrisKV 集群的一致性哈希拓扑。单节点示例：
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
Tips：
- 将 <REPLACE_WITH_SERVER_IP> 替换为 PrisKV Server 的 IP，并在 PrisKV 启动命令中同步更新 `-a` 参数的值。
- 若 9000 端口不可用，可在 `PRISKV_CLUSTER_META` 中改为其它空闲端口，并在 PrisKV 启动命令中同步更新 `-p` 参数的值。

#### PrisKV Server 命令行参数
请参考：https://github.com/aibrix/PrisKV/blob/main/README.md#server-command-line-arguments 获取全部可用参数说明。

#### 引擎配置
  - `ENGINE_PORT`：引擎监听端口（默认：`18000`）。
  - `MODEL`： Host 上的模型目录名（位于 `/data01/models` 下）。
  - `TP`： 张量并行度；确保与 `CUDA_VISIBLE_DEVICES` 的数量一致。
  - `VLLM_KV_CONFIG`：可设置为空字符串以禁用 KV Connector。
  - `SGLANG_HICACHE_STORAGE_BACKEND`：SGLang HiCache 存储后端。

### 部署
- 运行 `docker compose -f deploy.yaml up -d` 启动所有服务
- 运行 `docker compose -f deploy.yaml ps` 查看运行中的容器
- 运行 `docker compose -f deploy.yaml logs -f` 持续查看所有服务日志
- 或分别查看：`docker compose -f deploy.yaml logs -f engine` 和 `docker compose -f deploy.yaml logs -f bench`
- 运行 `docker compose -f deploy.yaml stop` 停止所有服务

### vLLM 示例：
1) 进入 vLLM 示例目录：`cd samples/kvcache-offloading/vllm`。
2) 确认 Host 上存在 `/data01/models/<MODEL>` 模型目录。若没有，请下载或把已有模型放置到该路径；或在 compose 文件中调整 `volumes:` 与 `--model` 的路径以匹配已存在的 Host 路径。
3) 打开 `deploy.yaml` 并调整：
   - `PRISKV_CLUSTER_META`：使用上述单节点示例，并将 `<REPLACE_WITH_SERVER_IP>` 替换为你的 PrisKV Server IP。
   - `ENGINE_PORT`：设置引擎监听端口（默认：`18000`）。
   - `MODEL`：设置为 `/data01/models` 下的目录名（例如 `Qwen3-32B`）。
   - `TP`：设置你将使用的 GPU 数量（例如 `4`）。
   - `CUDA_VISIBLE_DEVICES`：列出 GPU ID（例如 `0,1,2,3`）。
   - `priskv` service 命令：将 `-a` 更新为你的 Host IP 或 `0.0.0.0`（绑定所有网卡）；`-p` 保持为 9000，除非你需要其它端口。
   - 可选：修改 `AIBRIX_KV_CACHE_OL_PRISKV_PASSWORD`（注意引擎和 redis 环境需一致）。
4) 启动服务：`docker compose -f deploy.yaml up -d`。
5) 验证服务：
   - 验证 Redis 初始化：`docker compose -f deploy.yaml logs -f init`。
   - 查看 PrisKV 日志：`docker compose -f deploy.yaml logs -f priskv`（确认已在选择的地址/端口上监听）。
   - 查看引擎日志：`docker compose -f deploy.yaml logs -f engine`。
6) 基准测试：`bench` 容器将自动运行两轮基准测试。查看输出：`docker compose -f deploy.yaml logs -f bench`。
7) 停止服务：`docker compose -f deploy.yaml stop`。
8) 清理（可选）：`docker compose -f deploy.yaml down -v` 删除容器并清除 Redis 数据。

### 常见问题
- 引擎启动报模型路径错误：确保 `/data01/models/<MODEL>` 存在且已挂载；否则调整 `volumes:` 与 `--model` 路径。
- GPU 可见性：确保已安装 NVIDIA 驱动与运行时；可在引擎容器内执行 `nvidia-smi` 测试。
- PrisKV 绑定失败：将 `-a` 改为你的 Host IP 或 `0.0.0.0`；确保 `9000` 端口未被占用。
- Redis 认证错误：确认各处使用的密码与 `AIBRIX_KV_CACHE_OL_PRISKV_PASSWORD` 保持一致。