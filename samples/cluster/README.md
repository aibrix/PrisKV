# PrisKV KVCache Cluster × AIBrix Integration Example

This README describes how to deploy:

- **KVCache Controller** (Kubernetes control plane for KVCache clusters)
- A **PrisKV-based KVCache cluster** (HPKV data nodes + Redis-compatible metadata + Watcher)
- An **AIBrix-enabled vLLM inference service** that uses PrisKV as a remote KV cache backend

This example is intended as a step-by-step tutorial you can run on any compatible Kubernetes cluster (e.g., Volcengine VKE) with GPU nodes.

---

## 1. Architecture Overview

This example sets up the following components:

1. **KVCache Controller**  
   - Watches `KVCache` custom resources  
   - Creates and manages:
     - Redis-compatible metadata service  
     - Watcher Pod (for node discovery and registration)  
     - PrisKV data nodes

2. **PrisKV KVCache Cluster**  
   - Distributed KV cache backend
   - Uses Redis-compatible metadata and PrisKV data nodes for KV storage

3. **AIBrix-enabled vLLM Service**  
   - vLLM image extended with:
     - AIBrix KVCache Offloading connector  
     - PrisKV client SDK  
   - Uses `AIBrixOffloadingConnectorV1Type3` to offload KV tensors to PrisKV
   - Treats PrisKV as an L2 cache backend (L1 DRAM cache is optional)

---

## 2. Prerequisites

### 2.1 Kubernetes Cluster with GPUs

You need a Kubernetes cluster with GPU nodes. For example, on Volcengine VKE:

- Create a VKE cluster with GPU instances such as **H20** / **A800** that support RDMA.
- Official docs (examples):
  - Cluster creation: `https://www.volcengine.com/docs/6460/100936?LibVersion=2.27.0`
  - `kubectl` configuration: `https://www.volcengine.com/docs/6460/1374028-`

Other Kubernetes providers are also fine as long as:

- GPU nodes are available
- Networking is sufficient for PrisKV and inference pods (RDMA recommended but not strictly required for functional validation)

### 2.2 kubectl Access

Make sure `kubectl` can talk to the cluster:

```bash
kubectl get nodes
```

You should see your GPU nodes listed.

---

## 3. Install AIBrix

The KVCache Controller is part of AIBrix and it runs in its own namespace and reconciles `KVCache` CRs.

```bash
# Install envoy-gateway, this is not aibrix component. you can also use helm package to install it.
helm install eg oci://docker.io/envoyproxy/gateway-helm --version v1.2.8 -n envoy-gateway-system --create-namespace

# patch the configuration to enable EnvoyPatchPolicy, this is super important!
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: envoy-gateway-config
  namespace: envoy-gateway-system
data:
  envoy-gateway.yaml: |
    apiVersion: gateway.envoyproxy.io/v1alpha1
    kind: EnvoyGateway
    provider:
      type: Kubernetes
    gateway:
      controllerName: gateway.envoyproxy.io/gatewayclass-controller
    extensionApis:
      enableEnvoyPatchPolicy: true
EOF
```

```bash
# Install AIBrix CRDs. `--install-crds` is not available in local chart installation.
kubectl apply -f dist/chart/crds/

# Install AIBrix with the pinned release version:
helm install aibrix dist/chart -f dist/chart/stable.yaml -n aibrix-system --create-namespace
```

> At the moment, the controller is assumed to run in `aibrix-system`. Future versions may support custom namespaces.

Verify the controller is running:

```bash
kubectl get pods -n aibrix-system
kubectl get deployments -n aibrix-system
```

You should see controller-related Pods in `Running` state.

---

## 4. Deploy a PrisKV KVCache Cluster

With the controller up, define a `KVCache` custom resource and let the controller create the cluster.

### 4.1 Apply KVCache CR

```bash
kubectl apply -f kvcache.yaml
```

After the controller reconciles the resource, you should see pods similar to:

```bash
kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
debug                                 1/1     Running   0          23h
kvcache-cluster-0                     1/1     Running   0          8h
kvcache-cluster-1                     1/1     Running   0          8h
kvcache-cluster-2                     1/1     Running   0          8h
kvcache-cluster-kvcache-watcher-pod   1/1     Running   0          8h
kvcache-cluster-redis                 1/1     Running   0          8h
```

Roles:

- `kvcache-cluster-0/1/2` – PrisKV  data nodes  
- `kvcache-cluster-redis` – Redis-compatible metadata service  
- `kvcache-cluster-kvcache-watcher-pod` – Watcher that discovers and registers data nodes into metadata

### 4.2 Verify Redis Metadata

To confirm the cluster is writing metadata correctly:

```bash
kubectl exec -it kvcache-cluster-redis -- bash
```

Inside the container:

```bash
redis-cli -a kvcache_nodes
KEYS *
# Inspect keys according to your schema
```

If you can see keys representing nodes, shards or sessions, the KVCache cluster is healthy.

---

## 5. Deploy an AIBrix-Enabled vLLM Service Using PrisKV

Next, we deploy a vLLM service that:

- Uses the **AIBrix KV Offloading connector** (`AIBrixOffloadingConnectorV1Type3`)  
- Configures **PrisKV** as the L2 KV cache backend (through Redis metadata)


```yaml
kubectl apply -f vllm.yaml
```


---

## 6. End-to-End Validation

### 6.1 Check All Pods

```bash
kubectl get pods
```

You should see:

- `kvcache-cluster-*` pods from the PrisKV cluster
- `deepseek-r1-distill-llama-8b-*` inference pod

All should be `Running`.

### 6.2 Send Test Requests

Port-forward the service:

```bash
kubectl port-forward svc/deepseek-r1-distill-llama-8b 8000:8000
```

Then call the OpenAI-compatible endpoint:

```bash
curl http://127.0.0.1:8000/v1/chat/completions   -H "Content-Type: application/json"   -d '{
    "model": "deepseek-r1-distill-llama-8b",
    "messages": [
      {"role": "user", "content": "Hello, PrisKV and AIBrix!"}
    ]
  }'
```

These requests will generate KVCache traffic.

### 6.3 Check Redis Metadata

Inspect Redis again:

```bash
kubectl exec -it kvcache-cluster-redis -- bash
redis-cli -a kvcache_nodes

KEYS *
# Inspect keys to confirm entries related to sessions / nodes / chunks have been created
```

If new keys appear after you send requests, it means:

> The AIBrix-enabled vLLM instance is successfully using the PrisKV cluster as its remote KV cache backend.


## 7. Next Steps

From here, you can extend this example to:

- Share a single PrisKV cluster across multiple engines (vLLM, SGLang, etc.).
- Combine L1 DRAM cache with L2 PrisKV (multi-tier KV caching).
- Run benchmarks to evaluate performance and cost efficiency under real workloads.
- Integrate with your existing autoscaling and routing stack for production use.

If you have feedback or want to contribute improvements to the controller, cluster layout, or AIBrix integration, feel free to open an issue or pull request in this repository.
