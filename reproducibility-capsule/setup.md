# Setup

## Environment

`node1` to `node5` are the physical machines hosting the VMs. `node5` is selected to be the cluster with the closed-loop OpenDT deployed on it.

`cloud_controller_echui` is the VM on `node5` that serves as the control plane for the Kubernetes cluster.

## Copy kube config

In `node5`:

```bash
scp -i /home/echui/.ssh/id_rsa_continuum cloud_controller_echui@192.168.134.2:~/.kube/config ~/.kube/config
```

## Expose Prometheus

In `cloud_controller_echui`, run the following command to forward Prometheus port to 9090. It is recommended to run this in a [tmux](https://github.com/tmux/tmux/wiki) session.

```bash
kubectl port-forward -n monitoring svc/prometheus-k8s 9090:9090
```

More robust version:

```bash
while true; do
  kubectl -n monitoring port-forward --address 127.0.0.1 svc/prometheus-k8s 9090:9090
  echo "$(date) kubectl port-forward exited; restarting in 2s" >&2
  sleep 2
done
```

Verify that Prometheus is accessible using curl:

```bash
curl http://localhost:9090/-/healthy
curl http://localhost:9090/-/ready
```

---

In node5, run the following command to forward Prometheus port to 9091. It is recommended to run this in a [tmux](https://github.com/tmux/tmux/wiki) session.

```bash
ssh -g -L 0.0.0.0:9091:localhost:9090 cloud_controller_echui@192.168.134.2 -i /home/echui/.ssh/id_rsa_continuum
```

More robust:

```bash
while true; do
  ssh -N -T \
    -o ExitOnForwardFailure=yes \
    -o ServerAliveInterval=30 \
    -o ServerAliveCountMax=3 \
    -o TCPKeepAlive=yes \
    -g \
    -L 0.0.0.0:9091:127.0.0.1:9090 \
    cloud_controller_echui@192.168.134.2 \
    -i /home/echui/.ssh/id_rsa_continuum
  echo "$(date) ssh tunnel exited; restarting in 2s" >&2
  sleep 2
done
```

Verify that Prometheus is accessible using curl:

```bash
curl http://localhost:9091/-/healthy
curl http://localhost:9091/-/ready
```

## Move Prometheus

When there are too much load in Kubernetes, Prometheus may stopped working properly, so we move it to `cloud_controller_echui` as pods are normally not scheduled on control plane nodes.

In `cloud_controller_echui`

```bash
kubectl describe node cloudcontrollerechui | grep -A3 Taints

kubectl label node cloudcontrollerechui dedicated=prometheus-control

kubectl -n monitoring patch prometheus k8s --type merge -p '{
  "spec": {
    "nodeSelector": {
      "dedicated": "prometheus-control"
    },
    "tolerations": [
      {
        "key": "node-role.kubernetes.io/control-plane",
        "operator": "Exists",
        "effect": "NoSchedule"
      },
      {
        "key": "node-role.kubernetes.io/master",
        "operator": "Exists",
        "effect": "NoSchedule"
      }
    ]
  }
}'

kubectl -n monitoring delete pod prometheus-k8s-0 prometheus-k8s-1
```

## Tag Kubernets nodes

To identify the type of machines in the Kubernets cluster, we can add labels to the nodes.

In `cloud_controller_echui`, copy the [`scripts/k8s_control_panel.py`](../scripts/k8s_control_panel.py) script and install the dependencies:

```bash
pip install kubernetes click
python3 k8s_control_panel.py settype worker-1 cloud/edge/endpoint
```

### Scaphandre API for Remote RAPL Metrics

When `k8s-observer` needs to query RAPL energy metrics for a VM hosted on a different physical machine, the **Scaphandre API** must be deployed on that physical machine.

`k8s-observer` normally reads Scaphandre measurements from the local host. However, VMs in the Kubernetes cluster may be distributed across `node1`–`node5`. The Scaphandre API provides an HTTP interface that allows `NodePowerProducer` to retrieve the Scaphandre/RAPL energy readings associated with a remote VM.

The API should run directly on each physical machine whose VM energy metrics need to be queried. It exposes endpoints such as:

```text
GET /health
GET /scaphandre
GET /scaphandre/{node_name}
```

For example, if `cloud0_echui` is hosted on `node1`, `k8s-observer` can be configured to retrieve its energy reading from:

```yaml
scaphandre_sources:
  - name: "cloud0_echui"
    access_mode: "remote"
    url: "http://node1:8088/scaphandre/cloud0_echui"
```

The complete installation, configuration, and startup instructions are documented in the [Scaphandre API README](../services/scaphandre-api/scaphandre_api/README.md).

In particular, the setup includes starting Scaphandre in QEMU mode on the physical machine:

```bash
sudo scaphandre qemu
```

and running the API service on port `8088`. Refer to the README for Python dependencies, service startup commands, API tests, and networking considerations.

## Workload Generator

The Kubernetes workload generator used for the experiments is available at [EDChui/k8s-workload-generator](https://github.com/EDChui/k8s-workload-generator).

It can generate Kubernetes jobs in batches or according to a Poisson arrival process. It also supports multi-stage workload configurations through YAML files and reproducible workload generation using a random seed.

Clone the repository on the machine from which the workloads will be generated:

```bash
git clone https://github.com/EDChui/k8s-workload-generator.git
cd k8s-workload-generator
```

Follow the setup and usage instructions in the repository's `README.md`. The README includes examples for batch workloads, Poisson workloads configured through command-line arguments, and staged Poisson workloads configured through YAML.
