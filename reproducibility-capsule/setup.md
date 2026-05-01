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
