# Scaphandre API

A small FastAPI service that exposes local Scaphandre energy readings over HTTP.

# Requirements

- Python 3.8.10

# Purpose

While `k8s-observer` runs on one physical machine, it is possible that VMs can live on other physical machines.
This service is meant to run directly on each physical machine and expose that machine's local Scaphandre readings so [`NodePowerProducer`](../../k8s-observer/k8s_observer/producers/node_power_producer.py) in `k8s-observer` service can fetch them remotely.

# Endpoints

- `GET /health` - basic service health check
- `GET /scaphandre` - return readings for all locally available VM/node directories
- `GET /scaphandre/{node_name}` - return the reading for a single VM/node

Example response from `GET /scaphandre/cloud0_echui`:

```json
{
  "node_name": "cloud0_echui",
  "capture_time": "2026-04-20T12:00:00.000000+00:00",
  "energy_uj": 123456789,
  "path": "/var/lib/libvirt/scaphandre/cloud0_echui/intel-rapl:0/energy_uj"
}
```

# Configuration

Environment variables:

- `SCAPHANDRE_BASE_PATH` (default: `/var/lib/libvirt/scaphandre`)

## Setup

Assume you have virtual environment set up and activated.

Install dependencies, note that these dependencies are based on Python 3.8.10, so you may need to adjust them if you are using a different Python version:

```bash
pip install -r services/scaphandre-api/scaphandre_api/requirements.txt
```

# Run the service

Start scaphandre for VM power measurement on the machine, possibly in a [tmux](https://github.com/tmux/tmux/wiki) session:

```bash
sudo scaphandre qemu
```

From the repository root:

```bash
PYTHONPATH=libs/k8s-observability uvicorn --app-dir services/scaphandre-api scaphandre_api.main:app --host 0.0.0.0 --port 8088
```

If you need sudo permissions to read the Scaphandre files, you can run:

```bash
sudo env PYTHONPATH=libs/k8s-observability "$(which python)" -m uvicorn --app-dir services/scaphandre-api scaphandre_api.main:app --host 0.0.0.0 --port 8088
```

# Test the API

```bash
curl http://localhost:8088/health
curl http://localhost:8088/scaphandre
curl http://localhost:8088/scaphandre/cloud0_echui
```

# Notes for k8s-observer

Set remote sources in `config/default.yaml` to the machine-local API base, for example:

```yaml
scaphandre_sources:
  - name: "cloud0_echui"
    access_mode: "remote"
    url: "http://node1:8088/scaphandre/cloud0_echui"
```

`NodePowerProducer` will call `GET http://node1:8088/scaphandre/cloud0_echui`.

If hostname resolution from Docker does not work in your environment, use the machine IP instead, or add the required Docker networking / `extra_hosts` configuration.

# TODOs

- Make this containerized
  - Upgrade the Python version to newer version, e.g. 3.10
  - Use modern typing syntax in `main.py` and [`NodePowerProducer`](../../../libs/k8s-observability/k8s_observability/scaphandre/energy_reader.py), e.g. `list[str]` instead of `List[str]`
