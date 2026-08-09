<a href="atlarge-research.github.io/opendt/">
    <img src="logo/logo-128.png" alt="OpenDT logo" title="OpenDT" align="right" height="100" />
</a>

# OpenDT

**Open Digital Twin for Datacenters**

OpenDT is a distributed system that creates a real-time digital twin of datacenter infrastructure. It replays historical workload data through the [OpenDC](https://opendc.org/) simulator to predict power consumption, enabling What-If analysis without touching live hardware.

## Upstream Basis and Adaptation

**This work is based on [atlarge-research/opendt](https://github.com/atlarge-research/opendt).** The upstream OpenDT project provides the trace-processing, OpenDC simulation, and simulator-calibration foundation used here.

**Adaptation made in this work:** OpenDT's digital-shadow workflow is extended into a **trace-based closed-loop digital twin for a live Kubernetes environment**. In particular, this version adapts the upstream foundation by:

- replacing historical-only physical input with traces produced from live Kubernetes Pods/Jobs, resource usage, processor-domain power measurements, and observed cluster topology;
- evaluating multiple alternative worker-node configurations with OpenDC rather than only replaying a recorded configuration;
- adapting OpenDT's window-based CPU-power calibration to use recent physical evidence and to associate calibrated values with the topology that produced that evidence;
- adding policy-based comparison and automatic selection of simulated candidate outcomes; and
- closing the feedback loop with bounded Kubernetes actuation by changing worker-node schedulability, followed by re-observation of the physical system.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Docker](https://img.shields.io/badge/docker-required-blue.svg)](https://www.docker.com/)

---

## Quick Start

**Prerequisites:** Docker, Docker Compose, Make, Python 3.11+, [uv](https://astral.sh/uv)

```
make setup    # Install dependencies
make up       # Start services
```

**Access:**

- **Dashboard:** http://localhost:3000 (Grafana)
- **API:** http://localhost:3001 (OpenAPI docs)

---

## Repository Structure

```
opendt/
├── config/                    # Configuration files
│   ├── default.yaml           # Default configuration
│   └── experiments/           # Experiment-specific configs
├── data/                      # Run outputs (timestamped directories)
├── docs/                      # Documentation
├── libs/common/               # Shared library (Pydantic models, utilities)
├── opendc/                    # OpenDC simulator binary
├── reproducibility-capsule/   # Experiment reproduction scripts
├── services/                  # Microservices
│   ├── api/                   # REST API (FastAPI)
│   ├── calibrator/            # Topology calibration service
│   ├── grafana/               # Dashboard configuration
│   ├── k8s-observer/          # Kubernetes resources and power observer
│   ├── k8s-orchestrator/      # Orchestrator managing the closed-loop
│   ├── kafka-init/            # Kafka topic initialization
│   ├── postgres-init/         # PostgreSQL initialization
│   ├── scaphandre-api/        # Scaphandre API
│   └── simulator/             # OpenDC simulation engine
└── workload/                  # Workload datasets
    └── SURF/                  # SURF datacenter workload
```

---

## Commands

| Command                              | Description                      |
| ------------------------------------ | -------------------------------- |
| `make up`                            | Start all services               |
| `make up config=path/to/config.yaml` | Start with custom configuration  |
| `make down`                          | Stop all services                |
| `make setup`                         | Install development dependencies |
| `make test`                          | Run tests                        |
| `make logs-<service>`                | View logs for a service          |
| `make shell-<service>`               | Open shell in a container        |

Run `make help` for the complete list.

---

## Documentation

| Document                                   | Description                                          |
| ------------------------------------------ | ---------------------------------------------------- |
| [Getting Started](docs/GETTING_STARTED.md) | Installation, configuration, and running experiments |
| [Concepts](docs/CONCEPTS.md)               | Core concepts, data models, and system behavior      |
| [Configuration](docs/CONFIGURATION.md)     | Configuration file reference                         |

### Service Documentation

| Service                                     | Description                    |
| ------------------------------------------- | ------------------------------ |
| [simulator](services/simulator/README.md)   | Runs OpenDC simulations        |
| [calibrator](services/calibrator/README.md) | Calibrates topology parameters |
| [api](services/api/README.md)               | REST API for data queries      |

### Research

| Document                                                    | Description                 |
| ----------------------------------------------------------- | --------------------------- |
| [Reproducibility Capsule](reproducibility-capsule/setup.md) | Reproduce paper experiments |
