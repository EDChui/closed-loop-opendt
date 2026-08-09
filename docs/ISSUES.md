# Known Issues

This document lists known issues and pending TODOs identified during the implementation of the closed-loop OpenDT system.

## 1. Inconsistent CPU Utilization Calculation

### Description

Simulated and actual CPU utilization are currently aggregated over different sets of machines, which makes the two values unfair to compare directly.

In the simulation result analysis, CPU utilization is averaged across the hosts present in the simulation input:

```text
average(cpu_utilization across simulated hosts)
```

In the actual utilization query used by the API and Grafana dashboard, CPU utilization is averaged across all recorded physical nodes (excluding the control node):

```text
average(cpu_utilization across all recorded physical nodes)
```

For example, if 8 physical machines are available but only 3 machines are used in the simulation, the simulated utilization is averaged over the 3 simulated machines, while the actual utilization is averaged over 8 machines.

As a result, simulated utilization may appear different from actual utilization, even when both measurements are technically correct within their own contexts.

### Impact

1. **Unfair comparison between simulation runs**  
   When different simulation runs use different numbers of simulated hosts, their average CPU utilization values may not be directly comparable.

2. **Misleading Grafana dashboard values**  
   The Grafana dashboard may show simulated CPU utilization values that appear different from the actual values.

### Affected Files

The inconsistency is related to the CPU utilization aggregation logic in the following files:

- [`services/simulator/simulator/result_analyzer.py`](../services/simulator/simulator/result_analyzer.py)
- [`services/api/api/utilization_query.py`](../services/api/api/utilization_query.py)

### Why This Is Not Currently Fixed

This issue is not currently being fixed because CPU utilization is not used as an objective in the decision maker for the thesis experiments.

However, the issue should be addressed before CPU utilization is used for decision-making, reporting, or direct evaluation of simulation accuracy.
