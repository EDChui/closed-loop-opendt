import logging
from datetime import datetime
from abc import ABC, abstractmethod
from dataclasses import replace
from typing import Dict, List, Optional

from k8s_observability.models import K8sResourceUsageSnapshot, NodeUtilizationSnapshot
from k8s_observability.kubernetes import K8sJobUidResolver
from k8s_observability.utils import TimeUtils, UnitUtils
from k8s_observer.prometheus.client import PrometheusClient

logger = logging.getLogger(__name__)


def escape_quotes_and_backslashes(value: str) -> str:
        return value.replace("\\", "\\\\").replace('"', '\\"')


class PrometheusResourceCollector(ABC):
    @property
    @abstractmethod
    def resource_type(self) -> str:
        raise NotImplementedError

    @property
    @abstractmethod
    def result_label(self) -> str:
        raise NotImplementedError

    def _merge_vector(self, merged: Dict[str, dict], vector: List[dict], target_key: str) -> None:
        for series in vector:
            metric = series.get("metric") or {}
            resource_id = metric.get(self.result_label)

            if not resource_id:
                continue

            sample = series.get("value") or []
            if len(sample) != 2:
                continue

            value = float(sample[1])
            entry = merged.setdefault(resource_id, {})
            entry[target_key] = value

            uid = metric.get("uid")
            if uid:
                entry["uid"] = uid
    
    def collect_metrics(self, eval_dt: datetime) -> List:
        raise NotImplementedError


class PrometheusWorkloadResourceCollector(PrometheusResourceCollector, ABC):
    def __init__(
        self,
        prom: PrometheusClient,
        namespace: str,
        cpu_rate_window: str = "2m",
        resource_name_regex: Optional[str] = None,
    ):
        self.prom = prom
        self.namespace = namespace
        self.cpu_rate_window = cpu_rate_window
        self.resource_name_regex = resource_name_regex

    @abstractmethod
    def cpu_query(self) -> str:
        # Should use rate(container_cpu_usage_seconds_total{condition}[window]) and sum by (pod/job) with appropriate label selectors.
        # Ref: https://last9.io/blog/monitoring-container-cpu-usage/
        # Ref: https://oneuptime.com/blog/post/2025-12-17-prometheus-cpu-memory-kubernetes-pods/view
        raise NotImplementedError

    @abstractmethod
    def memory_query(self) -> str:
        # Should query container_memory_working_set_bytes and sum by (pod/job) with appropriate label selectors.
        # Ref: https://oneuptime.com/blog/post/2025-12-17-prometheus-cpu-memory-kubernetes-pods/view
        raise NotImplementedError

    def collect_metrics(self, eval_dt: datetime) -> List[K8sResourceUsageSnapshot]:
        eval_ts = TimeUtils.to_epoch_seconds(eval_dt)
        cpu_result = self.prom.query_instant(self.cpu_query(), eval_ts)
        mem_result = self.prom.query_instant(self.memory_query(), eval_ts)

        merged: Dict[str, dict] = {}
        self._merge_vector(merged, cpu_result, "cpu_usage_cores")
        self._merge_vector(merged, mem_result, "ram_usage_bytes")

        snapshots = []
        for resource_id in sorted(merged.keys()):
            values = merged[resource_id]
            cpu_usage_cores = float(values.get("cpu_usage_cores", 0.0))
            ram_usage_bytes = float(values.get("ram_usage_bytes", 0.0))

            snapshot = K8sResourceUsageSnapshot(
                resource_type=self.resource_type,
                namespace=self.namespace,
                name=resource_id,
                uid=values.get("uid"),
                capture_time=eval_dt,
                cpu_usage=cpu_usage_cores,
                mem_usage_mb=UnitUtils.bytes_to_mb(ram_usage_bytes),
            )
            snapshots.append(snapshot)
        return snapshots


class PodResourceCollector(PrometheusWorkloadResourceCollector):
    @property
    def resource_type(self) -> str:
        return "pod"

    @property
    def result_label(self) -> str:
        return "pod"

    def _container_selector(self) -> str:
        selector_parts = [
            f'namespace="{escape_quotes_and_backslashes(self.namespace)}"',
            'pod!=""',
            'container!=""',
            'container!="POD"',
        ]

        if self.resource_name_regex:
            selector_parts.append(f'pod=~"{escape_quotes_and_backslashes(self.resource_name_regex)}"')

        return ",".join(selector_parts)

    def cpu_query(self) -> str:
        return (
            "sum by (pod, uid) ("
            f'rate(container_cpu_usage_seconds_total{{{self._container_selector()}}}[{self.cpu_rate_window}])'
            " * on (namespace, pod) group_left(uid) "
            f'kube_pod_info{{namespace="{escape_quotes_and_backslashes(self.namespace)}"}}'
            ")"
        )

    def memory_query(self) -> str:
        return (
            "sum by (pod, uid) ("
            f'container_memory_working_set_bytes{{{self._container_selector()}}}'
            " * on (namespace, pod) group_left(uid) "
            f'kube_pod_info{{namespace="{escape_quotes_and_backslashes(self.namespace)}"}}'
            ")"
        )


class JobResourceCollector(PrometheusWorkloadResourceCollector):
    def __init__(
        self,
        prom: PrometheusClient,
        namespace: str,
        cpu_rate_window: str = "2m",
        resource_name_regex: Optional[str] = None,
        kubeconfig_file: Optional[str] = None,
    ):
        super().__init__(
            prom=prom,
            namespace=namespace,
            cpu_rate_window=cpu_rate_window,
            resource_name_regex=resource_name_regex,
        )
        self.uid_resolver = K8sJobUidResolver(config_file=kubeconfig_file)

    @property
    def resource_type(self) -> str:
        return "job"

    @property
    def result_label(self) -> str:
        return "owner_name"

    def _container_selector(self) -> str:
        selector_parts = [
            f'namespace="{escape_quotes_and_backslashes(self.namespace)}"',
            'pod!=""',
            'container!=""',
            'container!="POD"',
        ]
        return ",".join(selector_parts)

    def _owner_selector(self) -> str:
        selector_parts = [
            f'namespace="{escape_quotes_and_backslashes(self.namespace)}"',
            'owner_kind="Job"',
            'owner_is_controller="true"',
        ]

        if self.resource_name_regex:
            selector_parts.append(
                f'owner_name=~"{escape_quotes_and_backslashes(self.resource_name_regex)}"'
            )

        return ",".join(selector_parts)

    def cpu_query(self) -> str:
        return (
            "sum by (owner_name) ("
            f"rate(container_cpu_usage_seconds_total{{{self._container_selector()}}}[{self.cpu_rate_window}])"
            " * on (namespace, pod) group_left(owner_name) "
            f"kube_pod_owner{{{self._owner_selector()}}}"
            ")"
        )

    def memory_query(self) -> str:
        return (
            "sum by (owner_name) ("
            f"container_memory_working_set_bytes{{{self._container_selector()}}}"
            " * on (namespace, pod) group_left(owner_name) "
            f"kube_pod_owner{{{self._owner_selector()}}}"
            ")"
        )
    
    def collect_metrics(self, eval_dt: datetime) -> List[K8sResourceUsageSnapshot]:
        job_uid_map = self.uid_resolver.get_namespaced_job_uids(self.namespace)

        raw_snapshots = super().collect_metrics(eval_dt)
        # Cannot retrieve jobs UIDs directly from Prometheus
        # Use job name as key for look up, assume job name is unique.
        snapshots = []
        for snapshot in raw_snapshots:
            job_name = snapshot.name
            uid = job_uid_map.get(job_name)
            snapshot_with_uid = replace(snapshot, uid=uid)
            snapshots.append(snapshot_with_uid)
        
        return snapshots


class NodeResourceCollector(PrometheusResourceCollector):
    def __init__(
        self,
        prom: PrometheusClient,
        cpu_rate_window: str = "2m",
        resource_name_regex: Optional[str] = None,
    ):
        self.prom = prom
        self.cpu_rate_window = cpu_rate_window
        self.resource_name_regex = resource_name_regex

    @property
    def resource_type(self) -> str:
        return "node"

    @property
    def result_label(self) -> str:
        return "instance"

    def _node_selector(self) -> str:
        selector_parts = ['instance!=""']

        if self.resource_name_regex:
            selector_parts.append(
                f'instance=~"{escape_quotes_and_backslashes(self.resource_name_regex)}"'
            )

        return ",".join(selector_parts)

    def cpu_utilization_query(self) -> str:
        return (
            "(1 - avg by (instance) ("
            f'rate(node_cpu_seconds_total{{mode="idle",{self._node_selector()}}}[{self.cpu_rate_window}])'
            "))"
        )

    def memory_utilization_query(self) -> str:
        return (
            "(1 - "
            f'node_memory_MemAvailable_bytes{{{self._node_selector()}}} / '
            f'node_memory_MemTotal_bytes{{{self._node_selector()}}}'
            ")"
        )

    def collect_metrics(self, eval_dt: datetime) -> List[NodeUtilizationSnapshot]:
        eval_ts = TimeUtils.to_epoch_seconds(eval_dt)
        cpu_result = self.prom.query_instant(self.cpu_utilization_query(), eval_ts)
        mem_result = self.prom.query_instant(self.memory_utilization_query(), eval_ts)

        merged: Dict[str, dict] = {}
        self._merge_vector(merged, cpu_result, "cpu_utilization")
        self._merge_vector(merged, mem_result, "memory_utilization")

        snapshots = []
        for node_name in sorted(merged.keys()):
            values = merged[node_name]

            snapshot = NodeUtilizationSnapshot(
                node_name=node_name,
                capture_time=eval_dt,
                cpu_utilization=values.get("cpu_utilization", 0.0),
                memory_utilization=values.get("memory_utilization", 0.0),
            )
            snapshots.append(snapshot)

        return snapshots
