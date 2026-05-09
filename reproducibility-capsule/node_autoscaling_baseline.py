import click
import logging
import requests
import time
from dataclasses import dataclass
from kubernetes import client, config
from typing import Any, List, Dict, Optional

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


PROMETHEUS_BASE_URL = "http://localhost:9091"
NODE_TYPE_ORDER = ("endpoint", "edge", "cloud")


@dataclass
class UtilizationAutoscalerConfig:
    prometheus_url: str
    poll_seconds: int
    cpu_rate_window: str
    high_cpu: float
    low_cpu: float
    low_for_seconds: int
    initial_schedulable: int
    min_schedulable: int


@dataclass
class PendingPodAutoscalerConfig:
    namespace: str
    poll_seconds: int
    pending_threshold: int
    no_pending_for_seconds: int
    initial_schedulable: int
    min_schedulable: int


class PrometheusClient:
    def __init__(self, base_url: str, timeout_seconds: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def query_instant(self, promql: str, eval_ts: int) -> List[dict]:
        response = self.session.get(
            f"{self.base_url}/api/v1/query",
            params={"query": promql, "time": str(eval_ts)},
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")
        
        warnings = payload.get("warnings") or []
        for warning in warnings:
            logger.warning(f"[prometheus warning] {warning}")

        data = payload.get("data") or {}
        if data.get("resultType") != "vector":
            raise RuntimeError(f"Expected vector result, got {data.get('resultType')}")

        return data.get("result") or []


class K8sNode:
    @staticmethod
    def name(node: Any) -> str:
        return getattr(getattr(node, "metadata", None), "name", "unknown-node")

    @staticmethod
    def labels(node: Any) -> Dict[str, str]:
        return getattr(getattr(node, "metadata", None), "labels", {}) or {}

    @staticmethod
    def is_control_plane(node: Any) -> bool:
        return "node-role.kubernetes.io/control-plane" in K8sNode.labels(node)

    @staticmethod
    def is_worker(node: Any) -> bool:
        return not K8sNode.is_control_plane(node)

    @staticmethod
    def is_ready(node: Any) -> bool:
        conditions = getattr(getattr(node, "status", None), "conditions", None) or []
        for condition in conditions:
            if getattr(condition, "type", None) == "Ready":
                return getattr(condition, "status", None) == "True"
        return False

    @staticmethod
    def is_schedulable(node: Any) -> bool:
        return not getattr(getattr(node, "spec", None), "unschedulable", False)

    @staticmethod
    def is_available(node: Any) -> bool:
        return K8sNode.labels(node).get("k8s-observability/availability", "") != "unavailable"

    @staticmethod
    def node_type(node: Any) -> str:
        value = K8sNode.labels(node).get("k8s-observability/node-type", "unknown")
        return value if value in {"endpoint", "edge", "cloud"} else "unknown"

    @staticmethod
    def is_in_use(node: Any) -> bool:
        """In use means it is a worker node that is ready, available, and schedulable"""
        return (
            K8sNode.is_worker(node)
            and K8sNode.is_ready(node)
            and K8sNode.is_available(node)
            and K8sNode.is_schedulable(node)
        )


class K8sNodeController:
    def __init__(self, core_api: client.CoreV1Api):
        self.core_api = core_api

    def list_nodes(self) -> List[Any]:
        return self.core_api.list_node().items

    def available_workers(self, nodes: List[Any], node_type: Optional[str] = None) -> List[Any]:
        return [
            node for node in nodes
            if K8sNode.is_worker(node)
            and K8sNode.is_ready(node)
            and K8sNode.is_available(node)
            and (node_type is None or K8sNode.node_type(node) == node_type)
        ]

    def in_use_workers(self, nodes: List[Any], node_type: Optional[str] = None) -> List[Any]:
        return [
            node for node in nodes
            if K8sNode.is_in_use(node)
            and (node_type is None or K8sNode.node_type(node) == node_type)
        ]

    def set_schedulable(self, node_name: str, schedulable: bool) -> None:
        action = "uncordon" if schedulable else "cordon"

        self.core_api.patch_node(node_name, {"spec": {"unschedulable": not schedulable}})
        logger.info(f"{action.capitalize()}ed node {node_name}")
    
    def uncordon_one(self, nodes: List[Any]) -> bool:
        # Uncordon nodes in order of endpoint -> edge -> cloud.
        for node_type in NODE_TYPE_ORDER:
            for node in self.available_workers(nodes, node_type):
                if not K8sNode.is_schedulable(node):
                    self.set_schedulable(K8sNode.name(node), True)
                    return True

        logger.info("No available cordoned node found to uncordon")
        return False

    def cordon_one(self, nodes: List[Any], min_schedulable: int) -> bool:
        in_use_nodes = self.in_use_workers(nodes)

        if len(in_use_nodes) <= min_schedulable:
            logger.info(f"Not cordoning: {len(in_use_nodes)} schedulable nodes <= minimum {min_schedulable}")
            return False

        # Cordon nodes in order of cloud -> edge -> endpoint.
        for node_type in reversed(NODE_TYPE_ORDER):
            candidates = self.in_use_workers(nodes, node_type)
            for node in reversed(candidates):
                self.set_schedulable(K8sNode.name(node), False)
                return True

        logger.info("No schedulable node found to cordon")
        return False


class NodeCpuCollector:
    def __init__(self, prom: PrometheusClient, cpu_rate_window: str="5m"):
        self.prom = prom
        self.cpu_rate_window = cpu_rate_window

    def query(self) -> str:
        return (
            "(1 - avg by (instance) ("
            f'rate(node_cpu_seconds_total{{mode="idle",instance!=""}}[{self.cpu_rate_window}])'
            "))"
        )

    def collect(self) -> Dict[str, float]:
        result = self.prom.query_instant(self.query(), int(time.time()))
        cpu_by_node = {}

        for series in result:
            instance = (series.get("metric") or {}).get("instance")
            sample = series.get("value") or []

            if not instance or len(sample) != 2:
                continue

            node_name = instance.split(":", 1)[0]
            cpu_by_node[node_name] = float(sample[1])

        return cpu_by_node


class PendingPodCollector:
    def __init__(self, core_api: client.CoreV1Api, namespace: str):
        self.core_api = core_api
        self.namespace = namespace

    def collect(self) -> int:
        pods = self.core_api.list_namespaced_pod(namespace=self.namespace).items

        return sum(
            1
            for pod in pods
            if getattr(getattr(pod, "status", None), "phase", None) == "Pending"
        )


class UtilizationAutoscaler:
    def __init__(
        self,
        settings: UtilizationAutoscalerConfig,
        nodes: K8sNodeController,
        cpu: NodeCpuCollector,
    ):
        self.settings = settings
        self.nodes = nodes
        self.cpu = cpu
        self.low_since: Optional[float] = None

    def mean_cpu_of_in_use_nodes(self, all_nodes: List[Any], cpu_by_node: Dict[str, float]) -> Optional[float]:
        values = [
            cpu_by_node[K8sNode.name(node)] for node in self.nodes.in_use_workers(all_nodes)
            if K8sNode.name(node) in cpu_by_node
        ]

        if not values:
            return None

        return sum(values) / len(values)

    def reconcile_initial_schedulable_count(self) -> None:
        """Ensure the number of initially schedulable nodes matches the configured count on startup"""
        while True:
            all_nodes = self.nodes.list_nodes()
            current = len(self.nodes.in_use_workers(all_nodes))
            target = self.settings.initial_schedulable

            if current == target:
                logger.info(f"Initial schedulable node count is {target}")
                return

            if current < target:
                changed = self.nodes.uncordon_one(all_nodes)
            else:
                changed = self.nodes.cordon_one(all_nodes, min_schedulable=target)

            if not changed:
                logger.warning(f"Could not reconcile initial node count: current={current} target={target}")
                return

    def tick(self) -> None:
        all_nodes = self.nodes.list_nodes()
        cpu_by_node = self.cpu.collect()
        mean_cpu = self.mean_cpu_of_in_use_nodes(all_nodes, cpu_by_node)

        if mean_cpu is None:
            logger.warning("No CPU samples matched schedulable Kubernetes worker nodes")
            return

        in_use_count = len(self.nodes.in_use_workers(all_nodes))
        logger.info(f"In-use nodes={in_use_count} mean CPU={mean_cpu * 100.0:.1f}%")

        if mean_cpu > self.settings.high_cpu / 100.0:
            logger.info(f"Mean CPU above {self.settings.high_cpu:.1f}%: adding one node")
            self.nodes.uncordon_one(all_nodes)
            self.low_since = None
            return

        if mean_cpu < self.settings.low_cpu / 100.0:
            now = time.time()
            self.low_since = self.low_since or now

            low_duration = now - self.low_since
            logger.info(f"Mean CPU below {self.settings.low_cpu:.1f}% for {low_duration:.0f}s / {self.settings.low_for_seconds}s")

            if low_duration >= self.settings.low_for_seconds:
                logger.info("Low CPU window reached: removing one node")
                self.nodes.cordon_one(all_nodes, self.settings.min_schedulable)
                self.low_since = None

            return

        self.low_since = None

    def run(self) -> None:
        self.reconcile_initial_schedulable_count()

        while True:
            try:
                self.tick()
            except Exception:
                logger.exception("Autoscaler tick failed")

            time.sleep(self.settings.poll_seconds)


class PendingPodAutoscaler:
    def __init__(
        self,
        settings: PendingPodAutoscalerConfig,
        nodes: K8sNodeController,
        pending_pods: PendingPodCollector,
    ):
        self.settings = settings
        self.nodes = nodes
        self.pending_pods = pending_pods
        self.no_pending_since: Optional[float] = None

    def reconcile_initial_schedulable_count(self) -> None:
        """Ensure the number of initially schedulable nodes matches the configured count on startup"""
        while True:
            all_nodes = self.nodes.list_nodes()
            current = len(self.nodes.in_use_workers(all_nodes))
            target = self.settings.initial_schedulable

            if current == target:
                logger.info(f"Initial schedulable node count is {target}")
                return

            if current < target:
                changed = self.nodes.uncordon_one(all_nodes)
            else:
                changed = self.nodes.cordon_one(all_nodes, min_schedulable=target)

            if not changed:
                logger.warning(f"Could not reconcile initial node count: current={current} target={target}")
                return

    def tick(self) -> None:
        all_nodes = self.nodes.list_nodes()
        pending_count = self.pending_pods.collect()
        in_use_count = len(self.nodes.in_use_workers(all_nodes))

        logger.info(f"Namespace={self.settings.namespace} pending pods={pending_count} in-use nodes={in_use_count}")

        if pending_count >= self.settings.pending_threshold:
            logger.info(f"Pending pods >= {self.settings.pending_threshold}: adding one node")
            self.nodes.uncordon_one(all_nodes)
            self.no_pending_since = None
            return

        if pending_count == 0:
            now = time.time()
            self.no_pending_since = self.no_pending_since or now

            no_pending_duration = now - self.no_pending_since
            logger.info(f"No pending pods for {no_pending_duration:.0f}s / {self.settings.no_pending_for_seconds}s")

            if no_pending_duration >= self.settings.no_pending_for_seconds:
                logger.info("No-pending window reached: removing one node")
                self.nodes.cordon_one(all_nodes, self.settings.min_schedulable)
                self.no_pending_since = None

            return

        # There are pending pods, but fewer than the scale-up threshold.
        self.no_pending_since = None

    def run(self) -> None:
        self.reconcile_initial_schedulable_count()

        while True:
            try:
                self.tick()
            except Exception:
                logger.exception("Pending pod autoscaler tick failed")

            time.sleep(self.settings.poll_seconds)


def load_kubernetes_config(kubeconfig: Optional[str] = None) -> None:
    config.load_kube_config(config_file=kubeconfig)


@click.group()
def cli() -> None:
    pass


@cli.command("cpu")
@click.option("--prometheus-url", default=PROMETHEUS_BASE_URL, show_default=True)
@click.option("--kubeconfig", default=None)
@click.option("--poll-seconds", default=120, show_default=True, help="How often to check CPU utilization and adjust nodes")
@click.option("--cpu-rate-window", default="5m", show_default=True, help="The window to use when calculating CPU usage rate from Prometheus node_cpu_seconds_total metric")
@click.option("--high-cpu", default=70.0, show_default=True)
@click.option("--low-cpu", default=30.0, show_default=True)
@click.option("--low-for-seconds", default=60, show_default=True)
@click.option("--initial-schedulable", default=4, show_default=True)
@click.option("--min-schedulable", default=1, show_default=True)
def cpu(
    prometheus_url: str,
    kubeconfig: Optional[str],
    poll_seconds: int,
    cpu_rate_window: str,
    high_cpu: float,
    low_cpu: float,
    low_for_seconds: int,
    initial_schedulable: int,
    min_schedulable: int,
) -> None:
    load_kubernetes_config(kubeconfig)

    settings = UtilizationAutoscalerConfig(
        prometheus_url=prometheus_url,
        poll_seconds=poll_seconds,
        cpu_rate_window=cpu_rate_window,
        high_cpu=high_cpu,
        low_cpu=low_cpu,
        low_for_seconds=low_for_seconds,
        initial_schedulable=initial_schedulable,
        min_schedulable=min_schedulable,
    )

    prom = PrometheusClient(settings.prometheus_url)
    node_controller = K8sNodeController(client.CoreV1Api())
    cpu_collector = NodeCpuCollector(prom, settings.cpu_rate_window)

    UtilizationAutoscaler(settings, node_controller, cpu_collector).run()


@cli.command("pending-pods")
@click.option("--kubeconfig", default=None)
@click.option("--namespace", default="default", show_default=True)
@click.option("--poll-seconds", default=120, show_default=True, help="How often to check pending pod count and adjust nodes")
@click.option("--pending-threshold", default=10, show_default=True, help="Number of pending pods at which to add a node")
@click.option("--no-pending-for-seconds", default=60, show_default=True, help="How long to wait before removing a node when there are no pending pods")
@click.option("--initial-schedulable", default=4, show_default=True)
@click.option("--min-schedulable", default=1, show_default=True)
def pending_pods(
    kubeconfig: Optional[str],
    namespace: str,
    poll_seconds: int,
    pending_threshold: int,
    no_pending_for_seconds: int,
    initial_schedulable: int,
    min_schedulable: int,
) -> None:
    load_kubernetes_config(kubeconfig)

    core_api = client.CoreV1Api()

    settings = PendingPodAutoscalerConfig(
        namespace=namespace,
        poll_seconds=poll_seconds,
        pending_threshold=pending_threshold,
        no_pending_for_seconds=no_pending_for_seconds,
        initial_schedulable=initial_schedulable,
        min_schedulable=min_schedulable,
    )

    node_controller = K8sNodeController(core_api)
    pending_pod_collector = PendingPodCollector(core_api, namespace)

    PendingPodAutoscaler(settings, node_controller, pending_pod_collector).run()


if __name__ == "__main__":
    cli()
