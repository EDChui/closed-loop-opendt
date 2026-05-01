import logging
from collections import defaultdict
from kubernetes import client, config

from odt_common.models import Decision
from odt_common.models.topology import (
    Topology,
    Cluster,
    Host,
    CPU,
    Memory,
    MseCPUPowerModel,
    PowerSource
)
from k8s_observability.models import K8sNodeShape
from k8s_observability.kubernetes import K8sNodeExtractor
from k8s_orchestrator.domain.kubernetes import K8sSystemSnapshot, K8sActionKind
from k8s_orchestrator.application.ports import SystemPort

logger = logging.getLogger(__name__)


class K8sSystemAdapter(SystemPort):
    def __init__(
        self,
        kubeconfig_path: str,
        cpu_frequency_mhz: int,
    ):
        self.kubeconfig_path = kubeconfig_path
        # Assume all nodes have the same CPU frequency
        # TODO: (Low priority) Can be extracted from node info in the future if needed
        self.cpu_frequency_mhz = cpu_frequency_mhz

        config.load_kube_config(config_file=self.kubeconfig_path)
        self.core_api = client.CoreV1Api()

    def _build_topology(self, nodes: list) -> Topology:
        grouped_shapes: dict[K8sNodeShape, int] = defaultdict(int)

        for node in nodes:
            if not K8sNodeExtractor.is_worker_node(node):
                continue
            shape = K8sNodeExtractor.get_node_shape(node)
            if K8sNodeExtractor.is_worker_node_in_use(node):
                grouped_shapes[shape] += 1
            else:
                grouped_shapes[shape] += 0  # Ensure the shape is included in the topology even if there are currently no nodes of that shape in use

        if not grouped_shapes:
            raise ValueError("No valid worker nodes found in the cluster to build topology")
        
        # TODO: (Low priority) Temporary hardcoded power source
        power_source = PowerSource(carbonTracePath="/app/workload/carbon.parquet")
        
        # Assume for a single node type, they all have the same shape (same CPU and memory configuration),
        # so we can safely use the node type as the host name in the topology
        hosts: list[Host] = []
        for idx, (shape, count) in enumerate(grouped_shapes.items()):
            # TODO: (Low priority) Temporary hardcoded power model
            # TODO: Fine-tune power model parameters before experiment
            if shape.node_type == "cloud":
                cpu_power_model = MseCPUPowerModel(
                    modelType="mse",
                    power=300,
                    idlePower=0.03,
                    maxPower=5,
                    calibrationFactor=4
                )
            elif shape.node_type == "endpoint":
                cpu_power_model = MseCPUPowerModel(
                    modelType="mse",
                    power=300,
                    idlePower=0.02,
                    maxPower=1.15,
                    calibrationFactor=4
                )
            else:
                cpu_power_model = MseCPUPowerModel(
                    modelType="mse",
                    power=300,
                    idlePower=0.03,
                    maxPower=5,
                    calibrationFactor=4
                )
            host = Host(
                name=shape.node_type,
                count=count,
                cpu=CPU(coreCount=shape.cpu_count, coreSpeed=self.cpu_frequency_mhz),
                memory=Memory(memorySize=shape.memory_size_bytes),
                cpuPowerModel=cpu_power_model
            )
            hosts.append(host)

        # Assume single cluster for now
        cluster = Cluster(
            name="C01",
            hosts=hosts,
            powerSource=power_source
        )

        topology = Topology(clusters=[cluster])
        return topology

    async def fetch_status(self) -> K8sSystemSnapshot:
        logger.info("Fetching current system status from Kubernetes cluster")
        nodes = self.core_api.list_node().items
        topology = self._build_topology(nodes)

        max_available_node_count = {}

        for node_type in K8sSystemSnapshot.node_types:
            available_nodes = K8sNodeExtractor.get_available_worker_nodes(nodes=nodes, type_filter=node_type)
            max_available_node_count[node_type] = len(available_nodes)

        return K8sSystemSnapshot(
            topology=topology,
            max_available_node_count=max_available_node_count
        )

    # ====================
    # Apply decision
    # ====================

    def _set_node_schedulable(self, node_name: str, schedulable: bool) -> None:
        patch_body = {"spec": {"unschedulable": not schedulable}}
        self.core_api.patch_node(node_name, patch_body)

    def apply_scaling_decision(self, decision: Decision) -> None:
        if decision.details is None:
            logger.warning("Scaling decision missing details, cannot apply")
            return

        nodes = self.core_api.list_node().items
        
        for node_type in K8sSystemSnapshot.node_types:
            # Available worker nodes: not marked as (mock) unavailable
            # Current worker nodes: currently schedulable and ready
            available_worker_nodes = K8sNodeExtractor.get_available_worker_nodes(nodes=nodes, type_filter=node_type)
            current_worker_nodes = K8sNodeExtractor.get_in_use_worker_nodes(nodes=available_worker_nodes, type_filter=node_type)
            target_node_count = decision.details.get("to", {}).get(node_type, len(current_worker_nodes))

            target_node_count = min(target_node_count, len(available_worker_nodes))     # Do not exceed total nodes
            target_node_count = max(0, target_node_count)                               # Ensure non-negative count

            if target_node_count > len(current_worker_nodes):
                # Scale up: Mark additional nodes as schedulable
                nodes_to_enable = target_node_count - len(current_worker_nodes)
                for node in available_worker_nodes:
                    if K8sNodeExtractor.is_worker_node_in_use(node):
                        continue  # Skip already usable nodes
                    node_name = K8sNodeExtractor.get_node_name(node)
                    self._set_node_schedulable(node_name, True)
                    nodes_to_enable -= 1
                    if nodes_to_enable <= 0:
                        break
            elif target_node_count < len(current_worker_nodes):
                # Scale down: Mark excess nodes as unschedulable
                nodes_to_disable = len(current_worker_nodes) - target_node_count
                for node in reversed(current_worker_nodes):  # Reverse to disable last ones first
                    node_name = K8sNodeExtractor.get_node_name(node)
                    self._set_node_schedulable(node_name, False)
                    nodes_to_disable -= 1
                    if nodes_to_disable <= 0:
                        break

    async def apply_decision(self, decision: Decision) -> None:
        logger.info(f"✏️ Applying decision: {decision}")

        if decision.action == K8sActionKind.NO_OP:
            logger.info("No-op decision, nothing to apply")
        elif decision.action == K8sActionKind.CHANGE_NODE_COUNT:
            self.apply_scaling_decision(decision)
        else:
            logger.warning(f"Received decision with action {decision.action}, but apply_decision is not implemented yet")
