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
        # TODO: Assume all nodes have the same CPU frequency, can be extracted from node info in the future if needed
        self.cpu_frequency_mhz = cpu_frequency_mhz

        config.load_kube_config(config_file=self.kubeconfig_path)
        self.core_api = client.CoreV1Api()

    def _build_topology(self) -> Topology:
        nodes = self.core_api.list_node().items
        grouped_shapes: dict[K8sNodeShape, int] = defaultdict(int)

        for node in nodes:
            if not K8sNodeExtractor.is_ready_worker_node(node):
                continue

            shape = K8sNodeExtractor.extract_node_shape(node)
            grouped_shapes[shape] += 1

        if not grouped_shapes:
            raise ValueError("No valid worker nodes found in the cluster to build topology")
        
        # TODO: Temporary hardcoded power model and power source
        cpu_power_model = MseCPUPowerModel(
            modelType="mse",
            power=300,
            idlePower=50,
            maxPower=174,
            calibrationFactor=4
        )
        power_source = PowerSource(carbonTracePath="/app/workload/carbon.parquet")
        
        hosts: list[Host] = []
        for idx, (shape, count) in enumerate(grouped_shapes.items()):
            host = Host(
                name=f"H{(idx+1):02d}",
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
        topology = self._build_topology()
        return K8sSystemSnapshot(topology=topology)

    async def apply_decision(self, decision: Decision) -> None:
        logger.info(f"Applying decision: {decision}")
        # TODO: Implement me
        if decision.action == K8sActionKind.NO_OP:
            logger.info("No-op decision, nothing to apply")
        else:
            logger.warning(f"Received decision with action {decision.action}, but apply_decision is not implemented yet")
