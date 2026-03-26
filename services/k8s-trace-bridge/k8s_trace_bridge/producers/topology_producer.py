import logging
import threading
from collections import defaultdict
from datetime import datetime

from kubernetes import client, config

from odt_common.models.topology import (
    TopologySnapshot,
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
from k8s_trace_bridge.producers import BaseProducer

logger = logging.getLogger(__name__)


class TopologyProducer(BaseProducer):
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        kubeconfig_path: str,
        cpu_frequency_mhz: int,
        publish_interval_seconds: int = 30,
        start_barrier: threading.Barrier | None = None,
    ):
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
            name="TopologyProducer",
            start_barrier=start_barrier
        )
        self.kubeconfig_path = kubeconfig_path
        self.cpu_frequency_mhz = cpu_frequency_mhz
        self.publish_interval_seconds = publish_interval_seconds

        config.load_kube_config(config_file=self.kubeconfig_path)
        self.core_api = client.CoreV1Api()
        logger.info(f"  Publish interval (realtime): {publish_interval_seconds}s")

        # TODO: Temporary hardcoded power model
        self.cpu_power_model = MseCPUPowerModel(
            modelType="mse",
            power=300,
            idlePower=50,
            maxPower=174,
            calibrationFactor=4
        )

    def build_topology(self) -> Topology:
        nodes = self.core_api.list_node().items
        grouped_shapes: dict[K8sNodeShape, int] = defaultdict(int)

        for node in nodes:
            if not K8sNodeExtractor.is_ready_worker_node(node):
                continue

            shape = K8sNodeExtractor.extract_node_shape(node)
            grouped_shapes[shape] += 1

        if not grouped_shapes:
            raise ValueError("No valid worker nodes found in the cluster to build topology")
        
        hosts: list[Host] = []
        for idx, (shape, count) in enumerate(grouped_shapes.items()):
            host = Host(
                name=f"H{idx:02d}",
                count=count,
                cpu=CPU(coreCount=shape.cpu_count, coreSpeed=self.cpu_frequency_mhz),
                memory=Memory(memorySize=shape.memory_size_bytes),
                cpuPowerModel=self.cpu_power_model
            )
            hosts.append(host)

        # Assume single cluster for now
        # TODO: Add correct power source configuration if available
        # This likely to be a parquet file, perhaps come from [ElectricityMaps](https://portal.electricitymaps.com) and [ENTSO-E](https://www.entsoe.eu/) as mention in opec-demo
        cluster = Cluster(
            name="C01",
            hosts=hosts,
            powerSource=PowerSource(carbonTracePath="/app/workload/carbon.parquet")
        )

        topology = Topology(clusters=[cluster])
        return topology

    def publish_topology_snapshot(self) -> TopologySnapshot:
        topology = self.build_topology()
        snapshot = TopologySnapshot(timestamp=datetime.now(), topology=topology)
        self.emit_message(
            message=snapshot.model_dump(mode="json"),
            key="datacenter",  # Single key for compaction
        )
        self.flush()
        return snapshot

    def run(self) -> None:
        logger.info("TopologyProducer running")

        try:
            # Load topology once at startup
            snapshot = self.publish_topology_snapshot()
            logger.info(
                f"Loaded initial topology: {snapshot.topology.total_host_count()} hosts, "
                f"{snapshot.topology.total_core_count()} cores"
            )
            logger.info("Published initial topology")

            # Publish periodically
            while not self.should_stop():
                # Wait for the interval
                if self.wait_interruptible(self.publish_interval_seconds):
                    break

                # Publish topology message
                snapshot = self.publish_topology_snapshot()
                logger.debug("Published topology update")
        except Exception as e:
            logger.error(f"Fatal error in TopologyProducer: {e}", exc_info=True)
            raise
