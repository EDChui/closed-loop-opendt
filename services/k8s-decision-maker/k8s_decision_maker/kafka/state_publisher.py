import json
import logging
from aiokafka import AIOKafkaProducer

from odt_common.models import TopologySnapshot

from k8s_observability.utils import TimeUtils
from k8s_decision_maker.application.ports.state_publisher import StatePublisher
from k8s_decision_maker.domain import ObservedState
from k8s_decision_maker.domain.kubernetes import K8sSystemSnapshot


logger = logging.getLogger(__name__)


class KafkaStatePublisher(StatePublisher[K8sSystemSnapshot]):
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topology_topic: str
    ):
        """Initialize the Kafka state publisher.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            topology_topic: Kafka topic for real topology (dc.topology)
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topology_topic = topology_topic

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers
        )
        self._started = False

    async def start(self):
        logger.info("Starting KafkaStatePublisher")
        if not self._started:
            await self._producer.start()
            self._started = True

    async def stop(self):
        if self._started:
            try:
                await self._producer.flush()
                await self._producer.stop()
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
            self._started = False
        logger.info("KafkaStatePublisher stopped")

    async def publish_topology_snapshot(self, topology_snapshot: TopologySnapshot) -> bool:
        try:
            message_data = json.dumps(
                topology_snapshot.model_dump(mode="json")
            ).encode("utf-8")
            await self._producer.send_and_wait(
                topic=self.topology_topic,
                value=message_data,
                key=b"datacenter"
            )
            await self._producer.flush()
            logger.info(f"Published topology snapshot to {self.topology_topic}")
            return True
        except Exception as e:
            logger.error(f"Failed to publish topology snapshot: {e}")
            return False

    async def publish_system_state(self, state: ObservedState[K8sSystemSnapshot], cause: str) -> None:
        topology = state.snapshot.topology
        timestamp = TimeUtils.to_datetime(state.observed_at)
        topology_snapshot = TopologySnapshot(
            timestamp=timestamp,
            topology=topology
        )
        await self.publish_topology_snapshot(topology_snapshot)
