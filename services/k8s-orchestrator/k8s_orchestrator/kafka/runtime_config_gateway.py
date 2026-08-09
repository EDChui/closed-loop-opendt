import logging
import json
from aiokafka import AIOKafkaConsumer
from typing import AsyncIterator, Optional

from odt_common.models import DecisionPolicy, WeightedDecisionPolicy, RankedDecisionPolicy
from k8s_orchestrator.application.events import ConfigChange
from k8s_orchestrator.application.ports import RuntimeConfigGateway

logger = logging.getLogger(__name__)


class KafkaRuntimeConfigGateway(RuntimeConfigGateway):
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        objectives_topic: str,
        consumer_group: str = "k8s-orchestrator",
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.objectives_topic = objectives_topic

        topics = [self.objectives_topic]
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=consumer_group
        )
        self._started = False

    async def start(self):
        logger.info("Starting KafkaRuntimeConfigGateway")
        if not self._started:
            await self._consumer.start()
            self._started = True

    async def stop(self):
        if self._started:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}") 
            self._started = False
        logger.info("KafkaRuntimeConfigGateway stopped")

    def __aiter__(self) -> AsyncIterator[ConfigChange]:
        return self._iter_decision_policies()

    async def _iter_decision_policies(self) -> AsyncIterator[ConfigChange]:
        if not self._started:
            raise RuntimeError("KafkaRuntimeConfigGateway must be started before iterating")
        
        async for msg in self._consumer:
            try:
                decision_policy = self._decode_decision_policy(msg.value)
                if decision_policy is None:
                    logger.warning("Received empty decision policy, skipping")
                    continue
                logger.info(f"Received new decision policy")
                yield ConfigChange(new_policy=decision_policy)
            except Exception as e:
                logger.error(f"Error processing message: {e}")

    @staticmethod
    def _decode_decision_policy(raw: Optional[bytes]) -> Optional[DecisionPolicy]:
        if raw is None:
            return None
        data = json.loads(raw.decode("utf-8"))

        if data.get("policy_type") == "weighted":
            return WeightedDecisionPolicy.model_validate(data)
        elif data.get("policy_type") == "ranked":
            return RankedDecisionPolicy.model_validate(data)
        raise ValueError(f"Unknown decision policy type: {data.get('policy_type')}")
