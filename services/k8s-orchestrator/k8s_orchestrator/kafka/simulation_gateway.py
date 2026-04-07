import logging
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from typing import AsyncIterator, Optional

from odt_common.models import SimulationBatch, SimulationBatchReport
from k8s_orchestrator.application.ports import SimulationGateway

logger = logging.getLogger(__name__)


class KafkaSimulationGateway(SimulationGateway):
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        sim_batch_topic: str,
        sim_batch_report_topic: str,
        consumer_group: str = "k8s-orchestrator",
    ):
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.sim_batch_topic = sim_batch_topic
        self.sim_batch_report_topic = sim_batch_report_topic

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
        )

        topics = [self.sim_batch_report_topic]
        self._consumer = AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self.kafka_bootstrap_servers,
            group_id=consumer_group
        )
        self._started = False

    async def start(self):
        logger.info("Starting KafkaSimulationGateway")
        if not self._started:
            await self._producer.start()
            await self._consumer.start()
            self._started = True

    async def stop(self):
        if self._started:
            try:
                await self._producer.flush()
                await self._producer.stop()
            except Exception as e:
                logger.error(f"Error stopping producer: {e}")
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error(f"Error stopping consumer: {e}") 
            self._started = False
        logger.info("KafkaSimulationGateway stopped")

    async def submit_batch(self, batch: SimulationBatch) -> None:
        payload = self._encode_batch(batch)
        await self._producer.send_and_wait(
            topic=self.sim_batch_topic,
            value=payload,
            key=batch.batch_id.encode("utf-8")
        )
        await self._producer.flush()
        logger.info(f"Submitted simulation batch with batch_id={batch.batch_id}")

    def __aiter__(self) -> AsyncIterator[SimulationBatchReport]:
        return self._iter_reports()
    
    async def _iter_reports(self) -> AsyncIterator[SimulationBatchReport]:
        if not self._started:
            raise RuntimeError("KafkaSimulationGateway must be started before iterating reports")
        
        async for msg in self._consumer:
            try:
                report = self._decode_report(msg.value)
                if report is None:
                    logger.warning("Received empty report message")
                    continue
                logger.info(f"Received simulation batch report for batch_id={report.batch_id}")
                yield report
            except Exception as e:
                logger.error(f"Failed to decode simulation batch report: {e}")

    @classmethod
    def _encode_batch(cls, batch: SimulationBatch) -> bytes:
        return json.dumps(batch.model_dump(mode="json")).encode("utf-8")

    @staticmethod
    def _decode_report(raw: Optional[bytes]) -> Optional[SimulationBatchReport]:
        if raw is None:
            return None
        data = json.loads(raw.decode("utf-8"))
        return SimulationBatchReport.model_validate(data)
