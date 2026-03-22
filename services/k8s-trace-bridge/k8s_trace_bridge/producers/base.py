"""Base producer class for K8S-Trace-Bridge threaded producers."""

import logging
import threading
from typing import Any

from kafka import KafkaProducer
from k8s_trace_bridge.workers import BaseWorker
from odt_common.utils import get_kafka_producer
from odt_common.utils.kafka import send_message

logger = logging.getLogger(__name__)


class BaseProducer(BaseWorker):
    """Base class for threaded Kafka producers.

    Provides common functionality for all producers:
    - Kafka producer management
    - Thread lifecycle management
    - Message emission utilities
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        name: str | None = None,
        start_barrier: threading.Barrier | None = None,
    ):
        """Initialize the base producer.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name for this producer
            name: Optional producer name for logging
            start_barrier: Optional barrier for synchronized startup across producers
        """
        super().__init__(name=name, start_barrier=start_barrier)
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.topic = topic
        self._producer: KafkaProducer | None = None

        logger.info(f"  Topic: {self.topic}")

    def _get_producer(self) -> KafkaProducer:
        """Get or create the Kafka producer.

        Returns:
            Kafka producer instance
        """
        if self._producer is None:
            self._producer = get_kafka_producer(self.kafka_bootstrap_servers)
        return self._producer

    def emit_message(self, message: dict[str, Any], key: str | None = None) -> None:
        """Emit a message to Kafka.

        Args:
            message: Message payload (will be JSON serialized)
            key: Optional message key
        """
        try:
            send_message(
                self._get_producer(),
                topic=self.topic,
                message=message,
                key=key,
            )
        except Exception as e:
            logger.error(f"Failed to emit message to {self.topic}: {e}", exc_info=True)
            raise

    def flush(self) -> None:
        """Flush buffered messages to Kafka."""
        if self._producer:
            self._producer.flush()

    def _cleanup(self) -> None:
        """Clean up resources (called automatically on exit)."""
        if self._producer:
            try:
                self._producer.flush()
                self._producer.close()
                logger.info(f"{self.name} Kafka producer closed")
            except Exception as e:
                logger.error(f"Error closing {self.name} producer: {e}")
            finally:
                self._producer = None
