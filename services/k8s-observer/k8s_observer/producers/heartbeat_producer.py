"""Heartbeat producer for Kubernetes Trace Bridge."""

import logging
import threading
from datetime import datetime, timezone

from k8s_observer.producers.base import BaseProducer

logger = logging.getLogger(__name__)

MINUTE_IN_SECONDS = 60


class HeartbeatProducer(BaseProducer):
    """Emits periodic heartbeat messages to Kafka."""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        heartbeat_frequency_minutes: int = 1,
        start_barrier: threading.Barrier | None = None,
    ):
        """Initialize the heartbeat producer.
        
        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name for heartbeat messages
            heartbeat_frequency_minutes: Frequency of heartbeats in simulated minutes
            start_barrier: Optional barrier for synchronized startup across producers
        """
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
            name="HeartbeatProducer",
            start_barrier=start_barrier,
        )
        self.heartbeat_frequency_minutes = heartbeat_frequency_minutes

        logger.info(f"  Heartbeat frequency: {heartbeat_frequency_minutes} minutes")

    def _build_heartbeat_message(self, timestamp: datetime) -> dict[str, str]:
        return {
            "message_type": "heartbeat",
            "timestamp": timestamp.isoformat(),
        }
    
    def send_heartbeat_now(self) -> datetime:
        """Send a heartbeat message with the current timestamp."""
        now = datetime.now(timezone.utc)
        heartbeat_message = self._build_heartbeat_message(now)
        self.emit_message(heartbeat_message, key="heartbeat")
        self.flush()
        return now

    def run(self) -> None:
        logger.info("HeartbeatProducer running")

        # Send heartbeat immediately on startup
        timestamp = self.send_heartbeat_now()
        logger.info(f"Sent initial heartbeat at {timestamp.isoformat()}")

        try:
            while not self.should_stop():
                # Wait for the interval (with ability to interrupt)
                if self.wait_interruptible(self.heartbeat_frequency_minutes * MINUTE_IN_SECONDS):
                    break
                timestamp = self.send_heartbeat_now()
                logger.debug(f"Sent heartbeat at {timestamp.isoformat()}")
        except Exception as e:
            logger.error(f"Error in HeartbeatProducer: {e}", exc_info=True)
            raise
