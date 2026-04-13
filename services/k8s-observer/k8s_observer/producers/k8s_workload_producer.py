"""Workload producer for k8s-observer service.

Streams real-time task/workload events to Kafka.
"""
import time
import logging
import threading
from datetime import datetime
from typing import Literal, Sequence
from math import ceil

from odt_common import Fragment, Task
from k8s_observability.models import K8sTaskRecord, K8sResourceUsageSnapshot
from k8s_observability.kubernetes import K8sResourceTerminalStream
from k8s_observability.persistence import build_engine, build_session_factory, K8sTaskRecordRepository, K8sWorkloadResourceUsageSnapshotRepository
from k8s_observer.producers.base import BaseProducer


logger = logging.getLogger(__name__)

SECONDS_TO_MILLISECONDS = 1000
MAX_RETRIES = 20
RETRY_DELAY_SECONDS = 2


class K8sWorkloadProducer(BaseProducer):
    """Emits real-time workload (task) events to Kafka."""

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        kubeconfig_path: str,
        namespace: str,
        resource_type: Literal["pod", "job"],
        cpu_frequency_mhz: int,
        database_url: str,
        start_barrier: threading.Barrier | None = None,
    ):
        """Initialize the workload producer.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            topic: Kafka topic name for workload events
            start_barrier: Optional barrier for synchronized startup
        """
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
            name="K8sWorkloadProducer",
            start_barrier=start_barrier,
        )
        self.kubeconfig_path = kubeconfig_path
        self.namespace = namespace
        self.resource_type = resource_type
        self.cpu_frequency_mhz = cpu_frequency_mhz
        self.database_url = database_url

        self.terminal_metadata_stream = K8sResourceTerminalStream(
            namespace=self.namespace,
            resource_type=self.resource_type,
            config_file=self.kubeconfig_path,
        )
        self.db_engine = build_engine(self.database_url)
        self.db_session_factory = build_session_factory(self.db_engine)
        self.db_session = self.db_session_factory()
        self.task_record_repo = K8sTaskRecordRepository(self.db_session)
        self.resource_usage_repo = K8sWorkloadResourceUsageSnapshotRepository(self.db_session)

    def _build_task_message(self, task: Task) -> dict[str, object]:
        return {
            "message_type": "task",
            "timestamp": task.submission_time.isoformat(),
            "task": task.model_dump(mode="json")
        }
    
    def _convert_usage_snapshots_to_fragments(
        self,
        snapshots: Sequence[K8sResourceUsageSnapshot],
        task_id: int,
        start_time: datetime,
        finish_time: datetime,
        cpu_limit_count: float
    ) -> list[Fragment]:
        fragments = []
        last_capture_time = start_time
        for snapshot in snapshots:
            if snapshot.capture_time < start_time or snapshot.capture_time > finish_time:
                continue
            duration_ms = ceil((snapshot.capture_time - last_capture_time).total_seconds() * SECONDS_TO_MILLISECONDS)
            last_capture_time = snapshot.capture_time
            utilization_rate = snapshot.cpu_usage / cpu_limit_count
            cpu_count = ceil(cpu_limit_count)
            cpu_usage = utilization_rate * cpu_count * self.cpu_frequency_mhz
            fragments.append(Fragment(
                id=task_id,
                duration=duration_ms,
                cpu_count=cpu_count,
                cpu_usage=cpu_usage,
            ))
        return fragments
    
    def _convert_terminal_metadata_to_task(self, metadata: K8sTaskRecord, task_id: int, fragments: list[Fragment]) -> Task:
        duration_ms = ceil((metadata.finish_time - metadata.start_time).total_seconds() * SECONDS_TO_MILLISECONDS)
        cpu_count = ceil(metadata.cpu_limit_count)
        cpu_capacity = cpu_count * self.cpu_frequency_mhz
        return Task(
            id=task_id,
            submission_time=metadata.submission_time,
            duration=duration_ms,
            cpu_count=cpu_count,
            cpu_capacity=cpu_capacity,
            mem_capacity=metadata.mem_limit_capacity_mb,
            fragments=fragments
        )
    
    def _process_stream(self):
        for terminal_metadata in self.terminal_metadata_stream.stream():
            # Store in the database
            task_id = self.task_record_repo.add(terminal_metadata)
            self.task_record_repo.commit()

            # Fetch the corresponding resource usage snapshots from the database
            usage_snapshots = self.resource_usage_repo.list_by_uid(terminal_metadata.uid)
            logger.info(f"📷 Captured completed {self.resource_type} {terminal_metadata.uid} with {len(usage_snapshots)} resource usage snapshots")

            # Build Fragments
            fragments = self._convert_usage_snapshots_to_fragments(
                snapshots=usage_snapshots,
                task_id=task_id,
                start_time=terminal_metadata.start_time,
                finish_time=terminal_metadata.finish_time,
                cpu_limit_count=terminal_metadata.cpu_limit_count
            )

            # Convert to Task message
            task = self._convert_terminal_metadata_to_task(terminal_metadata, task_id, fragments)
            task_msg = self._build_task_message(task)
            
            # Emit to Kafka
            self.emit_message(message=task_msg, key=str(task_id))
            self.flush()

            if self.should_stop():
                logger.info("Stop signal received, ending stream processing loop")
                return

    def run(self) -> None:
        """Run the workload producer"""
        logger.info("K8sWorkloadProducer running")
        retry_count = 0

        try:
            while not self.should_stop() and retry_count < MAX_RETRIES:
                try:
                    self._process_stream()
                    retry_count = 0
                except Exception as e:
                    logger.error(f"Stream error: {e}", exc_info=True)
                logger.warning("Terminal metadata stream ended unexpectedly; reconnecting...")
                retry_count += 1
                time.sleep(RETRY_DELAY_SECONDS)
        except Exception as e:
            logger.error(f"Fatal error in K8sWorkloadProducer: {e}", exc_info=True)
            raise
        finally:
            self.db_session.close()
            logger.info("K8sWorkloadProducer stopped and database session closed")
