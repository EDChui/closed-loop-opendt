import logging
import os
import signal
import sys
import threading

from odt_common import load_config_from_env
from odt_common.utils import get_kafka_bootstrap_servers
from odt_common.config import K8sWorkloadContext
from k8s_observer.producers import HeartbeatProducer, K8sWorkloadProducer
from k8s_observer.workers import BaseWorker, K8sResourceUsageCollector

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)

class K8sObserverOrchestrator:
    """Orchestrates multiple threaded workers for the K8s Observer service."""

    def __init__(self):
        """Initialize the orchestrator"""
        self.heartbeat_producer: HeartbeatProducer | None = None
        self.workload_producer: K8sWorkloadProducer | None = None
        self.resource_usage_collector: K8sResourceUsageCollector | None = None
        self.shutdown_requested = False

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self.shutdown_requested = True
            self.stop_all()

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def start_all(
        self,
        workload_context: K8sWorkloadContext,
        kafka_bootstrap_servers: str,
        workload_topic: str,
        power_topic: str,                           # TODO: Power topic
        cpu_frequency_mhz: int,
        heartbeat_frequency_minutes: int = 1,
    ) -> None:
        """Start all workers.
        
        """
        logger.info("=" * 70)
        logger.info("Starting all workers...")
        logger.info("=" * 70)
        
        # Create a synchronization barrier for workers
        # This ensure they all start at the same wall-clock time
        num_workers = 3
        start_barrier = threading.Barrier(num_workers, timeout=30)
        logger.info(f"Created start barrier for {num_workers} workers")

        # 1. Start HeartbeatProducer
        logger.info("[1/3] Initializing HeartbeatProducer...")
        self.heartbeat_producer = HeartbeatProducer(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=workload_topic,
            heartbeat_frequency_minutes=heartbeat_frequency_minutes,
            start_barrier=start_barrier,
        )
        self.heartbeat_producer.start()

        # 2. Start K8sWorkloadProducer
        logger.info("[2/3] Initializing K8sWorkloadProducer...")
        self.workload_producer = K8sWorkloadProducer(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=workload_topic,
            kubeconfig_path=workload_context.kubeconfig_path,
            namespace=workload_context.namespace,
            resource_type=workload_context.resource_type,
            cpu_frequency_mhz=cpu_frequency_mhz,
            database_url=workload_context.database_url,
            start_barrier=start_barrier,
        )
        self.workload_producer.start()

        # 3. Start K8sResourceUsageCollector
        logger.info("[3/3] Initializing K8sResourceUsageCollector...")
        self.resource_usage_collector = K8sResourceUsageCollector(
            kubeconfig_path=workload_context.kubeconfig_path,
            namespace=workload_context.namespace,
            resource_type=workload_context.resource_type,
            prometheus_url=workload_context.prometheus_url,
            database_url=workload_context.database_url,
            start_barrier=start_barrier,
        )
        self.resource_usage_collector.start()

        logger.info("=" * 70)
        logger.info("✅ All workers started and synchronized")
        logger.info("=" * 70)

    def wait_for_completion(self) -> None:
        """Wait for all workers to complete or be interrupted."""
        logger.info("Waiting for workers to complete...")

        try:
            # Heartbeat producer runs indefinitely, but we wait for it to be interrupted
            if self.heartbeat_producer and self.heartbeat_producer._thread:
                if self.heartbeat_producer.is_running():
                    logger.info("Waiting for HeartbeatProducer to finish...")
                    self.heartbeat_producer._thread.join()

            # Workload producer runs indefinitely, o we stop it explicitly
            if self.workload_producer and self.workload_producer.is_running():
                logger.info("Stopping WorkloadProducer...")
                self.workload_producer.stop()

            # Resource usage collector runs indefinitely, so we stop it explicitly
            if self.resource_usage_collector and self.resource_usage_collector.is_running():
                logger.info("Stopping ResourceUsageCollector...")
                self.resource_usage_collector.stop()

            logger.info("All workers completed")
        except KeyboardInterrupt:
            logger.info("Received interrupt during wait")
            self.stop_all()

    def stop_all(self) -> None:
        """Stop all running workers."""
        logger.info("=" * 70 + "\nStopping all workers...\n" + "=" * 70)

        workers: list[tuple[str, BaseWorker | None]] = [
            ("HeartbeatProducer", self.heartbeat_producer),
            ("WorkloadProducer", self.workload_producer),
            ("ResourceUsageCollector", self.resource_usage_collector),
        ]

        for name, producer in workers:
            if producer and producer.is_running():
                logger.info(f"Stopping {name}...")
                producer.stop(timeout=5.0)
            elif producer:
                logger.info(f"{name} already stopped")

        logger.info("=" * 70 + "\n✅ All workers stop requested\n" + "=" * 70)

    def run(self) -> int:
        """Run the orchestrator.

        Returns:
            Exit code: 0 for success, 1 for error
        """
        try:
            # Load configuration
            logger.info("Loading configuration...")
            config = load_config_from_env()
            namespace = config.services.k8s_observer.namespace
            heartbeat_frequency_minutes = config.services.k8s_observer.heartbeat_frequency_minutes
            kubeconfig_path = os.getenv("KUBECONFIG", "/kube/config")
            prometheus_url = os.getenv("PROMETHEUS_URL", "http://host.docker.internal:9090")
            database_url = os.getenv("DATABASE_URL", "postgresql+psycopg://opendt:opendt@postgres:5432/opendt")
            cpu_frequency_mhz = config.global_config.cpu_frequency_mhz

            # Create workload context
            # Always use "pod" resource type as it is the most fundamental unit in K8s
            workload_context = K8sWorkloadContext(
                kubeconfig_path=kubeconfig_path,
                namespace=namespace,
                prometheus_url=prometheus_url,
                database_url=database_url,
            )

            # Get Kafka configuration from environment variable
            kafka_bootstrap_servers = get_kafka_bootstrap_servers()
            logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")

            # Get topic names
            workload_topic = config.kafka.topics["workload"].name
            power_topic = config.kafka.topics["power"].name
            logger.info(
                f"Topics: workload={workload_topic}, power={power_topic}"
            )

            # Setup signal handlers
            self.setup_signal_handlers()

            # Start all workers
            self.start_all(
                workload_context=workload_context,
                kafka_bootstrap_servers=kafka_bootstrap_servers,
                workload_topic=workload_topic,
                power_topic=power_topic,
                cpu_frequency_mhz=cpu_frequency_mhz,
                heartbeat_frequency_minutes=heartbeat_frequency_minutes,
            )

            # Wait for completion
            self.wait_for_completion()

            logger.info("✅ k8s-observer service completed successfully")
            return 0

        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
            self.stop_all()
            return 0
        except Exception as e:
            logger.error(f"❌ Error in k8s-observer service: {e}", exc_info=True)
            self.stop_all()
            return 1

def main() -> int:
    """Main entry point.
    
    Returns:
        Exit code: 0 for success, 1 for error
    """
    orchestrator = K8sObserverOrchestrator()
    return orchestrator.run()

if __name__ == "__main__":
    sys.exit(main())
    