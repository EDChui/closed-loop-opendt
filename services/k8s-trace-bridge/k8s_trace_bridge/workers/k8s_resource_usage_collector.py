import logging
import threading
from typing import Literal
from requests.exceptions import RequestException

from k8s_observability.persistence import build_engine, build_session_factory, K8sResourceUsageSnapshotRepository
from k8s_observability.utils import TimeUtils

from k8s_trace_bridge.workers.base import BaseWorker
from k8s_trace_bridge.prometheus import PrometheusClient, PrometheusResourceCollector, PodResourceCollector, JobResourceCollector


logger = logging.getLogger(__name__)


class K8sResourceUsageCollector(BaseWorker):
    def __init__(
        self,
        kubeconfig_path: str,
        namespace: str,
        resource_type: Literal["pod", "job"],
        prometheus_url: str,
        database_url: str,
        collector_frequency_seconds: int = 15,
        start_barrier: threading.Barrier | None = None,
    ):
        super().__init__(
            name="K8sResourceUsageCollector",
            start_barrier=start_barrier
        )
        self.kubeconfig_path = kubeconfig_path
        self.namespace = namespace
        self.resource_type = resource_type
        self.prometheus_url = prometheus_url
        self.database_url = database_url
        self.collector_frequency_seconds = collector_frequency_seconds

        # Initialize Prometheus client and resource collector
        self.prom_client = PrometheusClient(base_url=self.prometheus_url)
        self.collector = self._build_collector()

        # Initialize database session and repository
        self.db_engine = build_engine(self.database_url)
        self.db_session_factory = build_session_factory(self.db_engine)
        self.db_session = self.db_session_factory()
        self.resource_usage_repo = K8sResourceUsageSnapshotRepository(self.db_session)
    
    def _build_collector(self) -> PrometheusResourceCollector:
        if self.resource_type == "pod":
            return PodResourceCollector(
                prom=self.prom_client,
                namespace=self.namespace
            )
        elif self.resource_type == "job":
            return JobResourceCollector(
                prom=self.prom_client,
                namespace=self.namespace,
                kubeconfig_file=self.kubeconfig_path,
            )
        else:
            raise ValueError(f"Unsupported resource type: {self.resource_type}")

    def capture_and_save_resource_usage(self) -> None:
        now_dt = TimeUtils.now()
        try:
            snapshots = self.collector.collect_metrics(now_dt)
        except RequestException as e:
            logger.error(f"Error collecting metrics from Prometheus: {e}")
            snapshots = []
        except Exception as e:
            logger.error(f"Unexpected error during metric collection: {e}")
            snapshots = []
        
        self.resource_usage_repo.add_many(snapshots)
        self.resource_usage_repo.commit()

    def run(self) -> None:
        logger.info("K8sResourceUsageCollector running")

        # Capture resources immediately on startup
        self.capture_and_save_resource_usage()

        try:
            while not self.should_stop():
                if self.wait_interruptible(self.collector_frequency_seconds):
                    break
                self.capture_and_save_resource_usage()
        except Exception as e:
            logger.error(f"Error in K8sResourceUsageCollector: {e}", exc_info=True)
            raise