import logging
from typing import Optional, Literal
from kubernetes import client, config, watch

from k8s_trace_bridge.infrastructure.kubernetes import EventObjectExtractor, JobEventExtractor, PodEventExtractor

logger = logging.getLogger(__name__)


class TerminalMetadataStream:
    def __init__(
        self,
        namespace: str,
        resource_type: Literal["job", "pod"],
        config_file: Optional[str] = None,
    ):
        self.namespace = namespace
        self.resource_type = resource_type
        self.reported_uids = set()

        config.load_kube_config(config_file=config_file)
        self.watcher = watch.Watch()
        self.stream_fn = self._get_stream_function()
        self.extractor = self._get_extractor()

    def _get_stream_function(self):
        if self.resource_type == "pod":
            core = client.CoreV1Api()
            return core.list_namespaced_pod
        elif self.resource_type == "job":
            batch = client.BatchV1Api()
            return batch.list_namespaced_job
        else:
            raise ValueError(f"Unsupported resource type: {self.resource_type}")
        
    def _get_extractor(self) -> EventObjectExtractor:
        if self.resource_type == "pod":
            return PodEventExtractor()
        elif self.resource_type == "job":
            return JobEventExtractor()
        else:
            raise ValueError(f"Unsupported resource type: {self.resource_type}")
    
    def stream(self):
        logger.info(f"Starting terminal metadata stream for {self.resource_type}s in namespace '{self.namespace}'")
        try:
            for event in self.watcher.stream(self.stream_fn, namespace=self.namespace, timeout_seconds=0):
                obj = event.get("object")   # type: ignore
                if obj is None:
                    continue
                uid = obj.metadata.uid
                
                # Skip if we've already reported this UID
                if uid in self.reported_uids:
                    continue
                # Skip if this object is not in a terminal state
                if not self.extractor.is_terminal(obj):
                    continue

                metadata = self.extractor.extract_metadata(uid, obj)
                self.reported_uids.add(uid)

                yield metadata
        except Exception as e:
            logger.error(f"Error while streaming {self.resource_type} events: {e}")
            raise
