from datetime import datetime
from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class WorkloadCompletion:
    # Basic identifiers
    resource_type: str              # "job" or "pod"
    namespace: str
    name: str
    uid: str                        # uid of the pod or job
    # OpenDC-related metrics
    # Time-related fields
    submission_time: datetime
    start_time: datetime
    finish_time: datetime
    # Resource-related fields
    cpu_request_count: float        # Requested CPU cores in cores (e.g., 0.5 for 500m)
    cpu_limit_count: float          # Limited CPU cores in cores (e.g., 0.5 for 500m)
    mem_request_capacity_mb: int    # Requested memory in MB
    mem_limit_capacity_mb: int      # Limited memory in MB
    # Additional metadata fields
    success_complete: bool
    terminal_status: str


@dataclass(frozen=True)
class PodCompletion(WorkloadCompletion):
    node_name: str
    owner_kind: str
    owner_name: str


@dataclass(frozen=True)
class ResourceUsageSnapshot:
    resource_type: str
    namespace: str
    name: str
    uid: Optional[str]
    capture_time: datetime
    cpu_usage: float        # CPU usages in cores (e.g., 0.5 for 500m)
    mem_usage_mb: float     # Memory usage in MB


@dataclass(frozen=True)
class K8sNodeShape:
    cpu_count: int
    memory_size_bytes: int
    architecture: Optional[str]
    operating_system: Optional[str]
