from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class NodeUtilizationSnapshot:
    node_name: str
    capture_time: datetime
    cpu_utilization: float
    memory_utilization: float
