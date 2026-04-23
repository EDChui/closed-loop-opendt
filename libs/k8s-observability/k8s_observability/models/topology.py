from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class K8sNodeShape:
    node_type: str
    cpu_count: int
    memory_size_bytes: int
    # architecture: Optional[str]
    # operating_system: Optional[str]
