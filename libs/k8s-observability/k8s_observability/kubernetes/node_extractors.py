from typing import Any

from k8s_observability.models import K8sNodeShape
from k8s_observability.utils import UnitUtils


class K8sNodeExtractor:
    @staticmethod
    def is_ready_worker_node(node: Any) -> bool:
        metadata = getattr(node, "metadata", None)
        spec = getattr(node, "spec", None)
        status = getattr(node, "status", None)
        labels = getattr(metadata, "labels", {}) or {}

        # Check if node is unschedulable
        if getattr(spec, "unschedulable", False):
            return False

        # Check for control-plane nodes
        if "node-role.kubernetes.io/control-plane" in labels:
            return False

        # Check for Ready condition
        conditions = getattr(status, "conditions", None) or []
        for condition in conditions:
            if getattr(condition, "type", None) == "Ready":
                return getattr(condition, "status", None) == "True"

        return False
    
    @staticmethod
    def extract_node_shape(node: Any) -> K8sNodeShape:
        status = getattr(node, "status", None)
        metadata = getattr(node, "metadata", None)
        labels = getattr(metadata, "labels", {}) or {}
        allocatable = getattr(status, "allocatable", {}) or {}

        cpu_count = max(1, int(round(UnitUtils.parse_cpu_to_core(allocatable.get("cpu")))))
        memory_size_bytes = UnitUtils.parse_mem_to_bytes(allocatable.get("memory"))
        architecture = labels.get("kubernetes.io/arch", "unknown")
        operating_system = labels.get("kubernetes.io/os", "unknown")  # Using OS as a proxy for instance type
        return K8sNodeShape(
            cpu_count=cpu_count,
            memory_size_bytes=memory_size_bytes,
            architecture=architecture,
            operating_system=operating_system
        )
