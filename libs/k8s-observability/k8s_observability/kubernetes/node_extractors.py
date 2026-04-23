from typing import Any

from k8s_observability.models import K8sNodeShape
from k8s_observability.utils import UnitUtils


class K8sNodeExtractor:
    @staticmethod
    def is_control_plane_node(node: Any) -> bool:
        metadata = getattr(node, "metadata", None)
        labels = getattr(metadata, "labels", {}) or {}
        return "node-role.kubernetes.io/control-plane" in labels
    
    @staticmethod
    def is_worker_node(node: Any) -> bool:
        return not K8sNodeExtractor.is_control_plane_node(node)
    
    @staticmethod
    def is_node_ready(node: Any) -> bool:
        status = getattr(node, "status", None)
        conditions = getattr(status, "conditions", None) or []
        for condition in conditions:
            if getattr(condition, "type", None) == "Ready":
                return getattr(condition, "status", None) == "True"
        return False
    
    @staticmethod
    def is_node_schedulable(node: Any) -> bool:
        spec = getattr(node, "spec", None)
        return not getattr(spec, "unschedulable", False)
    
    @staticmethod
    def get_node_tags(node: Any) -> dict[str, str]:
        metadata = getattr(node, "metadata", None)
        labels = getattr(metadata, "labels", {}) or {}
        return labels
    
    @staticmethod
    def is_node_available(node: Any) -> bool:
        """Determines if a node is marked as available (mock)"""
        return K8sNodeExtractor.get_node_tags(node).get("k8s-observability/availability", "") != "unavailable"
    
    @staticmethod
    def get_node_type(node: Any):
        node_type = K8sNodeExtractor.get_node_tags(node).get("k8s-observability/node-type", "unknown")
        if node_type not in {"cloud", "edge", "endpoint"}:
            node_type = "unknown"
        return node_type

    @staticmethod
    def is_worker_node_in_use(node: Any) -> bool:
        """In use worker node means it's a worker node that is ready, available, and schedulable"""
        return K8sNodeExtractor.is_worker_node(node) \
            and K8sNodeExtractor.is_node_ready(node) \
            and K8sNodeExtractor.is_node_available(node) \
            and K8sNodeExtractor.is_node_schedulable(node)

    @staticmethod
    def get_node_name(node: Any) -> str:
        metadata = getattr(node, "metadata", None)
        return getattr(metadata, "name", "unknown-node")
    
    @staticmethod
    def get_node_status(node: Any) -> str:
        if not K8sNodeExtractor.is_node_ready(node):
            return "NotReady"
        if not K8sNodeExtractor.is_node_available(node):
            return "Unavailable"
        if K8sNodeExtractor.is_node_schedulable(node):
            return "Ready"
        else:
            return "Unschedulable"
        return "Unknown"
    
    @staticmethod
    def get_node_shape(node: Any) -> K8sNodeShape:
        status = getattr(node, "status", None)
        metadata = getattr(node, "metadata", None)
        labels = getattr(metadata, "labels", {}) or {}
        allocatable = getattr(status, "allocatable", {}) or {}

        node_type = K8sNodeExtractor.get_node_type(node)
        cpu_count = max(1, int(round(UnitUtils.parse_cpu_to_core(allocatable.get("cpu")))))
        memory_size_bytes = UnitUtils.parse_mem_to_bytes(allocatable.get("memory"))
        # Round memory size to 3 sig fig to avoid overly precise number
        memory_size_bytes = int(float(f"{memory_size_bytes:.3g}"))
        architecture = labels.get("kubernetes.io/arch", "unknown")
        operating_system = labels.get("kubernetes.io/os", "unknown")  # Using OS as a proxy for instance type

        return K8sNodeShape(
            node_type=node_type,
            cpu_count=cpu_count,
            memory_size_bytes=memory_size_bytes,
            # architecture=architecture,
            # operating_system=operating_system
        )
    
    @staticmethod
    def get_available_worker_nodes(nodes: list, type_filter: str | None = None) -> list:
        available_nodes = []
        for node in nodes:
            if K8sNodeExtractor.is_worker_node(node) and K8sNodeExtractor.is_node_available(node):
                if type_filter is None or K8sNodeExtractor.get_node_type(node) == type_filter:
                    available_nodes.append(node)
        return available_nodes

    @staticmethod
    def get_in_use_worker_nodes(nodes: list, type_filter: str | None = None) -> list:
        in_use_nodes = []
        for node in nodes:
            if K8sNodeExtractor.is_worker_node_in_use(node):
                if type_filter is None or K8sNodeExtractor.get_node_type(node) == type_filter:
                    in_use_nodes.append(node)
        return in_use_nodes
