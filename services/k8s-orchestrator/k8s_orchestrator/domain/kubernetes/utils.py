import copy
from typing import Tuple, Optional

from odt_common.models import Decision, Topology
from k8s_orchestrator.domain.kubernetes import K8sActionKind, K8sSystemSnapshot

class Utils:
    @staticmethod
    def _get_node_type_current_count(snapshot: K8sSystemSnapshot, node_type: str) -> int:
        """Helper method to get the current count of a specific node type from the snapshot."""
        # Assuming the topology names the hosts according to the node type (e.g., "cloud", "edge", "endpoint")
        for host in snapshot.topology.clusters[0].hosts:
            if host.name == node_type:
                return host.count
        return 0

    @staticmethod
    def get_requested_node_count(snapshot: K8sSystemSnapshot, relative_node_count_change: dict[str, int]) -> dict[str, int]:
        """
        Calculates the requested node count based on the current snapshot and the relative change.
        
        Example:
        - current snapshot has 3 cloud nodes, 2 edge nodes, and 1 endpoint node
        - relative_node_count_change = {"cloud": -1, "edge": 2} means we want to remove 1 cloud node and add 2 edge nodes compared to the current snapshot.
        - returns {"cloud": 2, "edge": 4, "endpoint": 1}
        """
        requested_node_count = {}
        for node_type in K8sSystemSnapshot.node_types:
            current_count = Utils._get_node_type_current_count(snapshot, node_type)
            change = relative_node_count_change.get(node_type, 0)
            if change == 0:
                requested_node_count[node_type] = current_count
                continue
            requested_count = current_count + change
            # Ensure the requested count does not exceed the maximum available node count in the snapshot and is not negative
            requested_count = min(snapshot.max_available_node_count.get(node_type, 0), requested_count)
            requested_count = max(0, requested_count)
            requested_node_count[node_type] = requested_count
        return requested_node_count
    
    @staticmethod
    def build_change_node_count_decision(
        topology: Topology,
        snapshot: K8sSystemSnapshot,
        request_node_count: dict[str, int]
    ) -> Tuple[Decision, Topology, bool]:
        topology = copy.deepcopy(topology)
        details = {"from": {}, "to": {}}
        changed = False

        for host in topology.clusters[0].hosts:
            node_type = host.name
            current_count = host.count
            requested_count = request_node_count.get(node_type, current_count)
            
            # Ensure the requested count does not exceed the maximum available node count in the snapshot and is not negative
            requested_count = min(snapshot.max_available_node_count.get(node_type, 0), requested_count)
            requested_count = max(0, requested_count)

            host.count = requested_count
            details["from"][node_type] = current_count
            details["to"][node_type] = requested_count

            if requested_count != current_count:
                changed = True
        
        decision = Decision(
            action=K8sActionKind.CHANGE_NODE_COUNT,
            details=details
        )
        return decision, topology, changed