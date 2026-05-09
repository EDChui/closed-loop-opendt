import copy
import logging
from typing import Optional


from odt_common.models import Decision, Proposal, SimulationBatch
from k8s_orchestrator.domain import ProposalGenerator, ObservedState
from k8s_orchestrator.domain.kubernetes import K8sActionKind, K8sSystemSnapshot

logger = logging.getLogger(__name__)


class K8sProposalGenerator(ProposalGenerator):
    def _get_node_type_current_count(self, snapshot: K8sSystemSnapshot, node_type: str) -> int:
        """Helper method to get the current count of a specific node type from the snapshot."""
        # Assuming the topology names the hosts according to the node type (e.g., "cloud", "edge", "endpoint")
        for host in snapshot.topology.clusters[0].hosts:
            if host.name == node_type:
                return host.count
        return 0

    def _get_requested_node_count(self, snapshot: K8sSystemSnapshot, relative_node_count_change: dict[str, int]) -> dict[str, int]:
        """
        Calculates the requested node count based on the current snapshot and the relative change.
        
        Example:
        - current snapshot has 3 cloud nodes, 2 edge nodes, and 1 endpoint node
        - relative_node_count_change = {"cloud": -1, "edge": 2} means we want to remove 1 cloud node and add 2 edge nodes compared to the current snapshot.
        - returns {"cloud": 2, "edge": 4, "endpoint": 1}
        """
        requested_node_count = {}
        for node_type in K8sSystemSnapshot.node_types:
            current_count = self._get_node_type_current_count(snapshot, node_type)
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

    def generate_change_node_count_proposal(self, snapshot: K8sSystemSnapshot, state_id: str, request_node_count: dict[str, int]) -> Optional[Proposal]:
        """Generates a proposal to change the node count based on the requested node count.
        
        Example:
        - current snapshot has 3 cloud nodes, 2 edge nodes, and 1 endpoint node
        - request_node_count = {"cloud": 2, "edge": 4} means we want to change to 2 cloud nodes, 4 edge nodes, and keep the same count for endpoint nodes as it is not specified in the request.
        - generates a proposal to change the node count to {"cloud": 2, "edge": 4, "endpoint": 1}
        """

        total_requested_node_count = sum(request_node_count.values())
        # Ensure at least 1 node is requested to avoid generating proposals that would remove all nodes
        if total_requested_node_count <= 0:
            logger.warning(f"Received request to change node count with non-positive total count ({total_requested_node_count}). No proposal will be generated.")
            return None

        new_topology = copy.deepcopy(snapshot.topology)
        details = {"from": {}, "to": {}}
        changed = False

        for host in new_topology.clusters[0].hosts:
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

        if not changed:
            logger.info(f"Requested node count is the same as the current snapshot for state {state_id}. No proposal will be generated.")
            return None

        proposal_id = f"proposal-{state_id}-change-node-count-" + "-".join(f"{node_type[:2]}{count}" for node_type, count in request_node_count.items())
        decision = Decision(
            action=K8sActionKind.CHANGE_NODE_COUNT,
            details=details
        )
        proposal = Proposal(
            proposal_id=proposal_id,
            based_on_state_id=state_id,
            candidate_topology=new_topology,
            decision=decision
        )
        return proposal

    def generate(self, state: ObservedState[K8sSystemSnapshot]) -> SimulationBatch:
        batch_id = f"batch-{state.state_id}"
        current_snapshot = state.snapshot
        current_topology = current_snapshot.topology

        logger.info(f"Generating proposals batch '{batch_id}' based on state revision {state.revision}")

        proposals: list[Proposal] = []

        # Proposal 0 always keep the current topology
        proposal_id = f"proposal-{state.state_id}-original"
        decision = Decision(action=K8sActionKind.NO_OP)
        proposal = Proposal(
            proposal_id=proposal_id,
            based_on_state_id=state.state_id,
            candidate_topology=current_topology,
            decision=decision
        )
        proposals.append(proposal)

        # TODO: Change me for different experiment setup
        relative_changes = [
            {"cloud": 2},
            {"cloud": 1},
            {"cloud": -1},
            {"cloud": -2},
        ]
        for change in relative_changes:
            request_node_count = self._get_requested_node_count(current_snapshot, relative_node_count_change=change)
            proposal = self.generate_change_node_count_proposal(current_snapshot, state.state_id, request_node_count)
            if proposal is not None:
                proposals.append(proposal)

        absolute_changes = [
            {"cloud": 8},
            {"cloud": 6},
            {"cloud": 4},
            {"cloud": 2},
        ]
        for change in absolute_changes:
            proposal = self.generate_change_node_count_proposal(current_snapshot, state.state_id, change)
            if proposal is not None:
                proposals.append(proposal)

        simulationBatch = SimulationBatch(
            batch_id=batch_id,
            based_on_state_id=state.state_id,
            proposals=proposals
        )

        logger.info(f"Generated {len(proposals)} proposals for batch '{batch_id}' based on state revision {state.revision}")
        
        return simulationBatch
