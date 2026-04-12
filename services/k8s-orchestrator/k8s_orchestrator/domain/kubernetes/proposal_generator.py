import copy
import logging
from typing import Optional


from odt_common.models import Decision, Proposal, SimulationBatch
from k8s_orchestrator.domain import ProposalGenerator, ObservedState
from k8s_orchestrator.domain.kubernetes import K8sActionKind, K8sSystemSnapshot

logger = logging.getLogger(__name__)


class K8sProposalGenerator(ProposalGenerator):
    def generate_scale_up_proposal(self, snapshot: K8sSystemSnapshot, state_id: str) -> Optional[Proposal]:
        new_topology = copy.deepcopy(snapshot.topology)
        current_node_count = new_topology.clusters[0].hosts[0].count
        target_node_count = min(snapshot.max_available_node_count, current_node_count + 1)
        if target_node_count == current_node_count:
            return None
        proposal_id = f"proposal-{state_id}-scale-up"
        new_topology.clusters[0].hosts[0].count = target_node_count
        decision = Decision(
            action=K8sActionKind.SCALE_UP,
            details={"target_node_count": target_node_count}
        )
        proposal = Proposal(
            proposal_id=proposal_id,
            based_on_state_id=state_id,
            candidate_topology=new_topology,
            decision=decision
        )
        return proposal

    def generate_scale_down_proposal(self, snapshot: K8sSystemSnapshot, state_id: str) -> Optional[Proposal]:
        new_topology = copy.deepcopy(snapshot.topology)
        current_node_count = new_topology.clusters[0].hosts[0].count
        target_node_count = max(1, current_node_count - 1)
        if target_node_count == current_node_count:
            return None
        proposal_id = f"proposal-{state_id}-scale-down"
        new_topology.clusters[0].hosts[0].count = target_node_count
        decision = Decision(
            action=K8sActionKind.SCALE_DOWN,
            details={"target_node_count": target_node_count}
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

        # Scale up proposals
        scale_up_proposal = self.generate_scale_up_proposal(current_snapshot, state.state_id)
        if scale_up_proposal is not None:
            proposals.append(scale_up_proposal)

        # Scale down proposals
        scale_down_proposal = self.generate_scale_down_proposal(current_snapshot, state.state_id)
        if scale_down_proposal is not None:
            proposals.append(scale_down_proposal)
        
        simulationBatch = SimulationBatch(
            batch_id=batch_id,
            based_on_state_id=state.state_id,
            proposals=proposals
        )

        logger.info(f"Generated {len(proposals)} proposals for batch '{batch_id}' based on state revision {state.revision}")
        
        return simulationBatch
