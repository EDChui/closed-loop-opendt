import logging


from odt_common.models import Decision, Proposal, SimulationBatch
from k8s_decision_maker.domain import ProposalGenerator, ObservedState
from k8s_decision_maker.domain.kubernetes import K8sActionKind, K8sSystemSnapshot

logger = logging.getLogger(__name__)


class K8sProposalGenerator(ProposalGenerator):
    def generate(self, state: ObservedState[K8sSystemSnapshot]) -> SimulationBatch:
        batch_id = f"batch-{state.state_id}"
        current_topology = state.snapshot.topology

        logger.info(f"Generating proposals batch '{batch_id}' based on state revision {state.revision}")

        proposals = []

        # Proposal 0 always keep the current topology
        proposal_id = f"proposal-{state.state_id}-0"
        decision = Decision(action=K8sActionKind.NO_OP)
        proposal = Proposal(
            proposal_id=proposal_id,
            based_on_state_id=state.state_id,
            candidate_config=current_topology,
            decision=decision
        )
        proposals.append(proposal)

        # TODO: Actual meaningful proposals
        for i in range(1, 4):
            proposal_id = f"proposal-{state.state_id}-{i}"
            new_topology = current_topology.model_copy()
            new_topology.clusters[0].hosts[0].count = max(1, i)
            decision = Decision(action=K8sActionKind.NO_OP)

            proposal = Proposal(
                proposal_id=proposal_id,
                based_on_state_id=state.state_id,
                candidate_config=new_topology,
                decision=decision
            )
            proposals.append(proposal)
        
        simulationBatch = SimulationBatch(
            batch_id=batch_id,
            based_on_state_id=state.state_id,
            proposals=proposals
        )

        logger.info(f"Generated {len(proposals)} proposals for batch '{batch_id}' based on state revision {state.revision}")
        
        return simulationBatch
