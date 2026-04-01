import logging
from typing import Optional

from odt_common.models import Decision, EvaluatedProposal
from k8s_orchestrator.domain import DecisionPolicy, SystemSnapshot
from k8s_orchestrator.domain.kubernetes import K8sActionKind

logger = logging.getLogger(__name__)


class K8sDecisionPolicy(DecisionPolicy):
    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        logger.info(f"Evaluating {len(proposals)} proposals against current snapshot")
        # TODO: Complete me

        accepted_proposal = proposals[0].proposal
        decision = accepted_proposal.decision

        logger.info(f"Accepted proposal {accepted_proposal.proposal_id} based on state ID {accepted_proposal.based_on_state_id} with action {decision.action}")
        return decision
