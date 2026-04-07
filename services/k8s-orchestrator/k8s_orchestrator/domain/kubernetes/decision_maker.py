import logging
from datetime import timedelta
from typing import Optional, Callable

from odt_common.models import Decision, EvaluatedProposal, ProposalOutcome, DecisionPolicy, ObjectiveSpec, MetricDirection
from k8s_orchestrator.domain import DecisionMaker, SystemSnapshot

logger = logging.getLogger(__name__)


class K8sDecisionMaker(DecisionMaker):
    def __init__(self, policy: DecisionPolicy) -> None:
        super().__init__(policy)

    def _enabled_objectives(self) -> list[ObjectiveSpec]:
        return [obj for obj in self.policy.objectives.values() if obj.weight > 0]

    def _get_score_bounds(self, proposals: list[EvaluatedProposal]) -> dict[str, tuple[Optional[float], Optional[float]]]:
        bounds = {}
        for objective in self._enabled_objectives():
            values = [proposals.outcome.result.get_metric(objective.name) for proposals in proposals]
            values = [v for v in values if v is not None]
            bounds[objective.name] = (min(values), max(values)) if values else (None, None)
        return bounds
    
    def _normalize(self, value: float | None, objective: ObjectiveSpec, bounds: dict[str, tuple[Optional[float], Optional[float]]]) -> Optional[float]:
        if value is None:
            return None
        
        min_val, max_val = bounds[objective.name]
        
        # Handle edge cases where bounds are not defined or equal
        if min_val is None or max_val is None or min_val == max_val:
            return 0.5

        if objective.direction == MetricDirection.MIN:
            return (max_val - value) / (max_val - min_val)
        return (value - min_val) / (max_val - min_val)
        
    def _score_proposals(self, proposals: list[EvaluatedProposal]) -> list[tuple[EvaluatedProposal, float]]:
        objectives = self._enabled_objectives()
        bounds = self._get_score_bounds(proposals)
        scored = []

        for proposal in proposals:
            weighted_sum = 0.0
            used_weight = 0.0

            for obj in objectives:
                value = proposal.outcome.result.get_metric(obj.name)
                normalized = self._normalize(value, obj, bounds)
                if normalized is None:
                    continue

                weighted_sum += obj.weight * normalized
                used_weight += obj.weight

            score = weighted_sum / used_weight if used_weight > 0 else float("-inf")
            scored.append((proposal, score))

        return scored

    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        logger.info(f"Evaluating {len(proposals)} proposals against current snapshot")

        scored_proposals = self._score_proposals(proposals)
        scored_proposals.sort(key=lambda x: x[1], reverse=True)
        
        best_proposal, best_score = scored_proposals[0]
        accepted_proposal = best_proposal.proposal
        decision = best_proposal.proposal.decision

        logger.info(f"Accepted proposal {accepted_proposal.proposal_id} based on state ID {accepted_proposal.based_on_state_id} with action {decision.action}")
        return decision
