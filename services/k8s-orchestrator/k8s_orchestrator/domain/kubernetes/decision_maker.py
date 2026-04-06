import logging
from datetime import timedelta
from typing import Optional, Callable

from odt_common.models import Decision, EvaluatedProposal, ProposalOutcome
from k8s_orchestrator.domain import DecisionMaker, SystemSnapshot
from k8s_orchestrator.domain.kubernetes import K8sActionKind

logger = logging.getLogger(__name__)


class K8sDecisionMaker(DecisionMaker):
    def __init__(self) -> None:
        super().__init__()
        self.objective_weights = {
            "runtime": 1,
            "utilization": 0
        }
        self.metric_directions = {
            "runtime": "min",
            "utilization": "max"
        }
        self.metric_extractors: dict[str, Callable[[ProposalOutcome], Optional[float]]] = {
            "runtime": lambda outcome: outcome.result.runtime.total_seconds() if outcome.result.runtime else None,
            "utilization": lambda outcome: outcome.result.utilization
        }

    def _get_score_bounds(self, proposals: list[EvaluatedProposal]) -> dict[str, tuple[Optional[float], Optional[float]]]:
        bounds = {}
        for metric, extractor in self.metric_extractors.items():
            values = []
            for proposal in proposals:
                value = extractor(proposal.outcome)
                if value is not None:
                    values.append(value)
            if values:
                bounds[metric] = (min(values), max(values))
            else:
                bounds[metric] = (None, None)
        return bounds
    
    def _normalize_score(self, value: Optional[float], metric: str, bounds: dict[str, tuple[Optional[float], Optional[float]]]) -> Optional[float]:
        if value is None:
            return None
        min_val, max_val = bounds[metric]
        if min_val is None or max_val is None or min_val == max_val:
            return 0.5  # Neutral score if we can't determine bounds
        if self.metric_directions[metric] == "min":
            return (max_val - value) / (max_val - min_val)
        else:  # "max"
            return (value - min_val) / (max_val - min_val)

    def _score_proposals(self, proposals:list[EvaluatedProposal]) -> list[tuple[EvaluatedProposal, float]]:
        bounds = self._get_score_bounds(proposals)
        scored_proposals = []
        for proposal in proposals:
            total_score = 0.0
            for metric, weight in self.objective_weights.items():
                value = self.metric_extractors[metric](proposal.outcome)
                normalized = self._normalize_score(value, metric, bounds)
                if normalized is not None:
                    total_score += weight * normalized
            scored_proposals.append((proposal, total_score))
        return scored_proposals

    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        logger.info(f"Evaluating {len(proposals)} proposals against current snapshot")

        scored_proposals = self._score_proposals(proposals)
        scored_proposals.sort(key=lambda x: x[1], reverse=True)
        
        best_proposal, best_score = scored_proposals[0]
        accepted_proposal = best_proposal.proposal
        decision = best_proposal.proposal.decision

        logger.info(f"Accepted proposal {accepted_proposal.proposal_id} based on state ID {accepted_proposal.based_on_state_id} with action {decision.action}")
        return decision
