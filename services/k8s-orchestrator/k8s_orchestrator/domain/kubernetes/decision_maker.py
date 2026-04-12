import logging
from datetime import timedelta
from typing import Optional, Callable

from odt_common.models import (
    Decision, EvaluatedProposal, 
    MetricDirection,
    BaseObjectiveSpec, WeightedObjectiveSpec, RankedObjectiveSpec,
    WeightedDecisionPolicy, RankedDecisionPolicy,
    DecisionPolicy
)
from k8s_orchestrator.domain import DecisionMaker, SystemSnapshot

logger = logging.getLogger(__name__)


class K8sDecisionMaker(DecisionMaker):
    def __init__(self, policy: DecisionPolicy) -> None:
        super().__init__(policy)

    def _get_score_bounds(self, proposals: list[EvaluatedProposal]) -> dict[str, tuple[Optional[float], Optional[float]]]:
        """Calculate the min and max values for each objective across the given proposals."""
        bounds = {}
        for objective in self.policy.objectives.values():
            values = [proposal.outcome.result.get_metric(objective.name) for proposal in proposals]
            values = [v for v in values if v is not None]
            bounds[objective.name] = (min(values), max(values)) if values else (None, None)
        return bounds
    
    def _normalize(self, value: float | None, objective: BaseObjectiveSpec, bounds: dict[str, tuple[Optional[float], Optional[float]]]) -> Optional[float]:
        if value is None:
            return None
        
        min_val, max_val = bounds[objective.name]
        
        # Handle edge cases where bounds are not defined or equal
        if min_val is None or max_val is None or min_val == max_val:
            return 0.5

        if objective.direction == MetricDirection.MIN:
            return (max_val - value) / (max_val - min_val)
        return (value - min_val) / (max_val - min_val)
        
    def score_proposals(self, proposals: list[EvaluatedProposal]) -> list[tuple[EvaluatedProposal, float]]:
        """Score proposals based on a weighted sum of normalized objective values."""
        if not isinstance(self.policy, WeightedDecisionPolicy):
            raise ValueError("Policy must be a WeightedDecisionPolicy to score proposals")

        objectives = self.policy.get_objectives()
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
    
    def rank_proposals_lexicographically(self, proposals: list[EvaluatedProposal]) -> list[EvaluatedProposal]:
        if not isinstance(self.policy, RankedDecisionPolicy):
            raise ValueError("Policy must be a RankedDecisionPolicy to rank proposals lexicographically")

        objectives = self.policy.get_objectives()

        def metric_value(proposal: EvaluatedProposal, objective: RankedObjectiveSpec) -> Optional[float]:
            return proposal.outcome.result.get_metric(objective.name)

        def sort_key(proposal: EvaluatedProposal, objective: RankedObjectiveSpec) -> tuple[int, float]:
            value = metric_value(proposal, objective)

            # Missing values are always worse than present values
            if value is None:
                return (1, 0.0)

            if objective.direction == MetricDirection.MIN:
                return (0, value)

            return (0, -value)

        def split_into_tie_groups(items: list[EvaluatedProposal], objective: RankedObjectiveSpec) -> list[list[EvaluatedProposal]]:
            ordered = sorted(items, key=lambda p: sort_key(p, objective))
            groups: list[list[EvaluatedProposal]] = []
            i = 0

            while i < len(ordered):
                anchor = metric_value(ordered[i], objective)
                group = [ordered[i]]
                i += 1

                # Group all missing values together at the end
                if anchor is None:
                    while i < len(ordered) and metric_value(ordered[i], objective) is None:
                        group.append(ordered[i])
                        i += 1
                    groups.append(group)
                    continue

                # Values within tie_tolerance of the group's best value are tied
                while i < len(ordered):
                    current = metric_value(ordered[i], objective)
                    if current is None:
                        break
                    if abs(current - anchor) <= objective.tie_tolerance:
                        group.append(ordered[i])
                        i += 1
                    else:
                        break

                groups.append(group)

            return groups

        def rank_group(items: list[EvaluatedProposal], objective_index: int) -> list[EvaluatedProposal]:
            if len(items) <= 1 or objective_index >= len(objectives):
                return items

            objective = objectives[objective_index]
            tie_groups = split_into_tie_groups(items, objective)

            ranked: list[EvaluatedProposal] = []
            for group in tie_groups:
                ranked.extend(rank_group(group, objective_index + 1))

            return ranked

        return rank_group(proposals, 0)


    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        logger.info(f"Evaluating {len(proposals)} proposals against current snapshot")

        if not proposals:
            return None
        
        if isinstance(self.policy, WeightedDecisionPolicy):
            scored_proposals = self.score_proposals(proposals)
            scored_proposals.sort(key=lambda x: x[1], reverse=True)
            best_proposal, _ = scored_proposals[0]
        elif isinstance(self.policy, RankedDecisionPolicy):
            ranked_proposals = self.rank_proposals_lexicographically(proposals)
            best_proposal = ranked_proposals[0]
        else:
            logger.error(f"Unsupported policy type: {type(self.policy)}")
            return None
        
        accepted_proposal = best_proposal.proposal
        decision = best_proposal.proposal.decision

        logger.info(f"Accepted proposal {accepted_proposal.proposal_id} based on state ID {accepted_proposal.based_on_state_id} with action {decision.action}")
        return decision
