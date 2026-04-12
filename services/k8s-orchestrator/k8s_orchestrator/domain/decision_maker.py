from typing import Optional, Protocol

from odt_common.models import Decision, EvaluatedProposal, DecisionPolicy
from k8s_orchestrator.domain.models import SystemSnapshot


class DecisionMaker(Protocol):
    policy: DecisionPolicy
    
    def __init__(self, policy: DecisionPolicy) -> None:
        self.policy = policy

    def update_policy(self, policy: DecisionPolicy) -> None:
        self.policy = policy

    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        """Given the current system snapshot and a list of evaluated proposals, choose one to execute."""
        ...
