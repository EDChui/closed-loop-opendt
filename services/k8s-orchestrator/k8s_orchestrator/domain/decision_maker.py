from typing import Optional, Protocol

from odt_common.models import Decision, EvaluatedProposal
from k8s_orchestrator.domain.models import SystemSnapshot


class DecisionMaker(Protocol):
    def choose(self, proposals: list[EvaluatedProposal], current_snapshot: SystemSnapshot) -> Optional[Decision]:
        """Given the current system snapshot and a list of evaluated proposals, choose one to execute."""
        ...
