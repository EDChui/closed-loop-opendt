from typing import Protocol

from odt_common.models import SimulationBatch
from k8s_decision_maker.domain.models import ObservedState


class ProposalGenerator(Protocol):
    def generate(self, state: ObservedState) -> SimulationBatch:
        """Given the current observed state, generate a batch of decision proposals to be evaluated."""
        ...
