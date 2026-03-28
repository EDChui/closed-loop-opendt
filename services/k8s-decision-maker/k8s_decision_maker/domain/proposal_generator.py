from typing import Protocol

from k8s_decision_maker.domain.models import ObservedState, SimulationBatch


class ProposalGenerator(Protocol):
    def generate(self, state: ObservedState) -> SimulationBatch:
        """Given the current observed state, generate a batch of decision proposals to be evaluated."""
        ...