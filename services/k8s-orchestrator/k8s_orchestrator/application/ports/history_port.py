from typing import Protocol

from odt_common.models import Decision
from k8s_orchestrator.domain import ObservedState


class HistoryPort(Protocol):
    async def record_observed_state(self, observed_state: ObservedState, cause: str) -> None: ...
    async def record_applied_decision(
        self,
        state_id: str,
        decision: Decision,
        success: bool,
        error_message: str = "",
        source: str = ""
    ) -> None: ...
