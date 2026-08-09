from typing import Protocol

from odt_common.models import Decision
from k8s_orchestrator.domain import SystemSnapshot


class SystemPort(Protocol):
    async def fetch_status(self) -> SystemSnapshot: ...
    async def fetch_backlog_count(self) -> int:
        """Fetch the number of pending actions in the system that are waiting to be applied."""
        # TODO: Make this more abstract and robust in the future
        # Currently it returns the number of pending pods in the implementation
        ...
    async def apply_decision(self, decision: Decision) -> None: ...
