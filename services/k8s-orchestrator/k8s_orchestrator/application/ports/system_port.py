from typing import Protocol

from odt_common.models import Decision
from k8s_orchestrator.domain import SystemSnapshot


class SystemPort(Protocol):
    async def fetch_status(self) -> SystemSnapshot: ...
    async def apply_decision(self, decision: Decision) -> None: ...
