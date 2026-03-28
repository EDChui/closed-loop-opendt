from typing import Protocol

from k8s_decision_maker.domain import SystemSnapshot, Decision


class SystemPort(Protocol):
    async def fetch_status(self) -> SystemSnapshot: ...
    async def apply_decision(self, decision: Decision) -> None: ...
