from typing import AsyncIterator, Protocol

from k8s_decision_maker.domain import SimulationBatch, SimulationBatchReport


class SimulationGateway(Protocol):
    async def submit_batch(self, batch: SimulationBatch) -> None: ...
    def __aiter__(self) -> AsyncIterator[SimulationBatchReport]: ...
