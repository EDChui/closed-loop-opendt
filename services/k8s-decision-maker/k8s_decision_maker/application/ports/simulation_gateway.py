from typing import AsyncIterator, Protocol

from odt_common.models import SimulationBatch, SimulationBatchReport


class SimulationGateway(Protocol):
    async def submit_batch(self, batch: SimulationBatch) -> None: ...
    def __aiter__(self) -> AsyncIterator[SimulationBatchReport]: ...
