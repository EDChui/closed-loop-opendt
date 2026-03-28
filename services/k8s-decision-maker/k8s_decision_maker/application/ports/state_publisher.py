from typing import Protocol

from k8s_decision_maker.domain import ObservedState, SnapshotType


class StatePublisher(Protocol[SnapshotType]):
    async def publish_system_state(self, state: ObservedState[SnapshotType], cause: str) -> None: ...
