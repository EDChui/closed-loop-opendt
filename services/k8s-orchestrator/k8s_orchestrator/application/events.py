import enum
from dataclasses import dataclass, field
from typing import Literal, Union

from odt_common.models import SimulationBatchReport, DecisionPolicy


@dataclass(frozen=True)
class RefreshTick:
    reason: Literal["startup", "periodic", "post_apply", "manual"]
    scheduled_at_monotonic: float

@dataclass(frozen=True)
class ConfigChange:
    new_policy: DecisionPolicy


Event = Union[RefreshTick, SimulationBatchReport, ConfigChange]


class Priority(enum.IntEnum):
    REFRESH = 0
    CONFIG = 5
    SIMULATION = 10


@dataclass(order=True)
class QueueItem:
    priority: int
    seq: int
    event: Event = field(compare=False)
