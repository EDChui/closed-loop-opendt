from dataclasses import dataclass
from enum import Enum


class MetricDirection(str, Enum):
    MIN = "min"
    MAX = "max"


@dataclass(frozen=True)
class ObjectiveSpec:
    name: str
    weight: float
    direction: MetricDirection


@dataclass(frozen=True)
class DecisionPolicy:
    objectives: dict[str, ObjectiveSpec]
