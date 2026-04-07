from enum import Enum
from pydantic import BaseModel, Field


class MetricDirection(str, Enum):
    MIN = "min"
    MAX = "max"


class ObjectiveSpec(BaseModel):
    name: str = Field(..., description="Name of the objective metric")
    weight: float = Field(..., description="Relative importance weight for this objective", ge=0)
    direction: MetricDirection = Field(..., description="Whether to minimize or maximize this metric")


class DecisionPolicy(BaseModel):
    objectives: dict[str, ObjectiveSpec] = Field(..., description="Mapping of objective names to their specifications")
