from enum import StrEnum
from typing import Any, Generic, Optional, TypeVar
from pydantic import BaseModel, Field


ActionType = TypeVar("ActionType", bound=StrEnum)


class Decision(BaseModel, Generic[ActionType]):
    """Represents a decision with an action and optional metadata."""

    action: ActionType = Field(..., description="Selected action type")
    details: Optional[dict[str, Any]] = Field(
        default=None,
        description="Optional action-specific metadata or parameters",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "action": "scale_up",
                "details": {"target_replicas": 3},
            }
        }
