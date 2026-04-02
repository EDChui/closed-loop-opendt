from enum import StrEnum
from typing import Any, Optional
from pydantic import BaseModel, Field


class Decision(BaseModel):
    """Represents a decision with an action and optional metadata."""

    action: str | StrEnum = Field(..., description="Selected action type")
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
