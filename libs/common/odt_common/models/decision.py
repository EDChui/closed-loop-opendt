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
