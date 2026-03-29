from enum import StrEnum
from typing import Any, Generic, Optional, TypeVar
from pydantic import BaseModel, Field


from .decision import Decision


class Proposal(BaseModel):
    """A proposal for a configuration change, based on a decision."""

    proposal_id: str = Field(..., description="Unique proposal identifier")
    based_on_state_id: str = Field(..., description="State identifier this proposal is based on")
    candidate_config: Any = Field(..., description="Candidate configuration evaluated by this proposal")
    decision: Decision[Any] = Field(..., description="Decision associated with this proposal")

    class Config:
        json_schema_extra = {
            "example": {
                "proposal_id": "proposal-001",
                "based_on_state_id": "state-123",
                "candidate_config": {"cpu": 4, "memory": 8192},
                "decision": {
                    "action": "scale_up",
                    "details": {"target_replicas": 3},
                },
            }
        }
