from datetime import timedelta
from typing import Optional
from pydantic import BaseModel, Field

from .proposal import Proposal

class SimulationResult(BaseModel):
    runtime: Optional[timedelta] = Field(
        None, description="Total runtime of the simulation"
    )
    utilization: Optional[float] = Field(
        None, description="Average CPU utilization during the simulation"
    )

class ProposalOutcome(BaseModel):
    """Outcome of simulating a single proposal."""

    proposal_id: str = Field(..., description="Identifier of the evaluated proposal")
    result: SimulationResult = Field(..., description="Simulation result for this proposal")

    class Config:
        json_schema_extra = {
            "example": {
                "proposal_id": "proposal-001",
                "result": {},
            }
        }


class SimulationBatch(BaseModel):
    """A batch of decision proposals to simulate together."""

    batch_id: str = Field(..., description="Unique simulation batch identifier")
    based_on_state_id: str = Field(..., description="State identifier all proposals are based on")
    proposals: list[Proposal] = Field(
        default_factory=list,
        description="List of decision proposals included in this batch",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "batch-001",
                "based_on_state_id": "state-123",
                "proposals": [],
            }
        }


class SimulationBatchReport(BaseModel):
    """Report containing the outcomes of a simulated batch."""

    batch_id: str = Field(..., description="Unique simulation batch identifier")
    based_on_state_id: str = Field(..., description="State identifier the batch was based on")
    created_at: float = Field(..., description="Batch creation timestamp as Unix epoch seconds", ge=0)
    received_at: float = Field(..., description="Batch report receipt timestamp as Unix epoch seconds", ge=0)
    outcomes: list[ProposalOutcome] = Field(
        default_factory=list,
        description="List of outcomes for proposals in the batch",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "batch_id": "batch-001",
                "based_on_state_id": "state-123",
                "created_at": 1710000000.0,
                "received_at": 1710000001.5,
                "outcomes": [],
            }
        }
