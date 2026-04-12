from pydantic import BaseModel, Field

from .topology import Topology
from .decision import Decision


class Proposal(BaseModel):
    """A proposal for a configuration change, based on a decision."""

    proposal_id: str = Field(..., description="Unique proposal identifier")
    based_on_state_id: str = Field(..., description="State identifier this proposal is based on")
    candidate_topology: Topology = Field(..., description="Candidate topology evaluated by this proposal")
    decision: Decision = Field(..., description="Decision associated with this proposal")
