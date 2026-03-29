from pydantic import BaseModel, Field

from .proposal import Proposal
from .simulation import ProposalOutcome

class EvaluatedProposal(BaseModel):
    """A proposal paired with its simulation outcome."""

    proposal: Proposal = Field(..., description="Original decision proposal")
    outcome: ProposalOutcome = Field(..., description="Simulation outcome for the proposal")

    class Config:
        json_schema_extra = {
            "example": {
                "proposal": {
                    "proposal_id": "proposal-001",
                    "based_on_state_id": "state-123",
                    "candidate_config": {"cpu": 4, "memory": 8192},
                    "decision": {
                        "action": "scale_up",
                        "details": {"target_replicas": 3},
                    },
                },
                "outcome": {
                    "proposal_id": "proposal-001",
                    "result": {},
                },
            }
        }
