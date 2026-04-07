from pydantic import BaseModel, Field

from .proposal import Proposal
from .simulation import ProposalOutcome

class EvaluatedProposal(BaseModel):
    """A proposal paired with its simulation outcome."""

    proposal: Proposal = Field(..., description="Original simulation proposal")
    outcome: ProposalOutcome = Field(..., description="Simulation outcome for the proposal")
