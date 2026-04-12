from dataclasses import dataclass
from pathlib import Path

from odt_common.models import Proposal


@dataclass
class ProposalExecutionPlan:
    index: int
    proposal: Proposal
    proposal_dir: Path


@dataclass
class ProposalExecutionResult:
    index: int
    proposal_id: str
    proposal_dir: Path
    output_dir: Path | None
    success: bool
    cached: bool
    error_message: str | None = None