from datetime import timedelta
from typing import Optional
from pydantic import BaseModel, Field

from .proposal import Proposal

class SimulationMetric(BaseModel):
    value: float = Field(..., description="Numeric value of the metric")
    unit: str | None = Field(None, description="Unit of the metric value, if applicable")


class SimulationResult(BaseModel):
    metrics: dict[str, SimulationMetric] = Field(default_factory=dict, description="Dictionary of simulation metrics by name")

    def get_metric(self, name: str) -> float | None:
        metric = self.metrics.get(name)
        return None if metric is None else metric.value
    
    def __str__(self) -> str:
        metric_strs = [f"{name}={metric.value}{f' {metric.unit}' if metric.unit else ''}" for name, metric in self.metrics.items()]
        return ", ".join(metric_strs) if metric_strs else "No metrics"


class ProposalOutcome(BaseModel):
    """Outcome of simulating a single proposal."""

    proposal_id: str = Field(..., description="Identifier of the evaluated proposal")
    result: SimulationResult = Field(..., description="Simulation result for this proposal")


class SimulationBatch(BaseModel):
    """A batch of decision proposals to simulate together."""

    batch_id: str = Field(..., description="Unique simulation batch identifier")
    based_on_state_id: str = Field(..., description="State identifier all proposals are based on")
    proposals: list[Proposal] = Field(
        default_factory=list,
        description="List of decision proposals included in this batch",
    )


class SimulationBatchReport(BaseModel):
    """Report containing the outcomes of a simulated batch."""

    batch_id: str = Field(..., description="Unique simulation batch identifier")
    based_on_state_id: str = Field(..., description="State identifier the batch was based on")
    created_at: float = Field(..., description="Batch creation timestamp as Unix epoch seconds", ge=0)
    outcomes: list[ProposalOutcome] = Field(
        default_factory=list,
        description="List of outcomes for proposals in the batch",
    )
