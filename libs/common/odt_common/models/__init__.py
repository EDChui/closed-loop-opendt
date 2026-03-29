"""Shared Pydantic models for ODT."""

from odt_common.models.consumption import Consumption
from odt_common.models.fragment import Fragment
from odt_common.models.task import Task
from odt_common.models.topology import (
    CPU,
    Cluster,
    CPUPowerModel,
    Host,
    Memory,
    Topology,
    TopologySnapshot,
)
from odt_common.models.workload_message import WorkloadMessage
from odt_common.models.decision import Decision
from odt_common.models.proposal import Proposal
from odt_common.models.evaluated_proposal import EvaluatedProposal
from odt_common.models.simulation import SimulationResult, ProposalOutcome, SimulationBatch, SimulationBatchReport

# Update forward references for Task.fragments
Task.model_rebuild()

__all__ = [
    "Task",
    "Fragment",
    "Consumption",
    "Topology",
    "TopologySnapshot",
    "Cluster",
    "Host",
    "CPU",
    "Memory",
    "CPUPowerModel",
    "WorkloadMessage",
    "Decision",
    "Proposal",
    "EvaluatedProposal",
    "SimulationResult",
    "ProposalOutcome",
    "SimulationBatch",
    "SimulationBatchReport",
]
