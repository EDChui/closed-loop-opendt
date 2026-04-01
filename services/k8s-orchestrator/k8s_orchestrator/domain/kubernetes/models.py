from enum import StrEnum
from dataclasses import dataclass

from odt_common.models.topology import Topology
from k8s_orchestrator.domain import SystemSnapshot


class K8sActionKind(StrEnum):
    NO_OP = "no_op"
    # TODO: Extend me


@dataclass(frozen=True)
class K8sSystemSnapshot(SystemSnapshot):
    topology: Topology
    # TODO: Scheduling policy in the future

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, K8sSystemSnapshot):
            return NotImplemented
        return self.topology == other.topology

    def __hash__(self) -> int:
        return hash(self.topology)
