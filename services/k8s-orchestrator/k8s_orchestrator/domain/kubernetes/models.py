from enum import StrEnum
from dataclasses import dataclass

from odt_common.models.topology import Topology
from k8s_orchestrator.domain import SystemSnapshot


class K8sActionKind(StrEnum):
    NO_OP = "no_op"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"


@dataclass(frozen=True)
class K8sSystemSnapshot(SystemSnapshot):
    topology: Topology
    max_available_node_count: int
    # TODO: Scheduling policy in the future?

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, K8sSystemSnapshot):
            return NotImplemented
        return self.topology == other.topology and self.max_available_node_count == other.max_available_node_count

    def __hash__(self) -> int:
        return hash((self.topology, self.max_available_node_count))
