from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

ENERGY_FILE_SUFFIX = Path("intel-rapl:0/energy_uj")


@dataclass(frozen=True)
class NodeEnergyCounter:
    node_name: str
    path: Path
    capture_time: datetime
    energy_uj: int


class ScaphandreEnergyReader:
    def __init__(self, root_path: str="/var/lib/libvirt/scaphandre/"):
        self.root_path = Path(root_path)

    def energy_path_for_node(self, node_name: str) -> Path:
        return self.root_path / node_name / ENERGY_FILE_SUFFIX
    
    def read_energy_counter(self, node_name: str, capture_time: datetime) -> NodeEnergyCounter:
        energy_path = self.energy_path_for_node(node_name)
        try:
            with energy_path.open("r") as f:
                energy_uj = int(f.read().strip())
            return NodeEnergyCounter(
                node_name=node_name, 
                path=energy_path,
                capture_time=capture_time,
                energy_uj=energy_uj
            )
        except Exception as e:
            logger.error(f"Failed to read energy counter for node {node_name} at path {energy_path}: {e}")
            raise
