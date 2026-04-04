from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class NodePowerReading:
    node_name: str
    capture_time: datetime
    energy_uj: int
    energy_usage_j: Optional[float]
    power_draw_w: Optional[float]
