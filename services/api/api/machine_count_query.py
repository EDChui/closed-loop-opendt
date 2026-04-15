"""Machine count history query module for dashboard API."""

import json
import logging
import os
from datetime import datetime
from typing import Any
from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class MachineCountResponse(BaseModel):
    """Response model for machine count history."""

    data: list[dict[str, Any]]
    metadata: dict[str, Any] = Field(default_factory=dict, description="Metadata about the query")


class MachineCountQuery:
    """Query working machine counts over time from observed state snapshots."""

    def __init__(self, run_id: str):
        self.run_id = run_id

        data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
        self.run_dir = data_dir / run_id
        self.history_path = self.run_dir / "history" / "observed_states.jsonl"

        logger.info(f"Initialized MachineCountQuery for run {run_id}")
        logger.info(f"Observed states history: {self.history_path}")

    def query(self, start_time: datetime | None = None) -> MachineCountResponse:
        if not self.history_path.exists():
            raise FileNotFoundError(f"Observed states history not found: {self.history_path}")

        start_time_utc = pd.to_datetime(start_time, utc=True) if start_time else None
        rows: list[dict[str, Any]] = []
        series_names: set[str] = set()

        with self.history_path.open(encoding="utf-8") as file_handle:
            for line in file_handle:
                if not line.strip():
                    continue

                payload = json.loads(line)
                timestamp = pd.to_datetime(payload["recorded_at"], utc=True)
                if start_time_utc is not None and timestamp < start_time_utc:
                    continue

                host_counts = self._extract_host_counts(payload.get("topology", {}))
                row = {
                    "timestamp": timestamp.to_pydatetime(),
                    "total_machine_count": sum(host_counts.values()),
                }
                row.update(host_counts)
                rows.append(row)
                series_names.update(host_counts.keys())

        rows.sort(key=lambda row: row["timestamp"])

        for row in rows:
            for series_name in series_names:
                row.setdefault(series_name, 0)

        metadata = {
            "run_id": self.run_id,
            "count": len(rows),
            "series_names": ["total_machine_count", *sorted(series_names)],
            "start_time": rows[0]["timestamp"].isoformat() if rows else None,
            "end_time": rows[-1]["timestamp"].isoformat() if rows else None,
            "history_path": str(self.history_path),
        }

        return MachineCountResponse(data=rows, metadata=metadata)

    def _extract_host_counts(self, topology_payload: dict[str, Any]) -> dict[str, int]:
        host_counts: dict[str, int] = {}

        for cluster in topology_payload.get("clusters", []):
            cluster_name = cluster.get("name") or "cluster"
            for host in cluster.get("hosts", []):
                host_name = host.get("name") or "host"
                series_name = f"{cluster_name}/{host_name}"
                host_counts[series_name] = host_counts.get(series_name, 0) + int(
                    host.get("count", 0)
                )

        return host_counts
