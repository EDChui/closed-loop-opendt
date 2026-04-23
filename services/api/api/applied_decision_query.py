"""Applied decision history query module for dashboard API."""

import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field


logger = logging.getLogger(__name__)


class AppliedDecisionResponse(BaseModel):
    """Response model for applied decision history."""

    data: list[dict[str, Any]]
    metadata: dict[str, Any] = Field(default_factory=dict, description="Metadata about the query")


class AppliedDecisionQuery:
    """Query applied decisions over time from orchestrator history."""

    def __init__(self, run_id: str):
        self.run_id = run_id

        data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
        self.run_dir = data_dir / run_id
        self.history_path = self.run_dir / "history" / "applied_decisions.jsonl"

        logger.info(f"Initialized AppliedDecisionQuery for run {run_id}")
        logger.info(f"Applied decisions history: {self.history_path}")

    def _flatten_details(self, value: Any, prefix: str = "") -> dict[str, Any]:
        """Flatten nested decision details into Grafana-friendly scalar columns."""
        if value is None:
            return {}

        flattened: dict[str, Any] = {}
        if isinstance(value, dict):
            for key, nested_value in value.items():
                next_prefix = f"{prefix}_{key}" if prefix else str(key)
                flattened.update(self._flatten_details(nested_value, next_prefix))
            return flattened

        if isinstance(value, (list, tuple)):
            flattened[prefix] = json.dumps(value)
            return flattened

        flattened[prefix] = value
        return flattened

    def query(
        self,
        start_time: datetime | None = None,
        include_failed: bool = False,
        include_no_op: bool = True,
    ) -> AppliedDecisionResponse:
        if not self.history_path.exists():
            raise FileNotFoundError(f"Applied decisions history not found: {self.history_path}")

        start_time_utc = pd.to_datetime(start_time, utc=True) if start_time else None
        rows: list[dict[str, Any]] = []
        action_names: set[str] = set()
        detail_field_names: set[str] = set()

        with self.history_path.open("r", encoding="utf-8") as file_handle:
            for line in file_handle:
                if not line.strip():
                    continue

                payload = json.loads(line)
                timestamp = pd.to_datetime(payload["recorded_at"], utc=True)
                if start_time_utc is not None and timestamp < start_time_utc:
                    continue

                success = bool(payload.get("success", False))
                if not include_failed and not success:
                    continue

                decision_payload = payload.get("decision") or {}
                action = str(decision_payload.get("action") or "unknown")
                if not include_no_op and action == "no_op":
                    continue

                details = decision_payload.get("details")
                flattened_details = self._flatten_details(details)

                row = {
                    "timestamp": timestamp.to_pydatetime(),
                    "state_id": payload.get("state_id"),
                    "success": success,
                    "error_message": payload.get("error_message") or None,
                    "action": action,
                    action: 1,
                    **flattened_details,
                }
                rows.append(row)
                action_names.add(action)
                detail_field_names.update(flattened_details.keys())

        rows.sort(key=lambda row: row["timestamp"])

        for row in rows:
            for action_name in action_names:
                row.setdefault(action_name, None)
            for field_name in detail_field_names:
                row.setdefault(field_name, None)

        metadata = {
            "run_id": self.run_id,
            "count": len(rows),
            "action_names": sorted(action_names),
            "detail_fields": sorted(detail_field_names),
            "include_failed": include_failed,
            "include_no_op": include_no_op,
            "start_time": rows[0]["timestamp"].isoformat() if rows else None,
            "end_time": rows[-1]["timestamp"].isoformat() if rows else None,
            "history_path": str(self.history_path),
        }

        return AppliedDecisionResponse(data=rows, metadata=metadata)
