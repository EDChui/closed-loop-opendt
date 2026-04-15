"""CPU utilization query and alignment module for dashboard API."""

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.engine import Engine


logger = logging.getLogger(__name__)

EXCLUDED_CONTROL_NODE = "cloudcontrollerechui"


class UtilizationDataPoint(BaseModel):
    """Single CPU utilization data point with simulated and actual values."""

    timestamp: datetime = Field(..., description="Timestamp (ISO 8601 format)")
    simulated_cpu_utilization: float = Field(..., description="Simulated CPU utilization rate as a value between 0 and 1")
    actual_cpu_utilization: float = Field(..., description="Actual CPU utilization rate as a value between 0 and 1")


class UtilizationResponse(BaseModel):
    """Response model for CPU utilization query."""

    data: list[UtilizationDataPoint]
    metadata: dict[str, Any] = Field(default_factory=dict, description="Metadata about the query")


class UtilizationQuery:
    """Query and align CPU utilization data from simulation and observations."""

    def __init__(self, run_id: str, db_engine: Engine):
        """Initialize CPU utilization query."""
        self.run_id = run_id
        self.db_engine = db_engine

        data_dir = Path(os.getenv("DATA_DIR", "/app/data"))
        self.run_dir = data_dir / run_id

        self.sim_results_path = self.run_dir / "simulator" / "agg_results.parquet"

        logger.info(f"Initialized UtilizationQuery for run {run_id}")
        logger.info(f"Simulation results: {self.sim_results_path}")

    def query(
        self, interval_seconds: int = 60, start_time: datetime | None = None
    ) -> UtilizationResponse:
        """Query aligned simulated and actual CPU utilization."""
        if not self.sim_results_path.exists():
            raise FileNotFoundError(f"Simulation results not found: {self.sim_results_path}")

        sim_df = pd.read_parquet(self.sim_results_path)
        logger.info(f"Loaded {len(sim_df)} simulated power records")

        actual_df = self._load_actual_data(start_time)
        logger.info(f"Loaded {len(actual_df)} actual CPU utilization records")

        sim_df = self._prepare_simulated_data(sim_df)

        if start_time:
            start_time_utc = pd.to_datetime(start_time, utc=True)
            sim_df = sim_df.loc[sim_df["timestamp"] >= start_time_utc].copy()
            actual_df = actual_df.loc[actual_df["timestamp"] >= start_time_utc].copy()

        aligned_df = self._align_timeseries(sim_df, actual_df, interval_seconds)

        data_points = []
        for _, row in aligned_df.iterrows():
            timestamp = row["timestamp"]
            timestamp_dt = (
                timestamp.to_pydatetime()
                if isinstance(timestamp, pd.Timestamp)
                else pd.to_datetime(timestamp).to_pydatetime()
            )
            data_points.append(
                UtilizationDataPoint(
                    timestamp=timestamp_dt,
                    simulated_cpu_utilization=float(row["simulated_cpu_utilization"]),
                    actual_cpu_utilization=float(row["actual_cpu_utilization"]),
                )
            )

        metadata = {
            "run_id": self.run_id,
            "interval_seconds": interval_seconds,
            "count": len(data_points),
            "excluded_nodes": [EXCLUDED_CONTROL_NODE],
            "start_time": (
                aligned_df["timestamp"].min().isoformat() if not aligned_df.empty else None
            ),
            "end_time": (
                aligned_df["timestamp"].max().isoformat() if not aligned_df.empty else None
            ),
        }

        return UtilizationResponse(data=data_points, metadata=metadata)

    def _prepare_simulated_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare simulated utilization data for alignment."""
        required_cols = ["timestamp", "utilization_rate"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Simulation data missing required columns: {missing}")

        df = df.copy()
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        result = df[["timestamp", "utilization_rate"]].rename(
            columns={"utilization_rate": "simulated_cpu_utilization"}
        )
        return result.sort_values("timestamp", ignore_index=True)

    def _load_actual_data(self, start_time: datetime | None = None) -> pd.DataFrame:
        query = text(
            f"""
            SELECT
                capture_time AS timestamp,
                AVG(cpu_utilization) AS actual_cpu_utilization
            FROM node_utilization_snapshots
            WHERE cpu_utilization IS NOT NULL
              AND COALESCE(node_name, '') != :excluded_control_node
              {"AND capture_time >= :start_time" if start_time else ""}
            GROUP BY capture_time
            ORDER BY capture_time
            """
        )

        params: dict[str, Any] = {"excluded_control_node": EXCLUDED_CONTROL_NODE}
        if start_time:
            params["start_time"] = start_time

        with self.db_engine.connect() as connection:
            actual_df = pd.read_sql_query(
                query,
                connection,
                params=params,
                parse_dates=["timestamp"],
            )

        if actual_df.empty:
            logger.warning("No actual CPU utilization data found in the database")
            return pd.DataFrame(
                {
                    "timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
                    "actual_cpu_utilization": pd.Series(dtype="float64"),
                }
            )

        actual_df["timestamp"] = pd.to_datetime(actual_df["timestamp"], utc=True)
        actual_df = actual_df.sort_values("timestamp", ignore_index=True)
        logger.info(f"Loaded {len(actual_df)} aggregated actual CPU utilization records")
        return actual_df

    def _align_timeseries(
        self, sim_df: pd.DataFrame, actual_df: pd.DataFrame, interval_seconds: int
    ) -> pd.DataFrame:
        """Align two CPU utilization timeseries to the same interval."""
        if sim_df.empty or actual_df.empty:
            logger.warning("Cannot align utilization timeseries because one input is empty")
            return pd.DataFrame(
                {
                    "timestamp": pd.Series(dtype="datetime64[ns, UTC]"),
                    "simulated_cpu_utilization": pd.Series(dtype="float64"),
                    "actual_cpu_utilization": pd.Series(dtype="float64"),
                }
            )

        start_time = max(sim_df["timestamp"].min(), actual_df["timestamp"].min())
        end_time = min(sim_df["timestamp"].max(), actual_df["timestamp"].max())

        logger.info(f"Aligning CPU utilization data from {start_time} to {end_time}")

        time_grid = pd.date_range(start=start_time, end=end_time, freq=f"{interval_seconds}s")

        sim_df = sim_df.set_index("timestamp")
        sim_interpolated = sim_df.reindex(sim_df.index.union(time_grid)).interpolate(method="time")
        sim_interpolated = sim_interpolated.reindex(time_grid)

        actual_df = actual_df.set_index("timestamp")
        actual_interpolated = actual_df.reindex(actual_df.index.union(time_grid)).interpolate(
            method="time"
        )
        actual_interpolated = actual_interpolated.reindex(time_grid)

        aligned = pd.DataFrame(
            {
                "timestamp": time_grid,
                "simulated_cpu_utilization": sim_interpolated[
                    "simulated_cpu_utilization"
                ].values,
                "actual_cpu_utilization": actual_interpolated["actual_cpu_utilization"].values,
            }
        )

        aligned = aligned.dropna()
        logger.info(f"Aligned CPU utilization timeseries: {len(aligned)} data points")
        return aligned
