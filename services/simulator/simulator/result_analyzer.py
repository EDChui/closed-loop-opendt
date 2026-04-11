import logging
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from odt_common.models.simulation import SimulationMetric, SimulationResult

logger = logging.getLogger(__name__)

class SimulationResultAnalyzer:
    @staticmethod
    def _get_parquet_file(output_dir: Path, filename: str) -> pd.DataFrame:
        files = list(output_dir.rglob(filename))

        if not files:
            logger.warning(f"No {filename} files found in {output_dir}")
            return pd.DataFrame()
        
        try:
            if len(files) > 1:
                logger.warning(f"Multiple {filename} files found in {output_dir}, using the first one: {files[0]}")
            return pd.read_parquet(files[0])
        except Exception as e:
            logger.error(f"Error reading {filename} from {files[0]}: {e}")
            return pd.DataFrame()

    @staticmethod
    def get_service(output_dir: Path) -> pd.DataFrame:
        return SimulationResultAnalyzer._get_parquet_file(output_dir, "service.parquet")

    @staticmethod
    def get_host(output_dir: Path) -> pd.DataFrame:
        return SimulationResultAnalyzer._get_parquet_file(output_dir, "host.parquet")

    @staticmethod
    def get_power(output_dir: Path) -> pd.DataFrame:
        return SimulationResultAnalyzer._get_parquet_file(output_dir, "powerSource.parquet")
    
    @staticmethod
    def _clip_data_timestamps(df: pd.DataFrame, from_time: datetime | None, to_time: datetime | None) -> pd.DataFrame:
        df = df.copy()

        # Convert timestamp_absolute (in milliseconds) to datetime
        if "timestamp_absolute" not in df.columns:
            logger.error("No timestamp_absolute column found in data")
            return pd.DataFrame()
        
        # Convert from milliseconds to datetime (UTC-aware)
        df["timestamp"] = pd.to_datetime(df["timestamp_absolute"], unit="ms", utc=True)
        logger.debug(f"Converted timestamp_absolute to datetime for {len(df)} rows")

        # Clip to only new data since from_time (start boundary)
        if from_time is not None:
            original_count = len(df)
            df = df.loc[df["timestamp"] > from_time].copy()
            logger.debug(
                f"Clipped data at start: {original_count} -> {len(df)} rows "
                f"(keeping data after {from_time.isoformat()})"
            )
        
        # Clip data at end to only include data up to to_time (end boundary)
        if to_time is not None:
            original_count = len(df)
            df = df.loc[df["timestamp"] <= to_time].copy()
            logger.debug(
                f"Clipped data at end: {original_count} -> {len(df)} rows "
                f"(keeping data up to {to_time.isoformat()})"
            )
        
        return df
    
    @staticmethod
    def get_clipped_power(output_dir: Path, from_time: datetime | None = None, to_time: datetime | None = None) -> pd.DataFrame:
        power_df = SimulationResultAnalyzer.get_power(output_dir)
        return SimulationResultAnalyzer._clip_data_timestamps(power_df, from_time, to_time)
    
    @staticmethod
    def get_aggregated_utilization(output_dir: Path, from_time: datetime | None = None, to_time: datetime | None = None) -> pd.Series:
        """Aggregate multiple host utilization entries into a averaged utilization rate per timestamp, after clipping to the specified time range."""
        host_df = SimulationResultAnalyzer.get_host(output_dir)
        clipped_host_df = SimulationResultAnalyzer._clip_data_timestamps(host_df, from_time, to_time)

        utilization_df = clipped_host_df \
            .groupby("timestamp_absolute", as_index=False)["cpu_utilization"] \
            .mean()
        utilization_df["utilization_rate"] = utilization_df["cpu_utilization"]
        return utilization_df

    @staticmethod
    def get_runtime(output_dir: Path) -> timedelta | None:
        service_df = SimulationResultAnalyzer.get_service(output_dir)

        if service_df.empty:
            return None
        
        try:
            return pd.to_timedelta(service_df.timestamp.max() - service_df.timestamp.min(), unit="ms")
        except Exception as e:
            logger.error(f"Error calculating runtime from service data: {e}")
            return None

    @staticmethod
    def analyze_result(sim_output_dir: Path, from_time: datetime | None, to_time: datetime | None) -> SimulationResult:
        runtime = SimulationResultAnalyzer.get_runtime(sim_output_dir)
        runtime_seconds = runtime.total_seconds() if runtime is not None else 0.0
        
        utilization_df = SimulationResultAnalyzer.get_aggregated_utilization(sim_output_dir, from_time, to_time)
        avg_utilization_rate = utilization_df["utilization_rate"].mean() if not utilization_df.empty else 0.0

        power_df = SimulationResultAnalyzer.get_clipped_power(sim_output_dir, from_time, to_time)
        avg_power = power_df["power_draw"].mean() if not power_df.empty else 0.0

        return SimulationResult(
            metrics={
                "runtime": SimulationMetric(value=runtime_seconds, unit="seconds"),
                "utilization": SimulationMetric(value=avg_utilization_rate, unit=""),
                "power": SimulationMetric(value=avg_power, unit="watts"),
            }
        )
