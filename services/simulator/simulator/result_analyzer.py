import logging
import pandas as pd
from datetime import timedelta
from pathlib import Path

from odt_common.models.simulation import SimulationResult

logger = logging.getLogger(__name__)

class SimulationResultAnalyzer:
    def get_service(self, output_dir: Path) -> pd.DataFrame:
        service_files = list(output_dir.rglob("service.parquet"))

        if not service_files:
            logger.warning(f"No service.parquet files found in {output_dir}")
            return pd.DataFrame()
        
        try:
            return pd.read_parquet(service_files[0])
        except Exception as e:
            logger.error(f"Error reading service.parquet from {service_files[0]}: {e}")
            return pd.DataFrame()
        
    def get_host(self, output_dir: Path) -> pd.DataFrame:
        host_files = list(output_dir.rglob("host.parquet"))

        if not host_files:
            logger.warning(f"No host.parquet files found in {output_dir}")
            return pd.DataFrame()
        
        try:
            return pd.read_parquet(host_files[0])
        except Exception as e:
            logger.error(f"Error reading host.parquet from {host_files[0]}: {e}")
            return pd.DataFrame()
        
    def get_runtime(self, df_service: pd.DataFrame) -> timedelta | None:
        if df_service.empty:
            return None
        
        try:
            return pd.to_timedelta(df_service.timestamp.max() - df_service.timestamp.min(), unit="ms")
        except Exception as e:
            logger.error(f"Error calculating runtime from service data: {e}")
            return None
        
    def get_utilization(self, df_host: pd.DataFrame) -> float | None:
        if df_host.empty:
            return None
        
        try:
            return df_host.cpu_utilization.mean()
        except Exception as e:
            logger.error(f"Error calculating utilization from host data: {e}")
            return None

    def analyze_result(self, sim_output_dir: Path) -> SimulationResult:
        df_service = self.get_service(sim_output_dir)
        df_host = self.get_host(sim_output_dir)

        runtime = self.get_runtime(df_service)
        utilization = self.get_utilization(df_host)

        return SimulationResult(
            runtime=runtime,
            utilization=utilization,
        )
