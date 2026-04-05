from dataclasses import dataclass
from datetime import UTC, datetime
import logging
import threading

from odt_common import Consumption
from k8s_observability.models import NodePowerReading
from k8s_observability.persistence import NodePowerReadingRepository, build_engine, build_session_factory

from k8s_observer.scaphandre.energy_reader import ScaphandreEnergyReader, NodeEnergyCounter
from k8s_observer.producers.base import BaseProducer

logger = logging.getLogger(__name__)

UJ_PER_JOULE = 1_000_000
DEFAULT_POLL_INTERVAL_SECONDS = 15
# Verify this exact number on your target machine:
# cat /sys/class/powercap/intel-rapl:0/max_energy_range_uj
RAPL_MAX_ENERGY_RANGE_UJ = 262_143_328_850


class NodePowerProducer(BaseProducer):
    def __init__(
        self,
        kafka_bootstrap_servers: str,
        topic: str,
        scaphandre_root: str,
        node_names: list[str],
        database_url: str,
        poll_interval_seconds: int = DEFAULT_POLL_INTERVAL_SECONDS,
        start_barrier: threading.Barrier | None = None,
    ):
        super().__init__(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            topic=topic,
            name="NodePowerProducer",
            start_barrier=start_barrier,
        )
        self.node_names = node_names
        self.database_url = database_url
        self.poll_interval_seconds = poll_interval_seconds

        self.db_engine = build_engine(self.database_url)
        self.db_session_factory = build_session_factory(self.db_engine)
        self.db_session = self.db_session_factory()
        self.node_power_repo = NodePowerReadingRepository(self.db_session)

        self.energy_reader = ScaphandreEnergyReader(scaphandre_root)
        self.prev_energy: dict[str, NodeEnergyCounter] = {}

    def _capture_current_energy(self, capture_time: datetime) -> dict[str, NodeEnergyCounter]:
        current_energy = {}

        for node_name in self.node_names:
            try:
                counter = self.energy_reader.read_energy_counter(node_name, capture_time)
                current_energy[node_name] = counter
            except Exception as e:
                logger.error(f"Failed to capture energy for node {node_name}: {e}", exc_info=True)
                continue
        return current_energy
    
    def _delta_energy_uj(self, prev_uj: int, current_uj: int) -> int:
        if current_uj >= prev_uj:
            return current_uj - prev_uj
        # Handle counter overflow for RAPL counters
        # See https://arxiv.org/pdf/2401.15985 Section IV-B
        delta = (RAPL_MAX_ENERGY_RANGE_UJ - prev_uj) + current_uj
        logger.warning(f"Energy counter overflow detected: prev={prev_uj}, current={current_uj}, delta={delta}")
        return delta
    
    def _build_power_readings(self, capture_time: datetime) -> list[NodePowerReading]:
        current_energy: dict[str, NodeEnergyCounter] = self._capture_current_energy(capture_time)
        power_readings: list[NodePowerReading] = []

        for node in self.node_names:
            prev_counter = self.prev_energy.get(node)
            current_counter = current_energy.get(node)

            # Case: No previous data - store current as baseline for next reading
            if prev_counter is None:
                logger.debug(f"No previous energy data for node {node}, skipping power calculation")
                if current_counter is not None:
                    logger.debug(f"Storing initial energy counter for node {node}")
                    # Store the current counter and append the reading
                    self.prev_energy[node] = current_counter
                    reading = NodePowerReading(
                        node_name=node,
                        capture_time=current_counter.capture_time,
                        energy_uj=current_counter.energy_uj,
                        energy_usage_j=None,
                        power_draw_w=None,
                    )
                    power_readings.append(reading)
                continue
        
            # Case: Current data missing - log and skip
            if current_counter is None:
                logger.warning(f"Current energy data missing for node {node}, skipping power calculation")
                continue

            # Calculate deltas
            delta_energy_uj = self._delta_energy_uj(prev_counter.energy_uj, current_counter.energy_uj)
            delta_time_seconds = (current_counter.capture_time - prev_counter.capture_time).total_seconds()
            energy_usage_j = delta_energy_uj / UJ_PER_JOULE
            power_draw_w = energy_usage_j / delta_time_seconds if delta_time_seconds > 0 else None
            
            # Store the current counter and append the reading
            self.prev_energy[node] = current_counter
            reading = NodePowerReading(
                node_name=node,
                capture_time=current_counter.capture_time,
                energy_uj=current_counter.energy_uj,
                energy_usage_j=energy_usage_j,
                power_draw_w=power_draw_w,
            )
            power_readings.append(reading)
        return power_readings
    
    def _save_power_readings(self, readings: list[NodePowerReading]):
        self.node_power_repo.add_many(readings)
        self.node_power_repo.commit()

    def _emit_power_readings(self, readings: list[NodePowerReading], capture_time: datetime):
        # TODO: Consume topology message to update node list dynamically
        total_power_draw = sum(r.power_draw_w for r in readings if r.power_draw_w is not None)
        total_energy_usage = sum(r.energy_usage_j for r in readings if r.energy_usage_j is not None)

        logger.info(f"⚡️ Power consumption: total_power_draw={total_power_draw:.2f} W, total_energy_usage={total_energy_usage:.2f} J across {len(readings)} nodes")

        consumption = Consumption(
            timestamp=capture_time,
            power_draw=total_power_draw,
            energy_usage=total_energy_usage,
        )
        self.emit_message(
            message=consumption.model_dump(mode="json"),
            key=None,  # No key for consumption
        )
        self.flush()

    def run(self):
        logger.info("NodePowerProducer running")

        # Initial capture to establish baselines for each node
        initial_capture_time = datetime.now(tz=UTC)
        self._build_power_readings(initial_capture_time)

        try:
            while not self.should_stop():
                # Wait for the interval (with ability to interrupt)
                if self.wait_interruptible(self.poll_interval_seconds):
                    break
                capture_time = datetime.now(tz=UTC)
                power_readings = self._build_power_readings(capture_time)
                self._save_power_readings(power_readings)
                self._emit_power_readings(power_readings, capture_time)
        except Exception as e:
            logger.error(f"Error in NodePowerProducer: {e}", exc_info=True)
            raise
        finally:
            self.db_session.close()
            logger.info("NodePowerProducer stopped and database session closed")
