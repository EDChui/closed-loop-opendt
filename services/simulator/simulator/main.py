"""Simulator Service - Main Entry Point."""

import copy
import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from odt_common import TaskAccumulator, load_config_from_env
from odt_common.models import Task, Topology, TopologySnapshot, SimulationBatch, SimulationBatchReport, ProposalOutcome, SimulationResult
from odt_common.odc_runner import OpenDCRunner
from odt_common.utils import get_kafka_bootstrap_servers, get_kafka_consumer, get_kafka_producer, send_message

from simulator.models import ProposalExecutionResult
from simulator.proposal_runner import ProposalRunner
from simulator.result_processor import SimulationResultProcessor
from simulator.result_analyzer import SimulationResultAnalyzer


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("odt_common").setLevel(logging.WARNING)


class SimulationService:
    """Core simulation service that processes workload and runs OpenDC simulations.

    The service:
    1. Listens to dc.workload (tasks), dc.topology (topology snapshots), 
       sim.batch (simulation batches) and sim.topology (calibrated real topology updates) Kafka topics
    2. Accumulates tasks chronologically
    3. Triggers simulations at specified frequency (simulated time)
    """

    def __init__(
        self,
        kafka_bootstrap_servers: str,
        workload_topic: str,
        topology_topic: str,
        sim_batch_topic: str,
        sim_batch_report_topic: str,
        sim_topology_topic: str,
        simulation_frequency_minutes: int,
        max_parallel_workers: int,
        speed_factor: float,
        run_output_dir: str,
        run_id: str,
        consumer_group: str = "simulators",
    ):
        """Initialize the simulation service.

        Args:
            kafka_bootstrap_servers: Kafka broker addresses
            workload_topic: Kafka topic name for workload events (dc.workload)
            topology_topic: Kafka topic name for topology updates (dc.topology)
            sim_batch_topic: Kafka topic name for simulation batch triggers (sim.batch)
            sim_batch_report_topic: Kafka topic name for simulation batch reports (sim.batch.report)
            sim_topology_topic: Kafka topic name for calibrated real topology updates (sim.topology)
            simulation_frequency_minutes: Simulation frequency in simulated time minutes
            speed_factor: Configured simulation speed multiplier
            run_output_dir: Base directory for run outputs
            run_id: Unique run ID for this session
            consumer_group: Kafka consumer group ID
        """
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.consumer_group = consumer_group
        self.workload_topic = workload_topic
        self.topology_topic = topology_topic
        self.sim_batch_topic = sim_batch_topic
        self.sim_batch_report_topic = sim_batch_report_topic
        self.calibration_topic = sim_topology_topic                                     # TODO: Rename to calibration_topic for clarity
        self.simulation_frequency = timedelta(minutes=simulation_frequency_minutes)
        self.max_parallel_workers = max_parallel_workers
        self.speed_factor = speed_factor
        self.run_id = run_id

        # Setup output directories - simulator writes to run_dir/simulator/
        self.output_base_dir = Path(run_output_dir) / run_id / "simulator"
        self.output_base_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Simulator output directory: {self.output_base_dir}")
        logger.info(f"Configured speed factor: {self.speed_factor}x")

        # Initialize result processor for aggregating simulation outputs
        self.result_processor = SimulationResultProcessor(self.output_base_dir)
        self.result_analyzer = SimulationResultAnalyzer()

        # Initialize Kafka consumer
        topics = [workload_topic, topology_topic, sim_batch_topic, sim_topology_topic]
        self.consumer = get_kafka_consumer(
            topics=topics,
            group_id=consumer_group,
            bootstrap_servers=kafka_bootstrap_servers,
        )

        # Initialize Kafka producer for reporting simulation results
        self.producer = get_kafka_producer(kafka_bootstrap_servers)

        # Initialize task accumulator
        self.task_accumulator = TaskAccumulator()

        # Initialize OpenDC runner
        try:
            self.opendc_runner = OpenDCRunner()
        except FileNotFoundError as e:
            logger.error(f"Failed to initialize OpenDC runner: {e}")
            logger.error("Simulation will not be available")
            self.opendc_runner = None

        self.proposal_runner = ProposalRunner(
            max_parallel_workers=max_parallel_workers,
        )

        # Topology state
        self.real_topology: Topology | None = None
        self.calibrated_real_topology: Topology | None = None
        self.sim_batch: SimulationBatch | None = None
        self.sim_batch_received_at: float | None = None

        # Statistics
        self.tasks_processed = 0
        self.simulations_run = 0
        self.run_number = 0

        # Speed tracking - to monitor if we're keeping up with configured speed
        self.first_simulation_wall_time: datetime | None = None
        self.first_simulation_sim_time: datetime | None = None

        logger.info(f"Initialized SimulationService with run ID: {run_id}")
        logger.info(f"Consumer group: {consumer_group}")
        logger.info(f"Subscribed: {workload_topic}, {topology_topic}, {sim_topology_topic}")
        logger.info(
            f"Simulation frequency: {simulation_frequency_minutes} minutes (simulated time)"
        )
        logger.info(f"Max parallel workers for simulations: {max_parallel_workers}")
    
    def _log_simulation_overview(self, all_tasks: list, aligned_simulated_time: datetime) -> None:
        # Get task time range
        first_task_time = min(task.submission_time for task in all_tasks)
        latest_task_time = max(task.submission_time for task in all_tasks)
        task_span_minutes = (latest_task_time - first_task_time).total_seconds() / 60

        # Log detailed simulation overview
        logger.info("=" * 80)
        logger.info(f"🔬 Simulation Run {self.run_number}")
        logger.info("=" * 80)
        logger.info(
            f"📋 OpenDC Input Data:\n"
            f"   Total tasks:      {len(all_tasks)}\n"
            f"   First task:       {first_task_time.isoformat()}\n"
            f"   Latest task:      {latest_task_time.isoformat()}\n"
            f"   Time span:        {task_span_minutes:.1f} minutes\n"
            f"   Simulation end:   {aligned_simulated_time.isoformat()}"
        )

        # Track speed and drift
        if self.first_simulation_wall_time is None or self.first_simulation_sim_time is None:
            # First simulation - establish baseline
            self.first_simulation_wall_time = datetime.now(UTC)
            self.first_simulation_sim_time = aligned_simulated_time
            logger.info(
                f"⏱️  First simulation baseline established:\n"
                f"   Wall time: {self.first_simulation_wall_time.strftime('%H:%M:%S')}\n"
                f"   Sim time:  {aligned_simulated_time.strftime('%H:%M:%S')}"
            )
        else:
            # Calculate speed tracking
            current_wall_time = datetime.now(UTC)
            total_sim_elapsed_sec = (
                aligned_simulated_time - self.first_simulation_sim_time
            ).total_seconds()
            total_wall_elapsed_sec = (
                current_wall_time - self.first_simulation_wall_time
            ).total_seconds()

            if total_wall_elapsed_sec > 0:
                actual_speedup = total_sim_elapsed_sec / total_wall_elapsed_sec

                if self.speed_factor > 0:
                    drift_percent = ((actual_speedup - self.speed_factor) / self.speed_factor) * 100
                    drift_status = "ON TRACK ✓" if abs(drift_percent) < 5 else "DRIFTING ⚠️"
                else:
                    # Max speed mode
                    drift_percent = 0
                    drift_status = "MAX SPEED"

                # Calculate expected vs actual position
                if self.speed_factor > 0:
                    expected_sim_elapsed = total_wall_elapsed_sec * self.speed_factor
                    sim_lag_sec = expected_sim_elapsed - total_sim_elapsed_sec
                    sim_lag_min = sim_lag_sec / 60
                else:
                    sim_lag_sec = 0
                    sim_lag_min = 0

                logger.info(
                    f"⏱️  Speed Tracking:\n"
                    f"   Configured speed:  {self.speed_factor}x\n"
                    f"   Actual speed:      {actual_speedup:.2f}x\n"
                    f"   Status:            {drift_status}\n"
                    f"   Drift:             {drift_percent:+.1f}%\n"
                    f"   \n"
                    f"   Total simulated:   {total_sim_elapsed_sec / 60:.1f} minutes "
                    f"({total_sim_elapsed_sec / 3600:.2f} hours)\n"
                    f"   Total wall time:   {total_wall_elapsed_sec / 60:.1f} minutes "
                    f"({total_wall_elapsed_sec / 3600:.2f} hours)\n"
                    f"   Simulation lag:    {abs(sim_lag_min):.1f} minutes "
                    f"({'behind' if sim_lag_sec < 0 else 'ahead'} of target)"
                )

                if abs(drift_percent) > 10 and self.speed_factor > 0:
                    logger.warning(
                        f"⚠️  WARNING: Simulator is {'lagging behind' if actual_speedup < self.speed_factor else 'running ahead of'} "
                        f"target speed by {abs(drift_percent):.1f}%!"
                    )

        logger.info("=" * 80)
    
    # ====================
    # Simulation post-processing
    # ====================

    def _update_simulation_result_metadata(self, proposal_dir: Path, simulation_result: SimulationResult) -> None:
        """Append simulation result to proposal metadata.json for easier access to key metrics without needing to read parquet files."""
        try:
            metadata_file = proposal_dir / "metadata.json"
            metadata = json.loads(metadata_file.read_text())
            metadata["simulation_result"] = simulation_result.model_dump(mode="json")
            metadata_file.write_text(json.dumps(metadata, indent=2))
            logger.debug(f"Updated simulation result metadata for proposal in {proposal_dir}")
        except Exception as e:
            logger.error(f"Failed to update simulation result metadata: {e}", exc_info=True)

    def _analyze_and_create_simulation_batch_report(
        self,
        proposal_execution_results: list[ProposalExecutionResult],
        last_processed_time: datetime | None,
        aligned_simulated_time: datetime | None,
    ) -> SimulationBatchReport:
        outcomes = []

        if self.sim_batch is None:
            logger.error("No simulation batch available when creating report")
            return SimulationBatchReport(
                batch_id="unknown",
                based_on_state_id="unknown",
                created_at=time.time(),
                outcomes=outcomes
            )
        
        for execution_result in proposal_execution_results:
            if not execution_result.output_dir:
                logger.warning(f"No output directory for proposal {execution_result.proposal_id}, skipping result analysis")
                continue

            result = self.result_analyzer.analyze_result(execution_result.output_dir, last_processed_time, aligned_simulated_time)
            self._update_simulation_result_metadata(execution_result.proposal_dir, result)
            outcome = ProposalOutcome(
                proposal_id=execution_result.proposal_id,
                result=result,
            )
            outcomes.append(outcome)

        sim_batch_report = SimulationBatchReport(
            batch_id=self.sim_batch.batch_id,
            based_on_state_id=self.sim_batch.based_on_state_id,
            created_at=time.time(),
            outcomes=outcomes
        )

        return sim_batch_report

    def _run_simulation(self) -> None:
        """Run OpenDC simulation with accumulated tasks for the currently loaded proposals"""
        if not self.opendc_runner:
            logger.warning("OpenDC runner not available, skipping simulation")
            return

        if not self.calibrated_real_topology:
            logger.warning("No topology available, skipping simulation")
            return
        
        if not self.sim_batch:
            logger.warning("No simulation batch available, skipping simulation")
            return
        
        if not self.sim_batch.proposals and len(self.sim_batch.proposals) == 0:
            logger.warning("Simulation batch contains no proposals, skipping simulation")
            return

        # Get all accumulated tasks
        all_tasks = self.task_accumulator.get_all_tasks()

        if not all_tasks:
            logger.info("No tasks to simulate, skipping")
            return

        # Calculate aligned simulation time
        aligned_simulated_time = self.task_accumulator.get_next_simulation_time(self.simulation_frequency)
        if aligned_simulated_time is None:
            logger.error("Cannot calculate aligned simulation time")
            return

        # Increment run number
        self.run_number += 1

        # Log simulation overview with speed tracking
        self._log_simulation_overview(all_tasks, aligned_simulated_time)

        # Run proposal simulations in parallel
        proposal_execution_results: list[ProposalExecutionResult] = self.proposal_runner.run(
            output_base_dir=self.output_base_dir,
            run_number=self.run_number,
            aligned_simulated_time=aligned_simulated_time,
            tasks=all_tasks,
            simulation_batch=self.sim_batch,
            calibrated_real_topology=self.calibrated_real_topology,
        )

        # Process and publish simulation batch report to Kafka
        # Use the same last process time from result processor to make sure all proposals are clipped to the same time range for fair comparison.
        last_processed_time = self.result_processor.get_last_processed_time()
        sim_batch_report = self._analyze_and_create_simulation_batch_report(proposal_execution_results, last_processed_time, aligned_simulated_time)
        self._publish_simulation_batch_report(sim_batch_report)

        # Process and aggregate simulation results
        # This step is for building agg_result.parquet files
        # Always assume the first proposal is the baseline proposal
        baseline_result = proposal_execution_results[0] if proposal_execution_results else None
        if baseline_result and baseline_result.output_dir:
            output_dir = baseline_result.output_dir
            was_cached = baseline_result.cached
            try:
                self.result_processor.process_simulation_results(
                    run_number=self.run_number,
                    output_dir=output_dir,
                    aligned_simulated_time=aligned_simulated_time,
                    cached=was_cached,
                )
            except Exception as e:
                logger.error(f"Failed to process simulation results: {e}", exc_info=True)

        # Update statistics and simulation time
        self.simulations_run += 1
        self.task_accumulator.last_simulation_time = aligned_simulated_time

        logger.info(
            f"📊 Total Stats: {self.tasks_processed} tasks processed, "
            f"{self.run_number} simulations completed\n"
        )

    # ====================
    # Message processing methods
    # ====================

    def _process_workload_message(self, message_data: dict[str, Any]) -> None:
        """Process a workload message (task or heartbeat) from Kafka.

        Args:
            message_data: Raw message data from Kafka
        """
        try:
            message_type = message_data.get("message_type")

            if message_type == "task":
                # Extract task
                task = Task(**message_data["task"])
                logger.debug(
                    f"Received task {task.id} at {task.submission_time} "
                    f"with {len(task.fragments)} fragments"
                )

                # Add to accumulator
                self.task_accumulator.add_task(task)
                self.tasks_processed += 1

            elif message_type == "heartbeat":
                # Parse heartbeat timestamp
                heartbeat_time = datetime.fromisoformat(message_data["timestamp"])
                logger.debug(f"Received heartbeat at {heartbeat_time}")

                # Check if we should trigger simulation
                if self.task_accumulator.should_simulate(heartbeat_time, self.simulation_frequency):
                    self._run_simulation()

            else:
                logger.warning(f"Unknown message_type: {message_type}")

        except Exception as e:
            logger.error(f"Error processing workload message: {e}", exc_info=True)

    def _process_topology_message(self, message_data: dict[str, Any]) -> None:
        """Process a topology message from Kafka.

        Args:
            message_data: Raw message data from Kafka
        """
        try:
            # Parse into TopologySnapshot model
            topology_snapshot = TopologySnapshot(**message_data)

            logger.info(
                f"📡 Received topology snapshot (timestamp: {topology_snapshot.timestamp})"
            )

            # Update real topology
            self.real_topology = topology_snapshot.topology

            # Update the calibrated real topology as well (initially the same, can be modified by sim topology updates from calibrator)
            self.calibrated_real_topology = copy.deepcopy(self.real_topology)

            # Log update details
            total_hosts = sum(host.count for cluster in topology_snapshot.topology.clusters for host in cluster.hosts)
            logger.info(f"   Total hosts: {total_hosts}")

        except Exception as e:
            logger.error(f"Error processing topology message: {e}", exc_info=True)
    
    def _process_sim_batch_message(self, message_data: dict[str, Any]) -> None:
        try:
            sim_batch = SimulationBatch(**message_data)
            self.sim_batch = sim_batch
            self.sim_batch_received_at = time.time()

            logger.info(f"📡 Received simulation batch: {sim_batch.batch_id} with {len(sim_batch.proposals)} proposals")

        except Exception as e:
            logger.error(f"Error processing sim batch message: {e}", exc_info=True)

    def _publish_simulation_batch_report(self, sim_batch_report: SimulationBatchReport) -> None:        
        logger.info(f"Publishing simulation batch report for batch {sim_batch_report.batch_id} with {len(sim_batch_report.outcomes)} outcomes")
        for outcome in sim_batch_report.outcomes:
            logger.info(f"   Proposal {outcome.proposal_id}: {outcome.result}")

        try:
            report_data = sim_batch_report.model_dump(mode="json")
            send_message(
                producer=self.producer,
                topic=self.sim_batch_report_topic,
                message=report_data,
                key=sim_batch_report.batch_id,
            )
            logger.info(f"Published simulation batch report for batch {sim_batch_report.batch_id}")

        except Exception as e:
            logger.error(f"Error publishing simulation batch report: {e}", exc_info=True)

    def _process_calibration_message(self, message_data: dict[str, Any]) -> None:
        """Process a calibrated real topology update message from Kafka.

        Args:
            message_data: Raw message data from Kafka (raw Topology, not TopologySnapshot)
        """
        try:
            # Parse into Topology model (not TopologySnapshot)
            # TODO: In the future, maybe only the calibrationFactor is being passed to here instead of the full topology
            topology = Topology(**message_data)

            logger.info(
                f"🔄 Received calibrated real topology update: {len(topology.clusters)} cluster(s)"
            )

            # Update calibrated real topology
            self.calibrated_real_topology = topology

            # Log update details
            total_hosts = sum(host.count for cluster in topology.clusters for host in cluster.hosts)
            logger.info(f"   Total hosts: {total_hosts}")

        except Exception as e:
            logger.error(f"Error processing topology update message: {e}", exc_info=True)

    def process_message(self, message):
        """Process a single Kafka message.

        Args:
            message: Kafka message
        """
        topic = message.topic
        value = message.value

        try:
            if topic == self.workload_topic:
                self._process_workload_message(value)
            elif topic == self.topology_topic:
                self._process_topology_message(value)
            elif topic == self.sim_batch_topic:
                self._process_sim_batch_message(value)
            elif topic == self.calibration_topic:
                self._process_calibration_message(value)
            else:
                logger.warning(f"Unknown topic: {topic}")

        except Exception as e:
            logger.error(f"Error processing message from {topic}: {e}", exc_info=True)

    def run(self):
        """Run the simulation service (main event loop)."""
        logger.info("Starting Simulation Service")
        logger.info("Waiting for messages...")

        try:
            for message in self.consumer:
                self.process_message(message)

        except KeyboardInterrupt:
            logger.info("Received interrupt signal, shutting down...")

        except Exception as e:
            logger.error(f"Error in simulation service: {e}", exc_info=True)
            raise

        finally:
            logger.info("Closing Kafka connections...")
            self.consumer.close()
            self.producer.close()
            logger.info("Simulation service stopped")


def main():
    """Main entry point."""
    # Load configuration from environment
    try:
        config = load_config_from_env()
        logger.info("Loaded configuration")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise

    # Get Kafka configuration from environment variable
    kafka_bootstrap_servers = get_kafka_bootstrap_servers()
    workload_topic = config.kafka.topics["workload"].name
    topology_topic = config.kafka.topics["topology"].name
    sim_batch_topic = config.kafka.topics["sim_batch"].name
    sim_batch_report_topic = config.kafka.topics["sim_batch_report"].name
    sim_topology_topic = config.kafka.topics["sim_topology"].name

    # Get simulator configuration
    simulation_frequency_minutes = config.services.simulator.simulation_frequency_minutes
    speed_factor = config.global_config.speed_factor
    max_parallel_workers = config.services.simulator.max_parallel_workers
    run_output_dir = Path(os.getenv("DATA_DIR", "/app/data"))

    logger.info(f"Kafka bootstrap servers: {kafka_bootstrap_servers}")
    logger.info(f"Workload topic: {workload_topic}")
    logger.info(f"Topology topic: {topology_topic}")
    logger.info(f"Sim batch topic: {sim_batch_topic}")
    logger.info(f"Sim batch report topic: {sim_batch_report_topic}")
    logger.info(f"Simulated topology topic: {sim_topology_topic}")
    logger.info(f"Simulation frequency: {simulation_frequency_minutes} minutes")
    logger.info(f"Speed factor: {speed_factor}x")
    logger.info(f"Data directory: {run_output_dir}")

    # Get run ID from environment
    run_id = os.getenv("RUN_ID")
    if not run_id:
        logger.error("RUN_ID environment variable not set")
        raise ValueError("RUN_ID environment variable is required")

    logger.info(f"Run ID: {run_id}")

    # Get consumer group from environment
    consumer_group = os.getenv("CONSUMER_GROUP", "simulators")

    # Wait for Kafka to be ready
    max_retries = 30
    retry_delay = 2

    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Kafka (attempt {attempt + 1}/{max_retries})")
            service = SimulationService(
                kafka_bootstrap_servers=kafka_bootstrap_servers,
                workload_topic=workload_topic,
                topology_topic=topology_topic,
                sim_batch_topic=sim_batch_topic,
                sim_batch_report_topic=sim_batch_report_topic,
                sim_topology_topic=sim_topology_topic,
                simulation_frequency_minutes=simulation_frequency_minutes,
                speed_factor=speed_factor,
                max_parallel_workers=max_parallel_workers,
                run_output_dir=str(run_output_dir),
                run_id=run_id,
                consumer_group=consumer_group,
            )
            service.run()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logger.warning(f"Connection failed: {e}. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to connect to Kafka after maximum retries")
                raise


if __name__ == "__main__":
    main()
