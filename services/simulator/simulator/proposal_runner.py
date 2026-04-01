import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from pathlib import Path
import shutil
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
import copy

from odt_common.models import Task, Topology, SimulationBatch, Proposal
from odt_common.odc_runner import OpenDCRunner
from simulator.models import ProposalExecutionPlan, ProposalExecutionResult

logger = logging.getLogger(__name__)
logging.getLogger("odt_common").setLevel(logging.WARNING)

def run_single_proposal_simulation(
        run_number: int,
        tasks: list[Task],
        proposal_index: int,
        proposal: Proposal,
        proposal_dir: Path,
        aligned_simulated_time: datetime,
        timeout_seconds: int
    ) -> ProposalExecutionResult:
        try:
            opendc_runner = OpenDCRunner()
            success, output_dir = opendc_runner.run_simulation(
                tasks=tasks,
                topology=proposal.candidate_topology,
                run_dir=proposal_dir,
                run_number=run_number,
                simulated_time=aligned_simulated_time,
                timeout_seconds=timeout_seconds
            )

            # Assume metadata.json is created by the OpenDCRunner in the proposal_dir with relevant metadata about the simulation run
            if success:
                metadata_file = proposal_dir / "metadata.json"
                metadata = json.loads(metadata_file.read_text()) if metadata_file.exists() else {}
                metadata["proposal_id"] = proposal.proposal_id
                metadata["based_on_state_id"] = proposal.based_on_state_id
                metadata["candidate_config"] = proposal.candidate_topology.model_dump() if proposal.candidate_topology else None
                metadata["decision"] = proposal.decision.model_dump() if proposal.decision else None

                metadata_file.write_text(json.dumps(metadata, indent=2))

            return ProposalExecutionResult(
                index=proposal_index,
                proposal_id=proposal.proposal_id,
                proposal_dir=proposal_dir,
                output_dir=output_dir if success else None,
                success=success,
                cached=False,
                error_message=None if success else "OpenDC simulation failed"
            )
        except Exception as e:
            logger.error(f"Error running simulation for proposal '{proposal.proposal_id}': {e}")
            return ProposalExecutionResult(
                index=proposal_index,
                proposal_id=proposal.proposal_id,
                proposal_dir=proposal_dir,
                output_dir=None,
                success=False,
                cached=False,
                error_message=str(e)
            )


class ProposalRunner:
    def __init__(
        self, 
        max_parallel_workers: int = 4,
        opendc_timeout_seconds: int = 120
    ):
        self.max_parallel_workers = max_parallel_workers
        self.opendc_timeout_seconds = opendc_timeout_seconds

    # ====================
    # Cache handling methods
    # ====================

    def _copy_cached_proposal_results(self, source_proposal_dir: Path, destination_proposal_dir: Path) -> None:
        logger.debug(f"♻️  Reusing cached results for proposal simulation (copying from {source_proposal_dir} to {destination_proposal_dir})")
        if source_proposal_dir is None or not source_proposal_dir.exists():
            return

        # Create parent directory if needed
        destination_proposal_dir.parent.mkdir(parents=True, exist_ok=True)

        # Remove destination if it exists
        if destination_proposal_dir.exists():
            shutil.rmtree(destination_proposal_dir)

        # Copy entire proposal directory recursively
        shutil.copytree(source_proposal_dir, destination_proposal_dir)

    def _update_cached_metadata(self, proposal_dir: Path, aligned_simulated_time: datetime) -> None:
        metadata_file = proposal_dir / "metadata.json"
        metadata = json.loads(metadata_file.read_text())
        metadata["simulated_time"] = aligned_simulated_time.replace(microsecond=0).isoformat()
        metadata["wall_clock_time"] = datetime.now(UTC).replace(microsecond=0, tzinfo=None).isoformat()
        metadata["cached"] = True

        metadata_file.write_text(json.dumps(metadata, indent=2))

    # ====================
    # Simulation execution methods
    # ====================
    
    def _run_proposal_parallel_simulation(
            self,
            run_number: int,
            tasks: list[Task],
            execution_plans: list[ProposalExecutionPlan],
            aligned_simulated_time: datetime,
            timeout_seconds: int
        ) -> list[ProposalExecutionResult]:
        proposal_results: list[ProposalExecutionResult] = []
        with ProcessPoolExecutor(max_workers=self.max_parallel_workers) as executor:
            futures = {
                executor.submit(
                    run_single_proposal_simulation,
                    run_number,
                    tasks,
                    plan.index,
                    plan.proposal,
                    plan.proposal_dir,
                    aligned_simulated_time,
                    timeout_seconds
                ): plan for plan in execution_plans
            }

            for future in as_completed(futures):
                result = future.result()
                proposal_results.append(result)
                if result.success:
                    logger.info(f"✓ Proposal simulation {result.index} (ID: {result.proposal_id}) completed successfully")
                else:
                    logger.error(f"✗ Proposal simulation {result.index} (ID: {result.proposal_id}) failed: {result.error_message}")
        
        return proposal_results

    # ====================
    # Main entry point
    # ====================
    
    def run(
        self,
        output_base_dir: Path,
        run_number: int,
        aligned_simulated_time: datetime,
        tasks: list[Task],
        simulation_batch: SimulationBatch,
        calibrated_real_topology: Topology,
    ) -> list[ProposalExecutionResult]:
        proposals = copy.deepcopy(simulation_batch.proposals)
        last_task_time = max((task.submission_time for task in tasks))

        # Create directories
        run_dir = output_base_dir / "opendc" / f"run_{run_number}"
        run_dir.mkdir(parents=True, exist_ok=True)

        execution_plans: list[ProposalExecutionPlan] = []
        proposal_results: list[ProposalExecutionResult] = []

        for proposal_idx, proposal in enumerate(proposals):
            # Create proposal-specific directory
            proposal_dir = run_dir / f"proposal_{proposal_idx}"
            proposal_dir.mkdir(parents=True, exist_ok=True)

            # Assume the first proposal is always the current system state
            # so we replace its topology with the calibrated real topology to ensure the simulator 
            # is using the most accurate representation of reality for the baseline
            if proposal_idx == 0 and calibrated_real_topology is not None:
                proposal.candidate_topology = copy.deepcopy(calibrated_real_topology)

            can_reuse = False   # TODO: Cache mechanism
            if can_reuse:
                logger.debug(
                    f"♻️  Reusing cached results for run {run_number} proposal '{proposal.proposal_id}'"
                    f"(topology unchanged, {len(tasks)} tasks)"
                )
                source_proposal_dir = Path("/path/to/cached/proposal/results")  # TODO: Cache mechanism - Get actual cached proposal results path
                self._copy_cached_proposal_results(source_proposal_dir, proposal_dir)
                self._update_cached_metadata(proposal_dir, aligned_simulated_time)
                simulation_result = ProposalExecutionResult(
                    index=proposal_idx,
                    proposal_id=proposal.proposal_id,
                    proposal_dir=proposal_dir,
                    output_dir=proposal_dir / "output",         # Assuming cached results have an output/ subdirectory
                    success=True,
                    cached=True,
                )
                proposal_results.append(simulation_result)
            else:
                execution_plan = ProposalExecutionPlan(
                    index=proposal_idx,
                    proposal=proposal,
                    proposal_dir=proposal_dir
                )
                execution_plans.append(execution_plan)
        
        # Run simulations in parallel for non-cached proposals
        if execution_plans:
            proposal_results.extend(
                self._run_proposal_parallel_simulation(
                    run_number=run_number,
                    tasks=tasks,
                    execution_plans=execution_plans,
                    aligned_simulated_time=aligned_simulated_time,
                    timeout_seconds=self.opendc_timeout_seconds
                )
            )

        proposal_results.sort(key=lambda r: r.index)

        # Build run metadata.json
        metadata_file = run_dir / "metadata.json"
        metadata = {
            "run_number": run_number,
            "batch_id": simulation_batch.batch_id,
            "based_on_state_id": simulation_batch.based_on_state_id,
            "simulated_time": aligned_simulated_time.replace(microsecond=0).isoformat(),
            "last_task_time": last_task_time.replace(microsecond=0).isoformat(),
            "task_count": len(tasks),
            "wall_clock_time": datetime.now(UTC).replace(microsecond=0, tzinfo=None).isoformat(),
            "proposal_results": [
                {
                    "proposal_id": result.proposal_id,
                    "success": result.success,
                    "cached": result.cached,
                    "error_message": result.error_message,
                    "output_dir": str(result.output_dir.relative_to(run_dir)) if result.output_dir else None
                }
                for result in proposal_results
            ]
        }
        metadata_file.write_text(json.dumps(metadata, indent=2))

        # TODO: Cache mechanism

        return proposal_results