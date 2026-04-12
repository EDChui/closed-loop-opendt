import logging
import asyncio
import itertools
import time
import uuid
from enum import Enum, auto
from typing import Optional

from odt_common.models import (
    SimulationBatch,
    SimulationBatchReport,
    EvaluatedProposal,
    DecisionPolicy
)
from k8s_orchestrator.application.config import DecisionOrchestratorConfig
from k8s_orchestrator.application.events import Event, Priority, QueueItem, RefreshTick, ConfigChange
from k8s_orchestrator.application.ports import SystemPort, SimulationGateway, StatePublisher, RuntimeConfigGateway
from k8s_orchestrator.domain import (
    DecisionMaker,
    ObservedState,
    ProposalGenerator,
)

logger = logging.getLogger(__name__)


class ReportDisposition(Enum):
    PROCESS = auto()
    DROP = auto()
    REQUEUE = auto()


class DecisionOrchestrator:
    def __init__(
        self,
        real_system: SystemPort,
        proposal_generator: ProposalGenerator,
        decision_maker: DecisionMaker,
        state_publisher: StatePublisher,
        simulation_gateway: SimulationGateway,
        runtime_config_gateway: RuntimeConfigGateway,
        config: DecisionOrchestratorConfig,
    ):
        self.real_system = real_system
        self.proposal_generator = proposal_generator
        self.decision_maker = decision_maker
        self.state_publisher = state_publisher
        self.simulation_gateway = simulation_gateway
        self.runtime_config_gateway = runtime_config_gateway
        self.config = config

        self.current_state: Optional[ObservedState] = None
        self.pending_batches: dict[str, SimulationBatch] = {}

        # Event queue
        self.queue: asyncio.PriorityQueue[QueueItem] = asyncio.PriorityQueue()
        self._seq = itertools.count()

        self.refresh_requested = asyncio.Event()
        self.next_refresh_due_monotonic = time.monotonic() + self.config.refresh_interval_seconds

    @staticmethod
    def _new_state_id() -> str:
        return str(uuid.uuid4())

    async def run(self) -> None:
        logger.info("Running DecisionOrchestrator")
        await self._refresh_cycle("startup")

        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._refresh_scheduler())
            tg.create_task(self._simulation_reader())
            tg.create_task(self._runtime_config_watcher())
            tg.create_task(self._event_loop())

    async def _enqueue(self, priority: Priority, event: Event) -> None:
        await self.queue.put(QueueItem(priority=int(priority), seq=next(self._seq), event=event))

    async def _refresh_scheduler(self) -> None:
        """Periodically triggers refresh cycles based on the configured interval."""
        while True:
            self.next_refresh_due_monotonic = time.monotonic() + self.config.refresh_interval_seconds
            await asyncio.sleep(self.config.refresh_interval_seconds)

            if not self.refresh_requested.is_set():
                self.refresh_requested.set()
                await self._enqueue(
                    Priority.REFRESH,
                    RefreshTick(
                        reason="periodic",
                        scheduled_at_monotonic=time.monotonic(),
                    ),
                )

    async def _simulation_reader(self) -> None:
        """Continuously reads simulation reports and enqueues them for processing."""
        async for report in self.simulation_gateway:
            await self._enqueue(Priority.SIMULATION, report)

    async def _schedule_report_retry(self, report: SimulationBatchReport, delay: float) -> None:
        async def _delayed():
            await asyncio.sleep(delay)
            await self._enqueue(Priority.SIMULATION, report)
        asyncio.create_task(_delayed())

    async def _runtime_config_watcher(self) -> None:
        async for new_config in self.runtime_config_gateway:
            await self._enqueue(Priority.CONFIG, new_config)

    async def _event_loop(self) -> None:
        """Main event loop that processes incoming events based on priority."""
        while True:
            try:
                item = await self.queue.get()
                event = item.event

                if isinstance(event, RefreshTick):
                    await self._handle_refresh(event)
                elif isinstance(event, SimulationBatchReport):
                    await self._handle_simulation_report(event)
                elif isinstance(event, ConfigChange):
                    await self._handle_config_change(event)
            except TimeoutError as e:
                logger.error(f"Timeout while processing event: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"Error processing event: {e}", exc_info=True)

    # ============================
    # Handle refresh event
    # ============================

    async def _handle_refresh(self, tick: RefreshTick) -> None:
        try:
            await self._refresh_cycle(tick.reason)
        finally:
            self.refresh_requested.clear()

    async def _refresh_cycle(self, cause: str) -> None:
        """Performs a refresh cycle: fetches the current state, publishes it, generates proposals, and submits them for simulation."""
        logger.info(f"🔁 Starting refresh cycle (cause={cause})")
        snapshot = await asyncio.wait_for(
            self.real_system.fetch_status(),
            timeout=self.config.fetch_timeout_seconds,
        )

        # Check if the snapshot has actually changed meaningfully
        is_changed = self.current_state is None or self.current_state.snapshot != snapshot
        if not is_changed:
            logger.info(f"⏹️ Refresh cycle completed (cause={cause}): no meaningful changes")
            return

        revision = 1 if self.current_state is None else self.current_state.revision + 1
        observed_state = ObservedState(
            state_id=self._new_state_id(),
            revision=revision,
            observed_at=time.time(),
            snapshot=snapshot,
        )

        logger.info(f"🆕 Refresh cycle completed (cause={cause}): observed new state with ID {observed_state.state_id} and revision {observed_state.revision}")
        self.current_state = observed_state

        self._drop_pending_batches_for_other_states(observed_state.state_id)

        await self.state_publisher.publish_system_state(observed_state, cause=cause)

        # Generate proposals and submit for simulation
        batch = self.proposal_generator.generate(observed_state)
        self.pending_batches[batch.batch_id] = batch
        await self.simulation_gateway.submit_batch(batch)
    
    def _drop_pending_batches_for_other_states(self, state_id: str) -> None:
        """Removes pending batches that are based on a different state ID than the current one."""
        stale_batch_ids = [
            batch_id
            for batch_id, batch in self.pending_batches.items()
            if batch.based_on_state_id != state_id
        ]
        for batch_id in stale_batch_ids:
            self.pending_batches.pop(batch_id, None)

    # ============================
    # Handle simulation report
    # ============================

    async def _handle_simulation_report(self, report: SimulationBatchReport) -> None:
        logger.info(f"📡 Received simulation report for batch ID {report.batch_id} based on state ID {report.based_on_state_id}")
        
        # Handle drops/requeues
        disposition, delay = self._classify_report(report)
        if disposition == ReportDisposition.DROP:
            return
        elif disposition == ReportDisposition.REQUEUE:
            if delay is None:
                delay = self.config.simulation_requeue_delay_seconds
            await self._schedule_report_retry(report, delay)
            return

        batch = self.pending_batches.get(report.batch_id)
        if batch is None:
            # Ignore reports for batches we didn't know about (might be old/stale)
            logger.warning(f"Received simulation report for unknown batch ID {report.batch_id}, ignoring")
            return

        evaluated = self._correlate_proposals(batch, report)

        # This should not happen due to _should_drop_report, add to pass type checker
        if self.current_state is None:
            return

        decision = self.decision_maker.choose(evaluated, self.current_state.snapshot)

        if decision is None:
            logger.warning(f"No decision chosen for batch ID {report.batch_id}, skipping application")
            return

        try:
            await asyncio.wait_for(
                self.real_system.apply_decision(decision),
                timeout=self.config.apply_timeout_seconds,
            )
        except TimeoutError as e:
            logger.error(f"Timeout while applying decision for batch ID {report.batch_id}: {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Error while applying decision for batch ID {report.batch_id}: {e}", exc_info=True)

        # Always re-read real state after acting
        await self._refresh_cycle(cause=f"post-apply:{decision.action}")

    def _classify_report(self, report: SimulationBatchReport) -> tuple[ReportDisposition, float | None]:
        now_wall = time.time()
        now_mono = time.monotonic()

        # Hard drops
        # 1. TTL check
        if now_wall - report.created_at > self.config.simulation_ttl_seconds:
            logger.info(f"🗑️ [1] DROP simulation report for batch ID {report.batch_id} - TTL exceed")
            return ReportDisposition.DROP, None
        
        # 2. Require initial state to exist
        if self.current_state is None:
            logger.info(f"🗑️ [2] DROP simulation report for batch ID {report.batch_id} - no current state is available")
            return ReportDisposition.DROP, None
        
        # 3. Only process reports based on the current state
        if report.based_on_state_id != self.current_state.state_id:
            logger.info(f"🗑️ [3] DROP simulation report for batch ID {report.batch_id} - state ID does not match ({report.based_on_state_id} != {self.current_state.state_id})")
            return ReportDisposition.DROP, None
        
        # Soft drops
        # 4. Refresh is happening
        if self.refresh_requested.is_set():
            logger.info(f"🔄 [4] REQUEUE simulation report for batch ID {report.batch_id} - refresh is in progress")
            return ReportDisposition.REQUEUE, self.config.simulation_requeue_delay_seconds

        # 5. Do not start a decision too close too the next scheduled refresh
        if now_mono + self.config.simulation_guard_window_seconds >= self.next_refresh_due_monotonic:
            logger.info(f"🔄 [5] REQUEUE simulation report for batch ID {report.batch_id} - too close to the next scheduled refresh")
            delay = max(self.config.simulation_requeue_delay_seconds, self.next_refresh_due_monotonic - now_mono + self.config.simulation_requeue_delay_seconds)
            return ReportDisposition.REQUEUE, delay

        return ReportDisposition.PROCESS, None

    def _correlate_proposals(self, batch: SimulationBatch, report: SimulationBatchReport) -> list[EvaluatedProposal]:
        proposals_by_id = {proposal.proposal_id: proposal for proposal in batch.proposals}
        evaluated: list[EvaluatedProposal] = []

        for outcome in report.outcomes:
            proposal = proposals_by_id.get(outcome.proposal_id)
            if proposal is None:
                continue
            evaluated.append(EvaluatedProposal(proposal=proposal, outcome=outcome))

        return evaluated
    
    # ============================
    # Handle config change
    # ============================

    async def _handle_config_change(self, new_config: ConfigChange) -> None:
        if isinstance(new_config, DecisionPolicy):
            logger.info(f"📡 Received new decision policy config: {new_config}")
            self.decision_maker.update_policy(new_config)
