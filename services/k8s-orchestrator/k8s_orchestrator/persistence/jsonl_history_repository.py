import json
import logging
from pathlib import Path

from odt_common.models import Decision
from k8s_observability.utils import TimeUtils
from k8s_orchestrator.domain import ObservedState
from k8s_orchestrator.domain.kubernetes import K8sSystemSnapshot
from k8s_orchestrator.application.ports import HistoryPort

logger = logging.getLogger(__name__)


class JsonlHistoryRepository(HistoryPort):
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _append_jsonl(self, file_path: Path, payload: dict) -> None:
        try:
            with file_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except Exception:
            logger.error(f"Failed to append JSONL record to {file_path}", exc_info=True)

    async def record_observed_state(self, observed_state: ObservedState[K8sSystemSnapshot], cause: str) -> None:
        output_file = self.output_dir / "observed_states.jsonl"

        payload = {
            "recorded_at": TimeUtils.now_utc_iso(),
            "cause": cause,
            "topology": observed_state.snapshot.topology.model_dump(mode="json")
        }

        self._append_jsonl(output_file, payload)

    async def record_applied_decision(
        self,
        state_id: str,
        decision: Decision,
        success: bool,
        error_message: str = "",
    ) -> None:
        output_file = self.output_dir / "applied_decisions.jsonl"

        payload = {
            "recorded_at": TimeUtils.now_utc_iso(),
            "state_id": state_id,
            "success": success,
            "error_message": error_message,
            "decision": decision.model_dump(mode="json")
        }

        self._append_jsonl(output_file, payload)