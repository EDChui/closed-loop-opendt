import logging
import requests
from typing import List

logger = logging.getLogger(__name__)


class PrometheusClient:
    def __init__(self, base_url: str, timeout_seconds: int = 15):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def query_instant(self, promql: str, eval_ts: int) -> List[dict]:
        response = self.session.get(
            f"{self.base_url}/api/v1/query",
            params={
                "query": promql,
                "time": str(eval_ts),
            },
            timeout=self.timeout_seconds
        )
        response.raise_for_status()

        payload = response.json()
        if payload.get("status") != "success":
            raise RuntimeError(f"Prometheus query failed: {payload}")

        warnings = payload.get("warnings") or []
        for warning in warnings:
            logger.warning(f"[prometheus warning] {warning}")

        data = payload.get("data") or {}
        if data.get("resultType") != "vector":
            raise RuntimeError(
                f"Expected instant-vector result from Prometheus, got: {data.get('resultType')}"
            )

        return data.get("result") or []
