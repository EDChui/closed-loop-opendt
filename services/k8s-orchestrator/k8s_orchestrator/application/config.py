from dataclasses import dataclass


@dataclass(frozen=True)
class DecisionOrchestratorConfig:
    refresh_interval_seconds: float
    simulation_ttl_seconds: float = 30.0
    fetch_timeout_seconds: float = 10.0
    apply_timeout_seconds: float = 10.0
    simulation_guard_window_seconds: float = 10.0
