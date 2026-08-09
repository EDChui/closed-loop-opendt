"""Generate the Chapter 5 evaluation outputs for the thesis.

The analysis follows the final Chapter 5 evaluation plan:

1. Static-topology simulation fidelity (ES1 and ES2)
   - signed relative makespan error
   - cumulative-completion NRMSE
   - processor-power MAE and sMAPE
   - processor-power zRMSE
   - fixed-duration segmented best-lag Pearson correlation

2. Effect of topology-aware calibration
   - matched comparisons ES1--ES6, ES2--ES7, ES3--ES8,
     ES4--ES9, and ES5--ES10 using the same four power metrics

3. Closed-loop decision-support value
   - ES4 and ES5 versus ES3
   - ES9 and ES10 versus ES8
   - physical workload runtime
   - RAPL-derived processor-domain energy
   - runtime--energy trade-off

The script produces exactly five main figures and three principal summary tables.
It also writes per-run metrics, diagnostic timing summaries, segment-level trend
results, a run inventory, and an analysis-parameter manifest.

Input
-----
A directory containing run folders named ``1_run_1`` through ``10_run_3``.
A .zip, .tar, .tar.gz, or .tgz archive is also accepted. For archives, only the
files required by this analysis are extracted into the output cache.

Examples
--------
Generate both PNG and PDF figures::

    python thesis_evaluation_analysis.py \
        --root experiment_data \
        --out evaluation_results \
        --figure-format both

Generate only PDF figures from a compressed archive::

    python thesis_evaluation_analysis.py \
        --root experiment_data.tar.gz \
        --out evaluation_results \
        --figure-format pdf

Dependencies
------------
Python 3.10+, NumPy, pandas, Matplotlib, and PyArrow.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import math
import re
import shutil
import sys
import tarfile
import zipfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any, Iterable, Mapping, Optional, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd


SCRIPT_VERSION = "2.0.1"
RUN_RE = re.compile(r"^(?P<setting>\d+)_run_(?P<repetition>\d+)$")
OPENDC_RUN_RE = re.compile(r"^run_(?P<number>\d+)$")

SETTING_LABELS: dict[int, str] = {
    1: "Eight-worker static",
    2: "Four-worker static",
    3: "Pending-Pod baseline",
    4: "Closed-loop runtime-oriented",
    5: "Closed-loop power-oriented",
    6: "Eight-worker static, calibrated",
    7: "Four-worker static, calibrated",
    8: "Pending-Pod baseline, calibrated",
    9: "Closed-loop runtime-oriented, calibrated",
    10: "Closed-loop power-oriented, calibrated",
}

SETTING_SHORT_LABELS: dict[int, str] = {
    1: "ES1\n8-worker static",
    2: "ES2\n4-worker static",
    3: "ES3\nPending-Pod",
    4: "ES4\nRuntime-oriented",
    5: "ES5\nPower-oriented",
    6: "ES6\n8-worker static\ncalibrated",
    7: "ES7\n4-worker static\ncalibrated",
    8: "ES8\nPending-Pod\ncalibrated",
    9: "ES9\nRuntime-oriented\ncalibrated",
    10: "ES10\nPower-oriented\ncalibrated",
}

CALIBRATION_PAIRS: tuple[tuple[int, int, str], ...] = (
    (1, 6, "Eight-worker static"),
    (2, 7, "Four-worker static"),
    (3, 8, "Pending-Pod baseline"),
    (4, 9, "Runtime-oriented policy"),
    (5, 10, "Power-oriented policy"),
)

STATIC_SETTINGS = (1, 2)
DECISION_SETTINGS = (3, 4, 5, 8, 9, 10)
MATCHING_BASELINE = {3: 3, 4: 3, 5: 3, 8: 8, 9: 8, 10: 8}
POWER_METRICS = ("mae_w", "smape_pct", "zrmse", "segmented_best_lag_r")
LOWER_IS_BETTER = {"mae_w", "smape_pct", "zrmse"}
HIGHER_IS_BETTER = {"segmented_best_lag_r"}

OBSERVED_COLOUR = "#0072B2"
SIMULATED_COLOUR = "#D55E00"
UNCALIBRATED_COLOUR = "#4C78A8"
CALIBRATED_COLOUR = "#F58518"


@dataclass(frozen=True)
class RunInfo:
    setting: int
    repetition: int
    path: Path

    @property
    def run_id(self) -> str:
        return f"{self.setting}_run_{self.repetition}"

    @property
    def setting_id(self) -> str:
        return f"ES{self.setting}"

    @property
    def label(self) -> str:
        return SETTING_LABELS.get(self.setting, f"Experimental setting {self.setting}")


@dataclass
class SegmentedTrendResult:
    mean_r: float = math.nan
    valid_segments: int = 0
    total_complete_segments: int = 0
    low_variation_segments: int = 0
    invalid_correlation_segments: int = 0
    trailing_points: int = 0


@dataclass
class AnalysisIssue:
    run_id: str
    component: str
    severity: str
    message: str


class IssueCollector:
    def __init__(self) -> None:
        self.rows: list[AnalysisIssue] = []

    def add(self, run_id: str, component: str, message: str, severity: str = "warning") -> None:
        self.rows.append(AnalysisIssue(run_id, component, severity, message))
        log_fn = logging.error if severity == "error" else logging.warning
        log_fn("%s [%s]: %s", run_id, component, message)

    def dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([asdict(row) for row in self.rows])

    @property
    def has_errors(self) -> bool:
        return any(row.severity == "error" for row in self.rows)


# ---------------------------------------------------------------------------
# General helpers
# ---------------------------------------------------------------------------


def setup_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)
    logging.getLogger("fontTools").setLevel(logging.WARNING)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def safe_float(value: Any) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return math.nan
    return result if math.isfinite(result) else math.nan


def read_json(path: Path) -> Optional[Any]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception as exc:
        logging.warning("Could not read JSON %s: %s", path, exc)
        return None


def read_parquet(path: Path, columns: Optional[list[str]] = None) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        return pd.read_parquet(path, columns=columns)
    except ImportError as exc:
        raise RuntimeError(
            "Reading the experiment data requires PyArrow. Install the dependencies "
            "with `python -m pip install -r requirements_thesis_evaluation.txt`."
        ) from exc


def save_csv(df: pd.DataFrame, path: Path) -> None:
    ensure_dir(path.parent)
    df.to_csv(path, index=False)
    logging.info("Wrote %s", path)


def save_json(data: Mapping[str, Any], path: Path) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=2, default=str)
    logging.info("Wrote %s", path)


def save_figure(
    fig: plt.Figure,
    out_dir: Path,
    stem: str,
    figure_format: str,
    dpi: int,
) -> list[Path]:
    ensure_dir(out_dir)
    written: list[Path] = []
    formats = ("png", "pdf") if figure_format == "both" else (figure_format,)
    for suffix in formats:
        path = out_dir / f"{stem}.{suffix}"
        kwargs: dict[str, Any] = {"bbox_inches": "tight"}
        if suffix == "png":
            kwargs["dpi"] = dpi
        fig.savefig(path, **kwargs)
        written.append(path)
        logging.info("Wrote %s", path)
    plt.close(fig)
    return written


def sample_sd(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) < 2:
        return math.nan
    return float(clean.std(ddof=1))


def mean_value(series: pd.Series) -> float:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    return float(clean.mean()) if not clean.empty else math.nan


def count_value(series: pd.Series) -> int:
    return int(pd.to_numeric(series, errors="coerce").notna().sum())


def errorbar_value(value: Any) -> float:
    number = safe_float(value)
    return number if math.isfinite(number) else 0.0


def set_plot_style() -> None:
    plt.rcParams.update(
        {
            "font.size": 10,
            "axes.labelsize": 10,
            "axes.titlesize": 11,
            "legend.fontsize": 9,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "figure.titlesize": 12,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )


def style_axis(ax: plt.Axes) -> None:
    ax.grid(axis="y", alpha=0.25, linewidth=0.7)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)


def natural_run_key(run: RunInfo) -> tuple[int, int]:
    return run.setting, run.repetition


# ---------------------------------------------------------------------------
# Archive handling and run discovery
# ---------------------------------------------------------------------------


def archive_stem(path: Path) -> str:
    name = path.name
    for suffix in (".tar.gz", ".tgz", ".tar", ".zip"):
        if name.lower().endswith(suffix):
            return name[: -len(suffix)]
    return path.stem


def split_archive_member(name: str) -> Optional[tuple[str, PurePosixPath]]:
    path = PurePosixPath(name)
    if path.is_absolute() or ".." in path.parts:
        return None
    for index, part in enumerate(path.parts):
        if RUN_RE.fullmatch(part):
            relative = PurePosixPath(*path.parts[index + 1 :])
            return part, relative
    return None


def required_archive_member(relative: PurePosixPath) -> bool:
    rel = relative.as_posix()
    exact = {
        "api/power.json",
        "postgres/k8s_task_records.parquet",
        "postgres/node_power_readings.parquet",
    }
    if rel in exact:
        return True
    patterns = (
        "simulator/opendc/run_*/metadata.json",
        "simulator/opendc/run_*/proposal_*/metadata.json",
        "simulator/opendc/run_*/proposal_*/output/run_*/raw-output/*/seed=*/task.parquet",
    )
    return any(fnmatch.fnmatch(rel, pattern) for pattern in patterns)


def write_archive_member(source: Any, target: Path) -> None:
    ensure_dir(target.parent)
    with target.open("wb") as handle:
        shutil.copyfileobj(source, handle)


def extract_required_archive_files(archive: Path, cache_dir: Path, refresh: bool) -> Path:
    marker = cache_dir / ".analysis_cache_complete"
    if refresh and cache_dir.exists():
        shutil.rmtree(cache_dir)
    if marker.exists():
        logging.info("Using extracted archive cache %s", cache_dir)
        return cache_dir

    if cache_dir.exists():
        shutil.rmtree(cache_dir)
    ensure_dir(cache_dir)

    extracted = 0
    lower_name = archive.name.lower()
    if lower_name.endswith(".zip"):
        with zipfile.ZipFile(archive, "r") as zip_file:
            for info in zip_file.infolist():
                if info.is_dir():
                    continue
                parsed = split_archive_member(info.filename)
                if parsed is None:
                    continue
                run_id, relative = parsed
                if not required_archive_member(relative):
                    continue
                target = cache_dir / run_id / Path(*relative.parts)
                with zip_file.open(info, "r") as source:
                    write_archive_member(source, target)
                extracted += 1
    elif lower_name.endswith((".tar", ".tar.gz", ".tgz")):
        mode = "r:gz" if lower_name.endswith((".tar.gz", ".tgz")) else "r:"
        with tarfile.open(archive, mode) as tar_file:
            for member in tar_file:
                if not member.isfile():
                    continue
                parsed = split_archive_member(member.name)
                if parsed is None:
                    continue
                run_id, relative = parsed
                if not required_archive_member(relative):
                    continue
                source = tar_file.extractfile(member)
                if source is None:
                    continue
                target = cache_dir / run_id / Path(*relative.parts)
                with source:
                    write_archive_member(source, target)
                extracted += 1
    else:
        raise ValueError(f"Unsupported archive type: {archive}")

    marker.write_text(
        f"source={archive.resolve()}\nfiles={extracted}\n",
        encoding="utf-8",
    )
    logging.info("Extracted %d required files into %s", extracted, cache_dir)
    return cache_dir


def parse_run_directory(path: Path) -> Optional[RunInfo]:
    match = RUN_RE.fullmatch(path.name)
    if match is None:
        return None
    setting = int(match.group("setting"))
    repetition = int(match.group("repetition"))
    if not any((path / component).exists() for component in ("api", "postgres", "simulator")):
        return None
    return RunInfo(setting, repetition, path)


def discover_runs(root: Path) -> list[RunInfo]:
    candidates = [root] if root.is_dir() else []
    if root.is_dir():
        candidates.extend(path for path in root.rglob("*") if path.is_dir())
    found: dict[tuple[int, int], RunInfo] = {}
    for path in candidates:
        run = parse_run_directory(path)
        if run is None:
            continue
        key = (run.setting, run.repetition)
        previous = found.get(key)
        if previous is None or len(str(path)) < len(str(previous.path)):
            found[key] = run
    return sorted(found.values(), key=natural_run_key)


def resolve_input_root(root: Path, out_dir: Path, refresh_cache: bool) -> Path:
    if root.is_dir():
        return root
    if not root.is_file():
        raise FileNotFoundError(root)
    cache = out_dir / "_input_cache" / archive_stem(root)
    return extract_required_archive_files(root, cache, refresh_cache)


def build_inventory(
    runs: Sequence[RunInfo],
    expected_settings: Sequence[int],
    expected_repetitions: Sequence[int],
) -> pd.DataFrame:
    lookup = {(run.setting, run.repetition): run for run in runs}
    keys = {(setting, rep) for setting in expected_settings for rep in expected_repetitions}
    keys.update(lookup)
    rows: list[dict[str, Any]] = []
    for setting, repetition in sorted(keys):
        run = lookup.get((setting, repetition))
        row: dict[str, Any] = {
            "setting": setting,
            "setting_id": f"ES{setting}",
            "setting_label": SETTING_LABELS.get(setting, ""),
            "repetition": repetition,
            "run_id": f"{setting}_run_{repetition}",
            "status": "present" if run is not None else "missing",
            "run_path": str(run.path) if run is not None else "",
            "has_api_power": False,
            "has_physical_tasks": False,
            "has_node_power": False,
            "simulator_task_candidates": 0,
        }
        if run is not None:
            row["has_api_power"] = (run.path / "api" / "power.json").exists()
            row["has_physical_tasks"] = (
                run.path / "postgres" / "k8s_task_records.parquet"
            ).exists()
            row["has_node_power"] = (
                run.path / "postgres" / "node_power_readings.parquet"
            ).exists()
            row["simulator_task_candidates"] = len(
                list(run.path.glob("simulator/opendc/run_*/proposal_*/output/run_*/raw-output/*/seed=*/task.parquet"))
            )
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Numeric metric helpers
# ---------------------------------------------------------------------------


def finite_pair(first: Sequence[float], second: Sequence[float]) -> tuple[np.ndarray, np.ndarray]:
    a = np.asarray(first, dtype=float)
    b = np.asarray(second, dtype=float)
    if a.shape != b.shape:
        raise ValueError("Metric inputs must have the same shape")
    mask = np.isfinite(a) & np.isfinite(b)
    return a[mask], b[mask]


def mae(observed: Sequence[float], simulated: Sequence[float]) -> float:
    obs, sim = finite_pair(observed, simulated)
    return float(np.mean(np.abs(sim - obs))) if len(obs) else math.nan


def rmse(observed: Sequence[float], simulated: Sequence[float]) -> float:
    obs, sim = finite_pair(observed, simulated)
    return float(np.sqrt(np.mean((sim - obs) ** 2))) if len(obs) else math.nan


def smape_pct(observed: Sequence[float], simulated: Sequence[float]) -> float:
    obs, sim = finite_pair(observed, simulated)
    if not len(obs):
        return math.nan
    denominator = np.abs(obs) + np.abs(sim)
    mask = denominator > 1e-12
    if not mask.any():
        return math.nan
    return float(np.mean(2.0 * np.abs(sim[mask] - obs[mask]) / denominator[mask]) * 100.0)


def zscore(values: Sequence[float]) -> np.ndarray:
    array = np.asarray(values, dtype=float)
    mean = float(np.nanmean(array))
    standard_deviation = float(np.nanstd(array, ddof=0))
    if not math.isfinite(standard_deviation) or standard_deviation <= 0:
        return np.full(array.shape, np.nan, dtype=float)
    return (array - mean) / standard_deviation


def pearson_correlation(first: Sequence[float], second: Sequence[float]) -> float:
    a, b = finite_pair(first, second)
    if len(a) < 3 or np.std(a) <= 0 or np.std(b) <= 0:
        return math.nan
    return float(np.corrcoef(a, b)[0, 1])


def lagged_pair(
    observed: np.ndarray,
    simulated: np.ndarray,
    lag_samples: int,
) -> tuple[np.ndarray, np.ndarray]:
    """Return a lagged pair.

    A positive lag shifts the simulated series later relative to the observed
    series. The sign is exported only as a diagnostic. The reported thesis
    metric uses the maximum correlation, not the lag itself.
    """

    if lag_samples > 0:
        return observed[:-lag_samples], simulated[lag_samples:]
    if lag_samples < 0:
        return observed[-lag_samples:], simulated[:lag_samples]
    return observed, simulated


def best_lag_pearson(
    observed: Sequence[float],
    simulated: Sequence[float],
    max_lag_samples: int,
    min_pairs: int,
) -> tuple[float, int, int]:
    obs, sim = finite_pair(observed, simulated)
    if len(obs) < min_pairs:
        return math.nan, 0, 0
    best_r = -math.inf
    best_lag = 0
    best_pairs = 0
    for lag in range(-max_lag_samples, max_lag_samples + 1):
        lagged_obs, lagged_sim = lagged_pair(obs, sim, lag)
        if len(lagged_obs) < min_pairs:
            continue
        correlation = pearson_correlation(lagged_obs, lagged_sim)
        if math.isfinite(correlation) and correlation > best_r:
            best_r = correlation
            best_lag = lag
            best_pairs = len(lagged_obs)
    if best_r == -math.inf:
        return math.nan, 0, 0
    return float(best_r), int(best_lag), int(best_pairs)


def segmented_best_lag_correlation(
    run: RunInfo,
    observed_z: Sequence[float],
    simulated_z: Sequence[float],
    resample_seconds: int,
    segment_minutes: float,
    max_lag_minutes: float,
    minimum_pairs: int,
    minimum_segment_standard_deviation: float,
) -> tuple[SegmentedTrendResult, list[dict[str, Any]]]:
    observed = np.asarray(observed_z, dtype=float)
    simulated = np.asarray(simulated_z, dtype=float)
    if observed.shape != simulated.shape:
        raise ValueError("Observed and simulated trend series must have equal length")

    segment_seconds = int(round(segment_minutes * 60.0))
    max_lag_seconds = int(round(max_lag_minutes * 60.0))
    if segment_seconds % resample_seconds != 0:
        raise ValueError("Trend segment duration must be divisible by the resampling interval")
    if max_lag_seconds % resample_seconds != 0:
        raise ValueError("Maximum lag must be divisible by the resampling interval")

    segment_samples = segment_seconds // resample_seconds
    max_lag_samples = max_lag_seconds // resample_seconds
    if segment_samples - max_lag_samples < minimum_pairs:
        raise ValueError(
            "Trend parameters are inconsistent: a maximum lag can leave fewer than "
            f"{minimum_pairs} paired observations in a segment"
        )

    total_segments = len(observed) // segment_samples
    trailing_points = len(observed) % segment_samples
    valid_values: list[float] = []
    rows: list[dict[str, Any]] = []
    low_variation = 0
    invalid_correlation = 0

    for segment_index in range(total_segments):
        start = segment_index * segment_samples
        stop = start + segment_samples
        obs_segment = observed[start:stop]
        sim_segment = simulated[start:stop]
        obs_sd = float(np.nanstd(obs_segment, ddof=0))
        sim_sd = float(np.nanstd(sim_segment, ddof=0))
        row: dict[str, Any] = {
            "setting": run.setting,
            "setting_id": run.setting_id,
            "repetition": run.repetition,
            "run_id": run.run_id,
            "segment_index": segment_index + 1,
            "segment_start_s": start * resample_seconds,
            "segment_end_s": stop * resample_seconds,
            "observed_segment_std": obs_sd,
            "simulated_segment_std": sim_sd,
            "valid": False,
            "exclusion_reason": "",
            "best_lag_r": math.nan,
            "best_lag_samples": math.nan,
            "best_lag_seconds": math.nan,
            "paired_observations": 0,
        }

        if (
            not math.isfinite(obs_sd)
            or not math.isfinite(sim_sd)
            or obs_sd < minimum_segment_standard_deviation
            or sim_sd < minimum_segment_standard_deviation
        ):
            row["exclusion_reason"] = "low_variation"
            low_variation += 1
            rows.append(row)
            continue

        correlation, lag, paired = best_lag_pearson(
            obs_segment,
            sim_segment,
            max_lag_samples=max_lag_samples,
            min_pairs=minimum_pairs,
        )
        if not math.isfinite(correlation):
            row["exclusion_reason"] = "no_valid_correlation"
            invalid_correlation += 1
            rows.append(row)
            continue

        row.update(
            {
                "valid": True,
                "best_lag_r": correlation,
                "best_lag_samples": lag,
                "best_lag_seconds": lag * resample_seconds,
                "paired_observations": paired,
            }
        )
        valid_values.append(correlation)
        rows.append(row)

    result = SegmentedTrendResult(
        mean_r=float(np.mean(valid_values)) if valid_values else math.nan,
        valid_segments=len(valid_values),
        total_complete_segments=total_segments,
        low_variation_segments=low_variation,
        invalid_correlation_segments=invalid_correlation,
        trailing_points=trailing_points,
    )
    return result, rows


# ---------------------------------------------------------------------------
# Task progress and timing
# ---------------------------------------------------------------------------


def coerce_utc(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce")


def infer_numeric_time_divisor(values: pd.Series) -> float:
    numeric = pd.to_numeric(values, errors="coerce").dropna().astype(float)
    if numeric.empty:
        return 1.0
    maximum = float(numeric.abs().max())
    span = float(numeric.max() - numeric.min()) if len(numeric) > 1 else maximum
    if maximum > 1e17:
        return 1e9
    if maximum > 1e14:
        return 1e6
    if maximum > 1e11 or span > 1e5:
        return 1e3
    return 1.0


def numeric_duration_seconds(end: pd.Series, start: pd.Series) -> pd.Series:
    end_numeric = pd.to_numeric(end, errors="coerce")
    start_numeric = pd.to_numeric(start, errors="coerce")
    divisor = infer_numeric_time_divisor(pd.concat([end_numeric, start_numeric], ignore_index=True))
    return (end_numeric - start_numeric) / divisor


def load_physical_tasks(run: RunInfo) -> pd.DataFrame:
    path = run.path / "postgres" / "k8s_task_records.parquet"
    frame = read_parquet(path)
    for column in ("submission_time", "start_time", "finish_time"):
        if column in frame.columns:
            frame[column] = coerce_utc(frame[column])
    return frame


def opendc_run_number(path: Path) -> int:
    for part in path.parts:
        match = OPENDC_RUN_RE.fullmatch(part)
        if match:
            return int(match.group("number"))
    return -1


def find_simulator_task_candidates(
    run: RunInfo,
    proposal_filter: str,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    metadata_paths = run.path.glob("simulator/opendc/run_*/proposal_*/metadata.json")
    for metadata_path in metadata_paths:
        metadata = read_json(metadata_path)
        if not isinstance(metadata, dict):
            continue
        proposal_id = str(metadata.get("proposal_id", ""))
        if proposal_filter and proposal_filter.lower() not in proposal_id.lower():
            continue
        task_paths = sorted(
            metadata_path.parent.glob("output/run_*/raw-output/*/seed=*/task.parquet")
        )
        for task_path in task_paths:
            candidates.append(
                {
                    "path": task_path,
                    "proposal_id": proposal_id,
                    "run_number": int(metadata.get("run_number", opendc_run_number(task_path))),
                    "task_count": safe_float(metadata.get("task_count")),
                }
            )

    if candidates:
        return candidates

    for task_path in run.path.glob(
        "simulator/opendc/run_*/proposal_*/output/run_*/raw-output/*/seed=*/task.parquet"
    ):
        proposal_directory = next(
            (parent for parent in task_path.parents if parent.name.startswith("proposal_")),
            None,
        )
        proposal_metadata = (
            read_json(proposal_directory / "metadata.json")
            if proposal_directory is not None
            else None
        )
        proposal_id = ""
        task_count = math.nan
        if isinstance(proposal_metadata, dict):
            proposal_id = str(proposal_metadata.get("proposal_id", ""))
            task_count = safe_float(proposal_metadata.get("task_count"))
        if proposal_filter and proposal_filter.lower() not in (
            proposal_id + " " + str(task_path)
        ).lower():
            continue
        candidates.append(
            {
                "path": task_path,
                "proposal_id": proposal_id,
                "run_number": opendc_run_number(task_path),
                "task_count": task_count,
            }
        )
    return candidates


def select_simulator_task_path(
    run: RunInfo,
    proposal_filter: str,
    expected_task_count: Optional[int],
) -> Optional[Path]:
    candidates = find_simulator_task_candidates(run, proposal_filter)
    if not candidates:
        return None

    def score(candidate: dict[str, Any]) -> tuple[int, int, int]:
        candidate_count = safe_float(candidate.get("task_count"))
        exact = int(
            expected_task_count is not None
            and math.isfinite(candidate_count)
            and int(candidate_count) == expected_task_count
        )
        count_known = int(math.isfinite(candidate_count))
        return exact, count_known, int(candidate.get("run_number", -1))

    return Path(max(candidates, key=score)["path"])


def load_simulated_completed_tasks(
    run: RunInfo,
    proposal_filter: str,
    expected_task_count: Optional[int],
) -> tuple[pd.DataFrame, Optional[Path]]:
    path = select_simulator_task_path(run, proposal_filter, expected_task_count)
    if path is None:
        return pd.DataFrame(), None
    frame = read_parquet(path)
    if "task_state" in frame.columns:
        completed = frame[frame["task_state"].astype(str).str.upper() == "COMPLETED"].copy()
    elif "finish_time" in frame.columns:
        completed = frame[frame["finish_time"].notna()].copy()
    else:
        completed = frame.copy()
    task_id_column = next(
        (column for column in ("task_id", "id", "task_name") if column in completed.columns),
        None,
    )
    if task_id_column is not None:
        completed = completed.drop_duplicates(task_id_column, keep="last")
    return completed, path


def physical_task_arrays(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    required = {"submission_time", "finish_time"}
    if not required.issubset(frame.columns):
        return {}
    valid = frame.dropna(subset=["submission_time", "finish_time"]).copy()
    if valid.empty:
        return {}
    zero = valid["submission_time"].min()
    completion = (valid["finish_time"] - zero).dt.total_seconds().to_numpy(dtype=float)
    result: dict[str, np.ndarray] = {"completion_s": completion[np.isfinite(completion)]}
    turnaround = (valid["finish_time"] - valid["submission_time"]).dt.total_seconds()
    result["turnaround_s"] = turnaround[
        pd.notna(turnaround) & (turnaround >= 0)
    ].to_numpy(dtype=float)
    if "start_time" in valid.columns:
        wait = (valid["start_time"] - valid["submission_time"]).dt.total_seconds()
        result["wait_s"] = wait[pd.notna(wait) & (wait >= 0)].to_numpy(dtype=float)
    return result


def simulated_task_arrays(frame: pd.DataFrame) -> dict[str, np.ndarray]:
    submission_column = next(
        (column for column in ("submission_time", "ts_enqueue", "enqueue_time") if column in frame.columns),
        None,
    )
    finish_column = next(
        (column for column in ("finish_time", "ts_end") if column in frame.columns),
        None,
    )
    schedule_column = next(
        (column for column in ("schedule_time", "start_time", "ts_start") if column in frame.columns),
        None,
    )
    if submission_column is None or finish_column is None or frame.empty:
        return {}

    submission = pd.to_numeric(frame[submission_column], errors="coerce")
    finish = pd.to_numeric(frame[finish_column], errors="coerce")
    divisor = infer_numeric_time_divisor(pd.concat([submission, finish], ignore_index=True))
    zero = float(submission.min())
    completion = ((finish - zero) / divisor).to_numpy(dtype=float)
    result: dict[str, np.ndarray] = {"completion_s": completion[np.isfinite(completion)]}
    turnaround = numeric_duration_seconds(frame[finish_column], frame[submission_column])
    result["turnaround_s"] = turnaround[
        pd.notna(turnaround) & (turnaround >= 0)
    ].to_numpy(dtype=float)
    if schedule_column is not None:
        wait = numeric_duration_seconds(frame[schedule_column], frame[submission_column])
        result["wait_s"] = wait[pd.notna(wait) & (wait >= 0)].to_numpy(dtype=float)
    return result


def cumulative_counts(completion_times: Sequence[float], grid_s: np.ndarray) -> np.ndarray:
    values = np.asarray(completion_times, dtype=float)
    values = np.sort(values[np.isfinite(values) & (values >= 0)])
    return np.searchsorted(values, grid_s, side="right").astype(float)


def distribution_summary(values: Sequence[float]) -> dict[str, float]:
    array = np.asarray(values, dtype=float)
    array = array[np.isfinite(array) & (array >= 0)]
    if len(array) == 0:
        return {
            "n": 0,
            "mean_s": math.nan,
            "median_s": math.nan,
            "p95_s": math.nan,
            "p99_s": math.nan,
        }
    return {
        "n": int(len(array)),
        "mean_s": float(np.mean(array)),
        "median_s": float(np.median(array)),
        "p95_s": float(np.percentile(array, 95)),
        "p99_s": float(np.percentile(array, 99)),
    }


def analyse_task_run(
    run: RunInfo,
    physical_frame: pd.DataFrame,
    simulated_frame: pd.DataFrame,
    simulator_path: Optional[Path],
    completion_bin_seconds: int,
    expected_task_count: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, np.ndarray]]:
    physical = physical_task_arrays(physical_frame)
    simulated = simulated_task_arrays(simulated_frame)
    physical_completion = physical.get("completion_s", np.asarray([], dtype=float))
    simulated_completion = simulated.get("completion_s", np.asarray([], dtype=float))

    row: dict[str, Any] = {
        "setting": run.setting,
        "setting_id": run.setting_id,
        "setting_label": run.label,
        "repetition": run.repetition,
        "run_id": run.run_id,
        "physical_task_count": len(physical_completion),
        "simulated_task_count": len(simulated_completion),
        "physical_makespan_s": math.nan,
        "simulated_makespan_s": math.nan,
        "makespan_error_pct": math.nan,
        "completion_nrmse_pct": math.nan,
        "completion_normaliser_jobs": expected_task_count,
        "simulator_task_path": str(simulator_path) if simulator_path is not None else "",
    }
    curve_data: dict[str, np.ndarray] = {
        "physical_completion_s": physical_completion,
        "simulated_completion_s": simulated_completion,
    }

    if len(physical_completion) and len(simulated_completion):
        physical_makespan = float(np.max(physical_completion))
        simulated_makespan = float(np.max(simulated_completion))
        row["physical_makespan_s"] = physical_makespan
        row["simulated_makespan_s"] = simulated_makespan
        if physical_makespan > 0:
            row["makespan_error_pct"] = (
                100.0 * (simulated_makespan - physical_makespan) / physical_makespan
            )
        maximum = max(physical_makespan, simulated_makespan)
        grid = np.arange(
            0.0,
            math.ceil(maximum / completion_bin_seconds) * completion_bin_seconds
            + completion_bin_seconds,
            completion_bin_seconds,
        )
        physical_curve = cumulative_counts(physical_completion, grid)
        simulated_curve = cumulative_counts(simulated_completion, grid)
        normaliser = max(float(expected_task_count), 1.0)
        row["completion_nrmse_pct"] = (
            100.0 * rmse(physical_curve, simulated_curve) / normaliser
        )

    diagnostic_rows: list[dict[str, Any]] = []
    for metric_name, key in (("wait_time", "wait_s"), ("turnaround_time", "turnaround_s")):
        physical_summary = distribution_summary(physical.get(key, []))
        simulated_summary = distribution_summary(simulated.get(key, []))
        diagnostic_rows.append(
            {
                "setting": run.setting,
                "setting_id": run.setting_id,
                "repetition": run.repetition,
                "run_id": run.run_id,
                "metric": metric_name,
                **{f"physical_{name}": value for name, value in physical_summary.items()},
                **{f"simulated_{name}": value for name, value in simulated_summary.items()},
                "simulated_zero_fraction": (
                    float(np.mean(np.isclose(simulated.get(key, []), 0.0)))
                    if len(simulated.get(key, []))
                    else math.nan
                ),
                "simulator_task_path": str(simulator_path) if simulator_path is not None else "",
            }
        )
    return row, diagnostic_rows, curve_data


# ---------------------------------------------------------------------------
# Processor-power fidelity
# ---------------------------------------------------------------------------


def load_power_pair(run: RunInfo) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = run.path / "api" / "power.json"
    obj = read_json(path)
    if obj is None:
        raise FileNotFoundError(path)
    metadata: dict[str, Any] = {}
    if isinstance(obj, dict):
        data = obj.get("data", [])
        metadata = obj.get("metadata", {}) if isinstance(obj.get("metadata", {}), dict) else {}
    elif isinstance(obj, list):
        data = obj
    else:
        raise ValueError(f"Unexpected power.json structure in {run.run_id}")
    frame = pd.DataFrame(data)
    required = {"timestamp", "actual_power", "simulated_power"}
    if not required.issubset(frame.columns):
        raise ValueError(
            f"{path} must contain timestamp, actual_power, and simulated_power"
        )
    frame = pd.DataFrame(
        {
            "timestamp": coerce_utc(frame["timestamp"]),
            "observed_power_w": pd.to_numeric(frame["actual_power"], errors="coerce"),
            "simulated_power_w": pd.to_numeric(frame["simulated_power"], errors="coerce"),
        }
    ).dropna()
    frame = (
        frame.sort_values("timestamp")
        .groupby("timestamp", as_index=False)
        .mean(numeric_only=True)
    )
    return frame, metadata


def median_sample_interval_seconds(frame: pd.DataFrame) -> float:
    if len(frame) < 2:
        return math.nan
    differences = frame["timestamp"].sort_values().diff().dt.total_seconds().dropna()
    return float(differences.median()) if not differences.empty else math.nan


def resample_power_pair(frame: pd.DataFrame, interval_seconds: int) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    frame = frame.sort_values("timestamp").dropna().copy()
    start = frame["timestamp"].iloc[0]
    elapsed = (frame["timestamp"] - start).dt.total_seconds().to_numpy(dtype=float)
    end = float(elapsed[-1])
    grid = np.arange(0.0, math.floor(end / interval_seconds) * interval_seconds + 0.1, interval_seconds)
    observed = np.interp(grid, elapsed, frame["observed_power_w"].to_numpy(dtype=float))
    simulated = np.interp(grid, elapsed, frame["simulated_power_w"].to_numpy(dtype=float))
    return pd.DataFrame(
        {
            "timestamp": start + pd.to_timedelta(grid, unit="s"),
            "elapsed_s": grid,
            "observed_power_w": observed,
            "simulated_power_w": simulated,
        }
    )


def smooth_power_pair(
    frame: pd.DataFrame,
    resample_seconds: int,
    smoothing_minutes: float,
) -> tuple[pd.DataFrame, int]:
    window_samples = max(1, int(round(smoothing_minutes * 60.0 / resample_seconds)))
    smoothed = frame.copy()
    for source in ("observed", "simulated"):
        column = f"{source}_power_w"
        smooth_column = f"{source}_power_smooth_w"
        z_column = f"{source}_power_z"
        smoothed[smooth_column] = (
            smoothed[column].rolling(window_samples, center=True, min_periods=1).mean()
        )
        smoothed[z_column] = zscore(smoothed[smooth_column].to_numpy(dtype=float))
    return smoothed, window_samples


def analyse_power_run(
    run: RunInfo,
    resample_seconds: int,
    smoothing_minutes: float,
    segment_minutes: float,
    max_lag_minutes: float,
    minimum_pairs: int,
    minimum_segment_standard_deviation: float,
) -> tuple[dict[str, Any], pd.DataFrame, list[dict[str, Any]]]:
    raw, metadata = load_power_pair(run)
    input_interval = median_sample_interval_seconds(raw)
    aligned = resample_power_pair(raw, resample_seconds)
    processed, smoothing_samples = smooth_power_pair(
        aligned,
        resample_seconds=resample_seconds,
        smoothing_minutes=smoothing_minutes,
    )

    observed = processed["observed_power_smooth_w"].to_numpy(dtype=float)
    simulated = processed["simulated_power_smooth_w"].to_numpy(dtype=float)
    observed_z = processed["observed_power_z"].to_numpy(dtype=float)
    simulated_z = processed["simulated_power_z"].to_numpy(dtype=float)
    segmented_result, segment_rows = segmented_best_lag_correlation(
        run,
        observed_z,
        simulated_z,
        resample_seconds=resample_seconds,
        segment_minutes=segment_minutes,
        max_lag_minutes=max_lag_minutes,
        minimum_pairs=minimum_pairs,
        minimum_segment_standard_deviation=minimum_segment_standard_deviation,
    )

    row = {
        "setting": run.setting,
        "setting_id": run.setting_id,
        "setting_label": run.label,
        "repetition": run.repetition,
        "run_id": run.run_id,
        "input_points": len(raw),
        "aligned_points": len(processed),
        "input_median_interval_s": input_interval,
        "metadata_interval_s": safe_float(metadata.get("interval_seconds")),
        "resample_interval_s": resample_seconds,
        "smoothing_minutes": smoothing_minutes,
        "smoothing_samples": smoothing_samples,
        "mae_w": mae(observed, simulated),
        "smape_pct": smape_pct(observed, simulated),
        "zrmse": rmse(observed_z, simulated_z),
        "segmented_best_lag_r": segmented_result.mean_r,
        "valid_segments": segmented_result.valid_segments,
        "total_complete_segments": segmented_result.total_complete_segments,
        "low_variation_segments": segmented_result.low_variation_segments,
        "invalid_correlation_segments": segmented_result.invalid_correlation_segments,
        "trailing_points": segmented_result.trailing_points,
    }
    return row, processed, segment_rows


# ---------------------------------------------------------------------------
# Physical runtime and processor-domain energy
# ---------------------------------------------------------------------------


def integrate_power_fallback(
    frame: pd.DataFrame,
    time_column: str,
    power_column: str,
) -> float:
    if frame.empty:
        return math.nan
    group_column = "node_name" if "node_name" in frame.columns else None
    groups: Iterable[tuple[Any, pd.DataFrame]]
    groups = frame.groupby(group_column) if group_column else [(None, frame)]
    total = 0.0
    valid_group = False
    for _, group in groups:
        group = group.sort_values(time_column).dropna(subset=[time_column, power_column])
        if len(group) < 2:
            continue
        elapsed = (group[time_column] - group[time_column].iloc[0]).dt.total_seconds().to_numpy(dtype=float)
        power = pd.to_numeric(group[power_column], errors="coerce").to_numpy(dtype=float)
        mask = np.isfinite(elapsed) & np.isfinite(power)
        if mask.sum() < 2:
            continue
        total += float(np.trapz(power[mask], elapsed[mask]))
        valid_group = True
    return total if valid_group else math.nan


def analyse_physical_outcome(
    run: RunInfo,
    physical_tasks: pd.DataFrame,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "setting": run.setting,
        "setting_id": run.setting_id,
        "setting_label": run.label,
        "repetition": run.repetition,
        "run_id": run.run_id,
        "task_count": 0,
        "runtime_s": math.nan,
        "energy_j": math.nan,
        "energy_source": "",
        "monitored_node_count": 0,
        "power_sample_count": 0,
        "negative_energy_increment_count": 0,
        "first_submission_time": "",
        "last_completion_time": "",
    }
    if not {"submission_time", "finish_time"}.issubset(physical_tasks.columns):
        return row
    valid_tasks = physical_tasks.dropna(subset=["submission_time", "finish_time"]).copy()
    if valid_tasks.empty:
        return row
    first_submission = valid_tasks["submission_time"].min()
    last_completion = valid_tasks["finish_time"].max()
    row.update(
        {
            "task_count": int(len(valid_tasks)),
            "runtime_s": float((last_completion - first_submission).total_seconds()),
            "first_submission_time": first_submission.isoformat(),
            "last_completion_time": last_completion.isoformat(),
        }
    )

    path = run.path / "postgres" / "node_power_readings.parquet"
    power = read_parquet(path)
    if "capture_time" not in power.columns:
        return row
    power["capture_time"] = coerce_utc(power["capture_time"])
    if "id" in power.columns:
        power = power.drop_duplicates("id", keep="last")
    clipped = power[
        (power["capture_time"] >= first_submission)
        & (power["capture_time"] <= last_completion)
    ].copy()
    row["power_sample_count"] = int(len(clipped))
    if "node_name" in clipped.columns:
        row["monitored_node_count"] = int(clipped["node_name"].nunique())

    if "energy_usage_j" in clipped.columns:
        energy = pd.to_numeric(clipped["energy_usage_j"], errors="coerce")
        row["negative_energy_increment_count"] = int((energy < 0).sum())
        row["energy_j"] = float(energy.dropna().sum())
        row["energy_source"] = "summed_energy_usage_j"
    elif "power_draw_w" in clipped.columns:
        row["energy_j"] = integrate_power_fallback(
            clipped,
            time_column="capture_time",
            power_column="power_draw_w",
        )
        row["energy_source"] = "integrated_power_draw_w"
    return row


# ---------------------------------------------------------------------------
# Aggregation and principal tables
# ---------------------------------------------------------------------------


def subset_for_setting(frame: pd.DataFrame, setting: int) -> pd.DataFrame:
    """Return rows for one setting, or an empty frame when no setting data exist."""

    if frame.empty or "setting" not in frame.columns:
        return frame.iloc[0:0].copy()
    return frame[frame["setting"] == setting]


def aggregate_metric(frame: pd.DataFrame, column: str) -> tuple[float, float, int]:
    if frame.empty or column not in frame.columns:
        return math.nan, math.nan, 0
    return mean_value(frame[column]), sample_sd(frame[column]), count_value(frame[column])


def build_static_summary(task_metrics: pd.DataFrame, power_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setting in STATIC_SETTINGS:
        task_subset = subset_for_setting(task_metrics, setting)
        power_subset = subset_for_setting(power_metrics, setting)
        row: dict[str, Any] = {
            "setting": setting,
            "setting_id": f"ES{setting}",
            "setting_label": SETTING_LABELS[setting],
        }
        for column, prefix in (
            ("physical_makespan_s", "physical_makespan_s"),
            ("simulated_makespan_s", "simulated_makespan_s"),
            ("makespan_error_pct", "makespan_error_pct"),
            ("completion_nrmse_pct", "completion_nrmse_pct"),
        ):
            mean, sd, count = aggregate_metric(task_subset, column)
            row[f"{prefix}_mean"] = mean
            row[f"{prefix}_sd"] = sd
            row[f"{prefix}_n"] = count
        for column in POWER_METRICS:
            mean, sd, count = aggregate_metric(power_subset, column)
            row[f"{column}_mean"] = mean
            row[f"{column}_sd"] = sd
            row[f"{column}_n"] = count
        row["valid_segments_total"] = int(
            pd.to_numeric(power_subset.get("valid_segments", pd.Series(dtype=float)), errors="coerce")
            .fillna(0)
            .sum()
        )
        row["complete_segments_total"] = int(
            pd.to_numeric(
                power_subset.get("total_complete_segments", pd.Series(dtype=float)),
                errors="coerce",
            )
            .fillna(0)
            .sum()
        )
        rows.append(row)
    return pd.DataFrame(rows)


def relative_change_pct(before: float, after: float) -> float:
    if not math.isfinite(before) or not math.isfinite(after) or before == 0:
        return math.nan
    return 100.0 * (after - before) / before


def build_calibration_summary(power_metrics: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    wide_rows: list[dict[str, Any]] = []
    long_rows: list[dict[str, Any]] = []
    for uncalibrated, calibrated, strategy in CALIBRATION_PAIRS:
        before = subset_for_setting(power_metrics, uncalibrated)
        after = subset_for_setting(power_metrics, calibrated)
        wide: dict[str, Any] = {
            "strategy": strategy,
            "uncalibrated_setting": uncalibrated,
            "uncalibrated_setting_id": f"ES{uncalibrated}",
            "calibrated_setting": calibrated,
            "calibrated_setting_id": f"ES{calibrated}",
        }
        for metric in POWER_METRICS:
            before_mean, before_sd, before_n = aggregate_metric(before, metric)
            after_mean, after_sd, after_n = aggregate_metric(after, metric)
            change = (
                after_mean - before_mean
                if math.isfinite(before_mean) and math.isfinite(after_mean)
                else math.nan
            )
            relative = (
                relative_change_pct(before_mean, after_mean)
                if metric in LOWER_IS_BETTER
                else math.nan
            )
            relative_improvement = -relative if math.isfinite(relative) else math.nan
            absolute_improvement = (
                before_mean - after_mean
                if metric in LOWER_IS_BETTER
                and math.isfinite(before_mean)
                and math.isfinite(after_mean)
                else change
            )
            prefix = metric
            wide.update(
                {
                    f"{prefix}_uncalibrated_mean": before_mean,
                    f"{prefix}_uncalibrated_sd": before_sd,
                    f"{prefix}_uncalibrated_n": before_n,
                    f"{prefix}_calibrated_mean": after_mean,
                    f"{prefix}_calibrated_sd": after_sd,
                    f"{prefix}_calibrated_n": after_n,
                    f"{prefix}_calibrated_minus_uncalibrated": change,
                    f"{prefix}_relative_change_pct": relative,
                    f"{prefix}_relative_improvement_pct_positive_is_better": relative_improvement,
                    f"{prefix}_absolute_improvement_positive_is_better": absolute_improvement,
                }
            )
            long_rows.append(
                {
                    "strategy": strategy,
                    "metric": metric,
                    "direction": "lower_is_better" if metric in LOWER_IS_BETTER else "higher_is_better",
                    "uncalibrated_setting": uncalibrated,
                    "calibrated_setting": calibrated,
                    "uncalibrated_mean": before_mean,
                    "uncalibrated_sd": before_sd,
                    "uncalibrated_n": before_n,
                    "calibrated_mean": after_mean,
                    "calibrated_sd": after_sd,
                    "calibrated_n": after_n,
                    "calibrated_minus_uncalibrated": change,
                    "relative_change_pct": relative,
                    "relative_improvement_pct_positive_is_better": relative_improvement,
                    "absolute_improvement_positive_is_better": absolute_improvement,
                }
            )
        wide_rows.append(wide)
    return pd.DataFrame(wide_rows), pd.DataFrame(long_rows)


def build_closed_loop_summary(physical_outcomes: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    means: dict[tuple[int, str], float] = {}
    for setting in DECISION_SETTINGS:
        subset = subset_for_setting(physical_outcomes, setting)
        for metric in ("runtime_s", "energy_j"):
            means[(setting, metric)] = aggregate_metric(subset, metric)[0]

    for setting in DECISION_SETTINGS:
        subset = subset_for_setting(physical_outcomes, setting)
        runtime_mean, runtime_sd, runtime_n = aggregate_metric(subset, "runtime_s")
        energy_mean, energy_sd, energy_n = aggregate_metric(subset, "energy_j")
        baseline = MATCHING_BASELINE[setting]
        runtime_change = relative_change_pct(means[(baseline, "runtime_s")], runtime_mean)
        energy_change = relative_change_pct(means[(baseline, "energy_j")], energy_mean)
        rows.append(
            {
                "setting": setting,
                "setting_id": f"ES{setting}",
                "setting_label": SETTING_LABELS[setting],
                "calibration_condition": "enabled" if setting >= 8 else "disabled",
                "matching_baseline_setting": baseline,
                "matching_baseline_setting_id": f"ES{baseline}",
                "runtime_mean_s": runtime_mean,
                "runtime_sd_s": runtime_sd,
                "runtime_n": runtime_n,
                "runtime_mean_h": runtime_mean / 3600.0 if math.isfinite(runtime_mean) else math.nan,
                "runtime_sd_h": runtime_sd / 3600.0 if math.isfinite(runtime_sd) else math.nan,
                "runtime_change_vs_baseline_pct": runtime_change,
                "energy_mean_j": energy_mean,
                "energy_sd_j": energy_sd,
                "energy_n": energy_n,
                "energy_mean_mj": energy_mean / 1e6 if math.isfinite(energy_mean) else math.nan,
                "energy_sd_mj": energy_sd / 1e6 if math.isfinite(energy_sd) else math.nan,
                "energy_change_vs_baseline_pct": energy_change,
            }
        )
    return pd.DataFrame(rows)


def build_task_diagnostic_summary(diagnostics: pd.DataFrame) -> pd.DataFrame:
    if diagnostics.empty:
        return diagnostics
    rows: list[dict[str, Any]] = []
    for (setting, metric), subset in diagnostics.groupby(["setting", "metric"]):
        row: dict[str, Any] = {
            "setting": setting,
            "setting_id": f"ES{setting}",
            "setting_label": SETTING_LABELS.get(int(setting), ""),
            "metric": metric,
            "runs": int(subset["run_id"].nunique()),
        }
        for column in (
            "physical_median_s",
            "simulated_median_s",
            "physical_p95_s",
            "simulated_p95_s",
            "simulated_zero_fraction",
        ):
            mean, sd, _ = aggregate_metric(subset, column)
            row[f"{column}_mean"] = mean
            row[f"{column}_sd"] = sd
        rows.append(row)
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Figure data and plots
# ---------------------------------------------------------------------------


def aggregate_completion_figure_data(
    runs_by_setting: Mapping[int, Sequence[RunInfo]],
    task_curve_cache: Mapping[str, dict[str, np.ndarray]],
    completion_bin_seconds: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setting in STATIC_SETTINGS:
        runs = [run for run in runs_by_setting.get(setting, []) if run.run_id in task_curve_cache]
        if not runs:
            continue
        maxima: list[float] = []
        for run in runs:
            data = task_curve_cache[run.run_id]
            for key in ("physical_completion_s", "simulated_completion_s"):
                values = data.get(key, np.asarray([], dtype=float))
                if len(values):
                    maxima.append(float(np.max(values)))
        if not maxima:
            continue
        maximum = max(maxima)
        grid = np.arange(
            0.0,
            math.ceil(maximum / completion_bin_seconds) * completion_bin_seconds
            + completion_bin_seconds,
            completion_bin_seconds,
        )
        physical_curves: list[np.ndarray] = []
        simulated_curves: list[np.ndarray] = []
        for run in runs:
            data = task_curve_cache[run.run_id]
            physical = data.get("physical_completion_s", np.asarray([], dtype=float))
            simulated = data.get("simulated_completion_s", np.asarray([], dtype=float))
            if len(physical):
                physical_curves.append(cumulative_counts(physical, grid))
            if len(simulated):
                simulated_curves.append(cumulative_counts(simulated, grid))
        for source, curves in (
            ("observed", physical_curves),
            ("simulated", simulated_curves),
        ):
            if not curves:
                continue
            matrix = np.vstack(curves)
            means = np.mean(matrix, axis=0)
            standard_deviations = (
                np.std(matrix, axis=0, ddof=1) if len(matrix) > 1 else np.zeros(len(grid))
            )
            for index, elapsed in enumerate(grid):
                rows.append(
                    {
                        "setting": setting,
                        "setting_id": f"ES{setting}",
                        "source": source,
                        "elapsed_s": elapsed,
                        "mean_completed_jobs": means[index],
                        "sd_completed_jobs": standard_deviations[index],
                        "runs": len(matrix),
                    }
                )
    return pd.DataFrame(rows)


def aggregate_power_figure_data(
    runs_by_setting: Mapping[int, Sequence[RunInfo]],
    power_series_cache: Mapping[str, pd.DataFrame],
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for setting in STATIC_SETTINGS:
        frames = [
            power_series_cache[run.run_id]
            for run in runs_by_setting.get(setting, [])
            if run.run_id in power_series_cache and not power_series_cache[run.run_id].empty
        ]
        if not frames:
            continue
        common_length = min(len(frame) for frame in frames)
        elapsed = frames[0]["elapsed_s"].to_numpy(dtype=float)[:common_length]
        for measure, observed_column, simulated_column in (
            (
                "absolute",
                "observed_power_smooth_w",
                "simulated_power_smooth_w",
            ),
            ("normalised", "observed_power_z", "simulated_power_z"),
        ):
            for source, column in (
                ("observed", observed_column),
                ("simulated", simulated_column),
            ):
                matrix = np.vstack(
                    [frame[column].to_numpy(dtype=float)[:common_length] for frame in frames]
                )
                means = np.mean(matrix, axis=0)
                standard_deviations = (
                    np.std(matrix, axis=0, ddof=1)
                    if len(matrix) > 1
                    else np.zeros(common_length)
                )
                for index, elapsed_s in enumerate(elapsed):
                    rows.append(
                        {
                            "setting": setting,
                            "setting_id": f"ES{setting}",
                            "measure": measure,
                            "source": source,
                            "elapsed_s": elapsed_s,
                            "mean": means[index],
                            "sd": standard_deviations[index],
                            "runs": len(matrix),
                        }
                    )
    return pd.DataFrame(rows)


def plot_static_task_progress(
    figure_data: pd.DataFrame,
    out_dir: Path,
    figure_format: str,
    dpi: int,
) -> None:
    if figure_data.empty:
        logging.warning("Skipping static task-progress figure because no curve data are available")
        return
    fig, axes = plt.subplots(1, 2, figsize=(11.6, 4.1), sharey=True, constrained_layout=True)
    for ax, setting in zip(axes, STATIC_SETTINGS):
        subset = figure_data[figure_data["setting"] == setting]
        for source, label, colour, linestyle in (
            ("observed", "Physical execution", OBSERVED_COLOUR, "-"),
            ("simulated", "OpenDC", SIMULATED_COLOUR, "--"),
        ):
            series = subset[subset["source"] == source].sort_values("elapsed_s")
            if series.empty:
                continue
            x = series["elapsed_s"].to_numpy(dtype=float) / 3600.0
            mean = series["mean_completed_jobs"].to_numpy(dtype=float)
            sd = series["sd_completed_jobs"].to_numpy(dtype=float)
            ax.plot(x, mean, label=label, color=colour, linestyle=linestyle, linewidth=1.9)
            if np.any(sd > 0):
                ax.fill_between(x, mean - sd, mean + sd, color=colour, alpha=0.14, linewidth=0)
        ax.set_title(f"ES{setting}: {SETTING_LABELS[setting]}")
        ax.set_xlabel("Elapsed time since first Job submission (h)")
        style_axis(ax)
    axes[0].set_ylabel("Cumulative completed Jobs")
    axes[1].legend(loc="lower right")
    save_figure(fig, out_dir, "static_task_progress", figure_format, dpi)


def plot_static_power_fidelity(
    figure_data: pd.DataFrame,
    out_dir: Path,
    figure_format: str,
    dpi: int,
) -> None:
    if figure_data.empty:
        logging.warning("Skipping static power-fidelity figures because no power data are available")
        return

    for measure, figure_name, ylabel in (
        ("absolute", "static_power_magnitude", "Power (W)"),
        ("normalised", "static_power_trend", "Standardized power"),
    ):
        fig, axes = plt.subplots(1, 2, figsize=(11.8, 4.7), sharex=True, constrained_layout=True)
        for ax, setting in zip(axes, STATIC_SETTINGS):
            subset = figure_data[
                (figure_data["setting"] == setting)
                & (figure_data["measure"] == measure)
            ]
            for source, label, colour, linestyle in (
                (
                    "observed",
                    "Observed processor-domain power" if measure == "absolute" else "Observed processor-domain power, standardized",
                    OBSERVED_COLOUR,
                    "-",
                ),
                (
                    "simulated",
                    "Simulated processor power" if measure == "absolute" else "Simulated processor power, standardized",
                    SIMULATED_COLOUR,
                    "--",
                ),
            ):
                series = subset[subset["source"] == source].sort_values("elapsed_s")
                if series.empty:
                    continue
                x = series["elapsed_s"].to_numpy(dtype=float) / 3600.0
                mean = series["mean"].to_numpy(dtype=float)
                sd = series["sd"].to_numpy(dtype=float)
                ax.plot(x, mean, label=label, color=colour, linestyle=linestyle, linewidth=1.8)
                if np.any(sd > 0):
                    ax.fill_between(x, mean - sd, mean + sd, color=colour, alpha=0.12, linewidth=0)
            ax.set_title(f"ES{setting}: {SETTING_LABELS[setting]}")
            ax.set_xlabel("Elapsed time (h)")
            ax.set_ylabel(ylabel)
            if measure == "normalised":
                ax.axhline(0.0, linewidth=0.8, color="black", alpha=0.35)
            style_axis(ax)
        axes[0].legend(loc="lower left")
        save_figure(fig, out_dir, figure_name, figure_format, dpi)


def calibration_plot_data(power_metrics: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for pair_index, (uncalibrated, calibrated, strategy) in enumerate(CALIBRATION_PAIRS):
        for condition, setting in (("Uncalibrated", uncalibrated), ("Calibrated", calibrated)):
            subset = subset_for_setting(power_metrics, setting)
            for metric in ("smape_pct", "zrmse"):
                mean, sd, count = aggregate_metric(subset, metric)
                rows.append(
                    {
                        "pair_index": pair_index,
                        "strategy": strategy,
                        "condition": condition,
                        "setting": setting,
                        "setting_id": f"ES{setting}",
                        "metric": metric,
                        "mean": mean,
                        "sd": sd,
                        "n": count,
                    }
                )
    return pd.DataFrame(rows)


# def plot_calibration_effect(
#     plot_data: pd.DataFrame,
#     power_metrics: pd.DataFrame,
#     out_dir: Path,
#     figure_format: str,
#     dpi: int,
# ) -> None:
#     if plot_data.empty or not pd.to_numeric(plot_data.get("mean"), errors="coerce").notna().any():
#         logging.warning("Skipping calibration figures because no power metrics are available")
#         return
#     strategy_labels = [pair[2] for pair in CALIBRATION_PAIRS]
#     x = np.arange(len(strategy_labels), dtype=float)
#     width = 0.36
#     for metric, ylabel, title, figure_name in (
#         ("smape_pct", "sMAPE (%)", "Power-magnitude error", "calibration_power_smape"),
#         ("zrmse", "zRMSE", "Globally aligned trend error", "calibration_power_zrmse"),
#     ):
#         fig, ax = plt.subplots(figsize=(8.1, 4.9), constrained_layout=True)
#         for condition, offset, colour, hatch in (
#             ("Uncalibrated", -width / 2, UNCALIBRATED_COLOUR, ""),
#             ("Calibrated", width / 2, CALIBRATED_COLOUR, ""),
#         ):
#             subset = plot_data[
#                 (plot_data["metric"] == metric)
#                 & (plot_data["condition"] == condition)
#             ].sort_values("pair_index")
#             values = subset["mean"].to_numpy(dtype=float)
#             errors = np.asarray([errorbar_value(value) for value in subset["sd"]], dtype=float)
#             ax.bar(
#                 x + offset,
#                 values,
#                 width,
#                 yerr=errors,
#                 capsize=3,
#                 label=condition,
#                 color=colour,
#                 alpha=0.85,
#                 hatch=hatch,
#                 linewidth=0.6,
#                 edgecolor="black",
#             )
#         ax.set_xticks(x)
#         ax.set_xticklabels(strategy_labels, rotation=18, ha="right")
#         ax.set_ylabel(ylabel)
#         # ax.set_title(title)
#         ax.legend(loc="upper left")
#         style_axis(ax)
#         save_figure(fig, out_dir, figure_name, figure_format, dpi)


def plot_calibration_effect(
    plot_data: pd.DataFrame,
    power_metrics: pd.DataFrame,
    out_dir: Path,
    figure_format: str,
    dpi: int,
) -> None:
    del power_metrics

    if (
        plot_data.empty
        or not pd.to_numeric(
            plot_data.get("mean"),
            errors="coerce",
        ).notna().any()
    ):
        logging.warning(
            "Skipping calibration figures because no power metrics are available"
        )
        return

    # Keep matched bars close together and reduce the overall figure width.
    pair_spacing = 1.80
    bar_offset = 0.40
    bar_width = 0.70
    horizontal_padding = 0.65

    legend_handles = [
        Patch(
            facecolor=UNCALIBRATED_COLOUR,
            edgecolor="black",
            alpha=0.85,
            label="Calibration disabled",
        ),
        Patch(
            facecolor=CALIBRATED_COLOUR,
            edgecolor="black",
            alpha=0.85,
            label="Calibration enabled",
        ),
    ]

    for metric, ylabel, figure_name in (
        ("smape_pct", "sMAPE (%)", "calibration_power_smape"),
        ("zrmse", "zRMSE", "calibration_power_zrmse"),
    ):

        pair_centres = (
            np.arange(len(CALIBRATION_PAIRS), dtype=float)
            * pair_spacing
        )

        positions = []
        values = []
        errors = []
        colours = []
        setting_labels = []

        for pair_index, (
            uncal_setting,
            cal_setting,
            _,
        ) in enumerate(CALIBRATION_PAIRS):

            pair_centre = pair_centres[pair_index]

            for setting, condition, offset, colour in (
                (
                    uncal_setting,
                    "Uncalibrated",
                    -bar_offset,
                    UNCALIBRATED_COLOUR,
                ),
                (
                    cal_setting,
                    "Calibrated",
                    bar_offset,
                    CALIBRATED_COLOUR,
                ),
            ):
                row = plot_data[
                    (plot_data["metric"] == metric)
                    & (plot_data["condition"] == condition)
                    & (plot_data["pair_index"] == pair_index)
                ]

                if row.empty:
                    mean_value = np.nan
                    sd_value = np.nan
                else:
                    mean_value = float(row.iloc[0]["mean"])
                    sd_value = float(row.iloc[0]["sd"])

                positions.append(pair_centre + offset)
                values.append(mean_value)
                errors.append(errorbar_value(sd_value))
                colours.append(colour)
                setting_labels.append(f"ES{setting}")

        fig, ax = plt.subplots(figsize=(10.8, 5.3))

        ax.bar(
            positions,
            values,
            width=bar_width,
            yerr=errors,
            capsize=3,
            color=colours,
            alpha=0.85,
            linewidth=0.6,
            edgecolor="black",
        )

        ax.set_xticks(positions)
        ax.set_xticklabels(
            setting_labels,
            rotation=0,
            ha="center",
            fontsize=9,
        )

        pair_labels = [
            "Eight-worker static",
            "Four-worker static",
            "Pending-Pod baseline",
            "Runtime-oriented\npolicy",
            "Power-oriented\npolicy",
        ]

        for centre, label in zip(pair_centres, pair_labels):
            ax.text(
                centre,
                -0.105,
                label,
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="top",
                fontsize=9,
            )

        for pair_index in range(len(CALIBRATION_PAIRS) - 1):
            separator = (
                pair_centres[pair_index]
                + pair_spacing / 2
            )
            ax.axvline(
                separator,
                color="black",
                linestyle=":",
                linewidth=0.8,
                alpha=0.25,
            )

        ax.set_ylabel(ylabel)
        ax.legend(
            handles=legend_handles,
            loc="upper left",
        )

        ax.set_xlim(
            min(positions) - horizontal_padding,
            max(positions) + horizontal_padding,
        )

        style_axis(ax)

        fig.subplots_adjust(
            left=0.08,
            right=0.99,
            top=0.97,
            bottom=0.25,
        )

        save_figure(
            fig,
            out_dir,
            figure_name,
            figure_format,
            dpi,
        )


def closed_loop_plot_data(summary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for _, row in summary.iterrows():
        for metric, mean_column, sd_column in (
            ("runtime_h", "runtime_mean_h", "runtime_sd_h"),
            ("energy_mj", "energy_mean_mj", "energy_sd_mj"),
        ):
            rows.append(
                {
                    "setting": int(row["setting"]),
                    "setting_id": row["setting_id"],
                    "setting_label": row["setting_label"],
                    "calibration_condition": row["calibration_condition"],
                    "metric": metric,
                    "mean": row[mean_column],
                    "sd": row[sd_column],
                }
            )
    return pd.DataFrame(rows)


# def plot_closed_loop_outcomes(
#     summary: pd.DataFrame,
#     physical_outcomes: pd.DataFrame,
#     out_dir: Path,
#     figure_format: str,
#     dpi: int,
# ) -> None:
#     del summary
#     available = pd.to_numeric(
#         physical_outcomes.get("runtime_s", pd.Series(dtype=float)), errors="coerce"
#     ).notna().any() or pd.to_numeric(
#         physical_outcomes.get("energy_j", pd.Series(dtype=float)), errors="coerce"
#     ).notna().any()
#     if physical_outcomes.empty or not available:
#         logging.warning("Skipping closed-loop outcome figures because no physical outcomes are available")
#         return

#     ordered_settings = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
#     positions = np.array([0, 1, 2, 3, 4, 6, 7, 8, 9, 10], dtype=float)
#     labels = [SETTING_SHORT_LABELS[setting] for setting in ordered_settings]
#     legend_handles = [
#         Patch(facecolor=UNCALIBRATED_COLOUR, edgecolor="black", alpha=0.84, label="Calibration disabled"),
#         Patch(facecolor=CALIBRATED_COLOUR, edgecolor="black", alpha=0.84, label="Calibration enabled"),
#     ]

#     summary_rows: list[dict[str, Any]] = []
#     for setting in ordered_settings:
#         subset = subset_for_setting(physical_outcomes, setting)
#         runtime_mean, runtime_sd, _ = aggregate_metric(subset, "runtime_s")
#         energy_mean, energy_sd, _ = aggregate_metric(subset, "energy_j")
#         summary_rows.append(
#             {
#                 "setting": setting,
#                 "runtime_mean_h": runtime_mean / 3600.0 if math.isfinite(runtime_mean) else math.nan,
#                 "runtime_sd_h": runtime_sd / 3600.0 if math.isfinite(runtime_sd) else math.nan,
#                 "energy_mean_mj": energy_mean / 1e6 if math.isfinite(energy_mean) else math.nan,
#                 "energy_sd_mj": energy_sd / 1e6 if math.isfinite(energy_sd) else math.nan,
#             }
#         )
#     ordered = pd.DataFrame(summary_rows).set_index("setting").reindex(ordered_settings)

#     for mean_column, sd_column, ylabel, title, figure_name in (
#         ("runtime_mean_h", "runtime_sd_h", "Physical workload runtime (h)", "Runtime", "closed_loop_runtime"),
#         ("energy_mean_mj", "energy_sd_mj", "RAPL-derived processor-domain energy (MJ)", "Processor-domain energy", "closed_loop_energy"),
#     ):
#         fig, ax = plt.subplots(figsize=(13.2, 5.2), constrained_layout=True)
#         means = ordered[mean_column].to_numpy(dtype=float)
#         errors = np.asarray([errorbar_value(value) for value in ordered[sd_column]], dtype=float)
#         colours = [UNCALIBRATED_COLOUR if setting < 6 else CALIBRATED_COLOUR for setting in ordered_settings]
#         ax.bar(
#             positions,
#             means,
#             yerr=errors,
#             capsize=3,
#             color=colours,
#             alpha=0.84,
#             edgecolor="black",
#             linewidth=0.6,
#         )
#         ax.axvline(5.0, color="black", linestyle=":", linewidth=1.0, alpha=0.7)
#         ax.set_xticks(positions)
#         ax.set_xticklabels(labels)
#         ax.set_ylabel(ylabel)
#         # ax.set_title(title)
#         ax.legend(handles=legend_handles, loc="upper center", bbox_to_anchor=(0.5, 1.13), ncol=2, frameon=True)
#         style_axis(ax)
#         save_figure(fig, out_dir, figure_name, figure_format, dpi)


def plot_closed_loop_outcomes(
    summary: pd.DataFrame,
    physical_outcomes: pd.DataFrame,
    out_dir: Path,
    figure_format: str,
    dpi: int,
) -> None:
    del summary

    runtime_available = pd.to_numeric(
        physical_outcomes.get(
            "runtime_s",
            pd.Series(dtype=float),
        ),
        errors="coerce",
    ).notna().any()

    energy_available = pd.to_numeric(
        physical_outcomes.get(
            "energy_j",
            pd.Series(dtype=float),
        ),
        errors="coerce",
    ).notna().any()

    if physical_outcomes.empty or not (
        runtime_available or energy_available
    ):
        logging.warning(
            "Skipping physical-outcome figures because no "
            "physical outcomes are available"
        )
        return

    # Each tuple is:
    # uncalibrated setting, calibrated setting, displayed strategy name
    outcome_pairs = (
        (1, 6, "Eight-worker static"),
        (2, 7, "Four-worker static"),
        (3, 8, "Pending-Pod baseline"),
        (4, 9, "Runtime-oriented\npolicy"),
        (5, 10, "Power-oriented\npolicy"),
    )

    # Match the compact spacing used by the calibration figures.
    pair_spacing = 1.80
    bar_offset = 0.40
    bar_width = 0.70
    horizontal_padding = 0.65

    pair_centres = (
        np.arange(len(outcome_pairs), dtype=float)
        * pair_spacing
    )

    legend_handles = [
        Patch(
            facecolor=UNCALIBRATED_COLOUR,
            edgecolor="black",
            alpha=0.84,
            label="Calibration disabled",
        ),
        Patch(
            facecolor=CALIBRATED_COLOUR,
            edgecolor="black",
            alpha=0.84,
            label="Calibration enabled",
        ),
    ]

    for metric, divisor, ylabel, figure_name in (
        (
            "runtime_s",
            3600.0,
            "Physical workload runtime (h)",
            "closed_loop_runtime",
        ),
        (
            "energy_j",
            1e6,
            "RAPL-derived processor-domain energy (MJ)",
            "closed_loop_energy",
        ),
    ):
        positions: list[float] = []
        means: list[float] = []
        errors: list[float] = []
        colours: list[str] = []
        setting_labels: list[str] = []

        for pair_index, (
            uncalibrated_setting,
            calibrated_setting,
            _,
        ) in enumerate(outcome_pairs):
            pair_centre = pair_centres[pair_index]

            for setting, offset, colour in (
                (
                    uncalibrated_setting,
                    -bar_offset,
                    UNCALIBRATED_COLOUR,
                ),
                (
                    calibrated_setting,
                    bar_offset,
                    CALIBRATED_COLOUR,
                ),
            ):
                subset = subset_for_setting(
                    physical_outcomes,
                    setting,
                )

                mean_value, sd_value, _ = aggregate_metric(
                    subset,
                    metric,
                )

                if math.isfinite(mean_value):
                    mean_value /= divisor

                if math.isfinite(sd_value):
                    sd_value /= divisor

                positions.append(pair_centre + offset)
                means.append(mean_value)
                errors.append(errorbar_value(sd_value))
                colours.append(colour)
                setting_labels.append(f"ES{setting}")

        fig, ax = plt.subplots(
            figsize=(10.8, 5.4),
        )

        ax.bar(
            positions,
            means,
            width=bar_width,
            yerr=errors,
            capsize=3,
            color=colours,
            alpha=0.84,
            edgecolor="black",
            linewidth=0.6,
        )

        # ES identifiers directly below individual bars.
        ax.set_xticks(positions)
        ax.set_xticklabels(
            setting_labels,
            rotation=0,
            ha="center",
            fontsize=9,
        )

        # One strategy label centred below each pair.
        for pair_centre, (_, _, pair_label) in zip(
            pair_centres,
            outcome_pairs,
        ):
            ax.text(
                pair_centre,
                -0.105,
                pair_label,
                transform=ax.get_xaxis_transform(),
                ha="center",
                va="top",
                fontsize=9,
            )

        # Visual separators between matched pairs.
        for pair_index in range(len(outcome_pairs) - 1):
            separator = (
                pair_centres[pair_index]
                + pair_spacing / 2
            )

            ax.axvline(
                separator,
                color="black",
                linestyle=":",
                linewidth=0.8,
                alpha=0.25,
            )

        ax.set_ylabel(ylabel)

        ax.legend(
            handles=legend_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, 1.12),
            ncol=2,
            frameon=True,
        )

        ax.set_xlim(
            min(positions) - horizontal_padding,
            max(positions) + horizontal_padding,
        )

        style_axis(ax)

        fig.subplots_adjust(
            left=0.08,
            right=0.99,
            top=0.88,
            bottom=0.24,
        )

        save_figure(
            fig,
            out_dir,
            figure_name,
            figure_format,
            dpi,
        )


def plot_runtime_energy_tradeoff(
    summary: pd.DataFrame,
    out_dir: Path,
    figure_format: str,
    dpi: int,
) -> None:
    finite_points = summary[
        pd.to_numeric(summary.get("runtime_mean_h"), errors="coerce").notna()
        & pd.to_numeric(summary.get("energy_mean_mj"), errors="coerce").notna()
    ] if not summary.empty else summary
    if finite_points.empty:
        logging.warning("Skipping runtime--energy figure because no physical outcomes are available")
        return
    fig, ax = plt.subplots(figsize=(7.4, 5.6), constrained_layout=True)
    for _, row in finite_points.iterrows():
        setting = int(row["setting"])
        marker = "o" if setting < 8 else "s"
        colour = UNCALIBRATED_COLOUR if setting < 8 else CALIBRATED_COLOUR
        ax.errorbar(
            row["runtime_mean_h"],
            row["energy_mean_mj"],
            xerr=errorbar_value(row["runtime_sd_h"]),
            yerr=errorbar_value(row["energy_sd_mj"]),
            marker=marker,
            markersize=7,
            linestyle="none",
            color=colour,
            ecolor=colour,
            capsize=3,
        )
        ax.annotate(
            row["setting_id"],
            (row["runtime_mean_h"], row["energy_mean_mj"]),
            xytext=(5, 5),
            textcoords="offset points",
            fontsize=9,
        )
    ax.set_xlabel("Physical workload runtime (h)")
    ax.set_ylabel("RAPL-derived processor-domain energy (MJ)")
    ax.set_title("Runtime--energy trade-off")
    style_axis(ax)
    handles = [
        plt.Line2D([], [], marker="o", linestyle="none", color=UNCALIBRATED_COLOUR, label="Calibration disabled"),
        plt.Line2D([], [], marker="s", linestyle="none", color=CALIBRATED_COLOUR, label="Calibration enabled"),
    ]
    ax.legend(handles=handles, loc="best")
    save_figure(fig, out_dir, "runtime_energy_tradeoff", figure_format, dpi)


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------


def parse_int_range_list(value: str) -> list[int]:
    output: list[int] = []
    for token in value.split(","):
        token = token.strip()
        if not token:
            continue
        if "-" in token:
            start_text, end_text = token.split("-", 1)
            start = int(start_text)
            end = int(end_text)
            output.extend(range(start, end + 1))
        else:
            output.append(int(token))
    return sorted(set(output))


def analyse_runs(args: argparse.Namespace) -> int:
    out_dir: Path = args.out
    figures_dir = out_dir / "figures"
    tables_dir = out_dir / "tables"
    data_dir = out_dir / "data"
    diagnostics_dir = out_dir / "diagnostics"
    for directory in (out_dir, figures_dir, tables_dir, data_dir, diagnostics_dir):
        ensure_dir(directory)

    input_root = resolve_input_root(args.root, out_dir, args.refresh_cache)
    runs = discover_runs(input_root)
    if not runs:
        logging.error("No run folders were found. Expected names such as 1_run_1 and 10_run_3.")
        return 1

    expected_settings = parse_int_range_list(args.expected_settings)
    expected_repetitions = parse_int_range_list(args.expected_repetitions)
    inventory = build_inventory(runs, expected_settings, expected_repetitions)
    save_csv(inventory, out_dir / "run_inventory.csv")
    missing = inventory[inventory["status"] == "missing"]
    if not missing.empty:
        logging.warning(
            "The input contains %d of %d expected runs. Missing runs are recorded in run_inventory.csv.",
            len(runs),
            len(expected_settings) * len(expected_repetitions),
        )

    runs_by_setting: dict[int, list[RunInfo]] = {}
    for run in runs:
        runs_by_setting.setdefault(run.setting, []).append(run)
    for setting in runs_by_setting:
        runs_by_setting[setting].sort(key=lambda run: run.repetition)

    issues = IssueCollector()
    task_rows: list[dict[str, Any]] = []
    timing_rows: list[dict[str, Any]] = []
    power_rows: list[dict[str, Any]] = []
    physical_rows: list[dict[str, Any]] = []
    segment_rows: list[dict[str, Any]] = []
    task_curve_cache: dict[str, dict[str, np.ndarray]] = {}
    power_series_cache: dict[str, pd.DataFrame] = {}

    for run in runs:
        logging.info("Analysing %s", run.run_id)
        physical_tasks = pd.DataFrame()
        try:
            physical_tasks = load_physical_tasks(run)
            physical_finished = int(
                physical_tasks.dropna(subset=["submission_time", "finish_time"]).shape[0]
            )
            if physical_finished != args.expected_task_count:
                issues.add(
                    run.run_id,
                    "physical_tasks",
                    f"Found {physical_finished} completed task records, expected {args.expected_task_count}",
                )
        except Exception as exc:
            issues.add(run.run_id, "physical_tasks", str(exc), severity="error")

        simulated_tasks = pd.DataFrame()
        simulator_path: Optional[Path] = None
        if not physical_tasks.empty:
            try:
                expected_count = int(
                    physical_tasks.dropna(subset=["submission_time", "finish_time"]).shape[0]
                )
                simulated_tasks, simulator_path = load_simulated_completed_tasks(
                    run,
                    proposal_filter=args.proposal_filter,
                    expected_task_count=expected_count,
                )
                if simulator_path is None:
                    issues.add(run.run_id, "simulator_tasks", "No matching OpenDC task.parquet was found", severity="error")
                elif len(simulated_tasks) != expected_count:
                    issues.add(
                        run.run_id,
                        "simulator_tasks",
                        f"Selected OpenDC output contains {len(simulated_tasks)} completed tasks, expected {expected_count}",
                    )
            except Exception as exc:
                issues.add(run.run_id, "simulator_tasks", str(exc), severity="error")

        if not physical_tasks.empty:
            try:
                task_row, diagnostic, curve_data = analyse_task_run(
                    run,
                    physical_tasks,
                    simulated_tasks,
                    simulator_path,
                    completion_bin_seconds=args.completion_bin_seconds,
                    expected_task_count=args.expected_task_count,
                )
                task_rows.append(task_row)
                timing_rows.extend(diagnostic)
                task_curve_cache[run.run_id] = curve_data
            except Exception as exc:
                issues.add(run.run_id, "task_metrics", str(exc), severity="error")

            try:
                physical_row = analyse_physical_outcome(run, physical_tasks)
                physical_rows.append(physical_row)
                if physical_row["monitored_node_count"] not in (0, args.expected_node_count):
                    issues.add(
                        run.run_id,
                        "physical_energy",
                        f"Found {physical_row['monitored_node_count']} monitored worker nodes, expected {args.expected_node_count}",
                    )
                if physical_row["negative_energy_increment_count"] > 0:
                    issues.add(
                        run.run_id,
                        "physical_energy",
                        f"Found {physical_row['negative_energy_increment_count']} negative energy increments",
                    )
            except Exception as exc:
                issues.add(run.run_id, "physical_energy", str(exc), severity="error")

        try:
            power_row, power_series, run_segment_rows = analyse_power_run(
                run,
                resample_seconds=args.power_resample_seconds,
                smoothing_minutes=args.power_smoothing_minutes,
                segment_minutes=args.trend_segment_minutes,
                max_lag_minutes=args.trend_max_lag_minutes,
                minimum_pairs=args.trend_minimum_pairs,
                minimum_segment_standard_deviation=args.trend_minimum_standard_deviation,
            )
            power_rows.append(power_row)
            power_series_cache[run.run_id] = power_series
            segment_rows.extend(run_segment_rows)
            if math.isfinite(power_row["input_median_interval_s"]) and not math.isclose(
                power_row["input_median_interval_s"],
                args.power_resample_seconds,
                rel_tol=0.0,
                abs_tol=1.0,
            ):
                issues.add(
                    run.run_id,
                    "power_sampling",
                    f"Input median interval is {power_row['input_median_interval_s']:.1f} s; analysis resamples to {args.power_resample_seconds} s",
                )
            if power_row["valid_segments"] == 0:
                issues.add(run.run_id, "trend_metric", "No valid fixed-duration trend segments were retained")
        except Exception as exc:
            issues.add(run.run_id, "power_metrics", str(exc), severity="error")

    task_metrics = pd.DataFrame(task_rows)
    timing_diagnostics = pd.DataFrame(timing_rows)
    power_metrics = pd.DataFrame(power_rows)
    physical_outcomes = pd.DataFrame(physical_rows)
    trend_segments = pd.DataFrame(segment_rows)

    if not task_metrics.empty:
        save_csv(task_metrics, data_dir / "task_fidelity_per_run.csv")
    if not timing_diagnostics.empty:
        save_csv(timing_diagnostics, diagnostics_dir / "task_timing_diagnostics_per_run.csv")
        timing_summary = build_task_diagnostic_summary(timing_diagnostics)
        save_csv(timing_summary, diagnostics_dir / "task_timing_diagnostics_summary.csv")
    if not power_metrics.empty:
        save_csv(power_metrics, data_dir / "power_fidelity_per_run.csv")
    if not physical_outcomes.empty:
        save_csv(physical_outcomes, data_dir / "physical_outcomes_per_run.csv")
    if not trend_segments.empty:
        save_csv(trend_segments, diagnostics_dir / "trend_segments_per_run.csv")
    if issues.rows:
        save_csv(issues.dataframe(), diagnostics_dir / "analysis_issues.csv")

    static_summary = build_static_summary(task_metrics, power_metrics)
    calibration_summary, calibration_long = build_calibration_summary(power_metrics)
    closed_loop_summary = build_closed_loop_summary(physical_outcomes)
    save_csv(static_summary, tables_dir / "static_fidelity_summary.csv")
    save_csv(calibration_summary, tables_dir / "calibration_fidelity_summary.csv")
    save_csv(calibration_long, data_dir / "calibration_fidelity_summary_long.csv")
    save_csv(closed_loop_summary, tables_dir / "closed_loop_outcomes_summary.csv")

    completion_figure_data = aggregate_completion_figure_data(
        runs_by_setting,
        task_curve_cache,
        completion_bin_seconds=args.completion_bin_seconds,
    )
    power_figure_data = aggregate_power_figure_data(runs_by_setting, power_series_cache)
    calibration_figure_data = calibration_plot_data(power_metrics)
    closed_loop_figure_data = closed_loop_plot_data(closed_loop_summary)
    if not completion_figure_data.empty:
        save_csv(completion_figure_data, data_dir / "static_task_progress_figure_data.csv")
    if not power_figure_data.empty:
        save_csv(power_figure_data, data_dir / "static_power_fidelity_figure_data.csv")
    if not calibration_figure_data.empty:
        save_csv(calibration_figure_data, data_dir / "calibration_figure_data.csv")
    if not closed_loop_figure_data.empty:
        save_csv(closed_loop_figure_data, data_dir / "closed_loop_figure_data.csv")

    plot_static_task_progress(
        completion_figure_data,
        figures_dir,
        args.figure_format,
        args.dpi,
    )
    plot_static_power_fidelity(
        power_figure_data,
        figures_dir,
        args.figure_format,
        args.dpi,
    )
    plot_calibration_effect(
        calibration_figure_data,
        power_metrics,
        figures_dir,
        args.figure_format,
        args.dpi,
    )
    plot_closed_loop_outcomes(
        closed_loop_summary,
        physical_outcomes,
        figures_dir,
        args.figure_format,
        args.dpi,
    )
    plot_runtime_energy_tradeoff(
        closed_loop_summary,
        figures_dir,
        args.figure_format,
        args.dpi,
    )

    manifest = {
        "script_version": SCRIPT_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "input_argument": str(args.root),
        "resolved_input_root": str(input_root),
        "output_directory": str(out_dir),
        "discovered_run_count": len(runs),
        "discovered_runs": [run.run_id for run in runs],
        "expected_settings": expected_settings,
        "expected_repetitions": expected_repetitions,
        "parameters": {
            "expected_task_count": args.expected_task_count,
            "expected_node_count": args.expected_node_count,
            "completion_bin_seconds": args.completion_bin_seconds,
            "power_resample_seconds": args.power_resample_seconds,
            "power_smoothing_minutes": args.power_smoothing_minutes,
            "trend_segment_minutes": args.trend_segment_minutes,
            "trend_max_lag_minutes": args.trend_max_lag_minutes,
            "trend_minimum_pairs": args.trend_minimum_pairs,
            "trend_minimum_standard_deviation": args.trend_minimum_standard_deviation,
            "proposal_filter": args.proposal_filter,
            "figure_format": args.figure_format,
            "dpi": args.dpi,
        },
        "software": {
            "python": sys.version,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "matplotlib": matplotlib.__version__,
        },
        "issue_count": len(issues.rows),
        "error_count": sum(issue.severity == "error" for issue in issues.rows),
    }
    save_json(manifest, out_dir / "analysis_parameters.json")

    logging.info("Analysis complete. Main figures are in %s", figures_dir)
    logging.info("Principal tables are in %s", tables_dir)

    if args.strict and (not missing.empty or issues.has_errors):
        logging.error("Strict mode failed because runs are missing or analysis errors were recorded")
        return 2
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate the five Chapter 5 figures and three principal summary tables.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--root",
        type=Path,
        required=True,
        help="Directory or archive containing run folders named like 1_run_1",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("evaluation_results"),
        help="Output directory",
    )
    parser.add_argument(
        "--figure-format",
        choices=("png", "pdf", "both"),
        default="both",
        help="Figure file format",
    )
    parser.add_argument("--dpi", type=int, default=300, help="PNG resolution")
    parser.add_argument(
        "--expected-settings",
        default="1-10",
        help="Expected experimental settings for the run inventory",
    )
    parser.add_argument(
        "--expected-repetitions",
        default="1-3",
        help="Expected repetitions for the run inventory",
    )
    parser.add_argument("--expected-task-count", type=int, default=370)
    parser.add_argument("--expected-node-count", type=int, default=8)
    parser.add_argument(
        "--completion-bin-seconds",
        type=int,
        default=60,
        help="Grid interval for cumulative-completion NRMSE and figures",
    )
    parser.add_argument(
        "--power-resample-seconds",
        type=int,
        default=60,
        help="Common grid interval for processor-power analysis",
    )
    parser.add_argument(
        "--power-smoothing-minutes",
        type=float,
        default=15.0,
        help="Centred moving-average duration",
    )
    parser.add_argument(
        "--trend-segment-minutes",
        type=float,
        default=30.0,
        help="Duration of each non-overlapping trend segment",
    )
    parser.add_argument(
        "--trend-max-lag-minutes",
        type=float,
        default=10.0,
        help="Maximum absolute lag searched within each trend segment",
    )
    parser.add_argument(
        "--trend-minimum-pairs",
        type=int,
        default=20,
        help="Minimum paired observations retained after applying a lag",
    )
    parser.add_argument(
        "--trend-minimum-standard-deviation",
        type=float,
        default=0.1,
        help="Minimum local standard deviation in each globally standardised segment",
    )
    parser.add_argument(
        "--proposal-filter",
        default="original",
        help="Substring used to select the OpenDC baseline/original proposal",
    )
    parser.add_argument(
        "--refresh-cache",
        action="store_true",
        help="Re-extract the required files when --root is an archive",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return a non-zero status when expected runs are missing or analysis errors occur",
    )
    parser.add_argument("--verbose", action="store_true")
    return parser


def validate_arguments(args: argparse.Namespace, parser: argparse.ArgumentParser) -> None:
    positive_integer_fields = (
        "dpi",
        "expected_task_count",
        "expected_node_count",
        "completion_bin_seconds",
        "power_resample_seconds",
        "trend_minimum_pairs",
    )
    for field in positive_integer_fields:
        if getattr(args, field) <= 0:
            parser.error(f"--{field.replace('_', '-')} must be greater than zero")
    if args.power_smoothing_minutes <= 0:
        parser.error("--power-smoothing-minutes must be greater than zero")
    if args.trend_segment_minutes <= 0:
        parser.error("--trend-segment-minutes must be greater than zero")
    if args.trend_max_lag_minutes < 0:
        parser.error("--trend-max-lag-minutes must not be negative")
    if args.trend_minimum_standard_deviation < 0:
        parser.error("--trend-minimum-standard-deviation must not be negative")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    validate_arguments(args, parser)
    setup_logging(args.verbose)
    set_plot_style()
    try:
        return analyse_runs(args)
    except Exception as exc:
        logging.exception("Analysis failed: %s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
