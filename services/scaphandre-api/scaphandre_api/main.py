"""Scaphandre API - FastAPI application for exposing local Scaphandre readings."""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel, Field

from k8s_observability.scaphandre.energy_reader import ENERGY_FILE_SUFFIX, ScaphandreEnergyReader

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_SCAPHANDRE_BASE_PATH = "/var/lib/libvirt/scaphandre"


class ScaphandreReadingResponse(BaseModel):
    node_name: str
    capture_time: datetime
    energy_uj: int
    source: str


class ScaphandreReadingListResponse(BaseModel):
    readings: List[ScaphandreReadingResponse]
    failed_nodes: List[str] = Field(default_factory=list)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    logger.info("Starting Scaphandre API service...")

    scaphandre_base_path = os.getenv("SCAPHANDRE_BASE_PATH", DEFAULT_SCAPHANDRE_BASE_PATH)
    app.state.energy_reader = ScaphandreEnergyReader(scaphandre_base_path)
    logger.info(f"Scaphandre base path configured: {scaphandre_base_path}")

    yield

    logger.info("Shutting down Scaphandre API service...")


app = FastAPI(
    title="Scaphandre API",
    description="REST API for exposing local Scaphandre energy readings",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to API documentation."""
    return RedirectResponse(url="/docs")


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    base_path = app.state.energy_reader.base_path
    return {
        "status": "healthy",
        "scaphandre_base_path": str(base_path),
        "base_path_exists": base_path.exists(),
        "base_path_readable": os.access(base_path, os.R_OK),
    }


def _list_scaphandre_nodes(base_path: Path) -> List[str]:
    if not base_path.exists():
        raise FileNotFoundError(f"Scaphandre base path does not exist: {base_path}")
    
    if not os.access(base_path, os.R_OK):
        raise PermissionError(f"Scaphandre base path is not readable: {base_path}")

    node_names: List[str] = []
    for entry in sorted(base_path.iterdir()):
        if not entry.is_dir():
            continue
        if (entry / ENERGY_FILE_SUFFIX).exists():
            node_names.append(entry.name)
    return node_names


def _build_response(node_name: str) -> ScaphandreReadingResponse:
    capture_time = datetime.now(timezone.utc)
    counter = app.state.energy_reader.read_energy_counter(node_name, capture_time)
    return ScaphandreReadingResponse(
        node_name=counter.node_name,
        capture_time=counter.capture_time,
        energy_uj=counter.energy_uj,
        source=str(counter.source),
    )


@app.get("/scaphandre", response_model=ScaphandreReadingListResponse)
async def list_scaphandre_readings():
    """Return current Scaphandre readings for all locally available nodes."""
    try:
        node_names = _list_scaphandre_nodes(app.state.energy_reader.base_path)
    except FileNotFoundError as exc:
        logger.error(f"Failed to list Scaphandre nodes: {exc}")
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:
        logger.error(f"Unexpected error while listing Scaphandre nodes: {exc}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list Scaphandre nodes: {exc}") from exc

    readings: List[ScaphandreReadingResponse] = []
    failed_nodes: List[str] = []

    for node_name in node_names:
        try:
            readings.append(_build_response(node_name))
        except Exception as exc:
            failed_nodes.append(node_name)
            logger.error(f"Failed to read Scaphandre energy for node {node_name}: {exc}", exc_info=True)

    return ScaphandreReadingListResponse(readings=readings, failed_nodes=failed_nodes)


@app.get("/scaphandre/{node_name}", response_model=ScaphandreReadingResponse)
async def get_scaphandre_reading(node_name: str):
    """Return the current Scaphandre reading for a single node/VM."""
    try:
        return _build_response(node_name)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Scaphandre reading not found for node {node_name}") from exc
    except Exception as exc:
        logger.error(f"Failed to read Scaphandre energy for node {node_name}: {exc}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to read Scaphandre energy for node {node_name}: {exc}",
        ) from exc
