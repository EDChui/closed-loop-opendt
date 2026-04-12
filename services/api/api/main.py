"""OpenDT API - FastAPI Application for datacenter simulation data."""

import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from odt_common import load_config_from_env
from odt_common.models import DecisionPolicy, MetricDirection, RankedDecisionPolicy, RankedObjectiveSpec
from odt_common.models.topology import (
    CPU,
    Cluster,
    Host,
    Memory,
    MseCPUPowerModel,
    PowerSource,
    Topology,
)
from odt_common.utils import get_kafka_producer
from odt_common.utils.kafka import send_message
from k8s_observability.persistence import build_engine, test_connection

from api.carbon_query import CarbonDataQuery, CarbonDataResponse
from api.power_query import PowerDataQuery, PowerDataResponse

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

DEFAULT_DATABASE_URL = "postgresql+psycopg://opendt:opendt@postgres:5432/opendt"


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    # Startup
    logger.info("Starting OpenDT API service...")

    # Load configuration
    try:
        app.state.config = load_config_from_env()
        logger.info("Configuration loaded")
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        app.state.config = None

    # Initialize Kafka producer (stored in app state for reuse)
    try:
        app.state.kafka_producer = get_kafka_producer()
        logger.info("Kafka producer initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Kafka producer: {e}")
        app.state.kafka_producer = None

    # Initialize database engine
    app.state.db_engine = None
    database_url = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
    try:
        app.state.db_engine = build_engine(database_url)
        if test_connection(app.state.db_engine):
            logger.info("Database engine initialized")
        else:
            logger.warning("Database engine initialized but connectivity check failed")
    except Exception as exc:
        logger.error("Failed to initialize database engine: %s", exc)
        app.state.db_engine = None

    yield

    # Shutdown
    logger.info("Shutting down OpenDT API service...")
    if app.state.kafka_producer:
        app.state.kafka_producer.close()
        logger.info("Kafka producer closed")
    if app.state.db_engine:
        app.state.db_engine.dispose()
        logger.info("Database engine disposed")


# Create FastAPI application
app = FastAPI(
    title="OpenDT API",
    description="Open Digital Twin - REST API for datacenter simulation",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for API access
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# ROOT REDIRECT
# ============================================================================


@app.get("/", include_in_schema=False)
async def root():
    """Redirect root to API documentation."""
    return RedirectResponse(url="/docs")


# ============================================================================
# API ENDPOINTS
# ============================================================================


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    kafka_status = "connected" if app.state.kafka_producer else "disconnected"
    config_status = "loaded" if app.state.config else "not loaded"
    database_status = "connected" if app.state.db_engine else "disconnected"

    return {
        "status": "healthy",
        "kafka": kafka_status,
        "config": config_status,
        "database": database_status,
    }


# ============================================================================
# TOPOLOGY MANAGEMENT
# ============================================================================


# Default topology for Swagger UI (matches SURF data)
DEFAULT_TOPOLOGY = Topology(
    clusters=[
        Cluster(
            name="C01",
            hosts=[
                Host(
                    name="H01",
                    count=277,
                    cpu=CPU(coreCount=16, coreSpeed=2100.0),
                    memory=Memory(memorySize=128000000),
                    cpuPowerModel=MseCPUPowerModel(
                        modelType="mse",
                        power=300.0,
                        idlePower=25.0,
                        maxPower=174.0,
                        calibrationFactor=10.0,
                    ),
                )
            ],
            powerSource=PowerSource(carbonTracePath="/app/workload/carbon.parquet"),
        )
    ]
)

# Example for OpenAPI docs
DEFAULT_TOPOLOGY_EXAMPLE = DEFAULT_TOPOLOGY.model_dump(mode="json")

@app.put("/api/topology", deprecated=True)

async def update_topology(
    topology: Annotated[
        Topology,
        Body(
            description="Datacenter topology configuration",
            openapi_examples={
                "default": {
                    "summary": "SURF datacenter topology",
                    "description": "Default SURF topology: 277 hosts, 16 cores each @ 2.1 GHz",
                    "value": DEFAULT_TOPOLOGY_EXAMPLE,
                }
            },
        ),
    ] = DEFAULT_TOPOLOGY,
):
    """Update the simulated datacenter topology.

    This endpoint validates the topology structure and publishes it to Kafka.
    The simulator will pick it up and use it for future simulations.

    Args:
        topology: Datacenter topology configuration with cluster details

    Returns:
        Success confirmation with topology details

    Raises:
        HTTPException: 500 if Kafka producer is not available
        HTTPException: 500 if publishing to Kafka fails
    """
    # Check if Kafka producer is available
    if not app.state.kafka_producer:
        logger.error("Kafka producer not initialized")
        raise HTTPException(status_code=500, detail="Kafka producer not available")

    # Check if config is loaded (to get topic name)
    if not app.state.config:
        logger.error("Configuration not loaded")
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    # Topology already validated by Pydantic
    logger.info(f"Topology validated: {len(topology.clusters)} cluster(s)")

    # Get sim.topology topic name from config
    sim_topology_topic = app.state.config.kafka.topics.get("sim_topology")
    if not sim_topology_topic:
        logger.error("sim.topology topic not configured")
        raise HTTPException(status_code=500, detail="sim.topology topic not configured")

    topic_name = sim_topology_topic.name

    # Publish to sim.topology Kafka topic with compacted key
    try:
        send_message(
            producer=app.state.kafka_producer,
            topic=topic_name,
            message=topology.model_dump(mode="json"),
            key="datacenter",
        )
        logger.info(f"Topology published to {topic_name}")
    except Exception as e:
        logger.error(f"Failed to publish topology to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish topology: {e}") from e

    return {
        "status": "updated",
        "message": f"Topology published to {topic_name}",
        "clusters": len(topology.clusters),
        "total_hosts": topology.total_host_count(),
        "total_cores": topology.total_core_count(),
        "topic": topic_name,
    }

# ============================================================================
# OBJECTIVE WEIGHT MANAGEMENT
# ============================================================================

DEFAULT_DECISION_POLICY = RankedDecisionPolicy(
    policy_type="ranked",
    objectives={
        "runtime": RankedObjectiveSpec(name="runtime", direction=MetricDirection.MIN, priority=1, tie_tolerance=30.0),
        "utilization": RankedObjectiveSpec(name="utilization", direction=MetricDirection.MAX, priority=2, tie_tolerance=0.05),
        "power": RankedObjectiveSpec(name="power", direction=MetricDirection.MIN, priority=3, tie_tolerance=0.0),
    }
)

DEFAULT_DECISION_POLICY_EXAMPLE = DEFAULT_DECISION_POLICY.model_dump(mode="json")

@app.put("/api/objectives")
async def update_objectives(
    objectives: Annotated[
        DecisionPolicy,
        Body(
            description="Objectives for decision making",
            openapi_examples={
                "default": {
                    "summary": "Default objectives",
                    "description": "Default weights: runtime=1, utilization=0",
                    "value": DEFAULT_DECISION_POLICY_EXAMPLE,
                }
            },
        ),
    ] = DEFAULT_DECISION_POLICY,
):
    # Check if Kafka producer is available
    if not app.state.kafka_producer:
        logger.error("Kafka producer not initialized")
        raise HTTPException(status_code=500, detail="Kafka producer not available")

    # Check if config is loaded (to get topic name)
    if not app.state.config:
        logger.error("Configuration not loaded")
        raise HTTPException(status_code=500, detail="Configuration not loaded")
    
    # Get sim.topology topic name from config
    objectives_topic = app.state.config.kafka.topics.get("objectives")
    if not objectives_topic:
        logger.error("dc.objectives topic not configured")
        raise HTTPException(status_code=500, detail="dc.objectives topic not configured")

    topic_name = objectives_topic.name

    # Publish to dc.objectives Kafka topic with compacted key
    try:
        send_message(
            producer=app.state.kafka_producer,
            topic=topic_name,
            message=objectives.model_dump(mode="json"),
            key="objectives",
        )
        logger.info(f"Objectives published to {topic_name}")
    except Exception as e:
        logger.error(f"Failed to publish objectives to Kafka: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to publish objectives: {e}") from e
    
    return {
        "status": "updated",
        "message": f"Objective weights published to {topic_name}",
        "objectives": objectives.model_dump(),
        "topic": topic_name,
    }


# ============================================================================
# POWER DATA QUERY
# ============================================================================


@app.get("/api/power", response_model=PowerDataResponse)
async def get_power_data(
    interval_seconds: int = Query(
        60, gt=0, le=3600, description="Sampling interval in seconds (1-3600)"
    ),
    start_time: datetime | None = Query(None, description="Optional start time (ISO 8601 format)"),
):
    """Query aligned power usage data from simulation and actual consumption.

    This endpoint reads from:
    - `agg_results.parquet`: Aggregated simulation results
    - `consumption.parquet`: Actual power consumption from workload

    The data is aligned to a common interval and clipped to the shortest timeseries.

    Args:
        interval_seconds: Sampling interval in seconds (default: 60)
        start_time: Optional start time filter (ISO 8601 format)

    Returns:
        PowerDataResponse with aligned timeseries data

    Raises:
        HTTPException: If data files are not found or cannot be processed
    """
    # Get run ID from environment
    run_id = os.getenv("RUN_ID")
    if not run_id:
        raise HTTPException(status_code=500, detail="RUN_ID environment variable not set")

    # Get workload context from config
    if not app.state.config:
        raise HTTPException(status_code=500, detail="Configuration not loaded")

    if not app.state.db_engine:
        raise HTTPException(status_code=500, detail="Database engine not available")

    try:
        # Initialize query
        query = PowerDataQuery(run_id=run_id, db_engine=app.state.db_engine)

        # Execute query
        result = query.query(interval_seconds=interval_seconds, start_time=start_time)

        logger.info(f"Power data query successful: {result.metadata['count']} data points")

        return result

    except FileNotFoundError as e:
        logger.error(f"Data file not found: {e}")
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        logger.error(f"Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error querying power data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


# ============================================================================
# CARBON EMISSION DATA QUERY
# ============================================================================


@app.get("/api/co2_emission", response_model=CarbonDataResponse)
async def get_carbon_emission_data(
    interval_seconds: int = Query(
        60, gt=0, le=3600, description="Sampling interval in seconds (1-3600)"
    ),
    start_time: datetime | None = Query(None, description="Optional start time (ISO 8601 format)"),
):
    """Query carbon emission data from simulation results.

    This endpoint reads from `agg_results.parquet` and calculates carbon emission
    based on power draw and carbon intensity.

    Carbon emission formula: power_draw (W) * carbon_intensity (gCO2/kWh) / 1000 = gCO2/h

    Args:
        interval_seconds: Sampling interval in seconds (default: 60)
        start_time: Optional start time filter (ISO 8601 format)

    Returns:
        CarbonDataResponse with carbon emission timeseries data

    Raises:
        HTTPException: If data files are not found or cannot be processed
    """
    # Get run ID from environment
    run_id = os.getenv("RUN_ID")
    if not run_id:
        raise HTTPException(status_code=500, detail="RUN_ID environment variable not set")

    try:
        # Initialize query
        query = CarbonDataQuery(run_id=run_id)

        # Execute query
        result = query.query(interval_seconds=interval_seconds, start_time=start_time)

        logger.info(f"Carbon emission query successful: {result.metadata['count']} data points")

        return result

    except FileNotFoundError as e:
        logger.error(f"Data file not found: {e}")
        raise HTTPException(status_code=404, detail=str(e)) from e
    except ValueError as e:
        logger.error(f"Invalid data: {e}")
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Error querying carbon emission data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}") from e


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
