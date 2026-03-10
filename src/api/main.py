"""
FastAPI application for event metrics.
"""

import os
from contextlib import asynccontextmanager
from pathlib import Path
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .endpoints import health_router, metrics_router, events_router
from .dependencies import set_metrics_df, set_events_df, set_rejected_events_df
from ..utils.logger import get_logger


logger = get_logger(__name__)
# Data file paths
BASE_DIR = Path(__file__).resolve().parent.parent.parent
EVENTS_FILE = BASE_DIR / "data" / "silver" / "slv_streaming_events.parquet"
REJECTED_EVENTS_FILE = BASE_DIR / "data" / "silver" / "rejected_events.jsonl"
METRICS_FILE = BASE_DIR / "data" / "gold" / "gld_aggregated_metrics.parquet"


def _load_data(
    events_file: Path = EVENTS_FILE,
    rejected_file: Path = REJECTED_EVENTS_FILE,
    metrics_file: Path = METRICS_FILE,
):
    """Load processed data into memory on startup."""
    logger.info("Starting Event Stream Metrics API...")

    # Load events
    if not events_file.exists():
        raise FileNotFoundError(f"Events file not found: {events_file}")
    try:
        events_df = pd.read_parquet(events_file)
        set_events_df(events_df)
        logger.info(f"Loaded {len(events_df):,} events")
    except Exception as e:
        raise RuntimeError(f"Failed to load events file: {events_file}") from e

    # Load rejected events
    if rejected_file.exists():
        try:
            rejected_events_df = pd.read_json(rejected_file, lines=True)
            set_rejected_events_df(rejected_events_df)
            logger.info(f"Loaded {len(rejected_events_df):,} rejected events")
        except Exception as e:
            logger.warning(f"Error loading rejected events: {rejected_file}. "
                           f"Error: {e}, continueing without rejected events data.")
    else:
        logger.warning(f"Rejected events file not found: {rejected_file}")

    # Load metrics
    if not metrics_file.exists():
        raise FileNotFoundError(f"Metrics file not found: {metrics_file}")
    try:
        metrics_df = pd.read_parquet(metrics_file)
        metrics_df['time_window'] = pd.to_datetime(metrics_df['time_window'],
                                                   utc=True, errors="coerce")
        set_metrics_df(metrics_df)
        logger.info(f"Loaded {len(metrics_df):,} metric records")
    except Exception as e:
        raise RuntimeError(f"Failed to load metrics file: {metrics_file}") from e

    logger.info("API ready at http://localhost:8000")
    logger.info("Documentation at http://localhost:8000/docs")


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        _load_data()
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise RuntimeError("Failed to start API due to data loading error") from e
    yield


# Initialize FastAPI app
app = FastAPI(
    title="Event Stream Metrics API",
    description="API for querying event stream metrics and aggregations",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(health_router)
app.include_router(metrics_router)
app.include_router(events_router)
