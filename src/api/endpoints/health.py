"""Health and status endpoints."""

from datetime import datetime
from fastapi import APIRouter

from ..models import HealthResponse
from ..dependencies import get_events_df, get_metrics_df


router = APIRouter(prefix="", tags=["Health"])


@router.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "name": "Event Stream Metrics API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "metrics": "/metrics",
            "events": "/events",
            "docs": "/docs"
        }
    }


@router.get("/health", response_model=HealthResponse)
async def health_check():
    """
    Health check endpoint.

    Returns API status and data availability.
    """
    events_df = get_events_df()
    metrics_df = get_metrics_df()

    return HealthResponse(
        status="healthy",
        timestamp=datetime.utcnow(),
        total_events=len(events_df) if events_df is not None else 0,
        total_metrics=len(metrics_df) if metrics_df is not None else 0
    )
