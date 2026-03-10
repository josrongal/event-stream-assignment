"""Metrics query endpoints."""

from typing import List, Optional
from fastapi import APIRouter, Depends, Query, HTTPException

from ..models import MetricRecord, MetricsQuery
from ..dependencies import get_metrics_df


router = APIRouter(prefix="/metrics", tags=["Metrics"])


@router.get("", response_model=List[MetricRecord])
async def get_metrics(
    filters: MetricsQuery = Depends(),
):
    """
    Get aggregated metrics with optional filters.

    Returns metrics aggregated per service per minute including:
    - Request count
    - Average latency
    - Error rate

    **Example:**

        GET /metrics?service=checkout&from=2025-01-12T09:00:00Z&to=2025-01-12T12:00:00Z
    """
    metrics_df = get_metrics_df()

    if metrics_df is None or metrics_df.empty:
        raise HTTPException(
            status_code=503,
            detail="Metrics data not available. Run pipeline first."
        )

    df = metrics_df.copy()

    if filters.service:
        df = df[df['service'] == filters.service]

    if filters.from_time:
        df = df[df['time_window'] >= filters.from_time]

    if filters.to_time:
        df = df[df['time_window'] <= filters.to_time]

    if df.empty:
        return []

    return df.to_dict(orient='records')


@router.get("/summary", tags=["Metrics"])
async def get_metrics_summary(
    service: Optional[str] = Query(None, description="Filter by service")
):
    """
    Get summary statistics across all metrics.

    Returns aggregated stats like total requests, avg error rate, etc.
    """
    metrics_df = get_metrics_df()

    if metrics_df is None or metrics_df.empty:
        raise HTTPException(
            status_code=503,
            detail="Metrics data not available"
        )

    df = metrics_df.copy()

    if service:
        df = df[df['service'] == service]

    summary = {
        "total_time_windows": len(df),
        "total_requests": int(df['request_count'].sum()),
        "avg_latency_ms": float(df['avg_latency_ms'].mean()),
        "avg_error_rate": float(df['error_rate'].mean()),
        "services": df['service'].unique().tolist() if not service else [service],
        "time_range": {
            "start": df['time_window'].min().isoformat() if len(df) > 0 else None,
            "end": df['time_window'].max().isoformat() if len(df) > 0 else None
        }
    }

    return summary
