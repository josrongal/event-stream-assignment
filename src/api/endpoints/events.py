"""Event query endpoints."""

import json
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Query, HTTPException

from ..models import EventRecord
from ..dependencies import get_events_df, get_rejected_events_df


router = APIRouter(prefix="/events", tags=["Events"])


@router.get("", response_model=List[EventRecord])
async def get_events(
    service: Optional[str] = Query(None, description="Filter by service"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events")
):
    """
    Get cleaned events with optional filters.

    Returns raw events after cleaning and validation.

    **Example:**

        GET /events?service=checkout&event_type=request_completed&limit=50
    """
    events_df = get_events_df()

    if events_df is None or events_df.empty:
        raise HTTPException(
            status_code=503,
            detail="Events data not available. Run pipeline first."
        )

    df = events_df.copy()

    # Apply filters
    if service:
        df = df[df['service'] == service]

    if event_type:
        df = df[df['event_type'] == event_type]

    # Apply limit
    df = df.head(limit)

    if df.empty:
        return []

    # Convert timestamp to ISO string for JSON serialization
    df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%dT%H:%M:%SZ')

    return df.to_dict(orient='records')


@router.get("/count", tags=["Events"])
async def get_event_count(
    service: Optional[str] = Query(None, description="Filter by service"),
    event_type: Optional[str] = Query(None, description="Filter by event type")
):
    """Get count of events matching filters."""
    events_df = get_events_df()

    if events_df is None or events_df.empty:
        raise HTTPException(status_code=503, detail="Events data not available")

    df = events_df.copy()

    if service:
        df = df[df['service'] == service]

    if event_type:
        df = df[df['event_type'] == event_type]

    return {
        "count": len(df),
        "filters": {
            "service": service,
            "event_type": event_type
        }
    }


@router.get("/rejected", response_model=List[Dict[str, Any]], tags=["Events"])
async def get_rejected_events(
    service: Optional[str] = Query(None, description="Filter by service"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    rejection_reason: Optional[str] = Query(None, description="Filter by rejection reason"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of events")
):
    """
    Get rejected events with optional filters.

    Returns rejected events after validation process.

    **Example:**

        GET /events/rejected?service=checkout&event_type=request_completed&limit=50
    """
    rejected_events_df = get_rejected_events_df()

    if rejected_events_df is None or rejected_events_df.empty:
        raise HTTPException(
            status_code=503,
            detail="Rejected events data not available. Run pipeline first."
        )

    df = rejected_events_df.copy()

    # Apply filters
    if service:
        df = df[df['service'] == service]

    if event_type:
        df = df[df['event_type'] == event_type]

    if rejection_reason:
        df = df[df['rejection_reason'] == rejection_reason]

    # Apply limit
    df = df.head(limit)

    if df.empty:
        return []

    return json.loads(df.to_json(orient='records'))


@router.get("/rejected/count", tags=["Events"])
async def get_rejected_event_count(
    service: Optional[str] = Query(None, description="Filter by service"),
    event_type: Optional[str] = Query(None, description="Filter by event type"),
    rejection_reason: Optional[str] = Query(None, description="Filter by rejection reason")
):
    """Get count of rejected events matching filters."""
    rejected_events_df = get_rejected_events_df()

    if rejected_events_df is None or rejected_events_df.empty:
        raise HTTPException(status_code=503, detail="Rejected events data not available")

    df = rejected_events_df.copy()

    if service:
        df = df[df['service'] == service]

    if event_type:
        df = df[df['event_type'] == event_type]

    if rejection_reason:
        df = df[df['rejection_reason'] == rejection_reason]

    return {
        "count": len(df),
        "filters": {
            "service": service,
            "event_type": event_type,
            "rejection_reason": rejection_reason
        }
    }
