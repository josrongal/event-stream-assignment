"""Pydantic models for API request/response validation."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class MetricsQuery(BaseModel):
    """Query parameters for metrics endpoint."""
    service: Optional[str] = Field(None, description="Filter by service name")
    from_time: Optional[datetime] = Field(None, alias="from", description="Start timestamp")
    to_time: Optional[datetime] = Field(None, alias="to", description="End timestamp")


class MetricRecord(BaseModel):
    """Single metric record."""
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})

    service: str
    time_window: datetime
    request_count: int
    avg_latency_ms: Optional[float]
    error_count: int
    error_rate: float


class EventRecord(BaseModel):
    """Single event record."""
    event_id: str
    timestamp: str
    service: str
    event_type: str
    user_id: Optional[str] = None
    latency_ms: Optional[float] = None
    status_code: Optional[int] = None


class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    timestamp: datetime
    total_events: int
    total_metrics: int
