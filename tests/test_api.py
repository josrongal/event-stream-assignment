"""Tests for FastAPI endpoints."""

import pytest
from unittest.mock import patch
from fastapi.testclient import TestClient
import pandas as pd
from pathlib import Path
import sys

# Import src as a package from project root (mirrors uvicorn src.api.main:app)
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.api.main import app, _load_data
from src.api.dependencies import (
    set_metrics_df,
    set_events_df,
    set_rejected_events_df,
)


@pytest.fixture(autouse=True)
def mock_data():
    """Seed in-memory data store before each test."""
    set_metrics_df(pd.DataFrame([
        {
            'service': 'checkout',
            'time_window': pd.Timestamp('2025-01-12 10:00:00+00:00'),
            'request_count': 10,
            'avg_latency_ms': 150.5,
            'error_count': 1,
            'error_rate': 0.1
        },
        {
            'service': 'payments',
            'time_window': pd.Timestamp('2025-01-12 10:00:00+00:00'),
            'request_count': 5,
            'avg_latency_ms': 200.0,
            'error_count': 0,
            'error_rate': 0.0
        }
    ]))

    set_events_df(pd.DataFrame([
        {
            'event_id': 'e1',
            'timestamp': pd.Timestamp('2025-01-12 10:00:00+00:00'),
            'service': 'checkout',
            'event_type': 'request_completed',
            'user_id': 'u123',
            'latency_ms': 100.0,
            'status_code': 200
        }
    ]))

    # Rejected events keep raw string timestamps (not datetime objects)
    set_rejected_events_df(pd.DataFrame([
        {
            'event_id': 'r1',
            'timestamp': '2025-13-40T25:61:61Z',
            'service': 'payments',
            'event_type': 'request_failed',
            'user_id': 'u999',
            'latency_ms': 42.0,
            'status_code': 500,
            'rejection_reason': 'invalid_timestamp'
        },
        {
            'event_id': 'r2',
            'timestamp': '2025-01-12T10:01:00Z',
            'service': 'checkout',
            'event_type': 'request_completed',
            'user_id': 'u100',
            'latency_ms': 80.0,
            'status_code': 200,
            'rejection_reason': 'duplicate_event_id'
        }
    ]))


@pytest.fixture
def client(mock_data):
    """FastAPI test client with lifespan startup bypassed."""
    with patch("src.api.main._load_data"):
        with TestClient(app) as c:
            yield c


# --- Health & Root ---

def test_root_endpoint(client):
    """Root endpoint returns API info with endpoints list."""
    response = client.get("/")
    assert response.status_code == 200
    assert "endpoints" in response.json()


def test_health_endpoint(client):
    """Health check reports healthy status and data counts."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["total_events"] == 1
    assert data["total_metrics"] == 2


# --- Metrics ---

def test_metrics_endpoint_no_filter(client):
    """Returns all metric rows when no filter is applied."""
    response = client.get("/metrics")
    assert response.status_code == 200
    assert len(response.json()) == 2


def test_metrics_endpoint_with_service_filter(client):
    """Service filter narrows metrics to matching rows only."""
    response = client.get("/metrics?service=checkout")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["service"] == "checkout"


def test_metrics_endpoint_unknown_service_returns_empty(client):
    """Unknown service returns 200 with an empty list (not a 404)."""
    response = client.get("/metrics?service=unknown")
    assert response.status_code == 200
    assert response.json() == []


def test_metrics_503_when_no_data(client):
    """503 is returned when metrics DataFrame is not loaded."""
    set_metrics_df(None)
    response = client.get("/metrics")
    assert response.status_code == 503


def test_metrics_summary_endpoint(client):
    """Summary aggregates totals across all services."""
    response = client.get("/metrics/summary")
    assert response.status_code == 200
    data = response.json()
    assert data["total_requests"] == 15  # 10 + 5
    assert "avg_latency_ms" in data
    assert "avg_error_rate" in data


def test_metrics_summary_with_service_filter(client):
    """Summary scoped to a single service returns its stats."""
    response = client.get("/metrics/summary?service=payments")
    assert response.status_code == 200
    data = response.json()
    assert data["total_requests"] == 5
    assert data["avg_error_rate"] == 0.0


# --- Events ---

def test_events_endpoint(client):
    """Events endpoint returns list within requested limit."""
    response = client.get("/events?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["event_id"] == "e1"


def test_events_endpoint_service_filter(client):
    """Service filter returns only matching events."""
    response = client.get("/events?service=checkout")
    assert response.status_code == 200
    assert len(response.json()) == 1


def test_events_endpoint_no_match_returns_empty(client):
    """Unknown service filter returns 200 with empty list."""
    response = client.get("/events?service=unknown")
    assert response.status_code == 200
    assert response.json() == []


def test_events_count_endpoint(client):
    """Count endpoint returns total matching events."""
    response = client.get("/events/count")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert "filters" in data


def test_events_count_with_service_filter(client):
    """Count respects service filter."""
    assert client.get("/events/count?service=checkout").json()["count"] == 1
    assert client.get("/events/count?service=payments").json()["count"] == 0


def test_events_503_when_no_data(client):
    """503 is returned when events DataFrame is not loaded."""
    set_events_df(None)
    assert client.get("/events").status_code == 503


# --- Rejected Events ---

def test_rejected_events_endpoint(client):
    """Rejected events endpoint returns all rejected rows."""
    response = client.get("/events/rejected?limit=10")
    assert response.status_code == 200
    assert len(response.json()) == 2


def test_rejected_events_filter_by_reason(client):
    """rejection_reason filter returns only matching events."""
    response = client.get("/events/rejected?rejection_reason=invalid_timestamp")
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["event_id"] == "r1"
    assert data[0]["rejection_reason"] == "invalid_timestamp"


def test_rejected_events_preserves_raw_timestamp(client):
    """Rejected events preserve the original invalid timestamp string."""
    response = client.get("/events/rejected?rejection_reason=invalid_timestamp")
    assert response.status_code == 200
    assert response.json()[0]["timestamp"] == "2025-13-40T25:61:61Z"


def test_rejected_events_count_endpoint(client):
    """Rejected count endpoint returns total rejected events."""
    response = client.get("/events/rejected/count")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 2
    assert "filters" in data


def test_rejected_events_count_by_reason(client):
    """Rejected count respects rejection_reason filter."""
    r = client.get("/events/rejected/count?rejection_reason=duplicate_event_id")
    assert r.json()["count"] == 1


def test_rejected_events_503_when_no_data(client):
    """503 is returned when rejected events DataFrame is not loaded."""
    set_rejected_events_df(None)
    assert client.get("/events/rejected").status_code == 503


# ---------------------------------------------------------------------------
# _load_data unit tests (main.py startup logic)
# ---------------------------------------------------------------------------

@pytest.fixture
def events_parquet(tmp_path):
    df = pd.DataFrame({
        'event_id': pd.array(['e1'], dtype='string'),
        'timestamp': pd.to_datetime(['2025-01-12T10:00:00Z'], utc=True),
        'service': pd.array(['checkout'], dtype='string'),
        'event_type': pd.array(['request_completed'], dtype='string'),
        'user_id': pd.array(['u1'], dtype='string'),
        'latency_ms': [100.0],
        'status_code': pd.array([200], dtype='Int64'),
    })
    p = tmp_path / 'events.parquet'
    df.to_parquet(p)
    return p


@pytest.fixture
def metrics_parquet(tmp_path):
    df = pd.DataFrame({
        'service': ['checkout'],
        'time_window': pd.to_datetime(['2025-01-12T10:00:00Z'], utc=True),
        'request_count': [10],
        'avg_latency_ms': [150.0],
        'error_count': [1],
        'error_rate': [0.1],
    })
    p = tmp_path / 'metrics.parquet'
    df.to_parquet(p)
    return p


@pytest.fixture
def rejected_jsonl(tmp_path):
    df = pd.DataFrame([{
        'event_id': 'r1', 'timestamp': '2025-13-40T25:61:61Z',
        'service': 'payments', 'event_type': 'request_failed',
        'rejection_reason': 'invalid_timestamp',
    }])
    p = tmp_path / 'rejected.jsonl'
    df.to_json(p, orient='records', lines=True)
    return p


def test_load_data_raises_when_events_file_missing(tmp_path, metrics_parquet):
    """FileNotFoundError when events parquet does not exist."""
    with pytest.raises(FileNotFoundError, match="Events file not found"):
        _load_data(
            events_file=tmp_path / 'nonexistent.parquet',
            rejected_file=tmp_path / 'rejected.jsonl',
            metrics_file=metrics_parquet,
        )


def test_load_data_raises_when_metrics_file_missing(tmp_path, events_parquet):
    """FileNotFoundError when metrics parquet does not exist."""
    with pytest.raises(FileNotFoundError, match="Metrics file not found"):
        _load_data(
            events_file=events_parquet,
            rejected_file=tmp_path / 'nonexistent.jsonl',
            metrics_file=tmp_path / 'nonexistent_metrics.parquet',
        )


def test_load_data_loads_all_files(tmp_path, events_parquet, metrics_parquet, rejected_jsonl):
    """Happy path: all three files present and loaded into memory."""
    _load_data(
        events_file=events_parquet,
        rejected_file=rejected_jsonl,
        metrics_file=metrics_parquet,
    )
    from src.api.dependencies import get_events_df, get_metrics_df, get_rejected_events_df
    assert get_events_df() is not None and len(get_events_df()) == 1
    assert get_metrics_df() is not None and len(get_metrics_df()) == 1
    assert get_rejected_events_df() is not None and len(get_rejected_events_df()) == 1


def test_load_data_skips_rejected_when_missing(tmp_path, events_parquet, metrics_parquet):
    """Startup succeeds and rejected store is unchanged when rejected file is absent."""
    set_rejected_events_df(None)
    _load_data(
        events_file=events_parquet,
        rejected_file=tmp_path / 'no_rejected.jsonl',
        metrics_file=metrics_parquet,
    )
    from src.api.dependencies import get_rejected_events_df
    assert get_rejected_events_df() is None


def test_load_data_warns_on_corrupt_rejected(tmp_path, events_parquet, metrics_parquet):
    """Corrupted rejected JSONL is skipped without raising (just a warning)."""
    bad_jsonl = tmp_path / 'bad.jsonl'
    bad_jsonl.write_text('NOT VALID JSON\n')
    # Should not raise
    _load_data(
        events_file=events_parquet,
        rejected_file=bad_jsonl,
        metrics_file=metrics_parquet,
    )


def test_lifespan_raises_runtime_error_on_data_failure():
    """Lifespan wraps _load_data errors as RuntimeError so the app fails fast."""
    with patch("src.api.main._load_data", side_effect=FileNotFoundError("no file")):
        with pytest.raises(RuntimeError, match="Failed to start API"):
            with TestClient(app):
                pass
