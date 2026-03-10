"""Tests for metrics aggregator."""

import pandas as pd
import pytest
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aggregator import MetricsAggregator


@pytest.fixture
def sample_clean_data():
    """Sample cleaned event data."""
    return pd.DataFrame([
        {
            'event_id': 'e1',
            'timestamp': pd.Timestamp('2025-01-12 10:00:30+00:00'),
            'service': 'checkout',
            'latency_ms': 100,
            'status_code': 200
        },
        {
            'event_id': 'e2',
            'timestamp': pd.Timestamp('2025-01-12 10:00:45+00:00'),
            'service': 'checkout',
            'latency_ms': 150,
            'status_code': 500  # Error
        },
        {
            'event_id': 'e3',
            'timestamp': pd.Timestamp('2025-01-12 10:01:15+00:00'),
            'service': 'payments',
            'latency_ms': 80,
            'status_code': 200
        }
    ])


def test_aggregator_groups_by_minute(sample_clean_data):
    """Test that aggregation groups events by minute."""
    agg = MetricsAggregator(sample_clean_data)
    agg.aggregate()
    
    metrics = agg.get_metrics()
    
    # Should have 2 groups: checkout@10:00 and payments@10:01
    assert len(metrics) == 2


def test_aggregator_calculates_request_count(sample_clean_data):
    """Test that request count is calculated correctly."""
    agg = MetricsAggregator(sample_clean_data)
    agg.aggregate()
    
    metrics = agg.get_metrics()
    
    checkout_count = metrics[metrics['service'] == 'checkout']['request_count'].values[0]
    assert checkout_count == 2


def test_aggregator_calculates_avg_latency(sample_clean_data):
    """Test that average latency is calculated."""
    agg = MetricsAggregator(sample_clean_data)
    agg.aggregate()
    
    metrics = agg.get_metrics()
    
    checkout_latency = metrics[metrics['service'] == 'checkout']['avg_latency_ms'].values[0]
    assert checkout_latency == 125.0  # (100 + 150) / 2


def test_aggregator_calculates_error_rate(sample_clean_data):
    """Test that error rate is calculated correctly."""
    agg = MetricsAggregator(sample_clean_data)
    agg.aggregate()
    
    metrics = agg.get_metrics()
    
    checkout_error_rate = metrics[metrics['service'] == 'checkout']['error_rate'].values[0]
    assert checkout_error_rate == 0.5  # 1 error out of 2 requests
    
    payments_error_rate = metrics[metrics['service'] == 'payments']['error_rate'].values[0]
    assert payments_error_rate == 0.0  # No errors
