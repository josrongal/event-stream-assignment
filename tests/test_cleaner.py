"""Tests for event cleaner."""

import json
import pandas as pd
import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from cleaner import EventCleaner, TARGET_COLUMNS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def sample_events_file(tmp_path):
    """JSONL file with a mix of valid, invalid, and edge-case events."""
    events = [
        # e1: fully valid
        {
            'event_id': 'e1', 'timestamp': '2025-01-12T10:00:00Z',
            'service': 'Checkout', 'event_type': 'request_completed',
            'latency_ms': 100, 'status_code': 200, 'user_id': 'u123',
            'extra_field': 'should_be_dropped'
        },
        # e2: valid but negative latency + missing user_id
        {
            'event_id': 'e2', 'timestamp': '2025-01-12T10:01:00Z',
            'service': 'payments', 'event_type': 'request_failed',
            'latency_ms': -5, 'status_code': 500, 'user_id': None
        },
        # missing event_id → will be rejected as missing_critical_fields
        {
            'event_id': None, 'timestamp': '2025-01-12T10:02:00Z',
            'service': 'catalog', 'event_type': 'request_completed',
            'latency_ms': 50, 'status_code': 200, 'user_id': 'u456'
        },
        # e4: invalid timestamp → will be rejected as invalid_timestamp
        {
            'event_id': 'e4', 'timestamp': 'not-a-date',
            'service': 'auth', 'event_type': 'request_completed',
            'latency_ms': 80, 'status_code': 200, 'user_id': 'u789'
        },
    ]
    path = tmp_path / "test.jsonl"
    path.write_text('\n'.join(json.dumps(e) for e in events))
    return path


@pytest.fixture
def duplicate_events_file(tmp_path):
    """JSONL file with delivery duplicates (same event_id+service+event_type)
    and a lifecycle pair (same event_id, different event_type)."""
    events = [
        # older delivery duplicate of dup1
        {
            'event_id': 'dup1', 'timestamp': '2025-01-12T10:00:00Z',
            'service': 'checkout', 'event_type': 'request_completed',
            'latency_ms': 100, 'status_code': 200, 'user_id': 'u1'
        },
        # newer delivery duplicate of dup1 (should be kept, older rejected)
        {
            'event_id': 'dup1', 'timestamp': '2025-01-12T10:01:00Z',
            'service': 'checkout', 'event_type': 'request_completed',
            'latency_ms': 120, 'status_code': 200, 'user_id': 'u1'
        },
        # same event_id as dup1 but different event_type → different lifecycle stage, keep both
        {
            'event_id': 'dup1', 'timestamp': '2025-01-12T09:59:00Z',
            'service': 'checkout', 'event_type': 'request_started',
            'latency_ms': 0, 'status_code': 200, 'user_id': 'u1'
        },
        # unique event
        {
            'event_id': 'e2', 'timestamp': '2025-01-12T10:02:00Z',
            'service': 'payments', 'event_type': 'request_completed',
            'latency_ms': 80, 'status_code': 200, 'user_id': 'u2'
        },
    ]
    path = tmp_path / "dup_test.jsonl"
    path.write_text('\n'.join(json.dumps(e) for e in events))
    return path


# ---------------------------------------------------------------------------
# Load
# ---------------------------------------------------------------------------

def test_cleaner_load_reads_jsonl(sample_events_file):
    """load() populates df_bronze with all rows from the JSONL file."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load()
    assert cleaner.df_bronze is not None
    assert len(cleaner.df_bronze) == 4


# ---------------------------------------------------------------------------
# Missing critical fields
# ---------------------------------------------------------------------------

def test_cleaner_drops_missing_required_fields(sample_events_file):
    """Events with null critical fields (event_id here) are excluded from silver."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    df_clean = cleaner.get_data()
    assert None not in df_clean['event_id'].values
    assert len(df_clean) == 2  # e1 and e2 survive


def test_cleaner_rejects_missing_critical_fields(sample_events_file):
    """Missing-field events appear in rejected with reason missing_critical_fields."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    reasons = cleaner.df_rejected['rejection_reason'].tolist()
    assert 'missing_critical_fields' in reasons


# ---------------------------------------------------------------------------
# Invalid timestamps
# ---------------------------------------------------------------------------

def test_cleaner_rejects_invalid_timestamps(sample_events_file):
    """Events with unparseable timestamps are rejected as invalid_timestamp."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    reasons = cleaner.df_rejected['rejection_reason'].tolist()
    assert 'invalid_timestamp' in reasons


def test_cleaner_preserves_original_timestamp_in_rejected(sample_events_file):
    """Rejected events retain the original (unparsed) timestamp value."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    rejected_ts = cleaner.df_rejected[
        cleaner.df_rejected['rejection_reason'] == 'invalid_timestamp'
    ]['timestamp'].values[0]
    assert rejected_ts == 'not-a-date'


def test_cleaner_normalizes_valid_timestamps(sample_events_file):
    """Valid timestamps are coerced to timezone-aware datetime64."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    dtype_str = str(cleaner.get_data()['timestamp'].dtype)
    assert 'datetime64' in dtype_str
    assert 'UTC' in dtype_str


# ---------------------------------------------------------------------------
# Imputation and normalization
# ---------------------------------------------------------------------------

def test_cleaner_imputes_missing_user_id(sample_events_file):
    """Missing user_id is filled with 'unknown_user' (lowercased)."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    e2 = cleaner.get_data()[cleaner.get_data()['event_id'] == 'e2']
    assert e2['user_id'].values[0] == 'unknown_user'


def test_cleaner_normalizes_strings_to_lowercase(sample_events_file):
    """String columns (service, event_type, etc.) are lowercased."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    # 'Checkout' should become 'checkout'
    services = cleaner.get_data()['service'].tolist()
    assert all(s == s.lower() for s in services)


# ---------------------------------------------------------------------------
# Range validation
# ---------------------------------------------------------------------------

def test_cleaner_nullifies_negative_latency(sample_events_file):
    """Negative latency_ms values are replaced with NaN."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    e2_latency = cleaner.get_data()[
        cleaner.get_data()['event_id'] == 'e2'
    ]['latency_ms'].values[0]
    assert pd.isna(e2_latency)


# ---------------------------------------------------------------------------
# Schema contract
# ---------------------------------------------------------------------------

def test_cleaner_applies_silver_schema(sample_events_file):
    """Cleaned DataFrame contains exactly the TARGET_COLUMNS (no extras)."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    assert set(cleaner.get_data().columns) == set(TARGET_COLUMNS)


def test_cleaner_drops_extra_columns(sample_events_file):
    """Columns not in TARGET_COLUMNS (like extra_field) are removed."""
    cleaner = EventCleaner(sample_events_file)
    cleaner.load().clean()
    assert 'extra_field' not in cleaner.get_data().columns


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def test_cleaner_deduplication_keeps_newest(duplicate_events_file):
    """Same (event_id, service, event_type) → keep most recent timestamp."""
    cleaner = EventCleaner(duplicate_events_file)
    cleaner.load().clean()
    df_clean = cleaner.get_data()
    completed = df_clean[
        (df_clean['event_id'] == 'dup1') & (df_clean['event_type'] == 'request_completed')
    ]
    assert len(completed) == 1
    assert completed['latency_ms'].values[0] == 120.0  # newer record


def test_cleaner_lifecycle_events_both_kept(duplicate_events_file):
    """Same event_id but different event_type → both lifecycle stages kept."""
    cleaner = EventCleaner(duplicate_events_file)
    cleaner.load().clean()
    df_clean = cleaner.get_data()
    dup1_rows = df_clean[df_clean['event_id'] == 'dup1']
    event_types = set(dup1_rows['event_type'].tolist())
    assert 'request_started' in event_types
    assert 'request_completed' in event_types


def test_cleaner_duplicate_goes_to_rejected(duplicate_events_file):
    """The older delivery duplicate ends up in rejected with reason duplicate_event_id."""
    cleaner = EventCleaner(duplicate_events_file)
    cleaner.load().clean()
    reasons = cleaner.df_rejected['rejection_reason'].tolist()
    assert 'duplicate_event_id' in reasons

