"""Tests for the data pipeline orchestrator."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from pipeline import run_pipeline


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bronze_file(base_dir: Path) -> Path:
    """Create a minimal bronze JSONL file under base_dir."""
    bronze_dir = base_dir / "data" / "bronze"
    bronze_dir.mkdir(parents=True)
    f = bronze_dir / "HomeAssignmentEvents.jsonl"
    f.write_text(
        '{"event_id":"e1","timestamp":"2025-01-12T10:00:00Z",'
        '"service":"checkout","event_type":"request_completed",'
        '"latency_ms":100,"status_code":200,"user_id":"u1"}\n'
    )
    return f


def _mock_cleaner(clean_df: pd.DataFrame = None):
    """Return a fully-configured MagicMock for EventCleaner."""
    if clean_df is None:
        clean_df = pd.DataFrame([{"event_id": "e1"}])
    m = MagicMock()
    m.load.return_value = m
    m.clean.return_value = m
    m.save.return_value = m
    m.get_data.return_value = clean_df
    return m


def _mock_aggregator(metrics_df: pd.DataFrame = None):
    """Return a fully-configured MagicMock for MetricsAggregator."""
    if metrics_df is None:
        metrics_df = pd.DataFrame([{"service": "checkout", "request_count": 1}])
    m = MagicMock()
    m.aggregate.return_value = m
    m.save.return_value = m
    m.get_metrics.return_value = metrics_df
    return m


# ---------------------------------------------------------------------------
# FileNotFoundError guard
# ---------------------------------------------------------------------------

def test_pipeline_raises_when_bronze_missing(tmp_path):
    """run_pipeline raises FileNotFoundError when the bronze source file is absent."""
    with patch("pipeline.BASE_DIR", tmp_path):
        with pytest.raises(FileNotFoundError, match="Source file not found"):
            run_pipeline()


def test_pipeline_runs_successfully(tmp_path):
    """Full pipeline runs without error when bronze file exists and classes succeed."""
    _make_bronze_file(tmp_path)
    mock_cleaner = _mock_cleaner()
    mock_aggregator = _mock_aggregator()

    with patch("pipeline.BASE_DIR", tmp_path), \
         patch("pipeline.EventCleaner", return_value=mock_cleaner), \
         patch("pipeline.MetricsAggregator", return_value=mock_aggregator):

        run_pipeline()

    mock_cleaner.load.assert_called_once()
    mock_cleaner.clean.assert_called_once()
    mock_cleaner.save.assert_called_once()
    mock_aggregator.aggregate.assert_called_once()
    mock_aggregator.save.assert_called_once()


def test_pipeline_passes_clean_df_to_aggregator(tmp_path):
    """MetricsAggregator receives the DataFrame returned by EventCleaner.get_data()."""
    _make_bronze_file(tmp_path)
    df_clean = pd.DataFrame([{"event_id": "e1", "service": "checkout"}])
    mock_cleaner = _mock_cleaner(clean_df=df_clean)
    mock_aggregator = _mock_aggregator()

    with patch("pipeline.BASE_DIR", tmp_path), \
         patch("pipeline.EventCleaner", return_value=mock_cleaner), \
         patch("pipeline.MetricsAggregator", return_value=mock_aggregator) as agg_cls:

        run_pipeline()

    # First positional arg to MetricsAggregator should be the cleaned DataFrame
    actual_df = agg_cls.call_args[0][0]
    pd.testing.assert_frame_equal(actual_df, df_clean)


# ---------------------------------------------------------------------------
# Error propagation
# ---------------------------------------------------------------------------

def test_pipeline_reraises_cleaner_exception(tmp_path):
    """Exceptions raised inside EventCleaner bubble up from run_pipeline."""
    _make_bronze_file(tmp_path)
    mock_cleaner = _mock_cleaner()
    mock_cleaner.clean.side_effect = ValueError("bad data")

    with patch("pipeline.BASE_DIR", tmp_path), \
         patch("pipeline.EventCleaner", return_value=mock_cleaner):

        with pytest.raises(ValueError, match="bad data"):
            run_pipeline()


def test_pipeline_reraises_aggregator_exception(tmp_path):
    """Exceptions raised inside MetricsAggregator bubble up from run_pipeline."""
    _make_bronze_file(tmp_path)
    mock_aggregator = _mock_aggregator()
    mock_aggregator.aggregate.side_effect = RuntimeError("aggregation failed")

    with patch("pipeline.BASE_DIR", tmp_path), \
         patch("pipeline.EventCleaner", return_value=_mock_cleaner()), \
         patch("pipeline.MetricsAggregator", return_value=mock_aggregator):

        with pytest.raises(RuntimeError, match="aggregation failed"):
            run_pipeline()
