"""Aggregate metrics from cleaned event data."""

from pathlib import Path
import pandas as pd
from utils.logger import get_logger


logger = get_logger(__name__)


class MetricsAggregator:
    """
    Generates aggregated metrics from event data.
    
    Produces:
    - Request count per service per minute
    - Average latency per service per minute
    - Error rate (status codes outside 200-299 range)
    """

    def __init__(self, df: pd.DataFrame):
        self.df = df.copy()
        self.metrics = None

    def aggregate(self):
        """Compute aggregated metrics."""
        df = self.df.copy()

        # Add time window (rounded to minute)
        df['time_window'] = df['timestamp'].dt.floor('1min')

        # Add error flag
        df['is_error'] = ~df['status_code'].between(200, 299, inclusive='both')

        # Aggregate by service and time window
        agg_metrics = df.groupby(['service', 'time_window']).agg(
            request_count=('event_id', 'count'),
            avg_latency_ms=('latency_ms', 'mean'),
            error_count=('is_error', 'sum')
        ).reset_index()

        # Calculate error rate
        agg_metrics['error_rate'] = (
            agg_metrics['error_count'] / agg_metrics['request_count']
        )

        # Clean up
        agg_metrics = agg_metrics.round({
            'avg_latency_ms': 2,
            'error_rate': 4
        })

        self.metrics = agg_metrics
        return self

    def save(self, output_file: Path):
        """Save metrics to parquet."""
        logger.info(f"Saving metrics to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if self.metrics is not None and not self.metrics.empty:
            try:
                self.metrics.to_parquet(output_file.with_suffix(".parquet"), index=False)
            except Exception as e:
                logger.error(f"Error saving metrics calculation: {e}.")
                raise
        return self

    def get_metrics(self) -> pd.DataFrame:
        """Return metrics DataFrame."""
        return self.metrics
