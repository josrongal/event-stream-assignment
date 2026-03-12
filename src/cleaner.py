"""Simple event data cleaner using pandas operations."""

from pathlib import Path
import pandas as pd
from utils.logger import get_logger


logger = get_logger(__name__)
TARGET_COLUMNS = [
    "event_id", "timestamp", "service", "event_type",
    "user_id", "latency_ms", "status_code"
]


class EventCleaner:
    """
    Cleans and validates event streaming data.

    Strategy:
    - Drop events missing critical fields: event_id, timestamp, service, event_type
    - Impute optional fields: user_id → 'UNKNOWN_USER'
    - Validate ranges: latency >= 0, status_code in [100, 599]
    """

    def __init__(self, input_file: Path):
        self.input_file = input_file
        self.df_bronze = None
        self.df_clean = None
        self.df_rejected = None

    def load(self):
        """Load raw JSONL data."""
        logger.info(f"Reading input file from path: {self.input_file}")
        try:
            self.df_bronze = pd.read_json(self.input_file, lines=True)
        except Exception as e:
            logger.error(f"Error reading input file: {e}.")
            raise
        return self

    def clean(self):
        """Apply cleaning transformations."""
        logger.info(f"Cleaning data with {len(self.df_bronze)} records...")
        df = self.df_bronze.copy()
        rejected_events = []

        # Drop rows with missing critical fields
        required_fields = ['event_id', 'timestamp', 'service', 'event_type']
        mask_missing_fields = df[required_fields].isna().any(axis=1)
        if mask_missing_fields.any():
            rejected = df[mask_missing_fields].copy()
            rejected['rejection_reason'] = 'missing_critical_fields'
            rejected_events.append(rejected)
            logger.warning(f"Rejected {mask_missing_fields.sum()} events: missing critical fields")
            df = df[~mask_missing_fields].copy()

        # Parse and normalize timestamps
        parsed_timestamps = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')
        mask_invalid_ts = parsed_timestamps.isna()
        if mask_invalid_ts.any():
            rejected = df[mask_invalid_ts].copy()
            rejected['rejection_reason'] = 'invalid_timestamp'
            rejected_events.append(rejected)
            logger.warning(f"Rejected {mask_invalid_ts.sum()} events: invalid timestamp")
            df = df[~mask_invalid_ts].copy()
            df['timestamp'] = parsed_timestamps[~mask_invalid_ts]

        # Deduplicate on composite key (event_id, service, event_type):
        # same event_id with a different event_type represents a distinct lifecycle
        # stage and is intentionally kept; only exact (id, service, type) matches
        # with a different timestamp are treated as delivery duplicates.
        DEDUP_KEY = ['event_id', 'service', 'event_type']
        df_sorted = df.sort_values(DEDUP_KEY + ['timestamp'], ascending=[True, True, True, False])
        dup_mask = df_sorted.duplicated(subset=DEDUP_KEY, keep='first')
        if dup_mask.any():
            dup_rejected = df_sorted[dup_mask].copy()
            dup_rejected['rejection_reason'] = 'duplicate_event_id'
            rejected_events.append(dup_rejected)
            logger.warning(f"Rejected {dup_mask.sum()} events: duplicate (event_id, service, event_type)")
            df = df_sorted[~dup_mask].copy()

        # Reject events with non-numeric, non-null status_code (e.g. 'ERR')
        sc_numeric = pd.to_numeric(df['status_code'], errors='coerce')
        mask_invalid_sc = sc_numeric.isna() & df['status_code'].notna()
        if mask_invalid_sc.any():
            rejected = df[mask_invalid_sc].copy()
            rejected['rejection_reason'] = 'invalid_status_code'
            rejected_events.append(rejected)
            logger.warning(f"Rejected {mask_invalid_sc.sum()} events: invalid status_code")
            df = df[~mask_invalid_sc].copy()

        df = self._apply_schema_contract(df)

        # Impute missing user_id
        df['user_id'] = df['user_id'].fillna('UNKNOWN_USER')

        # Normalize string fields
        df = self._normalize_strings(df)

        # Clean latency: negative values to NaN
        df.loc[df['latency_ms'] < 0, 'latency_ms'] = None

        # Clean status_code: out of HTTP range to NaN
        df.loc[(df['status_code'] < 100) | (df['status_code'] >= 600), 'status_code'] = None

        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)

        # Consolidate rejects
        if rejected_events:
            self.df_rejected = pd.concat(rejected_events, ignore_index=True)

        self.df_clean = df
        return self

    def _normalize_strings(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize string columns to lowercase and strip whitespace."""
        string_columns = ['service', 'event_type', 'user_id', 'event_id']
        for col in string_columns:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().str.lower()
        return df

    def _apply_schema_contract(self, df: pd.DataFrame) -> pd.DataFrame:
        unknown_cols = set(df.columns) - set(TARGET_COLUMNS)
        if unknown_cols:
            logger.warning(f"Found unknown columns: {unknown_cols}")

        # Drop unknown columns
        df = df[[c for c in df.columns if c in TARGET_COLUMNS]].copy()

        # Add missing columns
        for col in TARGET_COLUMNS:
            if col not in df.columns:
                df[col] = None

        # Reorder columns
        df = df[TARGET_COLUMNS]

        # Cast types safely
        df["event_id"] = df["event_id"].astype("string")
        df["service"] = df["service"].astype("string")
        df["event_type"] = df["event_type"].astype("string")
        df["user_id"] = df["user_id"].astype("string")

        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        lat = pd.to_numeric(df["latency_ms"], errors="coerce")
        mask = lat.isna() & df["latency_ms"].notna()
        if mask.any():
            extracted = df.loc[mask, "latency_ms"].astype(str).str.extract(r"^(\d+(?:\.\d+)?)")[0]
            lat = lat.copy()
            lat[mask] = pd.to_numeric(extracted, errors="coerce")
        df["latency_ms"] = lat.astype("float64")
        df["status_code"] = pd.to_numeric(df["status_code"], errors="coerce").astype("Int64")
        return df

    def save(self, output_file: Path):
        """Save cleaned data to parquet."""
        logger.info(f"Saving cleaned data to {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        if self.df_clean is not None and not self.df_clean.empty:
            try:
                self.df_clean.to_parquet(output_file.with_suffix(".parquet"), index=False)
            except Exception as e:
                logger.error(f"Error saving streaming events: {e}.")
                raise
        else:
            logger.warning("No event data to save.")

        if self.df_rejected is not None and not self.df_rejected.empty:
            dlq_file = output_file.parent / "rejected_events.jsonl"
            try:
                self.df_rejected.to_json(dlq_file, orient='records', lines=True)
                logger.info(f"Saved {len(self.df_rejected)} rejected events to {dlq_file}")
            except Exception as e:
                logger.error(f"Error saving rejected events dataframe: {e}.")
                raise
        return self

    def get_data(self) -> pd.DataFrame:
        """Return cleaned DataFrame."""
        return self.df_clean
