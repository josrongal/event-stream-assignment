"""Dependency injection for data access."""

from dataclasses import dataclass
import pandas as pd


@dataclass
class DataStore:
    """ In-memory container for application dataframes."""
    metrics_df: pd.DataFrame | None = None
    events_df: pd.DataFrame | None = None
    rejected_events_df: pd.DataFrame | None = None


data_store = DataStore()


def set_metrics_df(df: pd.DataFrame):
    data_store.metrics_df = df


def set_events_df(df: pd.DataFrame):
    data_store.events_df = df


def set_rejected_events_df(df: pd.DataFrame):
    data_store.rejected_events_df = df


def get_metrics_df() -> pd.DataFrame:
    return data_store.metrics_df


def get_events_df() -> pd.DataFrame:
    return data_store.events_df


def get_rejected_events_df() -> pd.DataFrame:
    return data_store.rejected_events_df
