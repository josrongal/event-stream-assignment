"""
Event Stream Data Pipeline - Main Entry Point

This pipeline processes streaming events, performing validation, cleaning,
and transformation of data from raw (bronze) to cleaned (silver) format.
It also generates aggregated metrics for downstream analysis (gold).
"""

from pathlib import Path
from utils.logger import get_logger
from cleaner import EventCleaner
from aggregator import MetricsAggregator


logger = get_logger(__name__)
BASE_DIR = Path(__file__).resolve().parent.parent


def run_pipeline():
    """
    Main data processing pipeline:
    1. Reads raw data from data/bronze
    2. Applies validation and cleaning
    3. Saves cleaned and rejected events to data/silver
    4. Generates aggregated metrics to data/gold
    """

    logger.info(" Starting data pipeline...")

    # Configuración de paths
    bronze_file = BASE_DIR / "data" / "bronze" / "HomeAssignmentEvents.jsonl"
    silver_dir = BASE_DIR / "data" / "silver"
    gold_dir = BASE_DIR / "data" / "gold"
    events_file = silver_dir / "slv_streaming_events.parquet"
    metrics_file = gold_dir / "gld_aggregated_metrics.parquet"

    # Verificar que existe el archivo fuente
    if not bronze_file.exists():
        raise FileNotFoundError(f"Error: Source file not found at {bronze_file}")

    try:
        logger.info("Cleaning events...")
        cleaner = EventCleaner(bronze_file)
        cleaner.load().clean().save(events_file)

        df_clean = cleaner.get_data()
        logger.info(f"Cleaned {len(df_clean):,} events")

        logger.info("Aggregating metrics...")
        aggregator = MetricsAggregator(df_clean)
        aggregator.aggregate().save(metrics_file)

        df_metrics = aggregator.get_metrics()
        logger.info(f"Generated {len(df_metrics)} metric records")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline error:: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    run_pipeline()
