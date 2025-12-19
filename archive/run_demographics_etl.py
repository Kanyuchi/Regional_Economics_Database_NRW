"""
Demographics ETL Pipeline Runner
Regional Economics Database for NRW

Complete ETL pipeline for demographics data from Regional Database Germany.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from extractors.regional_db.demographics_extractor import DemographicsExtractor
from transformers.demographics_transformer import DemographicsTransformer
from loaders.db_loader import DataLoader
from utils.logging import setup_logging, get_logger
from utils.config import get_config


def run_demographics_etl(
    regions: list = None,
    years: list = None,
    indicator_id: int = 1
):
    """
    Run complete ETL pipeline for demographics data.

    Args:
        regions: List of region codes to extract (None = all regions)
        years: List of years to extract (None = all available years)
        indicator_id: ID of indicator in dim_indicator table
    """
    # Setup logging
    setup_logging(level="INFO")
    logger = get_logger(__name__)

    logger.info("=" * 60)
    logger.info("Starting Demographics ETL Pipeline")
    logger.info("=" * 60)

    try:
        # Step 1: Extract
        logger.info("Step 1: Extracting data from Regional Database")
        extractor = DemographicsExtractor()

        # For demonstration, extract population total data
        raw_data = extractor.extract_population_total(regions=regions, years=years)

        if raw_data is None or raw_data.empty:
            logger.error("No data extracted. Aborting pipeline.")
            return False

        logger.info(f"Extracted {len(raw_data)} rows")

        # Step 2: Transform
        logger.info("Step 2: Transforming data")
        transformer = DemographicsTransformer()

        transformed_data = transformer.transform_population_data(
            raw_data,
            indicator_id=indicator_id,
            years_filter=raw_data.attrs.get('years_filter')  # Pass year filter from extractor
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("No data after transformation. Aborting pipeline.")
            return False

        logger.info(f"Transformed {len(transformed_data)} rows")

        # Validate data
        if not transformer.validate_data(transformed_data):
            logger.error("Data validation failed. Aborting pipeline.")
            return False

        # Step 3: Load
        logger.info("Step 3: Loading data into database")
        loader = DataLoader()

        records_loaded = loader.load_demographics_data(transformed_data)

        if records_loaded == 0:
            logger.warning("No records were loaded")
            return False

        logger.info(f"Successfully loaded {records_loaded} records")

        # Cleanup
        extractor.close()
        loader.close()

        logger.info("=" * 60)
        logger.info("Demographics ETL Pipeline Completed Successfully")
        logger.info("=" * 60)

        return True

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    # Example: Extract data for a few regions and recent years
    # You can modify these parameters as needed

    # NRW region codes (examples - you'll need actual codes)
    regions = None  # None = all regions

    # All available years for table 12411-03-03-4 (31.12.2011 - 31.12.2024)
    years = list(range(2011, 2025))  # 2011 through 2024

    # Run the pipeline
    success = run_demographics_etl(
        regions=regions,
        years=years,
        indicator_id=1  # This should match an entry in dim_indicator
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
