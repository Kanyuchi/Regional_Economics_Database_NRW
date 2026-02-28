"""
ICT Indicators ETL Pipeline
Regional Economics Database for NRW

Complete ETL pipeline for ICT indicators data from State Database NRW.
Table: 52911-01i - ICT indicators by districts and independent cities (2020-2025)

Usage:
    python -m src.pipelines.ict_indicators_pipeline
"""

import sys
from pathlib import Path
from typing import Optional, List

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from extractors.state_db.ict_indicators_extractor import ICTIndicatorsExtractor
from transformers.ict_indicators_transformer import ICTIndicatorsTransformer
from loaders.db_loader import DataLoader
from utils.logging import setup_logging, get_logger
from utils.config import get_config


def run_ict_indicators_etl(
    startyear: int = 2020,
    endyear: int = 2025,
    indicator_id_base: int = 50,
    load_to_db: bool = True
) -> bool:
    """
    Run complete ETL pipeline for ICT indicators data.

    Args:
        startyear: Start year for data extraction (default: 2020)
        endyear: End year for data extraction (default: 2025)
        indicator_id_base: Base indicator ID for ICT metrics (default: 50)
        load_to_db: Whether to load data to database (default: True)

    Returns:
        True if pipeline completed successfully, False otherwise
    """
    # Setup logging
    setup_logging(level="INFO")
    logger = get_logger(__name__)

    logger.info("=" * 80)
    logger.info("ICT INDICATORS ETL PIPELINE")
    logger.info("=" * 80)
    logger.info(f"Table: 52911-01i - ICT Indicators")
    logger.info(f"Period: {startyear}-{endyear}")
    logger.info(f"Load to database: {load_to_db}")
    logger.info("=" * 80)

    try:
        # ================================================================================
        # STEP 1: EXTRACT
        # ================================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: EXTRACTING DATA FROM STATE DATABASE NRW")
        logger.info("=" * 80)

        extractor = ICTIndicatorsExtractor()

        # Display table information
        table_info = extractor.get_table_info()
        logger.info(f"\nTable Information:")
        logger.info(f"  ID: {table_info['table_id']}")
        logger.info(f"  Name: {table_info['table_name']}")
        logger.info(f"  Source: {table_info['source_name']}")
        logger.info(f"  Period: {table_info['start_year']}-{table_info['end_year']}")
        logger.info(f"  Coverage: {table_info['geographic_coverage']}")

        # Extract data
        raw_data = extractor.extract_ict_data(startyear=startyear, endyear=endyear)

        if raw_data is None or raw_data.empty:
            logger.error("❌ No data extracted. Aborting pipeline.")
            extractor.close()
            return False

        logger.info(f"\n✅ Successfully extracted {len(raw_data)} rows")
        logger.info(f"   Columns: {len(raw_data.columns)}")
        logger.info(f"   Years: {sorted(raw_data['year'].unique())}")
        logger.info(f"   Regions: {raw_data['region_code'].nunique()} unique")

        # ================================================================================
        # STEP 2: TRANSFORM
        # ================================================================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("=" * 80)

        transformer = ICTIndicatorsTransformer()

        # Transform to long format
        transformed_data = transformer.transform_ict_data(
            raw_data,
            indicator_id_base=indicator_id_base,
            years_filter=None  # Use all extracted years
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("❌ No data after transformation. Aborting pipeline.")
            extractor.close()
            return False

        logger.info(f"\n✅ Successfully transformed {len(transformed_data)} rows")

        # Validate data
        logger.info("\nValidating transformed data...")
        if not transformer.validate_data(transformed_data):
            logger.error("❌ Data validation failed. Aborting pipeline.")
            extractor.close()
            return False

        # Save transformed data to CSV for inspection
        output_dir = Path(__file__).parent.parent.parent / "data" / "processed" / "state_db"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"ict_indicators_transformed_{startyear}_{endyear}.csv"
        transformed_data.to_csv(output_file, index=False, encoding='utf-8')
        logger.info(f"\n✅ Saved transformed data to: {output_file}")

        # ================================================================================
        # STEP 3: LOAD
        # ================================================================================
        if load_to_db:
            logger.info("\n" + "=" * 80)
            logger.info("STEP 3: LOADING DATA INTO DATABASE")
            logger.info("=" * 80)

            loader = DataLoader()

            # Load indicator metadata first (if needed)
            logger.info("\nLoading indicator metadata...")
            metadata = transformer.get_indicator_metadata(transformed_data, indicator_id_base)
            if metadata:
                metadata_count = loader.load_indicator_metadata(metadata)
                logger.info(f"✅ Loaded {metadata_count} indicator metadata records")

            # Load fact data
            logger.info("\nLoading ICT indicators fact data...")
            records_loaded = loader.load_ict_data(transformed_data)

            if records_loaded == 0:
                logger.warning("⚠️  No records were loaded to database")
                loader.close()
                extractor.close()
                return False

            logger.info(f"\n✅ Successfully loaded {records_loaded} records to database")

            # Cleanup
            loader.close()
        else:
            logger.info("\n" + "=" * 80)
            logger.info("STEP 3: LOAD SKIPPED (load_to_db=False)")
            logger.info("=" * 80)
            logger.info("Data has been extracted and transformed but not loaded to database")

        # Cleanup
        extractor.close()

        # ================================================================================
        # PIPELINE COMPLETE
        # ================================================================================
        logger.info("\n" + "=" * 80)
        logger.info("✅ ICT INDICATORS ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Summary:")
        logger.info(f"  - Extracted: {len(raw_data)} rows")
        logger.info(f"  - Transformed: {len(transformed_data)} records")
        if load_to_db:
            logger.info(f"  - Loaded: {records_loaded} records")
        logger.info(f"  - Years: {startyear}-{endyear}")
        logger.info(f"  - Indicators: {transformed_data['indicator_id'].nunique()} unique")
        logger.info("=" * 80)

        return True

    except Exception as e:
        logger.error(f"\n❌ Pipeline failed with error: {e}", exc_info=True)
        return False


def main():
    """
    Main entry point for the ICT indicators ETL pipeline.

    Configure the pipeline parameters here.
    """
    import argparse

    parser = argparse.ArgumentParser(
        description='ICT Indicators ETL Pipeline - Extract, Transform, Load ICT data from State Database NRW'
    )
    parser.add_argument(
        '--start-year',
        type=int,
        default=2020,
        help='Start year for data extraction (default: 2020)'
    )
    parser.add_argument(
        '--end-year',
        type=int,
        default=2025,
        help='End year for data extraction (default: 2025)'
    )
    parser.add_argument(
        '--indicator-id-base',
        type=int,
        default=50,
        help='Base indicator ID for ICT metrics (default: 50)'
    )
    parser.add_argument(
        '--no-load',
        action='store_true',
        help='Skip loading to database (extract and transform only)'
    )

    args = parser.parse_args()

    # Run the pipeline
    success = run_ict_indicators_etl(
        startyear=args.start_year,
        endyear=args.end_year,
        indicator_id_base=args.indicator_id_base,
        load_to_db=not args.no_load
    )

    # Exit with appropriate code
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
