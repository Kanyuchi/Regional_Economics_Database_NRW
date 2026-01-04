"""
ETL Pipeline for Outpatient Care Services Statistics
Table: 22411-01i (State Database NRW)
Period: 2017-2023 (biennial: 2017, 2019, 2021, 2023)

Metrics:
- Total outpatient care services (facilities)
- Staff in outpatient care services

Note: District-level data (Kreise and kreisfreie Städte)

Indicators: 77-78
"""

import sys
sys.path.append('.')

from src.extractors.state_db.outpatient_services_extractor import OutpatientServicesExtractor
from src.transformers.outpatient_services_transformer import OutpatientServicesTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for outpatient services data.

    Args:
        test_mode: If True, extract only 2023 for testing. If False, extract full 2017-2023.
    """

    # Determine year range
    if test_mode:
        start_year = 2023
        end_year = 2023
        logger.info("[TEST MODE] Extracting 2023 only")
    else:
        start_year = 2017
        end_year = 2023
        logger.info("[FULL MODE] Extracting 2017-2023 (biennial)")

    logger.info("=" * 80)
    logger.info("ETL Pipeline: Outpatient Care Services Statistics (22411-01i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year}")
    logger.info("Indicators: 77 (Services), 78 (Staff)")
    logger.info("Geographic Level: District (Kreis)")
    logger.info("=" * 80)

    extractor = None
    loader = None

    try:
        # ========================================
        # STEP 1: EXTRACT
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: EXTRACTING DATA")
        logger.info("=" * 80)
        logger.info("Connecting to State Database NRW API...")

        extractor = OutpatientServicesExtractor()

        # Extract services data
        logger.info(f"Extracting data for period: {start_year}-{end_year}")

        raw_data = extractor.extract_services_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Year range: {raw_data['year'].min()} - {raw_data['year'].max()}")
        logger.info(f"Unique regions: {raw_data['region_code'].nunique()}")
        logger.info(f"Sample data:\n{raw_data.head()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("=" * 80)

        transformer = OutpatientServicesTransformer()

        # Transform data into database format
        transformed_data = transformer.transform_services_data(
            raw_data,
            years_filter=None  # Use all extracted years
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("[FAILED] Transformation failed - no data output")
            return False

        logger.info(f"[SUCCESS] Transformation successful: {len(transformed_data)} transformed rows")
        logger.info(f"Indicators: {sorted(transformed_data['indicator_id'].unique())}")

        # Validate
        if not transformer.validate_data(transformed_data):
            logger.error("[FAILED] Data validation failed")
            return False

        # ========================================
        # STEP 3: LOAD
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: LOADING TO DATABASE")
        logger.info("=" * 80)

        loader = DataLoader()

        # Load data
        logger.info(f"Loading {len(transformed_data)} records to database...")
        success = loader.load_demographics_data(transformed_data)

        if not success:
            logger.error("[FAILED] Loading failed")
            return False

        logger.info("[SUCCESS] Loading successful")

        # ========================================
        # STEP 4: VERIFICATION
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: VERIFICATION")
        logger.info("=" * 80)

        # Query database directly to verify
        from sqlalchemy import text
        from src.utils.database import DatabaseManager
        db = DatabaseManager()

        with db.get_connection() as conn:
            # Count total records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 77 AND 78
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal outpatient services records: {total_count}")

            # Count by indicator
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name_en,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 77 AND 78
                GROUP BY i.indicator_id, i.indicator_name_en
                ORDER BY i.indicator_id
            """))

            logger.info("\nRecords by indicator:")
            for row in result:
                logger.info(f"  Indicator {row[0]}: {row[2]:,} records - {row[1]}")

            # Count by year
            result = conn.execute(text("""
                SELECT t.year, COUNT(*) as record_count
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 77 AND 78
                GROUP BY t.year
                ORDER BY t.year
            """))

            logger.info("\nRecords by year:")
            for row in result:
                logger.info(f"  {row[0]}: {row[1]:,} records")

            # NRW totals over time
            result = conn.execute(text("""
                SELECT
                    t.year,
                    SUM(CASE WHEN f.indicator_id = 77 THEN f.value END) as services,
                    SUM(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                JOIN dim_geography g ON f.geo_id = g.geo_id
                WHERE f.indicator_id BETWEEN 77 AND 78
                  AND g.region_code = '05'
                GROUP BY t.year
                ORDER BY t.year
            """))

            logger.info("\nNRW Outpatient Services Over Time:")
            logger.info(f"{'Year':<6} {'Services':>10} {'Staff':>10} {'Staff/Service':>15}")
            logger.info("-" * 45)
            for row in result:
                if row[1] and row[2]:
                    ratio = row[2] / row[1] if row[1] > 0 else 0
                    logger.info(f"{row[0]:<6} {row[1]:>10,.0f} {row[2]:>10,.0f} {ratio:>15.1f}")

        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records loaded: {total_count:,}")
        logger.info(f"Period: {start_year}-{end_year}")
        logger.info("Geographic Level: Districts")

        if test_mode:
            logger.info("\n[TEST MODE] TEST MODE COMPLETE")
            logger.info("To extract full data (2017-2023), run:")
            logger.info("  python pipelines/state_db/etl_22411_01i_outpatient_services.py --full")
        else:
            logger.info("\n[SUCCESS] OUTPATIENT SERVICES TABLE EXTRACTION COMPLETE!")
            logger.info("\nNext steps:")
            logger.info("  1. Verify data with SQL queries")
            logger.info("  2. Create verification script if needed")

        return True

    except Exception as e:
        logger.error(f"[FAILED] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        # Cleanup
        if extractor:
            extractor.close()
        if loader:
            loader.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ETL Pipeline for Outpatient Services Data (22411-01i)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (2017-2023). Default is test mode (2023 only)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (2023 only)')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        test_mode = False
    elif args.test:
        test_mode = True
    else:
        # Default to test mode for safety
        test_mode = True
        logger.info("[INFO] No mode specified. Running in TEST mode (2023).")
        logger.info("   To run full extraction, use: --full")

    success = main(test_mode=test_mode)

    sys.exit(0 if success else 1)
