"""
ETL Pipeline for Outpatient Care Recipients Statistics
Table: 22411-02i (State Database NRW)
Period: 2017-2023 (biennial: 2017, 2019, 2021, 2023)

Metrics:
- Outpatient care recipients by care level (Pflegegrad 1-5)

Care Levels (Pflegegrade):
- Pflegegrad 1: Slight impairment
- Pflegegrad 2: Significant impairment
- Pflegegrad 3: Severe impairment
- Pflegegrad 4: Most severe impairment
- Pflegegrad 5: Most severe with special care needs

Note: District-level data (Kreise and kreisfreie Städte)

Indicator: 76
"""

import sys
sys.path.append('.')

from src.extractors.state_db.outpatient_care_extractor import OutpatientCareExtractor
from src.transformers.outpatient_care_transformer import OutpatientCareTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for outpatient care data.

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
    logger.info("ETL Pipeline: Outpatient Care Recipients Statistics (22411-02i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year}")
    logger.info("Indicator: 76 (Outpatient Care Recipients by Care Level)")
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

        extractor = OutpatientCareExtractor()

        # Extract outpatient care data (year-by-year due to API limitation)
        logger.info(f"Extracting data for period: {start_year}-{end_year}")

        raw_data = extractor.extract_outpatient_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Year range: {raw_data['year'].min()} - {raw_data['year'].max()}")
        logger.info(f"Care levels: {raw_data['care_level_code'].unique().tolist()}")
        logger.info(f"Unique regions: {raw_data['region_code'].nunique()}")
        logger.info(f"Sample data:\n{raw_data.head()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("=" * 80)

        transformer = OutpatientCareTransformer()

        # Transform data into database format
        transformed_data = transformer.transform_outpatient_data(
            raw_data,
            years_filter=None  # Use all extracted years
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("[FAILED] Transformation failed - no data output")
            return False

        logger.info(f"[SUCCESS] Transformation successful: {len(transformed_data)} transformed rows")
        logger.info(f"Indicator: {transformed_data['indicator_id'].unique()}")

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
            # Count total outpatient care records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id = 76
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal outpatient care records: {total_count}")

            # Count by year
            result = conn.execute(text("""
                SELECT t.year, COUNT(*) as record_count
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id = 76
                GROUP BY t.year
                ORDER BY t.year
            """))

            logger.info("\nRecords by year:")
            for row in result:
                logger.info(f"  {row[0]}: {row[1]:,} records")

            # NRW totals by care level for latest year
            result = conn.execute(text("""
                SELECT
                    SPLIT_PART(f.notes, '|', 1) as care_level_code,
                    SPLIT_PART(f.notes, '|', 2) as care_level,
                    f.value
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                JOIN dim_geography g ON f.geo_id = g.geo_id
                WHERE f.indicator_id = 76
                  AND g.region_code = '05'
                  AND t.year = (SELECT MAX(year) FROM dim_time t2
                               JOIN fact_demographics f2 ON t2.time_id = f2.time_id
                               WHERE f2.indicator_id = 76)
                ORDER BY
                    CASE SPLIT_PART(f.notes, '|', 1)
                        WHEN 'care_level:total' THEN 0
                        WHEN 'care_level:level_1' THEN 1
                        WHEN 'care_level:level_2' THEN 2
                        WHEN 'care_level:level_3' THEN 3
                        WHEN 'care_level:level_4' THEN 4
                        WHEN 'care_level:level_5' THEN 5
                        ELSE 99
                    END
            """))

            logger.info("\nNRW Outpatient Care Recipients by Care Level (Latest Year):")
            logger.info(f"{'Care Level':<45} {'Recipients':>15}")
            logger.info("-" * 65)
            for row in result:
                logger.info(f"{row[1]:<45} {row[2]:>15,.0f}")

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
            logger.info("  python pipelines/state_db/etl_22411_02i_outpatient_care.py --full")
        else:
            logger.info("\n[SUCCESS] OUTPATIENT CARE TABLE EXTRACTION COMPLETE!")
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

    parser = argparse.ArgumentParser(description='ETL Pipeline for Outpatient Care Data (22411-02i)')
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
