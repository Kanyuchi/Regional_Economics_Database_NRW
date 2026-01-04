"""
ETL Pipeline for Care Recipients Statistics
Table: 22421-02i (State Database NRW)
Period: 2017-2023

Metrics:
- Total benefit recipients by care level
- Nursing home residents by care level
- Inpatient care recipients by care level
- Care allowance recipients by care level

Care Levels (Pflegegrade):
- Pflegegrad 1: Slight impairment
- Pflegegrad 2: Significant impairment
- Pflegegrad 3: Severe impairment
- Pflegegrad 4: Most severe impairment
- Pflegegrad 5: Most severe with special care needs

Note: District-level data (Kreise and kreisfreie Städte)

Indicators: 72-75
"""

import sys
sys.path.append('.')

from src.extractors.state_db.care_recipients_extractor import CareRecipientsExtractor
from src.transformers.care_recipients_transformer import CareRecipientsTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for care recipients data.

    Args:
        test_mode: If True, extract only 2022-2023 for testing. If False, extract full 2017-2023.
    """

    # Determine year range
    if test_mode:
        start_year = 2022
        end_year = 2023
        logger.info("[TEST MODE] Extracting 2022-2023 only")
    else:
        start_year = 2017
        end_year = 2023
        logger.info("[FULL MODE] Extracting 2017-2023 (7 years)")

    logger.info("=" * 80)
    logger.info("ETL Pipeline: Care Recipients Statistics (22421-02i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
    logger.info("Indicators: 72-75 (Benefit Total, Nursing Home, Inpatient, Care Allowance)")
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

        extractor = CareRecipientsExtractor()

        # Extract care recipients data (year-by-year due to API limitation)
        logger.info(f"Extracting data for {end_year - start_year + 1} years: {start_year}-{end_year}")

        raw_data = extractor.extract_care_data(
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

        transformer = CareRecipientsTransformer()

        # Transform data into database format
        # Indicators 72-75 for the four benefit types
        transformed_data = transformer.transform_care_data(
            raw_data,
            years_filter=None  # Use all extracted years
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("[FAILED] Transformation failed - no data output")
            return False

        logger.info(f"[SUCCESS] Transformation successful: {len(transformed_data)} transformed rows")
        logger.info(f"Indicators created: {sorted(transformed_data['indicator_id'].unique())}")

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
            # Count total care records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 72 AND 75
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal care recipients records: {total_count}")

            # Count by indicator
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name_en,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 72 AND 75
                GROUP BY i.indicator_id, i.indicator_name_en
                ORDER BY i.indicator_id
            """))

            logger.info("\nRecords by indicator:")
            for row in result:
                logger.info(f"  Indicator {row[0]}: {row[2]:,} records - {row[1]}")

            # Check year range
            result = conn.execute(text("""
                SELECT MIN(t.year), MAX(t.year)
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 72 AND 75
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"\nYear range: {year_range[0]} - {year_range[1]}")

            # Sample care recipients by care level for latest year
            result = conn.execute(text("""
                SELECT
                    SPLIT_PART(f.notes, '|', 1) as care_level_code,
                    SPLIT_PART(f.notes, '|', 2) as care_level,
                    SUM(CASE WHEN f.indicator_id = 72 THEN f.value END) as benefit_total,
                    SUM(CASE WHEN f.indicator_id = 73 THEN f.value END) as nursing_home,
                    SUM(CASE WHEN f.indicator_id = 74 THEN f.value END) as inpatient,
                    SUM(CASE WHEN f.indicator_id = 75 THEN f.value END) as care_allowance
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 72 AND 75
                  AND t.year = (SELECT MAX(year) FROM dim_time t2
                               JOIN fact_demographics f2 ON t2.time_id = f2.time_id
                               WHERE f2.indicator_id BETWEEN 72 AND 75)
                GROUP BY SPLIT_PART(f.notes, '|', 1), SPLIT_PART(f.notes, '|', 2)
                ORDER BY
                    CASE SPLIT_PART(f.notes, '|', 1)
                        WHEN 'care_level:total' THEN 0
                        WHEN 'care_level:level_1' THEN 1
                        WHEN 'care_level:level_2' THEN 2
                        WHEN 'care_level:level_3' THEN 3
                        WHEN 'care_level:level_4' THEN 4
                        WHEN 'care_level:level_5' THEN 5
                        WHEN 'care_level:not_assigned' THEN 6
                        ELSE 99
                    END
            """))

            logger.info("\nNRW Care Recipients by Care Level (Latest Year, All Districts Sum):")
            logger.info(f"{'Care Level':<45} {'Total':>12} {'Nursing Home':>15} {'Inpatient':>12} {'Allowance':>12}")
            logger.info("-" * 100)
            for row in result:
                total = f"{row[2]:,.0f}" if row[2] else "N/A"
                nursing = f"{row[3]:,.0f}" if row[3] else "N/A"
                inpatient = f"{row[4]:,.0f}" if row[4] else "N/A"
                allowance = f"{row[5]:,.0f}" if row[5] else "N/A"
                logger.info(f"{row[1]:<45} {total:>12} {nursing:>15} {inpatient:>12} {allowance:>12}")

        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "=" * 80)
        logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info(f"Total records loaded: {total_count:,}")
        logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
        logger.info("Geographic Level: Districts")

        if test_mode:
            logger.info("\n[TEST MODE] TEST MODE COMPLETE")
            logger.info("To extract full data (2017-2023), run:")
            logger.info("  python pipelines/state_db/etl_22421_02i_care_recipients.py --full")
        else:
            logger.info("\n[SUCCESS] CARE RECIPIENTS TABLE EXTRACTION COMPLETE!")
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

    parser = argparse.ArgumentParser(description='ETL Pipeline for Care Recipients Data (22421-02i)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (2017-2023). Default is test mode (2022-2023)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (2022-2023 only)')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        test_mode = False
    elif args.test:
        test_mode = True
    else:
        # Default to test mode for safety
        test_mode = True
        logger.info("[INFO] No mode specified. Running in TEST mode (2022-2023).")
        logger.info("   To run full extraction, use: --full")

    success = main(test_mode=test_mode)

    sys.exit(0 if success else 1)
