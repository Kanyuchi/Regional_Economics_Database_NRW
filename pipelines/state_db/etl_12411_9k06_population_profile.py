"""
ETL Pipeline for Population Profile Statistics
Table: 12411-9k06 (State Database NRW)
Period: 1975-2024 (50 years)

Metrics:
- Total population by age group
- Male population by age group
- Female population by age group
- German population by age group
- Foreign population by age group

Age Groups (10):
- Total, under 6, 6-18, 18-25, 25-30, 30-40, 40-50, 50-60, 60-65, 65+

Note: This table provides NRW state-level data only (not district-level)

Indicators: 67-71
"""

import sys
sys.path.append('.')

from src.extractors.state_db.population_profile_extractor import PopulationProfileExtractor
from src.transformers.population_profile_transformer import PopulationProfileTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for population profile data.

    Args:
        test_mode: If True, extract only 2020-2024 for testing. If False, extract full 1975-2024.
    """

    # Determine year range
    if test_mode:
        start_year = 2020
        end_year = 2024
        logger.info("[TEST MODE] Extracting 2020-2024 only")
    else:
        start_year = 1975
        end_year = 2024
        logger.info("[FULL MODE] Extracting 1975-2024 (50 years)")

    logger.info("="*80)
    logger.info("ETL Pipeline: Population Profile Statistics (12411-9k06)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
    logger.info("Indicators: 67-71 (Total, Male, Female, German, Foreign by Age)")
    logger.info("Geographic Level: NRW State Only")
    logger.info("="*80)

    extractor = None
    loader = None

    try:
        # ========================================
        # STEP 1: EXTRACT
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 1: EXTRACTING DATA")
        logger.info("="*80)
        logger.info("Connecting to State Database NRW API...")

        extractor = PopulationProfileExtractor()

        # Extract population data (year-by-year due to API limitation)
        logger.info(f"Extracting data for {end_year - start_year + 1} years: {start_year}-{end_year}")
        if not test_mode:
            logger.info("This will take several minutes (50 API calls for full extraction)...")

        raw_data = extractor.extract_population_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Year range: {raw_data['year'].min()} - {raw_data['year'].max()}")
        logger.info(f"Age groups: {raw_data['age_group_code'].unique().tolist()}")
        logger.info(f"Sample data:\n{raw_data.head()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)

        transformer = PopulationProfileTransformer()

        # Transform data into database format
        # Indicators 67-71 for the five population metrics
        transformed_data = transformer.transform_population_data(
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
        logger.info("\n" + "="*80)
        logger.info("STEP 3: LOADING TO DATABASE")
        logger.info("="*80)

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
        logger.info("\n" + "="*80)
        logger.info("STEP 4: VERIFICATION")
        logger.info("="*80)

        # Query database directly to verify
        from sqlalchemy import text
        from src.utils.database import DatabaseManager
        db = DatabaseManager()

        with db.get_connection() as conn:
            # Count total population profile records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 67 AND 71
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal population profile records: {total_count}")

            # Count by indicator
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name_en,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 67 AND 71
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
                WHERE f.indicator_id BETWEEN 67 AND 71
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"\nYear range: {year_range[0]} - {year_range[1]}")

            # Sample population by age group for latest year
            result = conn.execute(text("""
                SELECT
                    SPLIT_PART(f.notes, '|', 2) as age_group,
                    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total,
                    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_pop,
                    ROUND(MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) /
                          NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100, 2) as foreign_pct
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 67 AND 71
                  AND t.year = (SELECT MAX(year) FROM dim_time t2
                               JOIN fact_demographics f2 ON t2.time_id = f2.time_id
                               WHERE f2.indicator_id BETWEEN 67 AND 71)
                GROUP BY SPLIT_PART(f.notes, '|', 2)
                ORDER BY
                    CASE SPLIT_PART(f.notes, '|', 2)
                        WHEN 'Total' THEN 0
                        WHEN 'unter 6 Jahre' THEN 1
                        WHEN '6 bis unter 18 Jahre' THEN 2
                        WHEN '18 bis unter 25 Jahre' THEN 3
                        WHEN '25 bis unter 30 Jahre' THEN 4
                        WHEN '30 bis unter 40 Jahre' THEN 5
                        WHEN '40 bis unter 50 Jahre' THEN 6
                        WHEN '50 bis unter 60 Jahre' THEN 7
                        WHEN '60 bis unter 65 Jahre' THEN 8
                        WHEN '65 Jahre und mehr' THEN 9
                        ELSE 99
                    END
            """))

            logger.info("\nNRW Population by Age Group (Latest Year):")
            logger.info(f"{'Age Group':<30} {'Total':>15} {'Foreign':>12} {'Foreign %':>10}")
            logger.info("-" * 70)
            for row in result:
                total = f"{row[1]:,.0f}" if row[1] else "N/A"
                foreign = f"{row[2]:,.0f}" if row[2] else "N/A"
                foreign_pct = f"{row[3]:.1f}%" if row[3] else "N/A"
                logger.info(f"{row[0]:<30} {total:>15} {foreign:>12} {foreign_pct:>10}")

        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Total records loaded: {total_count:,}")
        logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
        logger.info("Geographic Level: NRW State")

        if test_mode:
            logger.info("\n[TEST MODE] TEST MODE COMPLETE")
            logger.info("To extract full data (1975-2024), run:")
            logger.info("  python pipelines/state_db/etl_12411_9k06_population_profile.py --full")
        else:
            logger.info("\n[SUCCESS] POPULATION PROFILE TABLE EXTRACTION COMPLETE!")
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

    parser = argparse.ArgumentParser(description='ETL Pipeline for Population Profile Data (12411-9k06)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (1975-2024). Default is test mode (2020-2024)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (2020-2024 only)')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        test_mode = False
    elif args.test:
        test_mode = True
    else:
        # Default to test mode for safety
        test_mode = True
        logger.info("[INFO] No mode specified. Running in TEST mode (2020-2024).")
        logger.info("   To run full extraction, use: --full")

    success = main(test_mode=test_mode)

    sys.exit(0 if success else 1)
