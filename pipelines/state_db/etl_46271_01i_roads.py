"""
ETL Pipeline for Interregional Roads Statistics
Table: 46271-01i (State Database NRW)
Period: 1996-2024 (29 years)

Metrics:
- Total interregional roads (km)
- Autobahnen / Motorways (km)
- Bundesstraßen / Federal roads (km)
- Landstraßen / State roads (km)
- Kreisstraßen / District roads (km)

Indicators: 62-66
"""

import sys
sys.path.append('.')

from src.extractors.state_db.roads_extractor import RoadsExtractor
from src.transformers.roads_transformer import RoadsTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for interregional roads data.

    Args:
        test_mode: If True, extract only 2020-2024 for testing. If False, extract full 1996-2024.
    """

    # Determine year range
    if test_mode:
        start_year = 2020
        end_year = 2024
        logger.info("[TEST MODE] Extracting 2020-2024 only")
    else:
        start_year = 1996
        end_year = 2024
        logger.info("[FULL MODE] Extracting 1996-2024 (29 years)")

    logger.info("="*80)
    logger.info("ETL Pipeline: Interregional Roads Statistics (46271-01i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
    logger.info("Indicators: 62 (Total), 63 (Motorway), 64 (Federal), 65 (State), 66 (District)")
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

        extractor = RoadsExtractor()

        # Extract roads data (year-by-year due to API limitation)
        logger.info(f"Extracting data for {end_year - start_year + 1} years: {start_year}-{end_year}")
        if not test_mode:
            logger.info("This will take several minutes (29 API calls for full extraction)...")

        raw_data = extractor.extract_roads_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Year range: {raw_data['year'].min()} - {raw_data['year'].max()}")
        logger.info(f"Sample data:\n{raw_data.head()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)

        transformer = RoadsTransformer()

        # Transform data into database format
        # Indicators 62-66 for the five road types
        transformed_data = transformer.transform_roads_data(
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
            # Count total roads records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 62 AND 66
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal roads records: {total_count}")

            # Count by indicator
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name_en,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 62 AND 66
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
                WHERE f.indicator_id BETWEEN 62 AND 66
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"\nYear range: {year_range[0]} - {year_range[1]}")

            # Check geographic coverage
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT g.region_code)
                FROM fact_demographics f
                JOIN dim_geography g ON f.geo_id = g.geo_id
                WHERE f.indicator_id BETWEEN 62 AND 66
            """))
            region_count = result.scalar() or 0
            logger.info(f"Regions covered: {region_count}")

            # Sample road lengths for NRW total
            result = conn.execute(text("""
                SELECT
                    t.year,
                    MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) as total_km,
                    MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) as motorway_km,
                    MAX(CASE WHEN f.indicator_id = 64 THEN f.value END) as federal_km,
                    MAX(CASE WHEN f.indicator_id = 65 THEN f.value END) as state_km,
                    MAX(CASE WHEN f.indicator_id = 66 THEN f.value END) as district_km
                FROM fact_demographics f
                JOIN dim_geography g ON f.geo_id = g.geo_id
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 62 AND 66
                  AND g.region_code = '05'
                GROUP BY t.year
                ORDER BY t.year DESC
                LIMIT 5
            """))

            logger.info("\nNRW Total Road Lengths (last 5 years):")
            logger.info(f"{'Year':<6} {'Total':>10} {'Motorway':>10} {'Federal':>10} {'State':>10} {'District':>10}")
            logger.info("-" * 66)
            for row in result:
                logger.info(f"{row[0]:<6} {row[1]:>10.1f} {row[2]:>10.1f} {row[3]:>10.1f} {row[4]:>10.1f} {row[5]:>10.1f}")

        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Total records loaded: {total_count:,}")
        logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
        logger.info(f"Regions: {region_count}")

        if test_mode:
            logger.info("\n[TEST MODE] TEST MODE COMPLETE")
            logger.info("To extract full data (1996-2024), run:")
            logger.info("  python pipelines/state_db/etl_46271_01i_roads.py --full")
        else:
            logger.info("\n[SUCCESS] ROADS TABLE EXTRACTION COMPLETE!")
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

    parser = argparse.ArgumentParser(description='ETL Pipeline for Roads Data (46271-01i)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (1996-2024). Default is test mode (2020-2024)')
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
