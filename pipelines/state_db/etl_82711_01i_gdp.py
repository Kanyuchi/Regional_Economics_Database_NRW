"""
ETL Pipeline for GDP and Gross Value Added
Table: 82711-01i (State Database NRW)
Period: 1991-2023 (33 years)

Economic Sectors (WZ 2008):
- Agriculture, forestry and fishing (A)
- Manufacturing (B-E)
- Construction (F)
- Trade, transport and hospitality (G-I)
- Information and communication (J)
- Finance and insurance (K)
- Real estate (L)
- Business services (M-N)
- Public and other services (O-U)

This is the FIRST State Database extraction!
"""

import sys
sys.path.append('.')

from src.extractors.state_db.gdp_extractor import GDPExtractor
from src.transformers.gdp_transformer import GDPTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for GDP and gross value added data.

    Args:
        test_mode: If True, extract only 2020-2023 for testing. If False, extract full 1991-2023.
    """

    # Determine year range
    if test_mode:
        start_year = 2020
        end_year = 2023
        logger.info("[TEST MODE] Extracting 2020-2023 only")
    else:
        start_year = 1991
        end_year = 2023
        logger.info("[FULL MODE] Extracting 1991-2023 (33 years)")

    logger.info("="*80)
    logger.info("ETL Pipeline: GDP and Gross Value Added (82711-01i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
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

        extractor = GDPExtractor()

        # Extract GDP data (year-by-year due to API limitation)
        logger.info(f"Extracting data for {end_year - start_year + 1} years: {start_year}-{end_year}")
        logger.info("This will take a while (33 API calls for full extraction)...")

        raw_data = extractor.extract_gdp_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Sample data:\n{raw_data.head()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)

        transformer = GDPTransformer()

        # Transform data into database format
        # Creates multiple indicators (29-40 for each sector)
        # Note: ID 28 is already used for Municipal Finances, so we start from 29
        transformed_data = transformer.transform_gdp_data(
            raw_data,
            indicator_id_base=29,  # Base indicator ID for GDP sectors (28 is taken)
            years_filter=None  # Use all extracted years
        )

        if transformed_data is None or transformed_data.empty:
            logger.error("[FAILED] Transformation failed - no data output")
            return False

        logger.info(f"[SUCCESS] Transformation successful: {len(transformed_data)} transformed rows")
        logger.info(f"Indicators created: {sorted(transformed_data['indicator_id'].unique())}")
        logger.info(f"Sectors: {transformed_data['sector'].unique().tolist()}")

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
            # Count total GDP records
            # Note: GDP indicators are 29-40 (ID 28 is used for Municipal Finances)
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 29 AND 41
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal GDP/GVA records: {total_count}")

            # Count by indicator (sector)
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 29 AND 41
                GROUP BY i.indicator_id, i.indicator_name
                ORDER BY i.indicator_id
            """))

            logger.info("\nRecords by sector:")
            for row in result:
                logger.info(f"  Indicator {row[0]}: {row[2]:,} records - {row[1]}")

            # Check year range
            result = conn.execute(text("""
                SELECT MIN(t.year), MAX(t.year)
                FROM fact_demographics f
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id BETWEEN 29 AND 41
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"\nYear range: {year_range[0]} - {year_range[1]}")

            # Check geographic coverage
            result = conn.execute(text("""
                SELECT COUNT(DISTINCT g.region_code)
                FROM fact_demographics f
                JOIN dim_geography g ON f.geo_id = g.geo_id
                WHERE f.indicator_id BETWEEN 29 AND 41
            """))
            region_count = result.scalar() or 0
            logger.info(f"Regions covered: {region_count}")

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
            logger.info("To extract full data (1991-2023), run:")
            logger.info("  python pipelines/state_db/etl_82711_01i_gdp.py --full")
        else:
            logger.info("\n[SUCCESS] FIRST STATE DATABASE TABLE COMPLETE!")
            logger.info("\nNext steps:")
            logger.info("  1. Verify data with: python scripts/verification/verify_extraction_timeseries.py --indicator 29")
            logger.info("  2. Create SQL analysis scripts for GDP data")
            logger.info("  3. Proceed with next State DB table (82711-06i - Employee compensation)")

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

    parser = argparse.ArgumentParser(description='ETL Pipeline for GDP Data (82711-01i)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (1991-2023). Default is test mode (2020-2023)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (2020-2023 only)')

    args = parser.parse_args()

    # Determine mode
    if args.full:
        test_mode = False
    elif args.test:
        test_mode = True
    else:
        # Default to test mode for safety
        test_mode = True
        logger.info("[INFO] No mode specified. Running in TEST mode (2020-2023).")
        logger.info("   To run full extraction, use: --full")

    success = main(test_mode=test_mode)

    sys.exit(0 if success else 1)
