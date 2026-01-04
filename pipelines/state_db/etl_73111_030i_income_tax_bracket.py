"""
ETL Pipeline for Wage and Income Tax by Income Bracket
Table: 73111-030i (State Database NRW)
Period: 2012-2021 (10 years)

Income Brackets (13 + 2):
- 13 income ranges: €1-5k through €1M+
- 1 total (insgesamt)
- 1 loss cases (Verlustfälle)

Indicators: 59-61 (Taxpayers, Income, Tax by bracket)
"""

import sys
sys.path.append('.')

from src.extractors.state_db.income_tax_bracket_extractor import IncomeTaxBracketExtractor
from src.transformers.income_tax_bracket_transformer import IncomeTaxBracketTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main(test_mode=False):
    """
    Execute ETL pipeline for wage and income tax by bracket data.

    Args:
        test_mode: If True, extract only 2020-2021 for testing. If False, extract full 2012-2021.
    """

    # Determine year range
    if test_mode:
        start_year = 2020
        end_year = 2021
        logger.info("[TEST MODE] Extracting 2020-2021 only")
    else:
        start_year = 2012
        end_year = 2021
        logger.info("[FULL MODE] Extracting 2012-2021 (10 years)")

    logger.info("="*80)
    logger.info("ETL Pipeline: Income Tax by Income Bracket (73111-030i)")
    logger.info("Source: State Database NRW (Landesdatenbank)")
    logger.info(f"Period: {start_year}-{end_year} ({end_year - start_year + 1} years)")
    logger.info("Indicators: 59 (Taxpayers), 60 (Income), 61 (Tax) - all by bracket")
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

        extractor = IncomeTaxBracketExtractor()

        logger.info(f"Extracting data for {end_year - start_year + 1} years: {start_year}-{end_year}")
        logger.info("Each year has ~6,660 rows (444 regions × 15 brackets)")

        raw_data = extractor.extract_income_tax_bracket_data(
            startyear=start_year,
            endyear=end_year
        )

        if raw_data is None or raw_data.empty:
            logger.error("[FAILED] Extraction failed - no data retrieved")
            return False

        logger.info(f"[SUCCESS] Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        logger.info(f"Year range: {raw_data['year'].min()} - {raw_data['year'].max()}")
        logger.info(f"Unique brackets: {raw_data['income_bracket'].nunique()}")

        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)

        transformer = IncomeTaxBracketTransformer()

        # Show bracket summary before transform
        logger.info("\nIncome distribution summary (all years combined):")
        summary = transformer.get_bracket_summary(raw_data[raw_data['region_code'] == '05'])
        logger.info(f"\n{summary.to_string()}")

        # Transform data (excluding totals to avoid duplication with 73111-010i)
        transformed_data = transformer.transform_bracket_data(
            raw_data,
            years_filter=None,
            exclude_totals=True  # Don't load 'insgesamt' and 'Verlustfälle'
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

        from sqlalchemy import text
        from src.utils.database import DatabaseManager
        db = DatabaseManager()

        with db.get_connection() as conn:
            # Count total bracket records
            result = conn.execute(text("""
                SELECT COUNT(*)
                FROM fact_demographics
                WHERE indicator_id BETWEEN 59 AND 61
            """))
            total_count = result.scalar() or 0
            logger.info(f"\nTotal income tax bracket records: {total_count}")

            # Count by indicator
            result = conn.execute(text("""
                SELECT
                    i.indicator_id,
                    i.indicator_name_en,
                    COUNT(f.fact_id) as record_count
                FROM dim_indicator i
                LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
                WHERE i.indicator_id BETWEEN 59 AND 61
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
                WHERE f.indicator_id BETWEEN 59 AND 61
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"\nYear range: {year_range[0]} - {year_range[1]}")

            # Sample: show bracket distribution for NRW 2021
            result = conn.execute(text("""
                SELECT
                    f.notes,
                    f.value
                FROM fact_demographics f
                JOIN dim_geography g ON f.geo_id = g.geo_id
                JOIN dim_time t ON f.time_id = t.time_id
                WHERE f.indicator_id = 59  -- Taxpayers by bracket
                  AND g.region_code = '05'
                  AND t.year = 2021
                ORDER BY f.notes
                LIMIT 13
            """))
            logger.info("\nNRW 2021 - Taxpayers by bracket (sample):")
            for row in result:
                bracket = row[0].split('|')[1] if '|' in row[0] else row[0]
                logger.info(f"  {bracket}: {row[1]:,.0f}")

        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("[SUCCESS] ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Total records loaded: {total_count:,}")
        logger.info(f"Period: {start_year}-{end_year}")

        if test_mode:
            logger.info("\n[TEST MODE] Complete. Run with --full for full extraction.")
        else:
            logger.info("\n[SUCCESS] INCOME TAX BRACKET TABLE COMPLETE!")

        return True

    except Exception as e:
        logger.error(f"[FAILED] Pipeline failed: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        if extractor:
            extractor.close()
        if loader:
            loader.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='ETL Pipeline for Income Tax by Bracket (73111-030i)')
    parser.add_argument('--full', action='store_true',
                       help='Run full extraction (2012-2021). Default is test mode (2020-2021)')
    parser.add_argument('--test', action='store_true',
                       help='Run test mode (2020-2021 only)')

    args = parser.parse_args()

    if args.full:
        test_mode = False
    elif args.test:
        test_mode = True
    else:
        test_mode = True
        logger.info("[INFO] No mode specified. Running in TEST mode.")

    success = main(test_mode=test_mode)
    sys.exit(0 if success else 1)
