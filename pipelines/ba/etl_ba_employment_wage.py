"""
ETL Pipeline for BA Employment and Wage Data
Source: Federal Employment Agency (BA)
Period: 2020-2024
Indicators: 89-91
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.ba.employment_wage_extractor import EmploymentWageExtractor
from src.transformers.employment_wage_transformer import EmploymentWageTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run complete ETL pipeline for BA employment and wage data."""
    print("=" * 80)
    print("ETL PIPELINE: BA EMPLOYMENT AND WAGE DATA")
    print("Source: Federal Employment Agency (BA)")
    print("Period: 2020-2024 | Indicators: 89-91")
    print("=" * 80)
    print()

    try:
        # Step 1: Extract
        print("[1/3] EXTRACTION")
        print("-" * 80)
        extractor = EmploymentWageExtractor()
        df_raw = extractor.extract_all_years()

        if df_raw.empty:
            logger.error("Extraction failed - no data retrieved")
            print("✗ Extraction failed - no data")
            return False

        print(f"✓ Extracted {len(df_raw):,} raw records")
        print(f"  Years: {df_raw['year'].min()}-{df_raw['year'].max()}")
        print(f"  Regions: {df_raw['region_code'].nunique()}")
        print(f"  Demographic categories: {df_raw['demographic_value'].nunique()}")
        print()

        # Save raw data
        raw_data_path = extractor.save_raw_data(df_raw)
        print(f"  Saved to: {raw_data_path}")
        print()

        # Step 2: Transform
        print("[2/3] TRANSFORMATION")
        print("-" * 80)
        transformer = EmploymentWageTransformer()
        df_transformed = transformer.transform(df_raw)

        if df_transformed.empty:
            logger.error("Transformation failed - no records created")
            print("✗ Transformation failed - no records")
            return False

        print(f"✓ Transformed into {len(df_transformed):,} records")
        print(f"  Indicators:")
        for ind_id, count in df_transformed.groupby('indicator_id').size().items():
            ind_name = {
                89: "Total Full-time Employees",
                90: "Median Wage",
                91: "Wage Distribution"
            }.get(ind_id, f"Unknown ({ind_id})")
            print(f"    {ind_id}: {ind_name} - {count:,} records")
        print()

        # Step 3: Load
        print("[3/3] LOADING")
        print("-" * 80)
        stats = transformer.load(df_transformed)

        print(f"✓ Loading complete")
        print(f"  Loaded: {stats['loaded']:,}")
        print(f"  Skipped: {stats['skipped']:,}")
        print(f"  Failed: {stats['failed']:,}")
        print()

        # Summary
        print("=" * 80)
        print("[SUCCESS] ETL PIPELINE COMPLETED")
        print("=" * 80)
        print(f"Total records loaded: {stats['loaded']:,}")
        print()

        print("Data Summary:")
        print(f"  Period: 2020-2024 (5 years)")
        print(f"  Regions: {df_raw['region_code'].nunique()} (54 NRW districts + Germany)")
        print(f"  Indicators:")
        print(f"    89 - Full-time Employees by Demographics")
        print(f"    90 - Median Monthly Gross Wage by Demographics")
        print(f"    91 - Wage Distribution by Brackets and Demographics")
        print()

        print("  Demographic Breakdowns:")
        print(f"    - Gender: male, female, total")
        print(f"    - Age groups: under 25, 25-55, 55+")
        print(f"    - Nationality: German, foreigner")
        print(f"    - Education: none, vocational, academic")
        print(f"    - Skill level: assistant, specialist, expert, highly qualified")
        print()

        print("  Wage Brackets (Indicator 91):")
        print(f"    - Under 2,000 EUR")
        print(f"    - 2,000 - 3,000 EUR")
        print(f"    - 3,000 - 4,000 EUR")
        print(f"    - 4,000 - 5,000 EUR")
        print(f"    - 5,000 - 6,000 EUR")
        print(f"    - Over 6,000 EUR")
        print()

        return True

    except Exception as e:
        logger.error("ETL pipeline failed", exc_info=True)
        print(f"\\n✗ Pipeline failed: {str(e)}")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
