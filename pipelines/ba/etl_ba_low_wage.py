"""
ETL Pipeline: BA Low-Wage Workers
Source: Federal Employment Agency (BA) - Sheet 8.5
Period: 2020-2024
Indicators: 98-100
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.ba.low_wage_extractor import LowWageExtractor
from src.transformers.low_wage_transformer import LowWageTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run complete ETL pipeline for BA low-wage workers data."""
    print("=" * 80)
    print("ETL PIPELINE: BA LOW-WAGE WORKERS DATA")
    print("Source: Federal Employment Agency (BA) - Sheet 8.5")
    print("Period: 2020-2024 | Indicators: 98-100")
    print("=" * 80)
    print()

    # Step 1: EXTRACTION
    print("[1/3] EXTRACTION")
    print("-" * 80)

    extractor = LowWageExtractor()
    df_raw = extractor.extract_all_years()

    if df_raw.empty:
        logger.error("Extraction failed - no data extracted")
        return

    print(f"✓ Extracted {len(df_raw):,} raw records")
    print(f"  Years: {sorted(df_raw['year'].unique())}")
    print(f"  Regions: {df_raw['region_code'].nunique()}")
    print()

    # Save raw data
    raw_data_path = extractor.save_raw_data(df_raw)
    print(f"  Saved to: {raw_data_path}")
    print()

    # Step 2: TRANSFORMATION
    print("[2/3] TRANSFORMATION")
    print("-" * 80)

    transformer = LowWageTransformer()
    df_transformed = transformer.transform(df_raw)

    print(f"✓ Transformed into {len(df_transformed):,} records")
    print(f"  Indicators:")
    for ind_id in sorted(df_transformed['indicator_id'].unique()):
        count = len(df_transformed[df_transformed['indicator_id'] == ind_id])
        print(f"    {ind_id}: {count:,} records")
    print()

    # Step 3: LOADING
    print("[3/3] LOADING")
    print("-" * 80)

    stats = transformer.load(df_transformed)

    print(f"✓ Loading complete:")
    print(f"  Loaded: {stats['loaded']:,}")
    print(f"  Skipped: {stats['skipped']:,}")
    print(f"  Failed: {stats['failed']:,}")
    print()

    # Summary
    print("=" * 80)
    print("✓ PIPELINE COMPLETE")
    print("=" * 80)
    print()
    print(f"Database updated with {stats['loaded']:,} records")
    print("Indicators 98-100 now available for analysis")
    print()


if __name__ == '__main__':
    main()
