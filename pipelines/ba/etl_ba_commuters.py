"""
ETL Pipeline: BA Commuter Statistics
Source: Federal Employment Agency (BA) - Pendlerstatistik
Period: 2002-2024
Indicators: 101-102 (Incoming/Outgoing Commuters)
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.ba.commuter_extractor import CommuterExtractor
from src.transformers.commuter_transformer import CommuterTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run complete ETL pipeline for BA commuter statistics."""
    print("=" * 80)
    print("ETL PIPELINE: BA COMMUTER STATISTICS")
    print("Source: Federal Employment Agency (BA) - Pendlerstatistik")
    print("Period: 2002-2024 | Indicators: 101-102")
    print("=" * 80)
    print()

    # Step 1: EXTRACTION
    print("[1/3] EXTRACTION")
    print("-" * 80)

    extractor = CommuterExtractor()

    # Extract summary data (faster, covers all years 2002-2024)
    # For detailed origin-destination flows, set include_detailed=True
    print("Extracting summary data (all years 2002-2024)...")
    df_raw = extractor.extract_all(include_detailed=False)

    if df_raw.empty:
        logger.error("Extraction failed - no data extracted")
        return

    print(f"✓ Extracted {len(df_raw):,} raw records")
    print(f"  Years: {sorted(df_raw['year'].unique())}")
    print(f"  Incoming commuters: {len(df_raw[df_raw['commuter_type'] == 'incoming']):,}")
    print(f"  Outgoing commuters: {len(df_raw[df_raw['commuter_type'] == 'outgoing']):,}")
    print()

    # Save raw data
    raw_data_path = extractor.save_raw_data(df_raw, suffix='summary_only')
    print(f"  Saved to: {raw_data_path}")
    print()

    # Step 2: TRANSFORMATION
    print("[2/3] TRANSFORMATION")
    print("-" * 80)

    transformer = CommuterTransformer()
    df_transformed = transformer.transform(df_raw)

    print(f"✓ Transformed into {len(df_transformed):,} records")
    print(f"  Indicators:")
    for ind_id in sorted(df_transformed['indicator_id'].unique()):
        count = len(df_transformed[df_transformed['indicator_id'] == ind_id])
        ind_name = {101: "Incoming Commuters", 102: "Outgoing Commuters"}
        print(f"    {ind_id} ({ind_name.get(ind_id, 'Unknown')}): {count:,} records")
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
    print(f"Database updated with {stats['loaded']:,} commuter records")
    print("Indicators 101-102 now available for analysis:")
    print("  - 101: Incoming Commuters (Einpendler)")
    print("  - 102: Outgoing Commuters (Auspendler)")
    print("  - 103: Net Balance can be calculated as (101 - 102)")
    print()
    print("Coverage: 2002-2024 (23 years)")
    print("Geographic breakdown: NRW districts (summary level)")
    print("Demographic breakdown: Gender, Nationality, Trainees")
    print()


if __name__ == '__main__':
    main()
