"""
Test Commuter Extractor
Quick test to verify the commuter data extraction works
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.ba.commuter_extractor import CommuterExtractor

def main():
    """Test the extractor."""
    extractor = CommuterExtractor()

    # Test extraction of summary data only (faster)
    print("=" * 80)
    print("Testing SUMMARY data extraction (2002-2024)")
    print("=" * 80)
    df_summary = extractor.extract_all(include_detailed=False)
    print(f"\nTotal summary records: {len(df_summary):,}")
    print(f"Years: {sorted(df_summary['year'].unique())}")
    print(f"Incoming: {len(df_summary[df_summary['commuter_type'] == 'incoming']):,}")
    print(f"Outgoing: {len(df_summary[df_summary['commuter_type'] == 'outgoing']):,}")

    print("\nSample incoming commuter data (2024):")
    sample_in = df_summary[(df_summary['commuter_type'] == 'incoming') & (df_summary['year'] == 2024)].head(5)
    if not sample_in.empty:
        print(sample_in[['year', 'workplace_name', 'workplace_code', 'total', 'men', 'women']].to_string())

    print("\nSample outgoing commuter data (2024):")
    sample_out = df_summary[(df_summary['commuter_type'] == 'outgoing') & (df_summary['year'] == 2024)].head(5)
    if not sample_out.empty:
        print(sample_out[['year', 'residence_name', 'residence_code', 'total', 'men', 'women']].to_string())

    # Save
    output_path = extractor.save_raw_data(df_summary, suffix='summary_only')
    print(f"\nSaved to: {output_path}")


if __name__ == '__main__':
    main()
