"""
Test script for BA Employment/Wage Extractor
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.ba.employment_wage_extractor import EmploymentWageExtractor


def main():
    """Test the BA extractor."""
    print("=" * 80)
    print("BA EMPLOYMENT/WAGE EXTRACTOR TEST")
    print("=" * 80)
    print()

    extractor = EmploymentWageExtractor()

    # Test single year first (2024)
    print("[1/2] Testing 2024 extraction:")
    print("-" * 80)
    df_2024 = extractor.extract_year(2024)

    if df_2024.empty:
        print("✗ No data extracted for 2024")
        return False

    print(f"✓ Extracted {len(df_2024):,} records")
    print(f"  Regions: {df_2024['region_code'].nunique()}")
    print(f"  Demographic categories: {df_2024['demographic_value'].nunique()}")
    print()

    # Show sample data
    print("Sample records (first 10 rows):")
    print(df_2024[['year', 'region_code', 'region_name', 'demographic_type',
                   'demographic_value', 'total_employees', 'median_wage_eur']].head(10))
    print()

    # Test all years
    print("[2/2] Testing all years (2020-2024):")
    print("-" * 80)
    df_all = extractor.extract_all_years()

    if df_all.empty:
        print("✗ No data extracted")
        return False

    print(f"✓ Extracted {len(df_all):,} total records")
    print(f"  Years: {sorted(df_all['year'].unique())}")
    print(f"  Regions: {df_all['region_code'].nunique()}")
    print(f"  Demographic categories: {df_all['demographic_value'].nunique()}")
    print()

    # Save raw data
    print("Saving raw data...")
    output_path = extractor.save_raw_data(df_all)
    print(f"✓ Saved to: {output_path}")
    print()

    # Summary statistics
    print("=" * 80)
    print("SUMMARY STATISTICS")
    print("=" * 80)
    print(f"Total records: {len(df_all):,}")
    print(f"Years: {df_all['year'].min()}-{df_all['year'].max()}")
    print(f"Regions: {df_all['region_code'].nunique()}")
    print()

    print("Records by year:")
    print(df_all.groupby('year').size().to_string())
    print()

    print("Demographic types:")
    print(df_all.groupby('demographic_type').size().to_string())
    print()

    # Sample NRW regions
    nrw_codes = sorted([c for c in df_all['region_code'].unique() if c.startswith('05')])
    print(f"NRW region codes ({len(nrw_codes)}):")
    print(f"  Sample: {nrw_codes[:10]}")
    print()

    print("=" * 80)
    print("✓ TEST COMPLETE")
    print("=" * 80)

    return True


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
