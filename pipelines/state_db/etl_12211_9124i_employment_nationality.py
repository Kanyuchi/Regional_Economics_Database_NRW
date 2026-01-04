"""
ETL Pipeline for Employment by Nationality Data
Table: 12211-9124i
Indicator: 87
Period: 1997-2019
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.state_db.employment_nationality_extractor import EmploymentNationalityExtractor
from src.transformers.employment_nationality_transformer import EmploymentNationalityTransformer
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Run complete ETL pipeline for employment by nationality data."""
    print("=" * 80)
    print("ETL PIPELINE: EMPLOYMENT BY NATIONALITY")
    print("Table: 12211-9124i | Indicator: 87 | Period: 1997-2019")
    print("=" * 80)
    print()

    try:
        # Step 1: Extract
        print("[1/3] EXTRACTION")
        print("-" * 80)
        extractor = EmploymentNationalityExtractor()
        df_raw = extractor.extract_all_years()

        if df_raw.empty:
            logger.error("Extraction failed - no data retrieved")
            print("✗ Extraction failed - no data")
            return False

        print(f"✓ Extracted {len(df_raw):,} raw records")
        print(f"  Years: {df_raw['year'].min()}-{df_raw['year'].max()}")
        print(f"  Regions: {df_raw['region_code'].nunique()}")
        print()

        # Save raw data
        raw_data_path = extractor.save_raw_data(df_raw)
        print(f"  Saved to: {raw_data_path}")
        print()

        # Step 2: Transform
        print("[2/3] TRANSFORMATION")
        print("-" * 80)
        transformer = EmploymentNationalityTransformer()
        df_transformed = transformer.transform(df_raw)

        if df_transformed.empty:
            logger.error("Transformation failed - no records created")
            print("✗ Transformation failed - no records")
            return False

        print(f"✓ Transformed into {len(df_transformed):,} records")
        print(f"  Unique combinations:")
        print(f"    Nationalities: {df_transformed['notes'].str.extract(r'nationality:([^|]+)')[0].nunique()}")
        print(f"    Employment statuses: {df_transformed['notes'].str.extract(r'employment_status:([^|]+)')[0].nunique()}")
        print(f"    Genders: {df_transformed['gender'].nunique()}")
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

        return True

    except Exception as e:
        logger.error("ETL pipeline failed", exc_info=True)
        print(f"\n✗ Pipeline failed: {str(e)}")
        return False


if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)
