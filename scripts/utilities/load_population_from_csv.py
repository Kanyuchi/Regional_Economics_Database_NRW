"""
Load Population Data from Local CSV File
Regional Economics Database for NRW

This script loads population data from a local CSV file and applies
the corrected transformation (filtering to only 'Total' age group).

Usage:
    python scripts/utilities/load_population_from_csv.py <path_to_csv>

    Or use the test data:
    python scripts/utilities/load_population_from_csv.py data/raw/test_population_data.csv
"""

import sys
from pathlib import Path
import pandas as pd
from io import StringIO

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.logging import setup_logging, get_logger
from transformers.demographics_transformer import DemographicsTransformer
from loaders.db_loader import DataLoader

# Setup logging
setup_logging()
logger = get_logger(__name__)


def parse_genesis_csv(file_path: str) -> pd.DataFrame:
    """
    Parse GENESIS CSV format from local file.

    Args:
        file_path: Path to the CSV file

    Returns:
        Parsed DataFrame with proper column names
    """
    logger.info(f"Parsing CSV file: {file_path}")

    # Read the raw file to detect data start
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Find where data starts (first line with date pattern)
    data_start_row = 0
    for i, line in enumerate(lines):
        if line.strip() and ';' in line:
            first_field = line.strip().split(';')[0]
            # Match date patterns
            if (len(first_field) == 10 and
                (first_field[4] == '-' and first_field[7] == '-' or
                 first_field[2] == '.' and first_field[5] == '.')):
                data_start_row = i
                logger.info(f"Found data starting at row {i}")
                break

    # Read CSV from data start row
    df = pd.read_csv(
        file_path,
        delimiter=';',
        encoding='utf-8',
        skiprows=data_start_row,
        header=None
    )

    # Assign proper column names for table 12411-03-03-4
    if len(df.columns) == 13:
        df.columns = [
            'date', 'region_code', 'region_name', 'age_group',
            'pop_total_total', 'pop_total_male', 'pop_total_female',
            'pop_german_total', 'pop_german_male', 'pop_german_female',
            'pop_foreign_total', 'pop_foreign_male', 'pop_foreign_female'
        ]
        logger.info("Assigned standard column names")
    else:
        logger.error(f"Unexpected column count: {len(df.columns)}")
        return None

    logger.info(f"Parsed {len(df)} rows")
    return df


def main():
    """Main execution function."""
    if len(sys.argv) < 2:
        print("Usage: python load_population_from_csv.py <path_to_csv>")
        print("\nExample:")
        print("  python scripts/utilities/load_population_from_csv.py data/raw/test_population_data.csv")
        sys.exit(1)

    csv_file = sys.argv[1]

    if not Path(csv_file).exists():
        logger.error(f"File not found: {csv_file}")
        sys.exit(1)

    logger.info("=" * 80)
    logger.info("LOAD POPULATION DATA FROM CSV")
    logger.info("=" * 80)
    logger.info(f"Source file: {csv_file}")

    try:
        # Step 1: Parse CSV
        logger.info("\nStep 1: Parsing CSV file...")
        raw_df = parse_genesis_csv(csv_file)

        if raw_df is None or raw_df.empty:
            logger.error("Failed to parse CSV")
            sys.exit(1)

        logger.info(f"✓ Parsed {len(raw_df)} rows")

        # Step 2: Transform data
        logger.info("\nStep 2: Transforming data with corrected transformer...")
        transformer = DemographicsTransformer()

        # Filter to years 2011-2024 for NRW data
        years_filter = list(range(2011, 2025))
        transformed_df = transformer.transform_population_data(
            raw_df,
            indicator_id=1,
            years_filter=years_filter
        )

        if transformed_df is None or transformed_df.empty:
            logger.error("Transformation failed")
            sys.exit(1)

        logger.info(f"✓ Transformed to {len(transformed_df)} rows")
        logger.info(f"  (Filtered to only 'total' age group)")

        # Step 3: Load to database
        logger.info("\nStep 3: Loading data into database...")
        loader = DataLoader()

        records_loaded = loader.load_demographics_data(transformed_df)

        logger.info(f"✓ Loaded {records_loaded} records")

        loader.close()

        # Success
        logger.info("\n" + "=" * 80)
        logger.info("✓ POPULATION DATA LOADED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("\nNext steps:")
        logger.info("1. Verify data with your MCP query")
        logger.info("2. Check that population values are now correct")
        logger.info("3. Duisburg should show ~500,000 (not ~1,000,000)")

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
