"""
Income Distribution Transformer for Regional Economics Database
Transforms personal net income distribution data into star schema format.

Source: State Database NRW, Table 12211-9114i
Indicator ID: 88
"""

import pandas as pd
import numpy as np
from typing import Dict, Any
from pathlib import Path
import sys
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.database import DatabaseManager
from utils.logging import get_logger

logger = get_logger(__name__)


# Indicator ID for income distribution data
INDICATOR_INCOME_DISTRIBUTION = 88


class IncomeDistributionTransformer:
    """
    Transformer for personal net income distribution data.

    Converts wide-format extraction data to long-format star schema.
    Each row represents a specific combination of:
    - Geography
    - Year
    - Gender
    - Income bracket
    """

    def __init__(self):
        """Initialize transformer."""
        self.db = DatabaseManager()
        logger.info("Income distribution transformer initialized")

    def transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw extraction data to star schema format.

        Args:
            df_raw: Raw extracted data

        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info(f"Transforming {len(df_raw)} raw records")

        if df_raw.empty:
            logger.warning("No data to transform")
            return pd.DataFrame()

        # Create long-format records
        records = []

        for _, row in df_raw.iterrows():
            # Base information
            region_code = row['region_code']
            year = int(row['year'])
            gender = self._standardize_gender(row['gender'])

            # Create records for each income bracket
            income_brackets = [
                ('under_150', row['income_under_150']),
                ('150_to_300', row['income_150_to_300']),
                ('300_to_500', row['income_300_to_500']),
                ('500_to_700', row['income_500_to_700']),
                ('700_to_900', row['income_700_to_900']),
                ('900_to_1100', row['income_900_to_1100']),
                ('1100_to_1300', row['income_1100_to_1300']),
                ('1300_to_1500', row['income_1300_to_1500']),
                ('1500_and_more', row['income_1500_and_more'])
            ]

            for bracket_name, value in income_brackets:
                if pd.notna(value):
                    records.append({
                        'region_code': region_code,
                        'year': year,
                        'indicator_id': INDICATOR_INCOME_DISTRIBUTION,
                        'value': value,
                        'gender': gender,
                        'notes': f'income_bracket:{bracket_name}'
                    })

        # Convert to DataFrame
        df_transformed = pd.DataFrame(records)

        if df_transformed.empty:
            logger.warning("No records created during transformation")
            return df_transformed

        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"Unique gender values: {sorted(df_transformed['gender'].unique())}")
        logger.info(f"Unique income brackets: {df_transformed['notes'].str.extract(r'income_bracket:([^|]+)')[0].nunique()}")

        return df_transformed

    def _standardize_gender(self, gender: str) -> str:
        """
        Standardize gender values.

        Args:
            gender: Raw gender value from extraction

        Returns:
            Standardized gender value
        """
        gender_map = {
            'männlich': 'Male',
            'weiblich': 'Female',
            'Insgesamt': 'Total',
            'insgesamt': 'Total'
        }

        return gender_map.get(gender, gender)

    def load(self, df_transformed: pd.DataFrame) -> Dict[str, Any]:
        """
        Load transformed data into database.

        Args:
            df_transformed: Transformed DataFrame

        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading {len(df_transformed)} records into database")

        if df_transformed.empty:
            logger.warning("No data to load")
            return {'loaded': 0, 'skipped': 0, 'failed': 0}

        loaded_count = 0
        skipped_count = 0
        failed_count = 0

        # Process records one at a time with individual transactions
        for _, row in df_transformed.iterrows():
            try:
                with self.db.get_connection() as conn:
                    # Get geography ID
                    geo_query = """
                        SELECT geo_id FROM dim_geography
                        WHERE region_code = :region_code
                    """
                    geo_result = conn.execute(
                        text(geo_query),
                        {'region_code': row['region_code']}
                    ).fetchone()

                    if not geo_result:
                        logger.debug(f"Region {row['region_code']} not found in dim_geography")
                        skipped_count += 1
                        continue

                    geo_id = geo_result[0]

                    # Get time ID
                    time_query = """
                        SELECT time_id FROM dim_time
                        WHERE year = :year AND month IS NULL
                    """
                    time_result = conn.execute(
                        text(time_query),
                        {'year': row['year']}
                    ).fetchone()

                    if not time_result:
                        logger.warning(f"Year {row['year']} not found in dim_time")
                        skipped_count += 1
                        continue

                    time_id = time_result[0]

                    # Insert into fact table
                    # Set nationality, age_group, migration_background to NULL for conflict detection
                    insert_query = """
                        INSERT INTO fact_demographics (
                            geo_id, time_id, indicator_id, value,
                            gender, nationality, age_group, migration_background,
                            notes, extracted_at, loaded_at
                        )
                        VALUES (
                            :geo_id, :time_id, :indicator_id, :value,
                            :gender, NULL, NULL, NULL,
                            :notes, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                        )
                        ON CONFLICT (geo_id, time_id, indicator_id, gender, nationality, age_group, migration_background)
                        DO UPDATE SET
                            value = EXCLUDED.value,
                            notes = EXCLUDED.notes,
                            loaded_at = CURRENT_TIMESTAMP
                    """

                    conn.execute(
                        text(insert_query),
                        {
                            'geo_id': geo_id,
                            'time_id': time_id,
                            'indicator_id': row['indicator_id'],
                            'value': float(row['value']),
                            'gender': row.get('gender'),
                            'notes': row.get('notes')
                        }
                    )

                    # Commit is handled by context manager
                    loaded_count += 1

            except Exception as e:
                logger.error(f"Error loading record: {e}")
                logger.debug(f"Problem record: {row.to_dict()}")
                failed_count += 1
                continue

        logger.info(f"Loading complete: {loaded_count} loaded, {skipped_count} skipped, {failed_count} failed")

        return {
            'loaded': loaded_count,
            'skipped': skipped_count,
            'failed': failed_count
        }


def main():
    """Main transformation function."""

    # Load raw data
    raw_data_path = Path('data/raw/state_db/income_distribution_raw_2005_2019.csv')

    if not raw_data_path.exists():
        print(f"✗ Raw data file not found: {raw_data_path}")
        print("  Run the extractor first!")
        sys.exit(1)

    print(f"Loading raw data from: {raw_data_path}")
    df_raw = pd.read_csv(raw_data_path)
    print(f"Loaded {len(df_raw)} raw records")

    # Transform
    transformer = IncomeDistributionTransformer()
    df_transformed = transformer.transform(df_raw)

    if df_transformed.empty:
        print("✗ Transformation failed - no records created")
        sys.exit(1)

    print(f"\n✓ Transformation complete!")
    print(f"  Records created: {len(df_transformed):,}")
    print(f"  Sample transformed data:")
    print(df_transformed.head(10).to_string())

    # Load into database
    print(f"\nLoading into database...")
    stats = transformer.load(df_transformed)

    print(f"\n✓ Loading complete!")
    print(f"  Loaded: {stats['loaded']:,}")
    print(f"  Skipped: {stats['skipped']:,}")
    print(f"  Failed: {stats['failed']:,}")


if __name__ == '__main__':
    main()
