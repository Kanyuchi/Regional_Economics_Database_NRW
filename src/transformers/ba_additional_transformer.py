"""
BA Additional Data Transformer
Transforms economic sector and occupation data for database loading
Handles: Sectors (indicators 92-94) and Occupations (indicators 95-97)
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List
from sqlalchemy import text

from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


class BAAdditionalTransformer:
    """
    Transformer for BA economic sector and occupation data.

    Handles two similar data types:
    1. Economic Sectors (Sheet 8.3): Indicators 92-94
    2. Occupations (Sheet 8.4): Indicators 95-97

    Both create 8 records per raw row:
    - 1 employee count record
    - 1 median wage record
    - 6 wage distribution records (one per bracket)
    """

    def __init__(self):
        """Initialize transformer with database connection."""
        self.db = DatabaseManager()
        logger.info("BA Additional Data transformer initialized")

    def transform_sectors(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw economic sector data to star schema format.

        Args:
            df_raw: Raw extraction DataFrame with columns:
                - year, region_code, sector_name
                - total_employees, wage brackets, median_wage_eur

        Returns:
            DataFrame for indicators 92-94
        """
        logger.info(f"Transforming {len(df_raw)} sector records")
        return self._transform_category_data(
            df_raw,
            category_col='sector_name',
            employee_indicator=92,
            median_indicator=93,
            distribution_indicator=94
        )

    def transform_occupations(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw occupation data to star schema format.

        Args:
            df_raw: Raw extraction DataFrame with columns:
                - year, region_code, occupation_name
                - total_employees, wage brackets, median_wage_eur

        Returns:
            DataFrame for indicators 95-97
        """
        logger.info(f"Transforming {len(df_raw)} occupation records")
        return self._transform_category_data(
            df_raw,
            category_col='occupation_name',
            employee_indicator=95,
            median_indicator=96,
            distribution_indicator=97
        )

    def _transform_category_data(self, df_raw: pd.DataFrame, category_col: str,
                                  employee_indicator: int, median_indicator: int,
                                  distribution_indicator: int) -> pd.DataFrame:
        """
        Transform categorized data (sectors or occupations) to star schema format.

        Creates 8 records per row:
        - 1 employee count
        - 1 median wage
        - 6 wage bracket distributions
        """
        records = []

        for _, row in df_raw.iterrows():
            year = row['year']
            region_code = row['region_code']
            category = row[category_col]

            # Record 1: Total employees
            if pd.notna(row['total_employees']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': employee_indicator,
                    'value': row['total_employees'],
                    'gender': None,
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': f'category:{category}',
                })

            # Record 2: Median wage
            if pd.notna(row['median_wage_eur']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': median_indicator,
                    'value': row['median_wage_eur'],
                    'gender': None,
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': f'category:{category}',
                })

            # Records 3-8: Wage distribution (6 brackets)
            wage_brackets = [
                ('under_2000', row['wage_under_2000']),
                ('2000_to_3000', row['wage_2000_to_3000']),
                ('3000_to_4000', row['wage_3000_to_4000']),
                ('4000_to_5000', row['wage_4000_to_5000']),
                ('5000_to_6000', row['wage_5000_to_6000']),
                ('over_6000', row['wage_over_6000']),
            ]

            for bracket_name, bracket_value in wage_brackets:
                if pd.notna(bracket_value):
                    records.append({
                        'year': year,
                        'region_code': region_code,
                        'indicator_id': distribution_indicator,
                        'value': bracket_value,
                        'gender': None,
                        'nationality': None,
                        'age_group': None,
                        'migration_background': None,
                        'notes': f'category:{category}|wage_bracket:{bracket_name}',
                    })

        df_transformed = pd.DataFrame(records)
        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"  Indicators: {sorted(df_transformed['indicator_id'].unique())}")

        return df_transformed

    def load(self, df_transformed: pd.DataFrame) -> Dict[str, int]:
        """
        Load transformed data into the database.

        Args:
            df_transformed: Transformed DataFrame

        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading {len(df_transformed)} records to database")

        stats = {'loaded': 0, 'skipped': 0, 'failed': 0}

        # Prepare insert query (outside loop for efficiency)
        insert_query = text("""
            INSERT INTO fact_demographics (
                geo_id, time_id, indicator_id, value,
                gender, nationality, age_group, migration_background, notes
            )
            VALUES (
                :geo_id, :time_id, :indicator_id, :value,
                :gender, :nationality, :age_group, :migration_background, :notes
            )
            ON CONFLICT (geo_id, time_id, indicator_id, gender, nationality, age_group, migration_background)
            DO UPDATE SET
                value = EXCLUDED.value,
                notes = EXCLUDED.notes,
                loaded_at = CURRENT_TIMESTAMP
        """)

        with self.db.get_connection() as conn:
            for _, record in df_transformed.iterrows():
                try:
                    # Get geo_id
                    geo_id = self._get_geo_id(conn, record['region_code'])
                    if geo_id is None:
                        logger.warning(f"Geography not found for code: {record['region_code']}")
                        stats['skipped'] += 1
                        continue

                    # Get time_id
                    time_id = self._get_time_id(conn, record['year'])
                    if time_id is None:
                        logger.warning(f"Time not found for year: {record['year']}")
                        stats['skipped'] += 1
                        continue

                    # Execute insert
                    conn.execute(insert_query, {
                        'geo_id': geo_id,
                        'time_id': time_id,
                        'indicator_id': record['indicator_id'],
                        'value': record['value'],
                        'gender': record['gender'],
                        'nationality': record['nationality'],
                        'age_group': record['age_group'],
                        'migration_background': record['migration_background'],
                        'notes': record['notes'],
                    })

                    stats['loaded'] += 1

                except Exception as e:
                    logger.error(f"Error loading record: {e}")
                    stats['failed'] += 1
                    continue

        logger.info(f"Loading complete: {stats['loaded']} loaded, {stats['skipped']} skipped, {stats['failed']} failed")
        return stats

    def _get_geo_id(self, conn, region_code: str) -> int:
        """Get geo_id for a region code."""
        result = conn.execute(
            text("SELECT geo_id FROM dim_geography WHERE region_code = :code"),
            {'code': region_code}
        ).fetchone()
        return result[0] if result else None

    def _get_time_id(self, conn, year: int) -> int:
        """Get time_id for a year."""
        result = conn.execute(
            text("SELECT time_id FROM dim_time WHERE year = :year"),
            {'year': year}
        ).fetchone()
        return result[0] if result else None


def main():
    """Test the transformer."""
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    # Test sector transformation
    sample_sector_data = pd.DataFrame([
        {
            'year': 2024,
            'region_code': '05',
            'sector_name': 'C Verarbeitendes Gewerbe',
            'total_employees': 1000000.0,
            'wage_under_2000': 50000.0,
            'wage_2000_to_3000': 150000.0,
            'wage_3000_to_4000': 250000.0,
            'wage_4000_to_5000': 200000.0,
            'wage_5000_to_6000': 150000.0,
            'wage_over_6000': 200000.0,
            'median_wage_eur': 4200.0,
        }
    ])

    transformer = BAAdditionalTransformer()
    df_transformed = transformer.transform_sectors(sample_sector_data)

    print("Transformed sector records:")
    print("=" * 80)
    print(df_transformed[['indicator_id', 'value', 'notes']].to_string())
    print(f"\nTotal records: {len(df_transformed)}")


if __name__ == '__main__':
    main()
