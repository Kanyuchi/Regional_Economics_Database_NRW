"""
Low-Wage Workers Transformer
Transforms BA low-wage employment data for database loading
Indicators: 98-100
"""

import pandas as pd
from pathlib import Path
from typing import Dict
from sqlalchemy import text

from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


class LowWageTransformer:
    """
    Transformer for BA low-wage workers data.

    Transforms raw extraction data into star schema format for fact_demographics table.
    Creates 7 records per raw row:
    - 1 baseline employee count (indicator 100)
    - 3 threshold counts (indicator 98): national, west, east
    - 3 threshold percentages (indicator 99): national, west, east
    """

    def __init__(self):
        """Initialize transformer with database connection."""
        self.db = DatabaseManager()
        logger.info("Low-wage transformer initialized")

    def transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw low-wage data to star schema format.

        Args:
            df_raw: Raw extraction DataFrame with columns:
                - year, region_code, region_name
                - total_employees
                - below_national_count, below_national_pct
                - below_west_count, below_west_pct
                - below_east_count, below_east_pct

        Returns:
            DataFrame with columns matching fact_demographics table
        """
        logger.info(f"Transforming {len(df_raw)} raw records")

        records = []

        for _, row in df_raw.iterrows():
            year = row['year']
            region_code = row['region_code']

            # Record 1: Baseline total employees (Indicator 100)
            if pd.notna(row['total_employees']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': 100,
                    'value': row['total_employees'],
                    'gender': None,
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': None,
                })

            # Records 2-4: Low-wage worker counts (Indicator 98)
            thresholds = [
                ('national', row['below_national_count']),
                ('west', row['below_west_count']),
                ('east', row['below_east_count']),
            ]

            for threshold_type, count_value in thresholds:
                if pd.notna(count_value):
                    records.append({
                        'year': year,
                        'region_code': region_code,
                        'indicator_id': 98,
                        'value': count_value,
                        'gender': None,
                        'nationality': None,
                        'age_group': None,
                        'migration_background': None,
                        'notes': f'threshold:{threshold_type}',
                    })

            # Records 5-7: Low-wage worker percentages (Indicator 99)
            threshold_pcts = [
                ('national', row['below_national_pct']),
                ('west', row['below_west_pct']),
                ('east', row['below_east_pct']),
            ]

            for threshold_type, pct_value in threshold_pcts:
                if pd.notna(pct_value):
                    records.append({
                        'year': year,
                        'region_code': region_code,
                        'indicator_id': 99,
                        'value': pct_value,
                        'gender': None,
                        'nationality': None,
                        'age_group': None,
                        'migration_background': None,
                        'notes': f'threshold:{threshold_type}',
                    })

        df_transformed = pd.DataFrame(records)
        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"  Indicators: {sorted(df_transformed['indicator_id'].unique())}")
        logger.info(f"  Years: {sorted(df_transformed['year'].unique())}")

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

    # Test with sample data
    sample_data = pd.DataFrame([
        {
            'year': 2024,
            'region_code': '05',
            'region_name': 'Nordrhein-Westfalen',
            'total_employees': 5000000.0,
            'below_national_count': 550000.0,
            'below_national_pct': 11.0,
            'below_west_count': 600000.0,
            'below_west_pct': 12.0,
            'below_east_count': None,
            'below_east_pct': None,
        }
    ])

    transformer = LowWageTransformer()
    df_transformed = transformer.transform(sample_data)

    print("Transformed records:")
    print("=" * 80)
    print(df_transformed.to_string())
    print(f"\nTotal records: {len(df_transformed)}")


if __name__ == '__main__':
    main()
