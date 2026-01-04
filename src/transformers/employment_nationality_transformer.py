"""
Employment by Nationality Transformer for Regional Economics Database
Transforms employment status by nationality data into star schema format.

Source: State Database NRW, Table 12211-9124i
Indicator ID: 87
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


# Indicator ID for employment by nationality data
INDICATOR_EMPLOYMENT_NATIONALITY = 87


class EmploymentNationalityTransformer:
    """
    Transformer for employment status by nationality data.

    Converts wide-format extraction data to long-format star schema.
    Each row represents a specific combination of:
    - Geography
    - Year
    - Gender
    - Nationality category
    - Employment status
    """

    def __init__(self):
        """Initialize transformer."""
        self.db = DatabaseManager()
        logger.info("Employment nationality transformer initialized")

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

            # Create records for each nationality × employment status combination
            records.extend(self._create_total_records(row, region_code, year, gender))
            records.extend(self._create_german_records(row, region_code, year, gender))
            records.extend(self._create_foreigner_records(row, region_code, year, gender))

        # Convert to DataFrame
        df_transformed = pd.DataFrame(records)

        if df_transformed.empty:
            logger.warning("No records created during transformation")
            return df_transformed

        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"Unique gender values: {sorted(df_transformed['gender'].unique())}")

        return df_transformed

    def _create_total_records(self, row: pd.Series, region_code: str,
                              year: int, gender: str) -> list:
        """Create records for total population (all nationalities)."""
        records = []

        # Total population, total
        if pd.notna(row['total_population']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['total_population'],
                'gender': gender,
                'notes': 'nationality:total|employment_status:total'
            })

        # Total population, employed
        if pd.notna(row['total_employed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['total_employed'],
                'gender': gender,
                'notes': 'nationality:total|employment_status:employed'
            })

        # Total population, unemployed
        if pd.notna(row['total_unemployed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['total_unemployed'],
                'gender': gender,
                'notes': 'nationality:total|employment_status:unemployed'
            })

        # Total population, not in labor force
        if pd.notna(row['total_not_in_labor_force']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['total_not_in_labor_force'],
                'gender': gender,
                'notes': 'nationality:total|employment_status:not_in_labor_force'
            })

        return records

    def _create_german_records(self, row: pd.Series, region_code: str,
                               year: int, gender: str) -> list:
        """Create records for German nationals."""
        records = []

        # Germans, total
        if pd.notna(row['german_total']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['german_total'],
                'gender': gender,
                'notes': 'nationality:german|employment_status:total'
            })

        # Germans, employed
        if pd.notna(row['german_employed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['german_employed'],
                'gender': gender,
                'notes': 'nationality:german|employment_status:employed'
            })

        # Germans, unemployed
        if pd.notna(row['german_unemployed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['german_unemployed'],
                'gender': gender,
                'notes': 'nationality:german|employment_status:unemployed'
            })

        # Germans, not in labor force
        if pd.notna(row['german_not_in_labor_force']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['german_not_in_labor_force'],
                'gender': gender,
                'notes': 'nationality:german|employment_status:not_in_labor_force'
            })

        return records

    def _create_foreigner_records(self, row: pd.Series, region_code: str,
                                   year: int, gender: str) -> list:
        """Create records for foreign nationals."""
        records = []

        # Foreigners, total
        if pd.notna(row['foreigner_total']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['foreigner_total'],
                'gender': gender,
                'notes': 'nationality:foreigner|employment_status:total'
            })

        # Foreigners, employed
        if pd.notna(row['foreigner_employed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['foreigner_employed'],
                'gender': gender,
                'notes': 'nationality:foreigner|employment_status:employed'
            })

        # Foreigners, unemployed
        if pd.notna(row['foreigner_unemployed']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['foreigner_unemployed'],
                'gender': gender,
                'notes': 'nationality:foreigner|employment_status:unemployed'
            })

        # Foreigners, not in labor force
        if pd.notna(row['foreigner_not_in_labor_force']):
            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_EMPLOYMENT_NATIONALITY,
                'value': row['foreigner_not_in_labor_force'],
                'gender': gender,
                'notes': 'nationality:foreigner|employment_status:not_in_labor_force'
            })

        return records

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
    raw_data_path = Path('data/raw/state_db/employment_nationality_raw_1997_2019.csv')

    if not raw_data_path.exists():
        print(f"✗ Raw data file not found: {raw_data_path}")
        print("  Run the extractor first!")
        sys.exit(1)

    print(f"Loading raw data from: {raw_data_path}")
    df_raw = pd.read_csv(raw_data_path)
    print(f"Loaded {len(df_raw)} raw records")

    # Transform
    transformer = EmploymentNationalityTransformer()
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
