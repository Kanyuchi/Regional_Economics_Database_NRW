"""
BA Employment and Wage Transformer
Transforms BA employment/wage data into star schema format.

Source: Federal Employment Agency (BA)
Period: 2020-2024
Creates multiple indicators for different wage metrics
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List
from pathlib import Path
import sys
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.database import DatabaseManager
from utils.logging import get_logger

logger = get_logger(__name__)


# Indicator IDs for BA employment/wage data
# These will be defined when adding indicators to the database
INDICATOR_BA_TOTAL_EMPLOYEES = 89  # Total full-time employees
INDICATOR_BA_MEDIAN_WAGE = 90  # Median wage
INDICATOR_BA_WAGE_DISTRIBUTION = 91  # Employee count by wage bracket


class EmploymentWageTransformer:
    """
    Transformer for BA employment and wage data.

    Converts wide-format extraction data to long-format star schema.
    Creates separate records for:
    - Total employees by demographic category
    - Median wage by demographic category
    - Employee count in each wage bracket by demographic category
    """

    def __init__(self):
        """Initialize transformer."""
        self.db = DatabaseManager()
        logger.info("BA employment/wage transformer initialized")

    def transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw extraction data to star schema format.

        Args:
            df_raw: Raw extracted data with demographic and wage columns

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
            demographic_type = row['demographic_type']
            demographic_value = row['demographic_value']

            # Create records for each indicator type
            records.extend(self._create_total_employee_records(
                row, region_code, year, demographic_type, demographic_value
            ))

            records.extend(self._create_median_wage_records(
                row, region_code, year, demographic_type, demographic_value
            ))

            records.extend(self._create_wage_distribution_records(
                row, region_code, year, demographic_type, demographic_value
            ))

        # Convert to DataFrame
        df_transformed = pd.DataFrame(records)

        if df_transformed.empty:
            logger.warning("No records created during transformation")
            return df_transformed

        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"  Indicators: {df_transformed['indicator_id'].nunique()}")
        logger.info(f"  Years: {sorted(df_transformed['year'].unique())}")

        return df_transformed

    def _create_total_employee_records(self, row: pd.Series, region_code: str,
                                       year: int, demographic_type: str,
                                       demographic_value: str) -> List[Dict]:
        """Create records for total employees by demographic category."""
        records = []

        if pd.notna(row['total_employees']):
            # Map demographic information to database fields
            gender, age_group, nationality, notes = self._map_demographic_fields(
                demographic_type, demographic_value
            )

            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_BA_TOTAL_EMPLOYEES,
                'value': float(row['total_employees']),
                'gender': gender,
                'age_group': age_group,
                'nationality': nationality,
                'migration_background': None,
                'notes': notes
            })

        return records

    def _create_median_wage_records(self, row: pd.Series, region_code: str,
                                    year: int, demographic_type: str,
                                    demographic_value: str) -> List[Dict]:
        """Create records for median wage by demographic category."""
        records = []

        if pd.notna(row['median_wage_eur']):
            # Map demographic information to database fields
            gender, age_group, nationality, notes = self._map_demographic_fields(
                demographic_type, demographic_value
            )

            records.append({
                'region_code': region_code,
                'year': year,
                'indicator_id': INDICATOR_BA_MEDIAN_WAGE,
                'value': float(row['median_wage_eur']),
                'gender': gender,
                'age_group': age_group,
                'nationality': nationality,
                'migration_background': None,
                'notes': notes
            })

        return records

    def _create_wage_distribution_records(self, row: pd.Series, region_code: str,
                                          year: int, demographic_type: str,
                                          demographic_value: str) -> List[Dict]:
        """Create records for employee count in each wage bracket."""
        records = []

        # Map demographic information to database fields (same for all brackets)
        gender, age_group, nationality, base_notes = self._map_demographic_fields(
            demographic_type, demographic_value
        )

        # Wage brackets with their column names
        wage_brackets = [
            ('wage_under_2000', 'under_2000'),
            ('wage_2000_to_3000', '2000_to_3000'),
            ('wage_3000_to_4000', '3000_to_4000'),
            ('wage_4000_to_5000', '4000_to_5000'),
            ('wage_5000_to_6000', '5000_to_6000'),
            ('wage_over_6000', 'over_6000'),
        ]

        for col_name, bracket_name in wage_brackets:
            if pd.notna(row[col_name]):
                # Add wage bracket to notes
                notes = f"wage_bracket:{bracket_name}"
                if base_notes:
                    notes = f"{base_notes}|{notes}"

                records.append({
                    'region_code': region_code,
                    'year': year,
                    'indicator_id': INDICATOR_BA_WAGE_DISTRIBUTION,
                    'value': float(row[col_name]),
                    'gender': gender,
                    'age_group': age_group,
                    'nationality': nationality,
                    'migration_background': None,
                    'notes': notes
                })

        return records

    def _map_demographic_fields(self, demographic_type: str,
                                demographic_value: str) -> tuple:
        """
        Map demographic type/value to database fields.

        Returns:
            Tuple of (gender, age_group, nationality, notes)
        """
        gender = None
        age_group = None
        nationality = None
        notes = None

        if demographic_type == 'gender':
            if demographic_value == 'male':
                gender = 'male'
            elif demographic_value == 'female':
                gender = 'female'
            elif demographic_value == 'total':
                gender = 'total'

        elif demographic_type == 'age_group':
            # Map age groups to standard categories
            age_mapping = {
                'age_under_25': 'under_25',
                'age_25_to_55': '25_to_55',
                'age_55_and_over': '55_and_over'
            }
            age_group = age_mapping.get(demographic_value, demographic_value)
            # For age group records, set gender to total
            gender = 'total'

        elif demographic_type == 'nationality':
            if demographic_value == 'german':
                nationality = 'german'
            elif demographic_value == 'foreigner':
                nationality = 'foreigner'
            # For nationality records, set gender to total
            gender = 'total'

        elif demographic_type in ['education', 'skill_level', 'other']:
            # Store in notes field
            notes = f"demographic_category:{demographic_value}"
            # Set gender to total for these categories
            gender = 'total'

        else:
            # Unknown demographic type - store in notes
            notes = f"demographic_type:{demographic_type}|demographic_value:{demographic_value}"
            gender = 'total'

        return gender, age_group, nationality, notes

    def load(self, df_transformed: pd.DataFrame) -> Dict[str, int]:
        """
        Load transformed data into the database.

        Args:
            df_transformed: Transformed DataFrame

        Returns:
            Dictionary with loading statistics
        """
        logger.info(f"Loading {len(df_transformed)} records to database")

        if df_transformed.empty:
            logger.warning("No data to load")
            return {'loaded': 0, 'skipped': 0, 'failed': 0}

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
                    # Look up geography ID
                    geo_id = self._get_geo_id(conn, record['region_code'])
                    if geo_id is None:
                        logger.warning(f"Geography not found for code: {record['region_code']}")
                        stats['skipped'] += 1
                        continue

                    # Look up time ID
                    time_id = self._get_time_id(conn, record['year'])
                    if time_id is None:
                        logger.warning(f"Time not found for year: {record['year']}")
                        stats['skipped'] += 1
                        continue

                    # Execute insert
                    conn.execute(insert_query, {
                        'geo_id': geo_id,
                        'time_id': time_id,
                        'indicator_id': int(record['indicator_id']),
                        'value': float(record['value']),
                        'gender': record.get('gender'),
                        'nationality': record.get('nationality'),
                        'age_group': record.get('age_group'),
                        'migration_background': record.get('migration_background'),
                        'notes': record.get('notes')
                    })

                    stats['loaded'] += 1

                except Exception as e:
                    logger.error(f"Error loading record: {e}", exc_info=True)
                    logger.debug(f"Problem record: {record.to_dict()}")
                    stats['failed'] += 1
                    continue

            # Commit is handled by context manager when it exits

        logger.info(f"Loading complete: {stats['loaded']} loaded, "
                   f"{stats['skipped']} skipped, {stats['failed']} failed")

        return stats

    def _get_geo_id(self, conn, region_code: str) -> int:
        """Look up geography ID by region code."""
        query = text("SELECT geo_id FROM dim_geography WHERE region_code = :code")
        result = conn.execute(query, {'code': region_code}).fetchone()
        return result[0] if result else None

    def _get_time_id(self, conn, year: int) -> int:
        """Look up time ID by year."""
        query = text("SELECT time_id FROM dim_time WHERE year = :year")
        result = conn.execute(query, {'year': year}).fetchone()
        return result[0] if result else None


def main():
    """Test the transformer."""
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root))

    print("=" * 80)
    print("BA EMPLOYMENT/WAGE TRANSFORMER TEST")
    print("=" * 80)
    print()

    # Read raw data
    raw_file = Path("data/raw/ba/employment_wage_raw_2020_2024.csv")

    if not raw_file.exists():
        print(f"✗ Raw data file not found: {raw_file}")
        print("  Run the extractor first to generate raw data")
        sys.exit(1)

    print(f"Reading raw data from: {raw_file}")
    df_raw = pd.read_csv(raw_file)
    print(f"✓ Loaded {len(df_raw):,} raw records")
    print()

    # Transform
    print("Transforming data...")
    transformer = EmploymentWageTransformer()
    df_transformed = transformer.transform(df_raw)

    print(f"✓ Transformed into {len(df_transformed):,} records")
    print()

    # Show sample
    print("Sample transformed records:")
    print(df_transformed[['region_code', 'year', 'indicator_id', 'value',
                          'gender', 'age_group', 'nationality', 'notes']].head(15))
    print()

    # Statistics
    print("Transformation statistics:")
    print(f"  Records by indicator:")
    print(df_transformed.groupby('indicator_id').size().to_string())
    print()

    print("=" * 80)
    print("✓ TEST COMPLETE")
    print("=" * 80)


if __name__ == '__main__':
    main()
