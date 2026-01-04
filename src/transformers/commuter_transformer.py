"""
BA Commuter Statistics Transformer
Transforms BA commuter data into star schema format.

Source: Federal Employment Agency (BA) - Pendlerstatistik
Period: 2002-2024
Creates indicators for incoming/outgoing commuters with demographic breakdowns
"""

import pandas as pd
from typing import Dict
from pathlib import Path
import sys
from sqlalchemy import text

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.database import DatabaseManager
from utils.logging import get_logger

logger = get_logger(__name__)


# Indicator IDs for commuter data
INDICATOR_INCOMING_COMMUTERS = 101
INDICATOR_OUTGOING_COMMUTERS = 102
# Note: Indicator 103 (Net Balance) will be calculated during querying or as a derived metric


class CommuterTransformer:
    """
    Transformer for BA commuter statistics.

    Converts wide-format extraction data to long-format star schema.
    Creates separate records for:
    - Total commuters
    - Commuters by gender
    - Commuters by nationality
    - Trainees
    """

    def __init__(self):
        """Initialize transformer."""
        self.db = DatabaseManager()
        logger.info("BA commuter transformer initialized")

    def transform(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Transform raw extraction data to star schema format.

        Args:
            df_raw: Raw extracted data with columns:
                - year
                - commuter_type ('incoming' or 'outgoing')
                - workplace_name, workplace_code
                - residence_name, residence_code
                - total, men, women, germans, foreigners, trainees

        Returns:
            Transformed DataFrame ready for loading into fact_demographics
        """
        logger.info(f"Transforming {len(df_raw)} raw commuter records")

        if df_raw.empty:
            logger.warning("No data to transform")
            return pd.DataFrame()

        # Create long-format records
        records = []

        for _, row in df_raw.iterrows():
            year = int(row['year'])
            commuter_type = row['commuter_type']

            # Determine indicator ID and region code based on commuter type
            if commuter_type == 'incoming':
                indicator_id = INDICATOR_INCOMING_COMMUTERS
                # For incoming: use workplace (where people work in NRW)
                region_code = row['workplace_code']
                region_name = row['workplace_name']
                # Origin information goes in notes
                notes_base = f"origin:{row['residence_name']}"
                if pd.notna(row['residence_code']) and row['residence_code'] != '0':
                    notes_base += f"|origin_code:{row['residence_code']}"
            else:  # outgoing
                indicator_id = INDICATOR_OUTGOING_COMMUTERS
                # For outgoing: use residence (where people live in NRW)
                region_code = row['residence_code']
                region_name = row['residence_name']
                # Destination information goes in notes
                notes_base = f"destination:{row['workplace_name']}"
                if pd.notna(row['workplace_code']) and row['workplace_code'] != '0':
                    notes_base += f"|destination_code:{row['workplace_code']}"

            # Record 1: Total commuters
            if pd.notna(row['total']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['total']),
                    'gender': None,
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base,
                })

            # Record 2: Men
            if pd.notna(row['men']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['men']),
                    'gender': 'male',
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base,
                })

            # Record 3: Women
            if pd.notna(row['women']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['women']),
                    'gender': 'female',
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base,
                })

            # Record 4: Germans
            if pd.notna(row['germans']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['germans']),
                    'gender': None,
                    'nationality': 'german',
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base,
                })

            # Record 5: Foreigners
            if pd.notna(row['foreigners']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['foreigners']),
                    'gender': None,
                    'nationality': 'foreigner',
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base,
                })

            # Record 6: Trainees (as a note-based distinction)
            if pd.notna(row['trainees']):
                records.append({
                    'year': year,
                    'region_code': region_code,
                    'indicator_id': indicator_id,
                    'value': float(row['trainees']),
                    'gender': None,
                    'nationality': None,
                    'age_group': None,
                    'migration_background': None,
                    'notes': notes_base + '|employment_type:trainee',
                })

        # Convert to DataFrame
        df_transformed = pd.DataFrame(records)

        if df_transformed.empty:
            logger.warning("No records created during transformation")
            return df_transformed

        logger.info(f"Created {len(df_transformed)} transformed records")
        logger.info(f"  Indicators: {sorted(df_transformed['indicator_id'].unique())}")
        logger.info(f"  Years: {sorted(df_transformed['year'].unique())}")
        logger.info(f"  Incoming records: {len(df_transformed[df_transformed['indicator_id'] == INDICATOR_INCOMING_COMMUTERS]):,}")
        logger.info(f"  Outgoing records: {len(df_transformed[df_transformed['indicator_id'] == INDICATOR_OUTGOING_COMMUTERS]):,}")

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

        # Prepare insert query
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
                        logger.debug(f"Geography not found for code: {record['region_code']}")
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
    # Create sample data
    sample_data = pd.DataFrame([
        {
            'year': 2024,
            'commuter_type': 'incoming',
            'workplace_name': 'Düsseldorf, Stadt',
            'workplace_code': '05111',
            'residence_name': 'Einpendler insgesamt',
            'residence_code': '0',
            'total': 290700,
            'men': 163100,
            'women': 127600,
            'germans': 245230,
            'foreigners': 45470,
            'trainees': 7400,
        },
        {
            'year': 2024,
            'commuter_type': 'outgoing',
            'residence_name': 'Düsseldorf, Stadt',
            'residence_code': '05111',
            'workplace_name': 'Auspendler insgesamt',
            'workplace_code': '0',
            'total': 99640,
            'men': 59280,
            'women': 40360,
            'germans': 87240,
            'foreigners': 12400,
            'trainees': 2370,
        },
    ])

    transformer = CommuterTransformer()
    df_transformed = transformer.transform(sample_data)

    print("=" * 100)
    print("TRANSFORMED COMMUTER RECORDS")
    print("=" * 100)
    print(df_transformed[['year', 'region_code', 'indicator_id', 'value', 'gender', 'nationality', 'notes']].to_string())
    print(f"\nTotal records: {len(df_transformed)}")
    print(f"  Incoming (101): {len(df_transformed[df_transformed['indicator_id'] == 101])}")
    print(f"  Outgoing (102): {len(df_transformed[df_transformed['indicator_id'] == 102])}")


if __name__ == '__main__':
    main()
