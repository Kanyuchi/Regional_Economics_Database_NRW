"""
Database Loader Module
Regional Economics Database for NRW

Handles loading transformed data into PostgreSQL database.
"""

import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import datetime
from sqlalchemy import text

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.database import get_database
from utils.logging import get_logger


logger = get_logger(__name__)


class DataLoader:
    """Loads transformed data into the database."""

    def __init__(self, db_name: str = 'regional_economics'):
        """
        Initialize the data loader.

        Args:
            db_name: Name of the database configuration to use
        """
        self.db = get_database(db_name)
        logger.info(f"Data loader initialized for database: {db_name}")

    def load_demographics_data(
        self,
        df: pd.DataFrame,
        geo_mapping: Optional[Dict[str, int]] = None,
        time_mapping: Optional[Dict[int, int]] = None
    ) -> int:
        """
        Load demographics data into fact_demographics table.

        Args:
            df: Transformed DataFrame with demographics data
            geo_mapping: Optional mapping of region_code to geo_id
            time_mapping: Optional mapping of year to time_id

        Returns:
            Number of records loaded
        """
        if df is None or df.empty:
            logger.warning("No data to load")
            return 0

        try:
            logger.info(f"Loading {len(df)} demographics records")

            # Get mappings if not provided
            if geo_mapping is None:
                geo_mapping = self._get_geography_mapping()

            if time_mapping is None:
                time_mapping = self._get_time_mapping()

            # Prepare records for insertion
            records = []

            for _, row in df.iterrows():
                # Map region code to geo_id
                region_code = str(row['region_code']).strip()
                geo_id = geo_mapping.get(region_code)

                if geo_id is None:
                    logger.warning(f"Unknown region code: {region_code}")
                    continue

                # Map year to time_id
                year = int(row['year'])
                time_id = time_mapping.get(year)

                if time_id is None:
                    # Create time dimension entry if it doesn't exist
                    time_id = self._create_time_entry(year)
                    time_mapping[year] = time_id

                # Build record
                record = {
                    'geo_id': geo_id,
                    'time_id': time_id,
                    'indicator_id': int(row['indicator_id']),
                    'value': float(row['value']),
                    'gender': row.get('gender', 'total'),
                    'nationality': row.get('nationality', 'total'),
                    'age_group': row.get('age_group'),
                    'migration_background': row.get('migration_background'),
                    'notes': row.get('notes'),
                    'data_quality_flag': row.get('data_quality_flag', 'V'),
                    'extracted_at': row.get('extracted_at', datetime.now()),
                    'loaded_at': datetime.now()
                }

                records.append(record)

            if not records:
                logger.warning("No valid records to load after mapping")
                return 0

            # Bulk insert into database
            count = self.db.bulk_insert('fact_demographics', records)

            logger.info(f"Successfully loaded {count} demographics records")

            # Log extraction
            self._log_extraction('regional_db', 'demographics', count, 'success')

            return count

        except Exception as e:
            logger.error(f"Error loading demographics data: {e}")
            self._log_extraction('regional_db', 'demographics', 0, 'failed', str(e))
            return 0

    def _get_geography_mapping(self) -> Dict[str, int]:
        """
        Get mapping of region codes to geo_ids.

        Returns:
            Dictionary mapping region_code to geo_id
        """
        query = "SELECT geo_id, region_code FROM dim_geography WHERE is_active = TRUE"
        results = self.db.execute_query(query)

        mapping = {str(row['region_code']): row['geo_id'] for row in results}

        logger.info(f"Loaded geography mapping with {len(mapping)} entries")

        return mapping

    def _get_time_mapping(self) -> Dict[int, int]:
        """
        Get mapping of years to time_ids.

        Returns:
            Dictionary mapping year to time_id
        """
        query = "SELECT time_id, year FROM dim_time"
        results = self.db.execute_query(query)

        mapping = {row['year']: row['time_id'] for row in results}

        logger.info(f"Loaded time mapping with {len(mapping)} entries")

        return mapping

    def _create_time_entry(self, year: int) -> int:
        """
        Create a new time dimension entry for the specified year.

        Args:
            year: Year to create entry for

        Returns:
            time_id of the new entry
        """
        query = """
        INSERT INTO dim_time (year, reference_type, reference_date)
        VALUES (:year, 'year_end', :reference_date)
        RETURNING time_id
        """

        reference_date = f"{year}-12-31"

        with self.db.get_connection() as conn:
            result = conn.execute(
                text(query),
                {'year': year, 'reference_date': reference_date}
            )
            time_id = result.fetchone()[0]

        logger.info(f"Created time entry for year {year} with time_id {time_id}")

        return time_id

    def _log_extraction(
        self,
        source_system: str,
        indicator_code: str,
        records_extracted: int,
        status: str,
        error_message: Optional[str] = None
    ) -> None:
        """
        Log extraction to data_extraction_log table.

        Args:
            source_system: Name of the source system
            indicator_code: Indicator code
            records_extracted: Number of records extracted
            status: Status ('success', 'partial', 'failed')
            error_message: Optional error message
        """
        try:
            query = """
            INSERT INTO data_extraction_log (
                source_system,
                indicator_code,
                extraction_timestamp,
                records_extracted,
                records_failed,
                status,
                error_message
            ) VALUES (
                :source_system,
                :indicator_code,
                :extraction_timestamp,
                :records_extracted,
                :records_failed,
                :status,
                :error_message
            )
            """

            params = {
                'source_system': source_system,
                'indicator_code': indicator_code,
                'extraction_timestamp': datetime.now(),
                'records_extracted': records_extracted,
                'records_failed': 0 if status == 'success' else records_extracted,
                'status': status,
                'error_message': error_message
            }

            self.db.execute_statement(query, params)

        except Exception as e:
            logger.error(f"Error logging extraction: {e}")

    def load_indicator_metadata(self, indicators: List[Dict[str, Any]]) -> int:
        """
        Load indicator metadata into dim_indicator table.

        Args:
            indicators: List of indicator dictionaries

        Returns:
            Number of indicators loaded
        """
        if not indicators:
            return 0

        try:
            logger.info(f"Loading {len(indicators)} indicator metadata records")

            count = self.db.bulk_insert('dim_indicator', indicators)

            logger.info(f"Successfully loaded {count} indicator metadata records")

            return count

        except Exception as e:
            logger.error(f"Error loading indicator metadata: {e}")
            return 0

    def close(self) -> None:
        """Close database connections."""
        self.db.close()
        logger.info("Data loader closed")
