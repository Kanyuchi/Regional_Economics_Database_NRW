"""
Demographics Data Extractor for Regional Database Germany
Regional Economics Database for NRW

Extracts population and demographic indicators.
"""

import pandas as pd
from io import StringIO
from typing import Optional, List, Dict, Any
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extractors.regional_db.base_extractor import RegionalDBExtractor
from utils.logging import get_logger


logger = get_logger(__name__)


class DemographicsExtractor(RegionalDBExtractor):
    """Extractor for demographics data from Regional Database."""

    # Table IDs for demographics indicators
    DEMOGRAPHICS_TABLES = {
        'population_total': '12411-03-03-4',  # Population by gender
        'population_age': '12411-04-03-4',     # Population by age groups
        'population_nationality': '12411-05-03-4',  # Population by nationality
    }

    def extract_population_total(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract total population data by gender.

        Args:
            regions: List of region codes (e.g., ['05111', '05112'])
            years: List of years to extract

        Returns:
            DataFrame with population data or None
        """
        table_id = self.DEMOGRAPHICS_TABLES['population_total']
        logger.info(f"Extracting population total data for table {table_id}")

        # Download ALL data (no year filters in API call - filter after transformation)
        # Note: The format is 'datencsv' for data CSV
        raw_data = self.get_table_data(table_id, format='datencsv', area='free')

        if raw_data is None:
            logger.error("Failed to download population data")
            return None

        try:
            # Parse CSV data
            # The GENESIS CSV has 8 metadata/header rows before data starts
            # Row 0-5: Metadata, Row 6-7: Multi-level headers, Row 8+: Data
            # We use header=None to prevent using first data row as column names
            skip_rows = self._detect_data_start_row(raw_data)
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None  # Don't use first row as header
            )
            
            # Assign proper column names for table 12411-03-03-4
            # Columns: date, region_code, region_name, age_group, then 9 population columns
            if len(df.columns) == 13:
                df.columns = [
                    'date', 'region_code', 'region_name', 'age_group',
                    'pop_total_total', 'pop_total_male', 'pop_total_female',
                    'pop_german_total', 'pop_german_male', 'pop_german_female',
                    'pop_foreign_total', 'pop_foreign_male', 'pop_foreign_female'
                ]
                logger.info("Assigned standard column names for population table")
            else:
                # Generic column names if structure differs
                df.columns = [f'col_{i}' for i in range(len(df.columns))]
                logger.warning(f"Unexpected column count: {len(df.columns)}, using generic names")

            logger.info(f"Parsed {len(df)} rows of population data")

            # Add metadata
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            # Store years for transformer to filter (since API returns all years)
            if years:
                df.attrs['years_filter'] = years
                logger.info(f"Stored years filter for transformer: {years}")

            return df

        except Exception as e:
            logger.error(f"Error parsing population data: {e}")
            return None

    def extract_population_by_age(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract population data by age groups.

        Args:
            regions: List of region codes
            years: List of years to extract

        Returns:
            DataFrame with population by age data or None
        """
        table_id = self.DEMOGRAPHICS_TABLES['population_age']
        logger.info(f"Extracting population by age data for table {table_id}")

        filters = {}
        if regions:
            filters['regional'] = ','.join(regions)
        if years:
            filters['years'] = ','.join(str(y) for y in years)

        raw_data = self.get_table_data(table_id, format='csv', **filters)

        if raw_data is None:
            logger.error("Failed to download population by age data")
            return None

        try:
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=self._detect_header_rows(raw_data)
            )

            logger.info(f"Parsed {len(df)} rows of age distribution data")

            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            return df

        except Exception as e:
            logger.error(f"Error parsing age distribution data: {e}")
            return None

    def extract_population_by_nationality(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract population data by nationality (German/Foreign).

        Args:
            regions: List of region codes
            years: List of years to extract

        Returns:
            DataFrame with population by nationality data or None
        """
        table_id = self.DEMOGRAPHICS_TABLES['population_nationality']
        logger.info(f"Extracting population by nationality data for table {table_id}")

        filters = {}
        if regions:
            filters['regional'] = ','.join(regions)
        if years:
            filters['years'] = ','.join(str(y) for y in years)

        raw_data = self.get_table_data(table_id, format='csv', **filters)

        if raw_data is None:
            logger.error("Failed to download population by nationality data")
            return None

        try:
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=self._detect_header_rows(raw_data)
            )

            logger.info(f"Parsed {len(df)} rows of nationality distribution data")

            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            return df

        except Exception as e:
            logger.error(f"Error parsing nationality distribution data: {e}")
            return None

    def extract_all_demographics(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Dict[str, pd.DataFrame]:
        """
        Extract all demographics indicators.

        Args:
            regions: List of region codes
            years: List of years to extract

        Returns:
            Dictionary mapping indicator names to DataFrames
        """
        logger.info("Extracting all demographics indicators")

        results = {}

        # Extract population total
        df_total = self.extract_population_total(regions, years)
        if df_total is not None:
            results['population_total'] = df_total

        # Extract population by age
        df_age = self.extract_population_by_age(regions, years)
        if df_age is not None:
            results['population_age'] = df_age

        # Extract population by nationality
        df_nationality = self.extract_population_by_nationality(regions, years)
        if df_nationality is not None:
            results['population_nationality'] = df_nationality

        logger.info(f"Extracted {len(results)} demographics datasets")

        return results

    def _detect_data_start_row(self, raw_data: str) -> int:
        """
        Detect the row number where actual data starts in CSV.
        
        GENESIS CSV structure:
        - Rows 0-5: Table metadata and descriptions
        - Row 6: First header row (nationality categories)
        - Row 7: Second header row (gender categories)
        - Row 8+: Actual data rows

        Args:
            raw_data: Raw CSV string

        Returns:
            Number of rows to skip to reach data
        """
        lines = raw_data.split('\n')

        for i, line in enumerate(lines):
            # Data rows start with a date pattern like "2011-12-31"
            # or "2020-12-31" or "31.12.2011", etc.
            stripped = line.strip()
            if stripped and ';' in stripped:
                # Check if line starts with a date-like pattern
                first_field = stripped.split(';')[0].strip()
                # Match patterns like "2011-12-31" or "2024-12-31"
                if len(first_field) == 10 and first_field[4] == '-' and first_field[7] == '-':
                    logger.info(f"Detected data start at row {i}: {first_field}")
                    return i
                # Also check German date format "31.12.2011"
                if len(first_field) == 10 and first_field[2] == '.' and first_field[5] == '.':
                    logger.info(f"Detected data start at row {i} (German date format): {first_field}")
                    return i

        # Default: skip first 8 rows (standard GENESIS format)
        logger.warning("Could not detect data start, using default skip=8")
        return 8
