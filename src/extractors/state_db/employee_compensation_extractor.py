"""
Employee Compensation Extractor for State Database NRW
Extracts employee compensation by economic sector from Landesdatenbank NRW.

Table: 82711-06i - Employee compensation by economic sectors (WZ 2008)
                   - independent cities and districts - year

Available period: 2000 - 2022

Regional Economics Database for NRW
"""

import pandas as pd
from io import StringIO
from typing import Optional, Dict, Any, List
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger
from .base_extractor import StateDBExtractor
from .job_cache import StateDBJobCache

logger = get_logger(__name__)


class EmployeeCompensationExtractor(StateDBExtractor):
    """
    Extractor for employee compensation data from State Database NRW.

    Handles extraction of table 82711-06i which contains:
    - Employee compensation (Arbeitnehmerentgelt) by economic sectors
    - Annual data from 2000 to 2022
    - Coverage: Independent cities and districts (kreisfreie Städte und Kreise)
    """

    # Table configuration
    TABLE_ID = "82711-06i"
    TABLE_NAME = "Employee Compensation by Economic Sectors"
    START_YEAR = 2000
    END_YEAR = 2023  # Updated: data available through 2023

    def __init__(self):
        """Initialize the employee compensation extractor."""
        super().__init__()
        logger.info(f"Employee Compensation Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR} ({self.END_YEAR - self.START_YEAR + 1} years)")

    def extract_compensation_data(
        self,
        startyear: int = 2000,
        endyear: int = 2022
    ) -> Optional[pd.DataFrame]:
        """
        Extract employee compensation data year-by-year.

        Note: The State Database API appears to only return the latest year
        when requesting a range. Therefore, we extract each year individually
        and combine the results.

        Args:
            startyear: Start year (default 2000)
            endyear: End year (default 2022)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING EMPLOYEE COMPENSATION DATA: {self.TABLE_ID}")
        logger.info(f"Period: {startyear}-{endyear} ({endyear - startyear + 1} years)")
        logger.info("="*80)
        logger.info("Note: Extracting year-by-year due to State DB API limitation")

        # Extract each year individually and combine
        all_dataframes = []
        successful_years = []
        failed_years = []

        for year in range(startyear, endyear + 1):
            logger.info(f"\n{'─'*80}")
            logger.info(f"YEAR {year} ({year - startyear + 1}/{endyear - startyear + 1})")
            logger.info(f"{'─'*80}")

            # Request data for single year
            raw_data = self.get_table_data(
                table_id=self.TABLE_ID,
                format='datencsv',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"❌ No data returned for year {year}")
                failed_years.append(year)
                continue

            # Parse the CSV data
            year_df = self._parse_compensation_data(raw_data, year)

            if year_df is not None and not year_df.empty:
                all_dataframes.append(year_df)
                successful_years.append(year)
                logger.info(f"✓ Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"❌ Failed to parse data for year {year}")
                failed_years.append(year)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Successful years: {len(successful_years)}/{endyear - startyear + 1}")
        logger.info(f"Failed years: {len(failed_years)}")
        if failed_years:
            logger.info(f"Failed: {failed_years}")

        if not all_dataframes:
            logger.error("No data extracted for any year")
            return None

        # Combine all years
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"\nCombined {len(all_dataframes)} years into {len(combined_df)} total rows")

        return combined_df

    def _parse_compensation_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from employee compensation table.

        The CSV format from GENESIS API has:
        - Lines 1-5: Metadata (table name, description)
        - Lines 6-7: Category headers
        - Line 8: Sector names (column headers)
        - Line 9: Units (Mio EUR)
        - Line 10+: Data rows

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted (for verification)

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of compensation data")

            # Save raw data for inspection
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"compensation_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            # Parse the lines to extract header and data
            lines = raw_data.strip().split('\n')

            # Find header line (contains sector names)
            header_line_idx = None
            data_start_idx = None

            for i, line in enumerate(lines):
                # Header line typically contains sector codes like A, B-E, etc.
                if 'Insgesamt' in line or 'Wirtschaftsbereiche' in line:
                    header_line_idx = i
                    data_start_idx = i + 2  # Skip unit line
                    break

            if header_line_idx is None:
                # Fallback: try standard positions
                header_line_idx = 7
                data_start_idx = 9

            # Extract column headers
            header_parts = lines[header_line_idx].split(';')
            column_names = ['year', 'region_code', 'region_name']

            for i, part in enumerate(header_parts[3:], start=3):
                part = part.strip().strip('"')
                if part:
                    column_names.append(part)
                else:
                    column_names.append(f'sector_{i}')

            logger.info(f"Extracted {len(column_names)} column names")

            # Read data rows
            df = pd.read_csv(
                StringIO(raw_data),
                sep=';',
                encoding='utf-8',
                skiprows=data_start_idx,
                header=None,
                dtype=str,
                on_bad_lines='skip'
            )

            if df.empty:
                logger.error("Parsed DataFrame is empty")
                return None

            # Trim to match column count
            if len(df.columns) > len(column_names):
                df = df.iloc[:, :len(column_names)]
            elif len(df.columns) < len(column_names):
                column_names = column_names[:len(df.columns)]

            # Assign column names
            df.columns = column_names

            # Add/verify year column
            df['year'] = year

            # Clean region codes and names
            df['region_code'] = df['region_code'].astype(str).str.strip()
            df['region_name'] = df['region_name'].astype(str).str.strip()

            # Filter to NRW regions (codes starting with 05)
            df = df[df['region_code'].str.startswith('05')]

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse compensation data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the employee compensation table.

        Returns:
            Dictionary with table metadata
        """
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Employee compensation by economic sectors for NRW regions",
            "sectors": [
                "Total (all sectors)",
                "Agriculture, forestry and fishing (A)",
                "Manufacturing (B-E)",
                "Construction (F)",
                "Trade, transport and hospitality (G-I)",
                "Information and communication (J)",
                "Finance and insurance (K)",
                "Real estate (L)",
                "Business services (M-N)",
                "Public and other services (O-U)"
            ]
        }
