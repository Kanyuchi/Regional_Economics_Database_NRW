"""
ICT Indicators Extractor for State Database NRW
Extracts ICT (Information and Communication Technology) indicators from Landesdatenbank NRW.

Table: 52911-01i - ICT indicators by districts and independent cities
Available period: 2020 - 2025

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


class ICTIndicatorsExtractor(StateDBExtractor):
    """
    Extractor for ICT indicators data from State Database NRW.

    Handles extraction of table 52911-01i which contains:
    - ICT adoption and usage indicators
    - Digital infrastructure metrics
    - Technology employment statistics
    - Annual data from 2020 to 2025
    - Coverage: Independent cities and districts (kreisfreie Städte und Kreise)
    """

    # Table configuration
    TABLE_ID = "52911-01i"
    TABLE_NAME = "ICT Indicators by Districts and Cities"
    START_YEAR = 2020
    END_YEAR = 2025

    def __init__(self):
        """Initialize the ICT indicators extractor."""
        super().__init__()
        logger.info(f"ICT Indicators Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR} ({self.END_YEAR - self.START_YEAR + 1} years)")

    def extract_ict_data(
        self,
        startyear: int = 2020,
        endyear: int = 2025
    ) -> Optional[pd.DataFrame]:
        """
        Extract ICT indicators data year-by-year.

        Note: The State Database API appears to only return the latest year
        when requesting a range. Therefore, we extract each year individually
        and combine the results.

        Args:
            startyear: Start year (default 2020)
            endyear: End year (default 2025)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING ICT INDICATORS DATA: {self.TABLE_ID}")
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
            year_df = self._parse_ict_data(raw_data, expected_year=year)

            if year_df is not None and not year_df.empty:
                all_dataframes.append(year_df)
                successful_years.append(year)
                logger.info(f"✅ Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"❌ Failed to parse data for year {year}")
                failed_years.append(year)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
        logger.info(f"✅ Successful years: {len(successful_years)}/{endyear - startyear + 1}")
        logger.info(f"   Years: {successful_years}")
        if failed_years:
            logger.warning(f"❌ Failed years: {len(failed_years)}")
            logger.warning(f"   Years: {failed_years}")

        if not all_dataframes:
            logger.error("❌ CRITICAL: No data extracted for any year")
            return None

        # Combine all years
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"\n✅ TOTAL: Combined {len(all_dataframes)} years into {len(combined_df):,} total rows")
        logger.info("="*80)

        return combined_df

    def retrieve_ict_data(self, job_id: str) -> Optional[pd.DataFrame]:
        """
        Retrieve ICT data using an existing job ID.

        Use this method when you already have a job ID from a previous
        extraction request (e.g., from manual API call or cached job).

        Args:
            job_id: Full job ID (e.g., '52911-01i_149084252')

        Returns:
            DataFrame with extracted data or None if error
        """
        logger.info(f"Retrieving ICT data with job ID: {job_id}")

        # Retrieve using existing job
        raw_data = self.retrieve_existing_job(job_id)

        if raw_data is None:
            logger.error(f"Failed to retrieve job {job_id}")
            StateDBJobCache.mark_failed(self.TABLE_ID)
            return None

        # Mark as retrieved in cache
        StateDBJobCache.mark_retrieved(self.TABLE_ID)

        # Parse the CSV data
        return self._parse_ict_data(raw_data)

    def _parse_ict_data(self, raw_data: str, expected_year: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from ICT indicators table.

        NOTE: Table 52911-01i returns STATE-LEVEL aggregate data for NRW, not district-level.

        The CSV format is:
        - Line 1: Table ID (e.g., "Tabelle: 52911-01i")
        - Line 2: Table description with semicolons
        - Line 3: Empty
        - Line 4: "Nordrhein-Westfalen;"
        - Line 5: ";2020" (year)
        - Line 6: ";Prozent" (unit)
        - Lines 7+: "Indicator name;value"

        Data structure:
        - Column 0: Indicator name (German)
        - Column 1: Value (numeric or '-' for missing)

        Args:
            raw_data: Raw CSV string from API
            expected_year: Expected year for validation

        Returns:
            DataFrame with columns: year, region_code, indicator_name, value
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of ICT data (STATE-LEVEL format)")

            # Save raw data for inspection
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)

            if expected_year:
                raw_file = raw_dir / f"ict_indicators_raw_{expected_year}.csv"
            else:
                raw_file = raw_dir / "ict_indicators_raw.csv"

            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            # Parse the lines
            lines = raw_data.strip().split('\n')

            if len(lines) < 7:
                logger.error(f"Insufficient lines in data: {len(lines)} (expected at least 7)")
                return None

            # Extract year from line 5 (format: ";2020")
            year_line = lines[4].strip()
            if ';' in year_line:
                year_str = year_line.split(';')[1]
                year = int(year_str) if year_str.isdigit() else expected_year
            else:
                year = expected_year

            logger.info(f"Extracted year: {year}")

            # Extract unit from line 6 (format: ";Prozent")
            unit_line = lines[5].strip()
            if ';' in unit_line:
                unit = unit_line.split(';')[1]
            else:
                unit = "Percent"

            logger.info(f"Unit: {unit}")

            # Parse indicator data starting from line 7
            records = []
            data_start_idx = 6  # Start parsing from line 7 (index 6)

            for line in lines[data_start_idx:]:
                line = line.strip()

                # Skip empty lines and footer
                if not line or line.startswith('_') or line.startswith('©'):
                    continue

                # Split by semicolon
                parts = line.split(';')

                if len(parts) >= 2:
                    indicator_name = parts[0].strip()
                    value_str = parts[1].strip()

                    # Skip if no indicator name
                    if not indicator_name:
                        continue

                    records.append({
                        'indicator_name': indicator_name,
                        'value_str': value_str
                    })

            if not records:
                logger.error("No indicator records found")
                return None

            # Create DataFrame
            df = pd.DataFrame(records)

            # Add year and region code (NRW state = "05")
            df['year'] = year
            df['region_code'] = '05'  # Nordrhein-Westfalen state code
            df['region_name'] = 'Nordrhein-Westfalen'
            df['unit'] = unit

            # Reorder columns
            df = df[['year', 'region_code', 'region_name', 'indicator_name', 'value_str', 'unit']]

            logger.info(f"Successfully parsed {len(df)} indicators for NRW state")
            logger.info(f"Sample indicators (first 5):")
            logger.info(f"\n{df.head(5).to_string()}")

            # Validate year if expected
            if expected_year and year != expected_year:
                logger.warning(f"Year mismatch: expected {expected_year}, got {year}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse ICT data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the ICT indicators table.

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
            "description": "ICT indicators by districts and independent cities in NRW",
            "indicator_categories": [
                "Digital infrastructure",
                "ICT adoption rates",
                "Technology employment",
                "Broadband availability",
                "Internet usage statistics"
            ],
            "geographic_coverage": "Independent cities and districts (Kreisfreie Städte und Kreise)",
            "frequency": "Annual"
        }
