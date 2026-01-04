"""
GDP Extractor for State Database NRW
Extracts GDP and gross value added data from Landesdatenbank NRW.

Table: 82711-01i - Gross domestic product and gross value added by
                   economic sectors (7) of WZ 2008 - independent cities
                   and districts - year

Available period: 1991 - 2023

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


class GDPExtractor(StateDBExtractor):
    """
    Extractor for GDP and gross value added data from State Database NRW.

    Handles extraction of table 82711-01i which contains:
    - Gross domestic product (GDP)
    - Gross value added by 7 economic sectors (WZ 2008 classification)
    - Annual data from 1991 to 2023
    - Coverage: Independent cities and districts (kreisfreie Städte und Kreise)
    """

    # Table configuration
    TABLE_ID = "82711-01i"
    TABLE_NAME = "GDP and Gross Value Added by Economic Sectors"
    START_YEAR = 1991
    END_YEAR = 2023

    def __init__(self):
        """Initialize the GDP extractor."""
        super().__init__()
        logger.info(f"GDP Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR} ({self.END_YEAR - self.START_YEAR + 1} years)")

    def extract_gdp_data(
        self,
        startyear: int = 1991,
        endyear: int = 2023
    ) -> Optional[pd.DataFrame]:
        """
        Extract GDP and gross value added data year-by-year.

        Note: The State Database API appears to only return the latest year
        when requesting a range. Therefore, we extract each year individually
        and combine the results.

        Args:
            startyear: Start year (default 1991)
            endyear: End year (default 2023)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING GDP DATA: {self.TABLE_ID}")
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
            year_df = self._parse_gdp_data(raw_data, expected_year=year)

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

    def retrieve_gdp_data(self, job_id: str) -> Optional[pd.DataFrame]:
        """
        Retrieve GDP data using an existing job ID.

        Use this method when you already have a job ID from a previous
        extraction request (e.g., from manual API call or cached job).

        Args:
            job_id: Full job ID (e.g., '82711-01i_149084252')

        Returns:
            DataFrame with extracted data or None if error
        """
        logger.info(f"Retrieving GDP data with job ID: {job_id}")

        # Retrieve using existing job
        raw_data = self.retrieve_existing_job(job_id)

        if raw_data is None:
            logger.error(f"Failed to retrieve job {job_id}")
            StateDBJobCache.mark_failed(self.TABLE_ID)
            return None

        # Mark as retrieved in cache
        StateDBJobCache.mark_retrieved(self.TABLE_ID)

        # Parse the CSV data
        return self._parse_gdp_data(raw_data)

    def _parse_gdp_data(self, raw_data: str, expected_year: Optional[int] = None) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from GDP table.

        The CSV format from GENESIS API has:
        - Lines 1-5: Metadata (table name, description)
        - Lines 6-7: Category headers
        - Line 8: Economic sector names (column headers)
        - Line 9: Units (millions EUR)
        - Line 10+: Data rows

        Data structure:
        - Col 0: Year
        - Col 1: Region code
        - Col 2: Region name
        - Cols 3+: GDP/GVA values by sector

        Args:
            raw_data: Raw CSV string from API
            expected_year: Expected year for validation

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of GDP data")

            # Save raw data for inspection (with year in filename if available)
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)

            if expected_year:
                raw_file = raw_dir / f"gdp_raw_{expected_year}.csv"
            else:
                raw_file = raw_dir / "gdp_raw.csv"

            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            # Parse the lines to extract header and data
            lines = raw_data.strip().split('\n')

            if len(lines) < 10:
                logger.error(f"Insufficient lines in data: {len(lines)} (expected at least 10)")
                return None

            # CSV has hierarchical headers:
            # Line 8 (index 7): Main categories
            # Line 9 (index 8): Subcategories
            # Line 10 (index 9): Units
            # Data starts at line 11 (index 10)
            category_line_idx = 7  # Main categories
            subcategory_line_idx = 8  # Subcategories
            data_start_idx = 10  # Data starts here

            # Extract both header lines
            category_parts = lines[category_line_idx].split(';')
            subcategory_parts = lines[subcategory_line_idx].split(';')

            # Build unique column names by combining category + subcategory
            column_names = ['year', 'region_code', 'region_name']

            # Combine headers to create unique column names
            for i in range(3, min(len(category_parts), len(subcategory_parts))):
                category = category_parts[i].strip()
                subcategory = subcategory_parts[i].strip()

                # Create unique column name
                if category and subcategory and category != subcategory:
                    # Combine both for uniqueness
                    col_name = f"{category}_{subcategory}"
                elif subcategory:
                    col_name = subcategory
                elif category:
                    col_name = category
                else:
                    col_name = f'sector_{i}'

                # Clean the column name
                col_name = col_name.replace('"', '').replace("'", "").strip()
                column_names.append(col_name)

            logger.info(f"Extracted {len(column_names)} column names")
            logger.info(f"Economic sectors: {column_names[3:]}")

            # Read data rows (skip metadata and header rows)
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
                logger.warning(f"More columns than expected: {len(df.columns)} > {len(column_names)}")
                df = df.iloc[:, :len(column_names)]
            elif len(df.columns) < len(column_names):
                logger.warning(f"Fewer columns than expected: {len(df.columns)} < {len(column_names)}")
                column_names = column_names[:len(df.columns)]

            # Assign column names
            df.columns = column_names

            # Clean region codes and names
            df['region_code'] = df['region_code'].astype(str).str.strip()
            df['region_name'] = df['region_name'].astype(str).str.strip()

            # Ensure year column is correct
            if expected_year:
                df['year'] = expected_year
            else:
                df['year'] = pd.to_numeric(df['year'], errors='coerce')

            logger.info(f"Successfully parsed {len(df)} rows with {len(df.columns)} columns")

            # Show sample
            if len(df) > 0:
                logger.info(f"Sample data (first 3 rows):")
                logger.info(f"\n{df.head(3).to_string()}")

            # Validate year if expected
            if expected_year:
                actual_years = df['year'].unique()
                if len(actual_years) == 1 and actual_years[0] == expected_year:
                    logger.info(f"✅ Year validation passed: {expected_year}")
                else:
                    logger.warning(f"Year mismatch: expected {expected_year}, got {actual_years}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse GDP data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the GDP table.

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
            "description": "GDP and gross value added by 7 economic sectors (WZ 2008)",
            "economic_sectors": [
                "Agriculture, forestry and fishing (A)",
                "Manufacturing (B-E)",
                "Construction (F)",
                "Trade, transport, hospitality (G-I)",
                "Information and communication (J)",
                "Finance and insurance (K)",
                "Real estate (L)",
                "Business services (M-N)",
                "Public and other services (O-U)"
            ],
            "geographic_coverage": "Independent cities and districts (Kreisfreie Städte und Kreise)",
            "unit": "Million EUR (current prices)"
        }
