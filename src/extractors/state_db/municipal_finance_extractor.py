"""
Municipal Finance Extractor for State Database NRW
Extracts municipal finance data (GFK) from Landesdatenbank NRW.

Table: 71517-01i - Finances (GFK) of municipalities and municipal associations:
                   Payments by selected payment types - Municipalities - Year

Available period: 2009 - 2024

Regional Economics Database for NRW
"""

import pandas as pd
from io import StringIO
from typing import Optional, Dict, Any, List

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger
from .base_extractor import StateDBExtractor
from .job_cache import StateDBJobCache

logger = get_logger(__name__)


class MunicipalFinanceExtractor(StateDBExtractor):
    """
    Extractor for municipal finance data from State Database NRW.
    
    Handles extraction of table 71517-01i which contains:
    - Municipal finances (GFK methodology)
    - Payments by payment types
    - Annual data from 2009 onwards
    """
    
    # Table configuration
    TABLE_ID = "71517-01i"
    TABLE_NAME = "Municipal Finances (GFK) - Payments by payment types"
    START_YEAR = 2009
    END_YEAR = 2024
    
    def __init__(self):
        """Initialize the municipal finance extractor."""
        super().__init__()
        logger.info(f"Municipal Finance Extractor initialized for table {self.TABLE_ID}")
    
    def extract_municipal_finances(
        self,
        startyear: int = 2009,
        endyear: int = 2024
    ) -> Optional[pd.DataFrame]:
        """
        Extract municipal finance data by submitting requests year-by-year.
        
        Note: The State Database API appears to only return the latest year
        when requesting a range. Therefore, we extract each year individually
        and combine the results.
        
        Args:
            startyear: Start year (default 2009)
            endyear: End year (default 2024)
            
        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info(f"Extracting municipal finances for {startyear}-{endyear}")
        logger.info("Note: Extracting year-by-year due to API limitation")
        
        # Extract each year individually and combine
        all_dataframes = []
        
        for year in range(startyear, endyear + 1):
            logger.info(f"Extracting year {year}...")
            
            # Request data for single year
            raw_data = self.get_table_data(
                table_id=self.TABLE_ID,
                format='datencsv',
                startyear=year,
                endyear=year
            )
            
            if raw_data is None:
                logger.warning(f"No data returned for year {year}")
                continue
            
            # Parse the CSV data
            year_df = self._parse_municipal_finance_data(raw_data)
            
            if year_df is not None and not year_df.empty:
                # Verify year is correct
                if 'year' in year_df.columns:
                    year_df['year'] = year  # Ensure correct year
                all_dataframes.append(year_df)
                logger.info(f"Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"Failed to parse data for year {year}")
        
        if not all_dataframes:
            logger.error("No data extracted for any year")
            return None
        
        # Combine all years
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"Combined {len(all_dataframes)} years into {len(combined_df)} total rows")
        
        return combined_df
    
    def retrieve_municipal_finances(self, job_id: str) -> Optional[pd.DataFrame]:
        """
        Retrieve municipal finance data using an existing job ID.
        
        Use this method when you already have a job ID from a previous
        extraction request (e.g., from manual API call or cached job).
        
        Args:
            job_id: Full job ID (e.g., '71517-01i_149084252')
            
        Returns:
            DataFrame with extracted data or None if error
        """
        logger.info(f"Retrieving municipal finances with job ID: {job_id}")
        
        # Retrieve using existing job
        raw_data = self.retrieve_existing_job(job_id)
        
        if raw_data is None:
            logger.error(f"Failed to retrieve job {job_id}")
            StateDBJobCache.mark_failed(self.TABLE_ID)
            return None
        
        # Mark as retrieved in cache
        StateDBJobCache.mark_retrieved(self.TABLE_ID)
        
        # Parse the CSV data
        return self._parse_municipal_finance_data(raw_data)
    
    def _parse_municipal_finance_data(self, raw_data: str) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from municipal finance table.
        
        The CSV format from GENESIS API has:
        - Lines 1-5: Metadata (table name, description)
        - Lines 6-7: Category headers
        - Line 8: Payment type names (column headers)
        - Line 9: Units (Tsd. EUR)
        - Line 10+: Data rows
        
        Data structure:
        - Col 0: Year
        - Col 1: Region code
        - Col 2: Region name
        - Cols 3+: Payment type values
        
        Args:
            raw_data: Raw CSV string from API
            
        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of municipal finance data")
            
            # Save raw data for inspection
            raw_file = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "municipal_finances_raw.csv"
            raw_file.parent.mkdir(parents=True, exist_ok=True)
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")
            
            # Parse the lines to extract header and data
            lines = raw_data.strip().split('\n')
            
            # Line 8 (index 7) contains the payment type names
            header_line_idx = 7  # 0-indexed
            data_start_idx = 9   # Data starts at line 10 (index 9)
            
            # Extract payment type names from header line
            header_parts = lines[header_line_idx].split(';')
            
            # Build column names
            column_names = ['year', 'region_code', 'region_name']
            for i, part in enumerate(header_parts[3:], start=3):
                part = part.strip()
                if part:
                    column_names.append(part)
                else:
                    column_names.append(f'value_{i}')
            
            logger.info(f"Extracted {len(column_names)} column names")
            logger.info(f"Payment types: {column_names[3:]}")
            
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
                df = df.iloc[:, :len(column_names)]
            elif len(df.columns) < len(column_names):
                column_names = column_names[:len(df.columns)]
            
            # Assign column names
            df.columns = column_names
            
            # Clean region codes and names
            df['region_code'] = df['region_code'].astype(str).str.strip()
            df['region_name'] = df['region_name'].astype(str).str.strip()
            
            logger.info(f"Successfully parsed {len(df)} rows with {len(df.columns)} columns")
            logger.info(f"Columns: {df.columns.tolist()}")
            logger.info(f"Sample data:\n{df.head(3).to_string()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Failed to parse municipal finance data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def get_table_info(self) -> Dict[str, Any]:
        """
        Get information about the municipal finance table.
        
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
            "description": "Municipal finance data (GFK methodology) for NRW municipalities",
            "payment_types": [
                "Tax revenue",
                "Grants from state",
                "Fees and charges",
                "Personnel expenditure",
                "Material expenditure",
                "Investment expenditure",
                "Debt service",
                "Transfers to other levels"
            ]
        }

