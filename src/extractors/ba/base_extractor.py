"""
Base Extractor for Federal Employment Agency (BA) Data
Regional Economics Database for NRW

Base class for extracting data from BA Excel files.
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import sys

# Add parent directories to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class BAExtractor:
    """
    Base class for extracting data from Federal Employment Agency Excel files.
    
    BA data is provided as downloadable Excel files with multiple sheets.
    Each file typically covers one year of data.
    """
    
    def __init__(self, file_path: Path = None):
        """
        Initialize BA extractor.
        
        Args:
            file_path: Path to BA Excel file
        """
        self.file_path = file_path
        logger.info("BA extractor initialized")
    
    def read_sheet(self, sheet_name: str, skip_rows: int = 9, 
                   header_row: int = None) -> pd.DataFrame:
        """
        Read a sheet from the BA Excel file.
        
        Args:
            sheet_name: Name of sheet to read
            skip_rows: Number of rows to skip before data starts
            header_row: Row number containing headers (if different from skip_rows-1)
        
        Returns:
            DataFrame with sheet data
        """
        if not self.file_path or not Path(self.file_path).exists():
            logger.error(f"File not found: {self.file_path}")
            return pd.DataFrame()
        
        try:
            # Read Excel file
            df = pd.read_excel(
                self.file_path,
                sheet_name=sheet_name,
                skiprows=skip_rows,
                header=None
            )
            
            logger.info(f"Read sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns")
            return df
            
        except Exception as e:
            logger.error(f"Error reading sheet '{sheet_name}': {e}")
            return pd.DataFrame()
    
    def get_sheet_names(self) -> List[str]:
        """
        Get list of all sheet names in the Excel file.
        
        Returns:
            List of sheet names
        """
        if not self.file_path or not Path(self.file_path).exists():
            logger.error(f"File not found: {self.file_path}")
            return []
        
        try:
            excel_file = pd.ExcelFile(self.file_path)
            sheet_names = excel_file.sheet_names
            excel_file.close()
            return sheet_names
        except Exception as e:
            logger.error(f"Error reading sheet names: {e}")
            return []
    
    def filter_nrw_regions(self, df: pd.DataFrame, region_col: int = 0) -> pd.DataFrame:
        """
        Filter DataFrame to include only NRW regions.
        
        NRW region codes start with '05'.
        
        Args:
            df: DataFrame to filter
            region_col: Column index containing region codes
        
        Returns:
            Filtered DataFrame
        """
        # Convert region column to string
        df[region_col] = df[region_col].astype(str).str.strip()
        
        # Filter for NRW (codes starting with '05') or Germany ('D')
        nrw_filter = (
            (df[region_col].str.startswith('05', na=False)) |
            (df[region_col] == 'D')  # Include Germany for comparison
        )
        
        df_filtered = df[nrw_filter].copy()
        
        logger.info(f"Filtered to {len(df_filtered)} NRW/Germany regions from {len(df)} total")
        return df_filtered
