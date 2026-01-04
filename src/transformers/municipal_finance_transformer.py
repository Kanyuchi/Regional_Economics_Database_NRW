"""
Municipal Finance Data Transformer
Transforms municipal finance data from State Database NRW for database loading.

Table: 71517-01i - Municipal Finances (GFK) - Payments by payment types

Regional Economics Database for NRW
"""

import pandas as pd
from typing import Optional, List, Dict
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class MunicipalFinanceTransformer:
    """
    Transform municipal finance data from State Database NRW.
    
    Converts wide-format payment type data into long format suitable
    for the fact_demographics table with indicator_id 28.
    """
    
    # Indicator ID for municipal finances in dim_indicator table
    INDICATOR_ID = 28
    
    # NRW region code prefix
    NRW_PREFIX = '05'
    
    def __init__(self):
        """Initialize transformer."""
        logger.info("Municipal Finance Transformer initialized")
    
    def transform_municipal_finances(
        self,
        df: pd.DataFrame,
        indicator_id: int = 28,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform municipal finance data into database format.
        
        This transforms the raw data from State Database NRW into
        long format (one row per payment type per region per year).
        
        The 'notes' field stores the payment type category.
        
        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table (default 28)
            years_filter: Optional list of years to filter
            
        Returns:
            Transformed DataFrame ready for database loading with columns:
            - region_code
            - year
            - value
            - indicator_id
            - notes (payment type)
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None
        
        try:
            logger.info(f"Transforming {len(df)} rows of municipal finance data")
            logger.info(f"Input columns: {df.columns.tolist()}")
            
            # Make a copy to avoid modifying original
            df = df.copy()
            
            # Identify columns
            # The extractor now provides standardized column names
            region_col = self._find_column(df, ['region_code', 'Schluessel', 'Code', 'code',
                                                'Regional', 'Regionalschluessel', 'Key'])
            
            # Look for region name column
            name_col = self._find_column(df, ['region_name', 'Name', 'name', 'Region', 'Gemeinde', 
                                              'Bezeichnung', 'Municipality'])
            
            # Look for year column
            year_col = self._find_column(df, ['year', 'Jahr', 'Year', 'Zeit', 'Time'])
            
            if region_col is None:
                logger.error("Could not find region code column")
                logger.error(f"Available columns: {df.columns.tolist()}")
                return None
            
            logger.info(f"Using columns - Region: {region_col}, Name: {name_col}, Year: {year_col}")
            
            # Standardize column names if needed
            if region_col != 'region_code':
                df = df.rename(columns={region_col: 'region_code'})
            if year_col and year_col != 'year':
                df = df.rename(columns={year_col: 'year'})
            
            # Clean region codes
            df['region_code'] = df['region_code'].astype(str).str.strip()
            
            # Filter to NRW regions only (codes starting with '05')
            initial_count = len(df)
            nrw_mask = df['region_code'].str.startswith(self.NRW_PREFIX, na=False)
            df = df[nrw_mask].copy()
            filtered_count = initial_count - len(df)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(df)} NRW rows")
            
            if df.empty:
                logger.warning("No NRW regions found in data after filtering")
                return None
            
            # Parse year if present
            if 'year' in df.columns:
                df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
                
                # Apply year filter if specified
                if years_filter:
                    df = df[df['year'].isin(years_filter)].copy()
                    logger.info(f"Filtered to years {years_filter}, {len(df)} rows remain")
            
            # Identify value columns (payment types)
            # These are typically numeric columns that aren't region/year identifiers
            id_cols = ['region_code', 'year']
            if 'region_name' in df.columns:
                id_cols.append('region_name')
            elif name_col and name_col in df.columns:
                id_cols.append(name_col)
            
            value_cols = [col for col in df.columns if col not in id_cols 
                         and not col.lower().startswith('unnamed')]
            
            logger.info(f"Value columns (payment types): {value_cols}")
            
            if not value_cols:
                logger.error("No value columns found for payment types")
                return None
            
            # Melt to long format
            # Each payment type becomes a separate row with the type in 'notes'
            df_long = pd.melt(
                df,
                id_vars=['region_code', 'year'] if 'year' in df.columns else ['region_code'],
                value_vars=value_cols,
                var_name='payment_type',
                value_name='value'
            )
            
            # Clean and convert values
            df_long['value'] = df_long['value'].astype(str).str.replace(',', '.').str.strip()
            df_long['value'] = df_long['value'].replace(['-', '.', '...', '', 'x', 'X'], pd.NA)
            df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
            
            # Remove rows with missing values
            rows_before = len(df_long)
            df_long = df_long.dropna(subset=['value'])
            logger.info(f"Removed {rows_before - len(df_long)} rows with missing values")
            
            # Add indicator ID
            df_long['indicator_id'] = indicator_id
            
            # Store payment type in notes field
            df_long['notes'] = df_long['payment_type'].apply(self._clean_payment_type)
            
            # Select final columns
            final_cols = ['region_code', 'year', 'value', 'indicator_id', 'notes']
            if 'year' not in df_long.columns:
                final_cols.remove('year')
            
            result = df_long[final_cols].copy()
            
            # Log summary
            logger.info(f"Transformation complete:")
            logger.info(f"  - Total records: {len(result):,}")
            logger.info(f"  - Unique regions: {result['region_code'].nunique()}")
            if 'year' in result.columns:
                logger.info(f"  - Years: {sorted(result['year'].dropna().unique())}")
            logger.info(f"  - Payment types: {result['notes'].nunique()}")
            
            return result
            
        except Exception as e:
            logger.error(f"Failed to transform municipal finance data: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def _find_column(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        """
        Find a column by checking multiple possible names.
        
        Args:
            df: DataFrame to search
            candidates: List of possible column names
            
        Returns:
            Matching column name or None
        """
        for candidate in candidates:
            # Exact match
            if candidate in df.columns:
                return candidate
            # Case-insensitive match
            for col in df.columns:
                if col.lower() == candidate.lower():
                    return col
                # Partial match
                if candidate.lower() in col.lower():
                    return col
        return None
    
    def _clean_payment_type(self, payment_type: str) -> str:
        """
        Clean and standardize payment type names for the notes field.
        
        Args:
            payment_type: Raw payment type column name
            
        Returns:
            Cleaned payment type string
        """
        if pd.isna(payment_type):
            return "Unknown"
        
        # Clean up the string
        cleaned = str(payment_type).strip()
        
        # Remove common prefixes/suffixes
        cleaned = cleaned.replace('EUR 1000', '').strip()
        cleaned = cleaned.replace('in 1000 EUR', '').strip()
        cleaned = cleaned.replace('1000 EUR', '').strip()
        
        # Truncate if too long (notes field limit)
        if len(cleaned) > 200:
            cleaned = cleaned[:197] + "..."
        
        return cleaned
    
    def get_payment_type_mapping(self) -> Dict[str, str]:
        """
        Get mapping of German payment type names to English equivalents.
        
        Returns:
            Dictionary mapping German to English names
        """
        return {
            "Steuern": "Tax Revenue",
            "Schlüsselzuweisungen": "Key Allocations",
            "Gebühren": "Fees and Charges",
            "Personalausgaben": "Personnel Expenditure",
            "Sachausgaben": "Material Expenditure",
            "Investitionen": "Investment Expenditure",
            "Zinsen": "Interest Payments",
            "Tilgung": "Debt Repayment",
            "Zuweisungen": "Grants",
            "Umlagen": "Apportionments"
        }

