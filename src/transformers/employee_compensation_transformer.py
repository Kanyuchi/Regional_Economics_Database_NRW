"""
Employee Compensation Data Transformer
Regional Economics Database for NRW

Transforms raw employee compensation data into database-ready format.
Handles data from State Database NRW table 82711-06i.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class EmployeeCompensationTransformer:
    """Transforms employee compensation data for database loading."""

    def __init__(self):
        """Initialize the employee compensation transformer."""

        # Sector name standardization mapping
        self.sector_mapping = {
            # German to English
            'Land- und Forstwirtschaft, Fischerei': 'Agriculture, forestry and fishing',
            'Produzierendes Gewerbe ohne Baugewerbe': 'Manufacturing excl. construction',
            'Verarbeitendes Gewerbe': 'Manufacturing (C)',
            'Baugewerbe': 'Construction',
            'Handel, Verkehr und Gastgewerbe': 'Trade, transport and hospitality',
            'Information und Kommunikation': 'Information and communication',
            'Finanz- und Versicherungsdienstleister': 'Finance and insurance',
            'Grundstücks- und Wohnungswesen': 'Real estate',
            'Unternehmensdienstleister': 'Business services',
            'Öffentliche und sonstige Dienstleister': 'Public and other services',
            'Dienstleistungsbereiche': 'Services total',
            'Insgesamt': 'Total',
            # Short forms
            'A': 'Agriculture, forestry and fishing',
            'B-E': 'Manufacturing',
            'C': 'Manufacturing (C)',
            'F': 'Construction',
            'G-I': 'Trade, transport and hospitality',
            'J': 'Information and communication',
            'K': 'Finance and insurance',
            'L': 'Real estate',
            'M-N': 'Business services',
            'O-U': 'Public and other services',
            'G-U': 'Services total'
        }

        logger.info("Employee Compensation transformer initialized")

    def transform_compensation_data(
        self,
        df: pd.DataFrame,
        indicator_id_base: int = 41,  # Base indicator ID for compensation
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform compensation data from wide format (sectors as columns) to long format.

        The raw data has structure:
        - year, region_code, region_name, sector1, sector2, ..., sectorN

        We transform to:
        - year, region_code, indicator_id, value, notes

        Args:
            df: Raw data DataFrame from extractor
            indicator_id_base: Base indicator ID (each sector gets +1)
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("="*80)
        logger.info("TRANSFORMING EMPLOYEE COMPENSATION DATA")
        logger.info("="*80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Identify sector columns (everything after year, region_code, region_name)
            id_cols = ['year', 'region_code', 'region_name']
            sector_cols = [col for col in df.columns if col not in id_cols]
            logger.info(f"Sector columns found: {len(sector_cols)}")
            logger.info(f"Sectors: {sector_cols}")

            # Melt from wide to long format
            melted = df.melt(
                id_vars=id_cols,
                value_vars=sector_cols,
                var_name='sector',
                value_name='value'
            )

            logger.info(f"Melted to {len(melted)} rows")

            # Clean and convert values
            melted['value'] = melted['value'].apply(self._clean_value)

            # Drop rows with no value
            before_count = len(melted)
            melted = melted.dropna(subset=['value'])
            logger.info(f"Dropped {before_count - len(melted)} rows with NULL values")

            # Map sectors to indicator IDs
            # Create a mapping from sector name/position to indicator ID
            sector_to_indicator = {}
            for i, sector in enumerate(sector_cols):
                sector_to_indicator[sector] = indicator_id_base + i

            melted['indicator_id'] = melted['sector'].map(sector_to_indicator)

            # Create notes field with sector information
            melted['notes'] = 'Employee Compensation: ' + melted['sector'].astype(str)

            # Add standard columns
            melted['gender'] = 'total'
            melted['nationality'] = 'total'
            melted['age_group'] = None
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'  # Verified
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            # Rename to match database schema
            result = melted.rename(columns={
                'region_code': 'region_code',
                'year': 'year'
            })

            # Select and order columns for database
            result = result[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at', 'sector'
            ]]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")

            return result

        except Exception as e:
            logger.error(f"Transformation error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _clean_value(self, value) -> Optional[float]:
        """Clean and convert a value to float."""
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        # String cleaning
        value_str = str(value).strip()

        # Handle special markers
        if value_str in ['-', '.', '...', 'x', '/', '–', '']:
            return None

        # Remove thousand separators and convert decimal
        try:
            value_str = value_str.replace(' ', '')
            value_str = value_str.replace(',', '.')
            return float(value_str)
        except ValueError:
            return None

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed data before loading.

        Args:
            df: Transformed DataFrame

        Returns:
            True if valid, False otherwise
        """
        if df is None or df.empty:
            logger.error("Validation failed: Empty DataFrame")
            return False

        # Check required columns
        required_cols = ['region_code', 'year', 'indicator_id', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Validation failed: Missing columns {missing_cols}")
            return False

        # Check for null indicator IDs
        null_indicators = df['indicator_id'].isnull().sum()
        if null_indicators > 0:
            logger.warning(f"Found {null_indicators} rows with NULL indicator_id")

        # Check value range (should be positive for compensation)
        negative_values = (df['value'] < 0).sum()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} negative values")

        logger.info("Validation passed")
        return True
