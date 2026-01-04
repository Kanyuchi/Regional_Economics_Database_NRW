"""
Outpatient Care Recipients Data Transformer
Regional Economics Database for NRW

Transforms raw outpatient care data by care level into database-ready format.
Handles data from State Database NRW table 22411-02i.

Indicator:
- 76: Outpatient Care Recipients by Care Level
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


class OutpatientCareTransformer:
    """Transforms outpatient care recipients data by care level for database loading."""

    # Indicator ID for outpatient care metric
    INDICATOR_OUTPATIENT_CARE = 76

    # Care level descriptions for notes
    CARE_LEVEL_DESCRIPTIONS = {
        'level_1': 'Slight impairment (Pflegegrad 1)',
        'level_2': 'Significant impairment (Pflegegrad 2)',
        'level_3': 'Severe impairment (Pflegegrad 3)',
        'level_4': 'Most severe impairment (Pflegegrad 4)',
        'level_5': 'Most severe with special needs (Pflegegrad 5)',
        'total': 'All care levels combined'
    }

    def __init__(self):
        """Initialize the outpatient care transformer."""
        logger.info("Outpatient Care transformer initialized")

    def transform_outpatient_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None,
        exclude_total_level: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Transform outpatient care data for database loading.

        The raw data has structure:
        - year, region_code, region_name, care_level, care_level_code, outpatient_recipients

        We transform to:
        - year, region_code, indicator_id, value, notes (containing care level info)

        Args:
            df: Raw data DataFrame from extractor
            years_filter: Optional list of years to filter
            exclude_total_level: If True, exclude 'total' care level rows

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("=" * 80)
        logger.info("TRANSFORMING OUTPATIENT CARE DATA")
        logger.info("=" * 80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Optionally exclude total care level
            if exclude_total_level:
                before = len(df)
                df = df[df['care_level_code'] != 'total']
                logger.info(f"Excluded total care level: {before} -> {len(df)} rows")

            # Create result DataFrame
            result = df.copy()

            # Set indicator ID
            result['indicator_id'] = self.INDICATOR_OUTPATIENT_CARE

            # Rename value column
            result['value'] = result['outpatient_recipients']

            # Create notes field with care level information (parseable format)
            result['notes'] = 'care_level:' + result['care_level_code'].astype(str) + \
                             '|' + result['care_level'].astype(str)

            # Add standard columns
            result['gender'] = 'total'  # Care data is not gender-disaggregated
            result['nationality'] = None
            result['age_group'] = None
            result['migration_background'] = None
            result['data_quality_flag'] = 'V'  # Verified
            result['extracted_at'] = datetime.now()
            result['loaded_at'] = datetime.now()

            # Select and order columns for database
            result = result[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at',
                'care_level_code'
            ]]

            # Drop rows with null values
            before_count = len(result)
            result = result.dropna(subset=['value'])
            logger.info(f"Dropped {before_count - len(result)} rows with NULL values")

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator ID: {self.INDICATOR_OUTPATIENT_CARE}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")
            logger.info(f"Unique care levels: {result['care_level_code'].nunique()}")
            logger.info(f"Unique regions: {result['region_code'].nunique()}")

            return result

        except Exception as e:
            logger.error(f"Transformation error: {e}")
            import traceback
            traceback.print_exc()
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

        # Check that indicator is 76
        indicators = df['indicator_id'].unique()
        if self.INDICATOR_OUTPATIENT_CARE not in indicators:
            logger.warning(f"Expected indicator {self.INDICATOR_OUTPATIENT_CARE} not found")

        # Check value ranges (counts should be non-negative)
        negative_values = (df['value'] < 0).sum()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} rows with negative values")

        # Check care levels
        care_levels = df['care_level_code'].unique()
        logger.info(f"Care levels in data: {care_levels.tolist()}")

        logger.info("Validation passed")
        return True

    def get_care_level_summary(self, df: pd.DataFrame, year: int = 2023) -> pd.DataFrame:
        """
        Get summary statistics by care level for a specific year.

        Args:
            df: Raw data DataFrame
            year: Year to summarize

        Returns:
            Summary DataFrame
        """
        year_df = df[df['year'] == year]

        if year_df.empty:
            logger.warning(f"No data for year {year}")
            return pd.DataFrame()

        # Group by care level (state-level totals)
        state_df = year_df[year_df['region_code'] == '05']

        summary = state_df[['care_level', 'care_level_code', 'outpatient_recipients']].copy()

        # Calculate percentages of total
        total_row = summary[summary['care_level_code'] == 'total']
        if not total_row.empty:
            total_recipients = total_row['outpatient_recipients'].values[0]
            summary['pct_of_total'] = (
                summary['outpatient_recipients'] / total_recipients * 100
            ).round(2)

        return summary
