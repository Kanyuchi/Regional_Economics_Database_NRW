"""
Income Tax by Bracket Data Transformer
Regional Economics Database for NRW

Transforms raw wage and income tax data by income brackets into database-ready format.
Handles data from State Database NRW table 73111-030i.
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


class IncomeTaxBracketTransformer:
    """Transforms wage and income tax data by income bracket for database loading."""

    # Indicator IDs for bracket data
    INDICATOR_TAXPAYERS_BRACKET = 59
    INDICATOR_INCOME_BRACKET = 60
    INDICATOR_TAX_BRACKET = 61

    # Metric column to indicator mapping
    METRIC_MAPPING = {
        'taxpayers': INDICATOR_TAXPAYERS_BRACKET,
        'total_income_tsd_eur': INDICATOR_INCOME_BRACKET,
        'tax_amount_tsd_eur': INDICATOR_TAX_BRACKET
    }

    def __init__(self):
        """Initialize the income tax bracket transformer."""
        logger.info("Income Tax Bracket transformer initialized")

    def transform_bracket_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None,
        exclude_totals: bool = True
    ) -> Optional[pd.DataFrame]:
        """
        Transform income tax bracket data from wide to long format.

        The raw data has structure:
        - year, region_code, region_name, income_bracket, income_bracket_code,
          taxpayers, total_income_tsd_eur, tax_amount_tsd_eur

        We transform to:
        - year, region_code, indicator_id, value, notes (containing bracket info)

        Args:
            df: Raw data DataFrame from extractor
            years_filter: Optional list of years to filter
            exclude_totals: If True, exclude 'insgesamt' and 'Verlustfälle' rows

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("="*80)
        logger.info("TRANSFORMING INCOME TAX BRACKET DATA")
        logger.info("="*80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Optionally exclude totals
            if exclude_totals:
                before = len(df)
                df = df[~df['income_bracket_code'].isin(['total', 'loss_cases'])]
                logger.info(f"Excluded totals/losses: {before} -> {len(df)} rows")

            # Melt from wide to long format (one row per metric)
            id_cols = ['year', 'region_code', 'region_name', 'income_bracket', 'income_bracket_code']
            metric_cols = ['taxpayers', 'total_income_tsd_eur', 'tax_amount_tsd_eur']

            melted = df.melt(
                id_vars=id_cols,
                value_vars=metric_cols,
                var_name='metric',
                value_name='value'
            )

            logger.info(f"Melted to {len(melted)} rows")

            # Drop rows with no value
            before_count = len(melted)
            melted = melted.dropna(subset=['value'])
            logger.info(f"Dropped {before_count - len(melted)} rows with NULL values")

            # Map metrics to indicator IDs
            melted['indicator_id'] = melted['metric'].map(self.METRIC_MAPPING)

            # Create notes field with bracket information (parseable format)
            melted['notes'] = 'bracket:' + melted['income_bracket_code'].astype(str) + \
                             '|' + melted['income_bracket'].astype(str)

            # Add standard columns
            melted['gender'] = 'total'
            melted['nationality'] = 'total'
            melted['age_group'] = None
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'  # Verified
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            # Select and order columns for database
            result = melted[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at',
                'metric', 'income_bracket_code'
            ]]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")
            logger.info(f"Unique brackets: {result['income_bracket_code'].nunique()}")

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

        # Check that we have all 3 indicators
        indicators = df['indicator_id'].unique()
        expected = [self.INDICATOR_TAXPAYERS_BRACKET, self.INDICATOR_INCOME_BRACKET, self.INDICATOR_TAX_BRACKET]
        if not all(ind in indicators for ind in expected):
            logger.warning(f"Not all expected indicators present: {indicators}")

        logger.info("Validation passed")
        return True

    def get_bracket_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary statistics by income bracket.

        Args:
            df: Raw data DataFrame

        Returns:
            Summary DataFrame
        """
        summary = df.groupby('income_bracket').agg({
            'taxpayers': 'sum',
            'total_income_tsd_eur': 'sum',
            'tax_amount_tsd_eur': 'sum'
        }).reset_index()

        summary['avg_income_eur'] = (summary['total_income_tsd_eur'] * 1000 / summary['taxpayers']).round(0)
        summary['effective_tax_rate'] = (summary['tax_amount_tsd_eur'] / summary['total_income_tsd_eur'] * 100).round(2)

        return summary
