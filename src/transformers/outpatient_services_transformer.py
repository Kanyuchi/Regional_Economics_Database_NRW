"""
Outpatient Services Data Transformer
Regional Economics Database for NRW

Transforms raw outpatient services data (facilities and staff) into database-ready format.
Handles data from State Database NRW table 22411-01i.

Indicators:
- 77: Total Outpatient Care Services (facilities count)
- 78: Staff in Outpatient Care Services
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


class OutpatientServicesTransformer:
    """Transforms outpatient services data (facilities and staff) for database loading."""

    # Indicator IDs
    INDICATOR_SERVICES_TOTAL = 77
    INDICATOR_STAFF_COUNT = 78

    # Metric mapping
    METRIC_MAPPING = {
        'total_services': INDICATOR_SERVICES_TOTAL,
        'staff_count': INDICATOR_STAFF_COUNT
    }

    METRIC_NAMES = {
        'total_services': 'Total Outpatient Care Services',
        'staff_count': 'Staff in Outpatient Care Services'
    }

    def __init__(self):
        """Initialize the outpatient services transformer."""
        logger.info("Outpatient Services transformer initialized")

    def transform_services_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform outpatient services data for database loading.

        The raw data has structure:
        - year, region_code, region_name, total_services, single_tier_*, multi_tier_*, staff_count

        We transform to:
        - year, region_code, indicator_id, value, notes

        Args:
            df: Raw data DataFrame from extractor
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("=" * 80)
        logger.info("TRANSFORMING OUTPATIENT SERVICES DATA")
        logger.info("=" * 80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Select only the metrics we want to store as indicators
            id_cols = ['year', 'reference_date', 'region_code', 'region_name']
            metric_cols = ['total_services', 'staff_count']

            # Melt from wide to long format
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

            # Create notes field with metric information
            melted['metric_name'] = melted['metric'].map(self.METRIC_NAMES)
            melted['notes'] = 'metric:' + melted['metric'].astype(str) + \
                             '|' + melted['metric_name'].astype(str)

            # Add standard columns
            melted['gender'] = 'total'
            melted['nationality'] = None
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
                'metric'
            ]]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")
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

        # Check that we have both indicators
        indicators = sorted(df['indicator_id'].unique())
        expected = [self.INDICATOR_SERVICES_TOTAL, self.INDICATOR_STAFF_COUNT]
        if not all(ind in indicators for ind in expected):
            logger.warning(f"Not all expected indicators present: {indicators}")

        # Check value ranges (counts should be non-negative)
        negative_values = (df['value'] < 0).sum()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} rows with negative values")

        logger.info("Validation passed")
        return True

    def get_services_summary(self, df: pd.DataFrame, year: int = 2023) -> pd.DataFrame:
        """
        Get summary statistics for a specific year.

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

        # NRW state-level summary
        nrw_df = year_df[year_df['region_code'] == '05']

        if nrw_df.empty:
            return pd.DataFrame()

        summary = {
            'Total Services': nrw_df['total_services'].values[0],
            'Single-tier Facilities': nrw_df['single_tier_total'].values[0],
            'Multi-tier Facilities': nrw_df['multi_tier_total'].values[0],
            'Staff Count': nrw_df['staff_count'].values[0],
            'Staff per Service': round(
                nrw_df['staff_count'].values[0] / nrw_df['total_services'].values[0], 1
            ) if nrw_df['total_services'].values[0] > 0 else 0
        }

        return pd.DataFrame([summary])
