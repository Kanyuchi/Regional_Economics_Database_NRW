"""
Care Recipients Data Transformer
Regional Economics Database for NRW

Transforms raw care recipients data by care level and benefit type into database-ready format.
Handles data from State Database NRW table 22421-02i.

Indicators:
- 72: Total Benefit Recipients by Care Level
- 73: Nursing Home Residents by Care Level
- 74: Inpatient Care Recipients by Care Level
- 75: Care Allowance Recipients by Care Level
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


class CareRecipientsTransformer:
    """Transforms care recipients data by care level and benefit type for database loading."""

    # Indicator IDs for care metrics
    INDICATOR_BENEFIT_TOTAL = 72
    INDICATOR_NURSING_HOME = 73
    INDICATOR_INPATIENT = 74
    INDICATOR_CARE_ALLOWANCE = 75

    # Metric column to indicator mapping
    METRIC_MAPPING = {
        'benefit_recipients_total': INDICATOR_BENEFIT_TOTAL,
        'nursing_home_residents': INDICATOR_NURSING_HOME,
        'inpatient_care': INDICATOR_INPATIENT,
        'care_allowance_recipients': INDICATOR_CARE_ALLOWANCE
    }

    # Human-readable metric names
    METRIC_NAMES = {
        'benefit_recipients_total': 'Total Benefit Recipients',
        'nursing_home_residents': 'Nursing Home Residents',
        'inpatient_care': 'Inpatient Care Recipients',
        'care_allowance_recipients': 'Care Allowance Recipients'
    }

    # Care level descriptions for notes
    CARE_LEVEL_DESCRIPTIONS = {
        'level_1': 'Slight impairment (Pflegegrad 1)',
        'level_2': 'Significant impairment (Pflegegrad 2)',
        'level_3': 'Severe impairment (Pflegegrad 3)',
        'level_4': 'Most severe impairment (Pflegegrad 4)',
        'level_5': 'Most severe with special needs (Pflegegrad 5)',
        'not_assigned': 'Not yet assigned to care level',
        'total': 'All care levels combined'
    }

    def __init__(self):
        """Initialize the care recipients transformer."""
        logger.info("Care Recipients transformer initialized")

    def transform_care_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None,
        exclude_total_level: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Transform care recipients data from wide to long format.

        The raw data has structure:
        - year, region_code, region_name, care_level, care_level_code,
          benefit_recipients_total, nursing_home_residents, inpatient_care, care_allowance_recipients

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
        logger.info("TRANSFORMING CARE RECIPIENTS DATA")
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

            # Melt from wide to long format (one row per metric)
            id_cols = ['year', 'reference_date', 'region_code', 'region_name',
                       'care_level', 'care_level_code']
            metric_cols = list(self.METRIC_MAPPING.keys())

            # Ensure metric columns exist
            available_metrics = [col for col in metric_cols if col in df.columns]
            if not available_metrics:
                logger.error(f"No metric columns found. Available: {df.columns.tolist()}")
                return None

            melted = df.melt(
                id_vars=[c for c in id_cols if c in df.columns],
                value_vars=available_metrics,
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

            # Create notes field with care level information (parseable format)
            melted['metric_name'] = melted['metric'].map(self.METRIC_NAMES)
            melted['notes'] = 'care_level:' + melted['care_level_code'].astype(str) + \
                             '|' + melted['care_level'].astype(str)

            # Add standard columns
            melted['gender'] = 'total'  # Care data is not gender-disaggregated
            melted['nationality'] = None
            melted['age_group'] = None  # Could potentially be extracted but not in this table
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'  # Verified
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            # Select and order columns for database
            result = melted[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at',
                'metric', 'care_level_code'
            ]]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
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

        # Check that we have all 4 indicators
        indicators = sorted(df['indicator_id'].unique())
        expected = [
            self.INDICATOR_BENEFIT_TOTAL,
            self.INDICATOR_NURSING_HOME,
            self.INDICATOR_INPATIENT,
            self.INDICATOR_CARE_ALLOWANCE
        ]
        if not all(ind in indicators for ind in expected):
            logger.warning(f"Not all expected indicators present: {indicators}")

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

        # Group by care level
        summary = year_df.groupby(['care_level', 'care_level_code']).agg({
            'benefit_recipients_total': 'sum',
            'nursing_home_residents': 'sum',
            'inpatient_care': 'sum',
            'care_allowance_recipients': 'sum'
        }).reset_index()

        # Calculate percentages of total
        total_row = summary[summary['care_level_code'] == 'total']
        if not total_row.empty:
            total_recipients = total_row['benefit_recipients_total'].values[0]
            summary['pct_of_total'] = (summary['benefit_recipients_total'] / total_recipients * 100).round(2)

            # Calculate home vs institutional care ratio
            summary['home_care_pct'] = (
                summary['care_allowance_recipients'] / summary['benefit_recipients_total'] * 100
            ).round(2)

        return summary
