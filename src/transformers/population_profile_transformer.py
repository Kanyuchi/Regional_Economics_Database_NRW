"""
Population Profile Data Transformer
Regional Economics Database for NRW

Transforms raw population data by gender, nationality, and age groups into database-ready format.
Handles data from State Database NRW table 12411-9k06.
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


class PopulationProfileTransformer:
    """Transforms population data by gender, nationality, and age groups for database loading."""

    # Indicator IDs for population metrics
    INDICATOR_POP_TOTAL_BY_AGE = 67
    INDICATOR_POP_MALE_BY_AGE = 68
    INDICATOR_POP_FEMALE_BY_AGE = 69
    INDICATOR_POP_GERMAN_BY_AGE = 70
    INDICATOR_POP_FOREIGN_BY_AGE = 71

    # Metric column to indicator mapping
    METRIC_MAPPING = {
        'population_total': INDICATOR_POP_TOTAL_BY_AGE,
        'population_male': INDICATOR_POP_MALE_BY_AGE,
        'population_female': INDICATOR_POP_FEMALE_BY_AGE,
        'population_german': INDICATOR_POP_GERMAN_BY_AGE,
        'population_foreign': INDICATOR_POP_FOREIGN_BY_AGE
    }

    # Human-readable metric names
    METRIC_NAMES = {
        'population_total': 'Total Population',
        'population_male': 'Male Population',
        'population_female': 'Female Population',
        'population_german': 'German Population',
        'population_foreign': 'Foreign Population'
    }

    def __init__(self):
        """Initialize the population profile transformer."""
        logger.info("Population Profile transformer initialized")

    def transform_population_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None,
        exclude_total_age: bool = False
    ) -> Optional[pd.DataFrame]:
        """
        Transform population data from wide to long format.

        The raw data has structure:
        - year, region_code, region_name, age_group, age_group_code,
          population_total, population_male, population_female, population_german, population_foreign

        We transform to:
        - year, region_code, indicator_id, value, notes (containing age group info)

        Args:
            df: Raw data DataFrame from extractor
            years_filter: Optional list of years to filter
            exclude_total_age: If True, exclude 'Total' age group rows

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("="*80)
        logger.info("TRANSFORMING POPULATION PROFILE DATA")
        logger.info("="*80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Optionally exclude total age group
            if exclude_total_age:
                before = len(df)
                df = df[df['age_group_code'] != 'total']
                logger.info(f"Excluded total age group: {before} -> {len(df)} rows")

            # Melt from wide to long format (one row per metric)
            id_cols = ['year', 'region_code', 'region_name', 'age_group', 'age_group_code']
            metric_cols = list(self.METRIC_MAPPING.keys())

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

            # Create notes field with age group information (parseable format)
            melted['metric_name'] = melted['metric'].map(self.METRIC_NAMES)
            melted['notes'] = 'age_group:' + melted['age_group_code'].astype(str) + \
                             '|' + melted['age_group'].astype(str)

            # Add standard columns
            # Map gender based on metric
            melted['gender'] = melted['metric'].apply(
                lambda x: 'male' if x == 'population_male' else ('female' if x == 'population_female' else 'total')
            )
            # Map nationality based on metric
            melted['nationality'] = melted['metric'].apply(
                lambda x: 'german' if x == 'population_german' else ('foreign' if x == 'population_foreign' else 'total')
            )
            melted['age_group_col'] = melted['age_group_code']  # Store for potential use
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'  # Verified
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            # Select and order columns for database
            result = melted[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group_col', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at',
                'metric', 'age_group_code'
            ]]

            # Rename for database compatibility
            result = result.rename(columns={'age_group_col': 'age_group'})

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")
            logger.info(f"Unique age groups: {result['age_group_code'].nunique()}")

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

        # Check that we have all 5 indicators
        indicators = sorted(df['indicator_id'].unique())
        expected = [
            self.INDICATOR_POP_TOTAL_BY_AGE,
            self.INDICATOR_POP_MALE_BY_AGE,
            self.INDICATOR_POP_FEMALE_BY_AGE,
            self.INDICATOR_POP_GERMAN_BY_AGE,
            self.INDICATOR_POP_FOREIGN_BY_AGE
        ]
        if not all(ind in indicators for ind in expected):
            logger.warning(f"Not all expected indicators present: {indicators}")

        # Check value ranges (population should be positive)
        negative_values = (df['value'] < 0).sum()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} rows with negative values")

        logger.info("Validation passed")
        return True

    def get_age_group_summary(self, df: pd.DataFrame, year: int = 2024) -> pd.DataFrame:
        """
        Get summary statistics by age group for a specific year.

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

        summary = year_df[[
            'age_group', 'age_group_code',
            'population_total', 'population_male', 'population_female',
            'population_german', 'population_foreign'
        ]].copy()

        # Calculate percentages
        total_pop = summary[summary['age_group_code'] == 'total']['population_total'].values[0]
        summary['pct_of_total'] = (summary['population_total'] / total_pop * 100).round(2)
        summary['foreign_pct'] = (summary['population_foreign'] / summary['population_total'] * 100).round(2)
        summary['female_pct'] = (summary['population_female'] / summary['population_total'] * 100).round(2)

        return summary
