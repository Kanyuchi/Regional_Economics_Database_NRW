"""
Transformer for Physicians Data
Converts raw physicians data into database format.

Transforms full-time physicians in hospitals by gender
into the standardized fact_demographics table format.

Regional Economics Database for NRW
"""

import pandas as pd
from typing import Optional, List
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)

# Indicator ID for physicians
INDICATOR_PHYSICIANS = 83


class PhysiciansTransformer:
    """Transformer for physicians data."""

    def __init__(self):
        """Initialize the transformer."""
        logger.info("Physicians Transformer initialized")
        logger.info(f"Indicator ID: {INDICATOR_PHYSICIANS}")

    def transform_physicians_data(
        self,
        raw_data: pd.DataFrame,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform physicians data into database format.

        Creates records for:
        - Total physicians
        - Male physicians
        - Female physicians

        Args:
            raw_data: Raw data from extractor
            years_filter: Optional list of years to include

        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("=" * 80)
        logger.info("TRANSFORMING PHYSICIANS DATA")
        logger.info("=" * 80)

        if raw_data is None or raw_data.empty:
            logger.error("No raw data provided")
            return None

        logger.info(f"Input: {len(raw_data)} raw rows")

        # Filter years if specified
        if years_filter:
            raw_data = raw_data[raw_data['year'].isin(years_filter)]
            logger.info(f"Filtered to years {years_filter}: {len(raw_data)} rows")

        all_records = []

        # Process each row
        for idx, row in raw_data.iterrows():
            year = int(row['year'])
            region_code = str(row['region_code'])
            region_name = str(row['region_name'])

            # 1. Total physicians
            if pd.notna(row['total_physicians']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_PHYSICIANS,
                    'value': row['total_physicians'],
                    'notes': 'gender:total|Total'
                })

            # 2. Male physicians
            if pd.notna(row['male_physicians']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_PHYSICIANS,
                    'value': row['male_physicians'],
                    'notes': 'gender:male|Male'
                })

            # 3. Female physicians
            if pd.notna(row['female_physicians']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_PHYSICIANS,
                    'value': row['female_physicians'],
                    'notes': 'gender:female|Female'
                })

        if not all_records:
            logger.error("No records created during transformation")
            return None

        df = pd.DataFrame(all_records)

        logger.info(f"Output: {len(df)} transformed rows")
        logger.info(f"Years: {sorted(df['year'].unique())}")
        logger.info(f"Regions: {df['region_code'].nunique()}")
        logger.info(f"Indicator: {INDICATOR_PHYSICIANS}")

        # Log breakdown by gender
        logger.info("\nRecords by gender:")
        for gender in ['total', 'male', 'female']:
            count = len(df[df['notes'].str.startswith(f'gender:{gender}')])
            if count > 0:
                logger.info(f"  {gender}: {count} records")

        return df

    def validate_data(self, transformed_data: pd.DataFrame) -> bool:
        """
        Validate transformed data.

        Args:
            transformed_data: Transformed DataFrame

        Returns:
            True if valid, False otherwise
        """
        logger.info("\n" + "=" * 80)
        logger.info("VALIDATING TRANSFORMED DATA")
        logger.info("=" * 80)

        if transformed_data is None or transformed_data.empty:
            logger.error("❌ No data to validate")
            return False

        required_columns = ['year', 'region_code', 'indicator_id', 'value', 'notes']
        missing_columns = [col for col in required_columns if col not in transformed_data.columns]

        if missing_columns:
            logger.error(f"❌ Missing required columns: {missing_columns}")
            return False

        logger.info(f"✓ All required columns present: {required_columns}")

        # Check for null values in critical columns
        null_checks = {
            'year': transformed_data['year'].isnull().sum(),
            'region_code': transformed_data['region_code'].isnull().sum(),
            'indicator_id': transformed_data['indicator_id'].isnull().sum(),
            'value': transformed_data['value'].isnull().sum()
        }

        has_nulls = False
        for col, null_count in null_checks.items():
            if null_count > 0:
                logger.warning(f"⚠ Column '{col}' has {null_count} null values")
                has_nulls = True

        if not has_nulls:
            logger.info("✓ No null values in critical columns")

        # Check indicator IDs
        unique_indicators = transformed_data['indicator_id'].unique()
        expected_indicator = INDICATOR_PHYSICIANS

        if len(unique_indicators) == 1 and unique_indicators[0] == expected_indicator:
            logger.info(f"✓ Indicator ID correct: {expected_indicator}")
        else:
            logger.error(f"❌ Unexpected indicator IDs: {unique_indicators}")
            logger.error(f"   Expected: {expected_indicator}")
            return False

        # Check value ranges
        min_value = transformed_data['value'].min()
        max_value = transformed_data['value'].max()

        logger.info(f"Value range: {min_value:,.0f} to {max_value:,.0f}")

        if min_value < 0:
            logger.warning(f"⚠ Found negative values (min: {min_value})")

        # Check years
        years = sorted(transformed_data['year'].unique())
        logger.info(f"Years present: {min(years)} - {max(years)} ({len(years)} years)")

        logger.info("\n" + "=" * 80)
        logger.info("✓ VALIDATION COMPLETE")
        logger.info("=" * 80)

        return True
