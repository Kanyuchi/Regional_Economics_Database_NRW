"""
Transformer for Nursing Home Recipients Data
Converts raw nursing home recipients data into database format.

Transforms nursing home care recipients by care level and care type
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

# Indicator ID for nursing home recipients
INDICATOR_NURSING_HOME_RECIPIENTS = 82


class NursingHomeRecipientsTransformer:
    """Transformer for nursing home recipients data."""

    def __init__(self):
        """Initialize the transformer."""
        logger.info("Nursing Home Recipients Transformer initialized")
        logger.info(f"Indicator ID: {INDICATOR_NURSING_HOME_RECIPIENTS}")

    def transform_recipients_data(
        self,
        raw_data: pd.DataFrame,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform nursing home recipients data into database format.

        Creates records for:
        - Total recipients
        - Recipients by care type (full/partial inpatient)
        - Recipients by care level (1-5, not assigned)

        Args:
            raw_data: Raw data from extractor
            years_filter: Optional list of years to include

        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("=" * 80)
        logger.info("TRANSFORMING NURSING HOME RECIPIENTS DATA")
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

            # 1. Total recipients
            if pd.notna(row['total_recipients']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_NURSING_HOME_RECIPIENTS,
                    'value': row['total_recipients'],
                    'notes': 'category:total'
                })

            # 2. Recipients by care type
            care_types = [
                ('full_inpatient', 'Full Inpatient Care'),
                ('partial_inpatient', 'Partial Inpatient Care')
            ]

            for col_name, care_type_label in care_types:
                if pd.notna(row[col_name]):
                    all_records.append({
                        'year': year,
                        'region_code': region_code,
                        'region_name': region_name,
                        'indicator_id': INDICATOR_NURSING_HOME_RECIPIENTS,
                        'value': row[col_name],
                        'notes': f'care_type:{col_name}|{care_type_label}'
                    })

            # 3. Recipients by care level
            care_levels = [
                ('not_assigned', 'Not Yet Assigned'),
                ('care_level_5', 'Care Level 5'),
                ('care_level_4', 'Care Level 4'),
                ('care_level_3', 'Care Level 3'),
                ('care_level_2', 'Care Level 2'),
                ('care_level_1', 'Care Level 1')
            ]

            for col_name, care_level_label in care_levels:
                if pd.notna(row[col_name]):
                    all_records.append({
                        'year': year,
                        'region_code': region_code,
                        'region_name': region_name,
                        'indicator_id': INDICATOR_NURSING_HOME_RECIPIENTS,
                        'value': row[col_name],
                        'notes': f'care_level:{col_name}|{care_level_label}'
                    })

        if not all_records:
            logger.error("No records created during transformation")
            return None

        df = pd.DataFrame(all_records)

        logger.info(f"Output: {len(df)} transformed rows")
        logger.info(f"Years: {sorted(df['year'].unique())}")
        logger.info(f"Regions: {df['region_code'].nunique()}")
        logger.info(f"Indicator: {INDICATOR_NURSING_HOME_RECIPIENTS}")

        # Log breakdown by category
        logger.info("\nRecords by category:")
        for category in ['total', 'care_type:full_inpatient', 'care_type:partial_inpatient',
                        'care_level:care_level_5', 'care_level:care_level_4',
                        'care_level:care_level_3', 'care_level:care_level_2',
                        'care_level:care_level_1', 'care_level:not_assigned']:
            count = len(df[df['notes'].str.startswith(category)])
            if count > 0:
                logger.info(f"  {category}: {count} records")

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
        expected_indicator = INDICATOR_NURSING_HOME_RECIPIENTS

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
        logger.info(f"Years present: {years}")

        expected_years = [2017, 2019, 2021, 2023]  # Biennial
        missing_years = [y for y in expected_years if y not in years]
        if missing_years:
            logger.warning(f"⚠ Missing expected years: {missing_years}")

        logger.info("\n" + "=" * 80)
        logger.info("✓ VALIDATION COMPLETE")
        logger.info("=" * 80)

        return True
