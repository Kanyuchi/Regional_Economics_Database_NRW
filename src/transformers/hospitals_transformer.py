"""
Transformer for Hospitals Data
Converts raw hospitals data into database format.

Transforms hospitals and beds by operator type
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

# Indicator IDs
INDICATOR_HOSPITALS = 84  # Number of hospitals
INDICATOR_BEDS = 85       # Number of beds


class HospitalsTransformer:
    """Transformer for hospitals data."""

    def __init__(self):
        """Initialize the transformer."""
        logger.info("Hospitals Transformer initialized")
        logger.info(f"Indicator IDs: {INDICATOR_HOSPITALS} (Hospitals), {INDICATOR_BEDS} (Beds)")

    def transform_hospitals_data(
        self,
        raw_data: pd.DataFrame,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform hospitals data into database format.

        Creates records for:
        - Total hospitals and by operator type (public, private, non-profit)
        - Total beds and by operator type

        Args:
            raw_data: Raw data from extractor
            years_filter: Optional list of years to include

        Returns:
            Transformed DataFrame ready for loading
        """
        logger.info("=" * 80)
        logger.info("TRANSFORMING HOSPITALS DATA")
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

            # ===== HOSPITALS (Indicator 84) =====

            # 1. Total hospitals
            if pd.notna(row['total_hospitals']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_HOSPITALS,
                    'value': row['total_hospitals'],
                    'notes': 'operator:total|Total'
                })

            # 2. Public hospitals
            if pd.notna(row['public_hospitals']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_HOSPITALS,
                    'value': row['public_hospitals'],
                    'notes': 'operator:public|Public'
                })

            # 3. Private hospitals
            if pd.notna(row['private_hospitals']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_HOSPITALS,
                    'value': row['private_hospitals'],
                    'notes': 'operator:private|Private'
                })

            # 4. Non-profit hospitals
            if pd.notna(row['nonprofit_hospitals']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_HOSPITALS,
                    'value': row['nonprofit_hospitals'],
                    'notes': 'operator:nonprofit|Non-profit'
                })

            # ===== BEDS (Indicator 85) =====

            # 1. Total beds
            if pd.notna(row['total_beds']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_BEDS,
                    'value': row['total_beds'],
                    'notes': 'operator:total|Total'
                })

            # 2. Public beds
            if pd.notna(row['public_beds']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_BEDS,
                    'value': row['public_beds'],
                    'notes': 'operator:public|Public'
                })

            # 3. Private beds
            if pd.notna(row['private_beds']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_BEDS,
                    'value': row['private_beds'],
                    'notes': 'operator:private|Private'
                })

            # 4. Non-profit beds
            if pd.notna(row['nonprofit_beds']):
                all_records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': INDICATOR_BEDS,
                    'value': row['nonprofit_beds'],
                    'notes': 'operator:nonprofit|Non-profit'
                })

        if not all_records:
            logger.error("No records created during transformation")
            return None

        df = pd.DataFrame(all_records)

        logger.info(f"Output: {len(df)} transformed rows")
        logger.info(f"Years: {sorted(df['year'].unique())}")
        logger.info(f"Regions: {df['region_code'].nunique()}")
        logger.info(f"Indicators: {sorted(df['indicator_id'].unique())}")

        # Log breakdown
        logger.info("\nRecords by indicator:")
        for ind_id in sorted(df['indicator_id'].unique()):
            count = len(df[df['indicator_id'] == ind_id])
            ind_name = "Hospitals" if ind_id == INDICATOR_HOSPITALS else "Beds"
            logger.info(f"  {ind_id} ({ind_name}): {count} records")

        logger.info("\nRecords by operator type:")
        for op_type in ['total', 'public', 'private', 'nonprofit']:
            count = len(df[df['notes'].str.startswith(f'operator:{op_type}')])
            if count > 0:
                logger.info(f"  {op_type}: {count} records")

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
        unique_indicators = sorted(transformed_data['indicator_id'].unique())
        expected_indicators = [INDICATOR_HOSPITALS, INDICATOR_BEDS]

        if unique_indicators == expected_indicators:
            logger.info(f"✓ Indicator IDs correct: {expected_indicators}")
        else:
            logger.error(f"❌ Unexpected indicator IDs: {unique_indicators}")
            logger.error(f"   Expected: {expected_indicators}")
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
