"""
ICT Indicators Data Transformer
Regional Economics Database for NRW

Transforms raw ICT indicators data into database-ready format.
Handles data from State Database NRW table 52911-01i.
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


class ICTIndicatorsTransformer:
    """Transforms ICT indicators data for database loading."""

    def __init__(self):
        """Initialize the ICT indicators transformer."""

        # Indicator name standardization mapping (German to English)
        self.indicator_mapping = {
            # Common ICT indicators - adjust based on actual data
            'Breitbandverfügbarkeit': 'Broadband availability',
            'Internetnutzung': 'Internet usage',
            'Digitale Infrastruktur': 'Digital infrastructure',
            'IKT-Beschäftigung': 'ICT employment',
            'Digitalisierungsgrad': 'Digitalization level',
            # Add more mappings as needed based on actual table structure
        }

        logger.info("ICT indicators transformer initialized")

    def transform_ict_data(
        self,
        df: pd.DataFrame,
        indicator_id_base: int = 50,  # Base indicator ID for ICT metrics
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform ICT data for state-level indicators.

        NOTE: The raw data is already in semi-long format from the extractor:
        - year, region_code, region_name, indicator_name, value_str, unit

        We transform to:
        - year, region_code, indicator_id, value, indicator_name, notes

        Args:
            df: Raw data DataFrame from extractor
            indicator_id_base: Base indicator ID (each indicator gets sequential ID)
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info("="*80)
            logger.info("TRANSFORMING ICT INDICATORS DATA (STATE-LEVEL)")
            logger.info("="*80)
            logger.info(f"Input: {len(df)} rows × {len(df.columns)} columns")
            logger.info(f"Columns: {df.columns.tolist()}")

            # Make a copy
            df_clean = df.copy()

            # Ensure year is numeric
            df_clean['year'] = pd.to_numeric(df_clean['year'], errors='coerce')

            # Clean region codes
            df_clean['region_code'] = df_clean['region_code'].astype(str).str.strip()

            # Filter years if specified
            if years_filter:
                df_clean = df_clean[df_clean['year'].isin(years_filter)]
                logger.info(f"Filtered to {len(df_clean)} rows for years: {years_filter}")

            logger.info(f"Processing {len(df_clean)} indicators...")

            # Process each indicator row
            records = []

            for idx, row in df_clean.iterrows():
                year = row['year']
                region_code = row['region_code']
                region_name = row['region_name']
                indicator_name = row['indicator_name']
                value_str = row['value_str']
                unit = row.get('unit', 'Percent')

                # Clean value
                if pd.notna(value_str) and str(value_str).strip() != '':
                    value_str = str(value_str).strip()

                    # Skip non-numeric values (like '-', 'x', '/', for missing data)
                    if value_str in ['-', 'x', '.', '...', '/']:
                        logger.debug(f"Skipping missing value '{value_str}' for indicator: {indicator_name}")
                        continue

                    # Convert to float (handle German format: 97.5 or 97,5)
                    try:
                        if ',' in value_str and '.' in value_str:
                            # Format: 1.234,56 (German)
                            value_clean = float(value_str.replace('.', '').replace(',', '.'))
                        elif ',' in value_str:
                            # Format: 1234,56 (German)
                            value_clean = float(value_str.replace(',', '.'))
                        else:
                            # Format: 1234.56 (English) or 1234
                            value_clean = float(value_str)
                    except (ValueError, AttributeError) as e:
                        logger.warning(f"Could not convert value '{value_str}' for indicator '{indicator_name}': {e}")
                        continue
                else:
                    # Skip empty/missing values
                    continue

                # Standardize indicator name (translate German to English if mapping exists)
                indicator_standard = self.indicator_mapping.get(indicator_name, indicator_name)

                # Create record with sequential indicator_id
                records.append({
                    'year': int(year),
                    'region_code': region_code,
                    'region_name': region_name,
                    'indicator_id': indicator_id_base + idx,  # Sequential ID
                    'indicator_name': indicator_standard,
                    'value': value_clean,
                    'unit': unit,
                    'notes': f"ICT: {indicator_standard} ({unit})",
                    'extracted_at': datetime.now()
                })

            # Create transformed DataFrame
            transformed = pd.DataFrame(records)

            if transformed.empty:
                logger.error("No valid records after transformation")
                return None

            logger.info("\n" + "="*80)
            logger.info("TRANSFORMATION COMPLETE")
            logger.info("="*80)
            logger.info(f"Output: {len(transformed)} records")
            logger.info(f"Years: {sorted(transformed['year'].unique())}")
            logger.info(f"Regions: {len(transformed['region_code'].unique())} unique (state-level)")
            logger.info(f"Region: {transformed['region_code'].unique()[0]} - {transformed['region_name'].unique()[0]}")
            logger.info(f"Indicators: {len(transformed['indicator_name'].unique())} unique")

            # Show sample
            logger.info("\nSample transformed data (first 10):")
            logger.info(transformed.head(10).to_string())

            # Validation
            logger.info("\n" + "─"*80)
            logger.info("VALIDATION")
            logger.info("─"*80)
            logger.info(f"✅ Total records: {len(transformed):,}")
            logger.info(f"✅ Years range: {transformed['year'].min()} - {transformed['year'].max()}")
            logger.info(f"✅ Region: {transformed['region_code'].unique()[0]} (NRW state)")
            logger.info(f"✅ Value range: {transformed['value'].min():.2f} - {transformed['value'].max():.2f} {transformed['unit'].unique()[0]}")

            # Check for nulls
            null_counts = transformed.isnull().sum()
            if null_counts.sum() > 0:
                logger.warning(f"Null values found:\n{null_counts[null_counts > 0]}")
            else:
                logger.info("✅ No null values")

            return transformed

        except Exception as e:
            logger.error(f"Transformation failed: {e}")
            import traceback
            traceback.print_exc()
            return None

    def create_fact_records(
        self,
        transformed_df: pd.DataFrame,
        geo_mapping: Dict[str, int],
        time_mapping: Dict[int, int]
    ) -> Optional[pd.DataFrame]:
        """
        Convert transformed data to final fact table records.

        Maps region codes to geo_id and years to time_id.

        Args:
            transformed_df: Transformed DataFrame from transform_ict_data()
            geo_mapping: Dict mapping region_code to geo_id
            time_mapping: Dict mapping year to time_id

        Returns:
            DataFrame with fact table structure (geo_id, time_id, indicator_id, value, notes)
        """
        if transformed_df is None or transformed_df.empty:
            logger.warning("Empty DataFrame provided for fact record creation")
            return None

        try:
            logger.info("Creating fact table records...")

            fact_df = transformed_df.copy()

            # Map region codes to geo_id
            fact_df['geo_id'] = fact_df['region_code'].map(geo_mapping)

            # Map years to time_id
            fact_df['time_id'] = fact_df['year'].map(time_mapping)

            # Check for unmapped regions
            unmapped_regions = fact_df[fact_df['geo_id'].isna()]['region_code'].unique()
            if len(unmapped_regions) > 0:
                logger.warning(f"Unmapped regions: {unmapped_regions[:10]}...")
                logger.info(f"Total unmapped regions: {len(unmapped_regions)}")
                fact_df = fact_df[fact_df['geo_id'].notna()]

            # Check for unmapped years
            unmapped_years = fact_df[fact_df['time_id'].isna()]['year'].unique()
            if len(unmapped_years) > 0:
                logger.warning(f"Unmapped years: {unmapped_years}")
                fact_df = fact_df[fact_df['time_id'].notna()]

            # Select final columns for fact table
            fact_df = fact_df[[
                'geo_id',
                'time_id',
                'indicator_id',
                'value',
                'indicator_name',
                'notes',
                'extracted_at'
            ]]

            # Convert IDs to integers
            fact_df['geo_id'] = fact_df['geo_id'].astype(int)
            fact_df['time_id'] = fact_df['time_id'].astype(int)
            fact_df['indicator_id'] = fact_df['indicator_id'].astype(int)

            logger.info(f"✅ Created {len(fact_df):,} fact records")
            logger.info(f"Sample fact records:\n{fact_df.head().to_string()}")

            return fact_df

        except Exception as e:
            logger.error(f"Failed to create fact records: {e}")
            import traceback
            traceback.print_exc()
            return None

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate ICT indicators data for completeness and accuracy.

        Args:
            df: DataFrame to validate

        Returns:
            True if validation passes, False otherwise
        """
        if df is None or df.empty:
            logger.error("Validation failed: Empty DataFrame")
            return False

        logger.info("\n" + "="*80)
        logger.info("DATA VALIDATION")
        logger.info("="*80)

        # Check required columns
        required_cols = ['year', 'region_code', 'indicator_id', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Missing required columns: {missing_cols}")
            return False
        logger.info(f"✅ All required columns present")

        # Check for null values in key columns
        for col in required_cols:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.error(f"Null values in {col}: {null_count}")
                return False
        logger.info(f"✅ No null values in key columns")

        # Check value ranges
        if df['value'].min() < 0:
            logger.warning(f"Negative values found: min = {df['value'].min()}")

        logger.info(f"✅ Value range: {df['value'].min():.2f} to {df['value'].max():.2f}")

        # Check year coverage
        years = sorted(df['year'].unique())
        logger.info(f"✅ Years covered: {years[0]} - {years[-1]} ({len(years)} years)")

        # Check data completeness
        expected_years = list(range(2020, 2026))
        missing_years = [y for y in expected_years if y not in years]
        if missing_years:
            logger.warning(f"Missing years: {missing_years}")
        else:
            logger.info(f"✅ All expected years present (2020-2025)")

        # Check for duplicate records
        duplicates = df.duplicated(subset=['year', 'region_code', 'indicator_id']).sum()
        if duplicates > 0:
            logger.error(f"Duplicate records found: {duplicates}")
            return False
        logger.info(f"✅ No duplicate records")

        logger.info("="*80)
        logger.info("✅ VALIDATION PASSED")
        logger.info("="*80)

        return True

    def get_indicator_metadata(
        self,
        df: pd.DataFrame,
        indicator_id_base: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Extract indicator metadata from transformed data for dim_indicator table.

        Args:
            df: Transformed DataFrame
            indicator_id_base: Base indicator ID used in transformation

        Returns:
            List of dictionaries with indicator metadata
        """
        if df is None or df.empty:
            return []

        try:
            # Get unique indicators
            indicators = df[['indicator_id', 'indicator_name', 'unit']].drop_duplicates()

            metadata = []
            for _, row in indicators.iterrows():
                metadata.append({
                    'indicator_id': int(row['indicator_id']),
                    'indicator_code': f"ICT_{row['indicator_id']}",
                    'indicator_name': row['indicator_name'],
                    'indicator_category': 'ICT',  # REQUIRED field
                    'indicator_subcategory': 'Technology Indicators',
                    'source_system': 'state_db',  # REQUIRED field
                    'source_table_id': '52911-01i',
                    'unit_of_measure': row.get('unit', 'Percent'),
                    'description': f"ICT indicator: {row['indicator_name']}",
                    'update_frequency': 'Annual',
                    'data_type': 'Percentage',
                    'is_active': True,
                    'created_at': datetime.now()
                })

            logger.info(f"Generated metadata for {len(metadata)} indicators")
            return metadata

        except Exception as e:
            logger.error(f"Failed to generate indicator metadata: {e}")
            return []
