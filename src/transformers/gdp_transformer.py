"""
GDP Data Transformer
Regional Economics Database for NRW

Transforms raw GDP and gross value added data into database-ready format.
Handles data from State Database NRW table 82711-01i.
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


class GDPTransformer:
    """Transforms GDP and gross value added data for database loading."""

    def __init__(self):
        """Initialize the GDP transformer."""

        # Sector name standardization mapping
        self.sector_mapping = {
            # German to English
            'Land- und Forstwirtschaft, Fischerei': 'Agriculture, forestry and fishing',
            'Produzierendes Gewerbe ohne Baugewerbe': 'Manufacturing',
            'Baugewerbe': 'Construction',
            'Handel, Verkehr und Gastgewerbe': 'Trade, transport and hospitality',
            'Information und Kommunikation': 'Information and communication',
            'Finanz- und Versicherungsdienstleister': 'Finance and insurance',
            'Grundstücks- und Wohnungswesen': 'Real estate',
            'Unternehmensdienstleister': 'Business services',
            'Öffentliche und sonstige Dienstleister': 'Public and other services',
            # Short forms
            'A': 'Agriculture, forestry and fishing',
            'B-E': 'Manufacturing',
            'F': 'Construction',
            'G-I': 'Trade, transport and hospitality',
            'J': 'Information and communication',
            'K': 'Finance and insurance',
            'L': 'Real estate',
            'M-N': 'Business services',
            'O-U': 'Public and other services'
        }

        logger.info("GDP transformer initialized")

    def transform_gdp_data(
        self,
        df: pd.DataFrame,
        indicator_id_base: int = 28,  # Base indicator ID for GDP
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform GDP data from wide format (sectors as columns) to long format.

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

        try:
            logger.info("="*80)
            logger.info("TRANSFORMING GDP DATA")
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

            # Identify sector columns (all columns after year, region_code, region_name)
            metadata_cols = ['year', 'region_code', 'region_name']
            sector_cols = [col for col in df_clean.columns if col not in metadata_cols]

            logger.info(f"Sector columns identified: {len(sector_cols)}")
            logger.info(f"Sectors: {sector_cols}")

            # Transform from wide to long format
            logger.info("\nTransforming from WIDE to LONG format...")

            records = []

            for idx, row in df_clean.iterrows():
                year = row['year']
                region_code = row['region_code']
                region_name = row['region_name']

                # Create one record per sector
                for sector_idx, sector_col in enumerate(sector_cols):
                    value = row[sector_col]

                    # Clean value (remove thousands separators, convert to numeric)
                    if pd.notna(value) and str(value).strip() != '':
                        # Handle German number format (comma as decimal separator)
                        value_str = str(value).replace(' ', '').replace('\xa0', '')

                        # Convert to float
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
                        except (ValueError, AttributeError):
                            logger.warning(f"Could not convert value '{value}' for {region_code}/{year}/{sector_col}")
                            continue
                    else:
                        # Skip empty/missing values
                        continue

                    # Standardize sector name
                    sector_name = sector_col.strip()
                    sector_standard = self.sector_mapping.get(sector_name, sector_name)

                    # Create record
                    records.append({
                        'year': int(year),
                        'region_code': region_code,
                        'region_name': region_name,
                        'indicator_id': indicator_id_base + sector_idx,
                        'sector': sector_standard,
                        'value': value_clean,
                        'notes': f"GDP/GVA: {sector_standard}"
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
            logger.info(f"Regions: {len(transformed['region_code'].unique())} unique")
            logger.info(f"Sectors: {transformed['sector'].unique().tolist()}")

            # Show sample
            logger.info("\nSample transformed data:")
            logger.info(transformed.head(10).to_string())

            # Validation
            logger.info("\n" + "─"*80)
            logger.info("VALIDATION")
            logger.info("─"*80)
            logger.info(f"✅ Total records: {len(transformed):,}")
            logger.info(f"✅ Years range: {transformed['year'].min()} - {transformed['year'].max()}")
            logger.info(f"✅ Unique regions: {transformed['region_code'].nunique()}")
            logger.info(f"✅ Value range: {transformed['value'].min():.2f} - {transformed['value'].max():.2f} million EUR")

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
            transformed_df: Transformed DataFrame from transform_gdp_data()
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
                fact_df = fact_df[fact_df['geo_id'].notna()]

            # Check for unmapped years
            unmapped_years = fact_df[fact_df['time_id'].isna()]['year'].unique()
            if len(unmapped_years) > 0:
                logger.warning(f"Unmapped years: {unmapped_years}")
                fact_df = fact_df[fact_df['time_id'].notna()]

            # Select final columns
            fact_df = fact_df[['geo_id', 'time_id', 'indicator_id', 'value', 'notes']]

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
        Validate GDP data for completeness and accuracy.

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

        if df['value'].max() > 1_000_000:
            logger.warning(f"Very large values found: max = {df['value'].max()} million EUR")

        logger.info(f"✅ Value range: {df['value'].min():.2f} to {df['value'].max():.2f}")

        # Check year coverage
        years = sorted(df['year'].unique())
        logger.info(f"✅ Years covered: {years[0]} - {years[-1]} ({len(years)} years)")

        logger.info("="*80)
        logger.info("✅ VALIDATION PASSED")
        logger.info("="*80)

        return True
