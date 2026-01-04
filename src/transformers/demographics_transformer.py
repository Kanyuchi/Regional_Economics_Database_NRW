"""
Demographics Data Transformer
Regional Economics Database for NRW

Transforms raw demographics data into database-ready format.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger


logger = get_logger(__name__)


class DemographicsTransformer:
    """Transforms demographics data for database loading."""

    def __init__(self):
        """Initialize the demographics transformer."""
        self.gender_mapping = {
            'Männlich': 'male',
            'Weiblich': 'female',
            'Insgesamt': 'total',
            'male': 'male',
            'female': 'female',
            'total': 'total'
        }

        self.nationality_mapping = {
            'Deutsche': 'german',
            'Ausländer': 'foreign',
            'Insgesamt': 'total',
            'german': 'german',
            'foreign': 'foreign',
            'total': 'total'
        }

        logger.info("Demographics transformer initialized")

    def transform_population_data(
        self,
        df: pd.DataFrame,
        indicator_id: int = 1,
        years_filter: Optional[list] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform raw population data into database format.
        
        Handles the standard GENESIS table 12411-03-03-4 format with columns:
        date, region_code, region_name, age_group, and 9 population value columns
        for combinations of Total/German/Foreign × Total/Male/Female.

        Args:
            df: Raw data DataFrame from extractor (with proper column names)
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter (e.g., [2020, 2021, 2022, 2023])

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of population data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            logger.info(f"Sample data:\n{df.head(3)}")

            # Create a copy to avoid modifying original
            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                logger.info(f"Parsed years: {transformed['year'].unique()[:5]}...")
            else:
                # Fallback: try first column
                first_col = transformed.columns[0]
                transformed['year'] = pd.to_datetime(transformed[first_col], errors='coerce').dt.year
            
            # Use region_code column directly if available
            if 'region_code' not in transformed.columns:
                # Try to find it
                for col in transformed.columns[:5]:
                    sample = transformed[col].astype(str).str.strip()
                    if sample.str.match(r'^[A-Z0-9]{2,5}$').sum() > len(transformed) * 0.5:
                        transformed['region_code'] = sample
                        logger.info(f"Identified region code from column: {col}")
                        break
            
            # Clean region_code
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to only NRW regions (codes starting with '05')
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Get population value - use total population (all nationalities, all genders)
            # Expected column name from extractor: 'pop_total_total'
            value_col = None
            if 'pop_total_total' in transformed.columns:
                value_col = 'pop_total_total'
            else:
                # Fallback: find columns with population data (numeric columns after first 4)
                numeric_cols = []
                for col in transformed.columns[4:]:  # Skip date, region_code, region_name, age_group
                    try:
                        # Try to convert to numeric
                        test_vals = pd.to_numeric(transformed[col], errors='coerce')
                        if test_vals.notna().sum() > len(transformed) * 0.8:
                            numeric_cols.append(col)
                    except:
                        pass
                
                if numeric_cols:
                    value_col = numeric_cols[0]  # Use first numeric column (typically total)
                    logger.info(f"Using fallback value column: {value_col}")
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Using value column: {value_col}")
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify population value column!")
                return None

            # Keep age_group information if available
            if 'age_group' in transformed.columns:
                transformed['age_group'] = transformed['age_group'].astype(str).str.strip()
                # Normalize 'Total' to 'total' for consistency with other dimensions
                transformed['age_group'] = transformed['age_group'].replace('Total', 'total')
                logger.info("Normalized age_group: 'Total' → 'total' for consistency")
            
            # Set defaults for categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'  # V = Validated

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns for database
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'data_quality_flag', 'extracted_at', 'loaded_at']

            # Keep only columns that exist
            available_cols = [col for col in required_cols if col in transformed.columns]
            final_df = transformed[available_cols].copy()

            # Remove rows with missing critical values
            critical_cols = ['value', 'year', 'region_code']
            final_df = final_df.dropna(subset=critical_cols)
            logger.info(f"After removing nulls: {len(final_df)} rows")

            # Filter by year if requested
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")

            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            # Log summary statistics
            logger.info(f"Year range: {final_df['year'].min()} - {final_df['year'].max()}")
            logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
            logger.info(f"Records per year: {len(final_df) // final_df['year'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming population data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed data before loading.

        Args:
            df: Transformed DataFrame

        Returns:
            True if validation passes, False otherwise
        """
        if df is None or df.empty:
            logger.error("Validation failed: Empty DataFrame")
            return False

        # Check required columns
        required_columns = ['region_code', 'year', 'indicator_id', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logger.error(f"Validation failed: Missing columns {missing_columns}")
            return False

        # Check for null values in critical columns
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")
            # This is a warning, not a failure

        # Check value ranges
        if (df['value'] < 0).any():
            logger.warning("Found negative values in data")

        # Check year range
        current_year = datetime.now().year
        if (df['year'] > current_year).any():
            logger.error("Validation failed: Future years found in data")
            return False

        if (df['year'] < 1990).any():
            logger.warning("Found data before 1990")

        logger.info("Data validation passed")
        return True

    def aggregate_data(
        self,
        df: pd.DataFrame,
        group_by: list
    ) -> pd.DataFrame:
        """
        Aggregate data by specified dimensions.

        Args:
            df: DataFrame to aggregate
            group_by: List of columns to group by

        Returns:
            Aggregated DataFrame
        """
        try:
            aggregated = df.groupby(group_by).agg({
                'value': 'sum'
            }).reset_index()

            logger.info(f"Aggregated data to {len(aggregated)} rows")

            return aggregated

        except Exception as e:
            logger.error(f"Error aggregating data: {e}")
            return df
