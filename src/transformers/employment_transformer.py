"""
Employment Data Transformer
Regional Economics Database for NRW

Transforms raw employment data into database-ready format.
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


class EmploymentTransformer:
    """Transforms employment data for database loading."""

    def __init__(self):
        """Initialize the employment transformer."""
        self.gender_mapping = {
            'Männlich': 'male',
            'Weiblich': 'female',
            'Insgesamt': 'total',
            'männlich': 'male',
            'weiblich': 'female',
            'male': 'male',
            'female': 'female',
            'total': 'total'
        }

        self.nationality_mapping = {
            'Deutsche': 'german',
            'Ausländer': 'foreign',
            'Ausländer/-innen': 'foreign',
            'Insgesamt': 'total',
            'german': 'german',
            'foreign': 'foreign',
            'total': 'total'
        }

        logger.info("Employment transformer initialized")

    def transform_workplace_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 2,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment at workplace data into database format.
        
        Table: 13111-01-03-4 - Employees by gender and nationality
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of employment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            logger.info(f"Sample data:\n{df.head(3)}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                logger.info(f"Parsed years: {sorted(transformed['year'].dropna().unique())[:5]}...")
            else:
                first_col = transformed.columns[0]
                transformed['year'] = pd.to_datetime(transformed[first_col], errors='coerce').dt.year
            
            # Use region_code column directly if available
            if 'region_code' not in transformed.columns:
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
            
            # Get employment value - use total employees (all nationalities, all genders)
            # Expected column name from extractor: 'emp_total_total'
            value_col = None
            if 'emp_total_total' in transformed.columns:
                value_col = 'emp_total_total'
            else:
                # Fallback: find first numeric column after base columns
                for col in transformed.columns[3:]:
                    try:
                        test_vals = pd.to_numeric(transformed[col], errors='coerce')
                        if test_vals.notna().sum() > len(transformed) * 0.8:
                            value_col = col
                            break
                    except:
                        pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Using value column: {value_col}")
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            
            # Employment data doesn't have age groups, but we keep the field for consistency
            transformed['age_group'] = None

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns for database
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming employment data: {e}")
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

        required_columns = ['region_code', 'year', 'indicator_id', 'value']
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            logger.error(f"Validation failed: Missing columns {missing_columns}")
            return False

        # Check for null values
        null_counts = df[required_columns].isnull().sum()
        if null_counts.any():
            logger.warning(f"Found null values: {null_counts[null_counts > 0].to_dict()}")

        # Check value ranges - employment should be positive
        if (df['value'] < 0).any():
            logger.warning("Found negative values in employment data")

        # Check year range
        current_year = datetime.now().year
        if (df['year'] > current_year).any():
            logger.error("Validation failed: Future years found in data")
            return False

        logger.info("Data validation passed")
        return True

    def transform_sector_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 9,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment by sector data into database format.
        
        Table: 13111-07-05-4 - Employees by economic sectors
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of sector employment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            logger.info(f"Sample data:\n{df.head(3)}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                years_found = sorted(transformed['year'].dropna().unique())
                logger.info(f"Parsed years from date column: {years_found}")
            else:
                first_col = transformed.columns[0]
                transformed['year'] = pd.to_datetime(transformed[first_col], errors='coerce').dt.year
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # sector column should already be present from extractor
            if 'sector' not in transformed.columns:
                logger.warning("sector column not found, sector info will not be available")

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use the emp_total_total column as value (total employment across all groups)
            value_col = None
            if 'emp_total_total' in transformed.columns:
                value_col = 'emp_total_total'
                logger.info(f"Using employment total column: {value_col}")
            else:
                # Fallback: Find first numeric column
                for col in transformed.columns:
                    if col.startswith('emp_'):
                        try:
                            test_vals = pd.to_numeric(transformed[col], errors='coerce')
                            if test_vals.notna().sum() > len(transformed) * 0.8:
                                value_col = col
                                logger.info(f"Using fallback value column: {value_col}")
                                break
                        except:
                            pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store sector in notes field if available
            if 'sector' in transformed.columns:
                transformed['notes'] = 'Sector: ' + transformed['sector'].astype(str)
            else:
                transformed['notes'] = None

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming sector employment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_scope_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 3,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment by scope (full-time/part-time) data into database format.
        
        Table: 13111-03-02-4 - Employees by employment scope
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of scope employment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                years_found = sorted(transformed['year'].dropna().unique())
                logger.info(f"Parsed years from date column: {years_found}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use the emp_total_total column as value (total employment)
            value_col = None
            if 'emp_total_total' in transformed.columns:
                value_col = 'emp_total_total'
                logger.info(f"Using employment total column: {value_col}")
            else:
                # Fallback: Find first numeric column
                for col in transformed.columns:
                    if col.startswith('emp_'):
                        try:
                            test_vals = pd.to_numeric(transformed[col], errors='coerce')
                            if test_vals.notna().sum() > len(transformed) * 0.8:
                                value_col = col
                                logger.info(f"Using fallback value column: {value_col}")
                                break
                        except:
                            pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store employment scope in notes field if available
            if 'employment_scope' in transformed.columns:
                transformed['notes'] = 'Scope: ' + transformed['employment_scope'].astype(str)
                logger.info(f"Added employment scope to notes: {transformed['employment_scope'].unique()}")
            else:
                transformed['notes'] = None

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming scope employment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_qualification_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 4,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment by vocational qualification data into database format.
        
        Table: 13111-11-04-4 - Employees by qualification type
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of qualification employment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                years_found = sorted(transformed['year'].dropna().unique())
                logger.info(f"Parsed years from date column: {years_found}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use the emp_total_total column as value
            value_col = None
            if 'emp_total_total' in transformed.columns:
                value_col = 'emp_total_total'
                logger.info(f"Using employment total column: {value_col}")
            else:
                # Fallback: Find first numeric column
                for col in transformed.columns:
                    if col.startswith('emp_'):
                        try:
                            test_vals = pd.to_numeric(transformed[col], errors='coerce')
                            if test_vals.notna().sum() > len(transformed) * 0.8:
                                value_col = col
                                logger.info(f"Using fallback value column: {value_col}")
                                break
                        except:
                            pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store qualification type in notes field if available
            if 'qualification_type' in transformed.columns:
                transformed['notes'] = 'Qualification: ' + transformed['qualification_type'].astype(str)
                logger.info(f"Added qualification type to notes: {transformed['qualification_type'].unique()[:3]}")
            else:
                transformed['notes'] = None

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming qualification employment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_residence_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 5,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment at residence data into database format.
        
        Table: 13111-02-02-4 - Employees by gender/nationality at residence
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of residence employment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                years_found = sorted(transformed['year'].dropna().unique())
                logger.info(f"Parsed years from date column: {years_found}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use the emp_total_total column as value
            value_col = None
            if 'emp_total_total' in transformed.columns:
                value_col = 'emp_total_total'
                logger.info(f"Using employment total column: {value_col}")
            else:
                logger.error("Could not find emp_total_total column!")
                return None
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Mark as residence data in notes
            transformed['notes'] = 'Location: Residence (Wohnort)'

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming residence employment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_residence_scope_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 6,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment at residence by scope data into database format.
        
        Table: 13111-04-02-4 - Employees at residence by scope
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        # Use the same logic as workplace scope, just different indicator_id
        return self.transform_scope_employment(df, indicator_id, years_filter)

    def transform_residence_qualification_employment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 7,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employment at residence by qualification data into database format.
        
        Table: 13111-12-03-4 - Employees at residence by qualification
        Reference date: June 30

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        # Use the same logic as workplace qualification, just different indicator_id
        return self.transform_qualification_employment(df, indicator_id, years_filter)

    def transform_unemployment(
        self,
        df: pd.DataFrame,
        indicator_id: int = 18,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform unemployment data into database format.
        
        Table: 13211-02-05-4 - Unemployed persons and unemployment rates
        Reference: Annual average

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of unemployment data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            logger.info(f"Sample data:\n{df.head(3)}")

            transformed = df.copy()
            
            # Parse year from date column
            # Note: For unemployment table, date is just the year (e.g., "2023")
            if 'date' in transformed.columns:
                # Try parsing as year directly first
                try:
                    transformed['year'] = pd.to_numeric(transformed['date'], errors='coerce').astype('Int64')
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years directly from date column: {years_found}")
                except:
                    # Fallback to datetime parsing
                    transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years via datetime from date column: {years_found}")
            else:
                # Try first column
                first_col = transformed.columns[0]
                transformed['year'] = pd.to_numeric(transformed[first_col], errors='coerce').astype('Int64')
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use unemployed_total as the primary value
            value_col = None
            if 'unemployed_total' in transformed.columns:
                value_col = 'unemployed_total'
                logger.info(f"Using unemployed_total column: {value_col}")
            else:
                # Fallback: Find first numeric column after base columns
                for col in transformed.columns[4:]:
                    try:
                        test_vals = pd.to_numeric(transformed[col], errors='coerce')
                        if test_vals.notna().sum() > len(transformed) * 0.5:
                            value_col = col
                            logger.info(f"Using fallback value column: {value_col}")
                            break
                    except:
                        pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify unemployment value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store category in notes field if available
            if 'category' in transformed.columns:
                transformed['notes'] = 'Category: ' + transformed['category'].astype(str)
                unique_cats = transformed['category'].unique()[:5]
                logger.info(f"Categories found: {list(unique_cats)}")
            else:
                transformed['notes'] = 'Unemployment (annual average)'

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming unemployment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_employed_by_sector(
        self,
        df: pd.DataFrame,
        indicator_id: int = 19,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employed persons by sector data into database format.
        
        Table: 13312-01-05-4 - Employed persons by economic sectors
        Reference: Annual average

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of employed by sector data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column
            if 'date' in transformed.columns:
                # Try parsing as year directly first
                try:
                    transformed['year'] = pd.to_numeric(transformed['date'], errors='coerce').astype('Int64')
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years directly from date column: {years_found}")
                except:
                    # Fallback to datetime parsing
                    transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years via datetime: {years_found}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use employed_total as the primary value
            value_col = None
            if 'employed_total' in transformed.columns:
                value_col = 'employed_total'
                logger.info(f"Using employed_total column: {value_col}")
            else:
                # Fallback: Find first numeric column after base columns
                for col in transformed.columns[4:]:
                    try:
                        test_vals = pd.to_numeric(transformed[col], errors='coerce')
                        if test_vals.notna().sum() > len(transformed) * 0.5:
                            value_col = col
                            logger.info(f"Using fallback value column: {value_col}")
                            break
                    except:
                        pass
            
            if value_col:
                transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
                logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")
            else:
                logger.error("Could not identify employed value column!")
                return None

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # This table uses WIDE format (sectors in columns), not long format
            # We're using the "Total" column which aggregates all sectors
            transformed['notes'] = 'Employed persons - all sectors (annual average)'

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming employed by sector data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_unemployment(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract unemployment data and rates.
        
        Table: 13211-02-05-4

        Args:
            regions: List of region codes
            years: List of years to extract

        Returns:
            DataFrame with unemployment data or None
        """
        table_id = self.EMPLOYMENT_TABLES['unemployed_rates']
        logger.info(f"Extracting unemployment data for table {table_id}")

        raw_data = self.get_table_data(table_id, format='datencsv', area='free')

        if raw_data is None:
            logger.error("Failed to download unemployment data")
            return None

        try:
            skip_rows = self._detect_data_start_row(raw_data)
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            logger.info(f"Parsed {len(df)} rows of unemployment data")

            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            if years:
                df.attrs['years_filter'] = years

            return df

        except Exception as e:
            logger.error(f"Error parsing unemployment data: {e}")
            return None

    def transform_construction_industry(
        self,
        df: pd.DataFrame,
        indicator_id: int = 20,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform construction industry data into database format.
        
        Table: 44231-01-03-4 - Construction industry statistics
        Reference: 30.06

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of construction industry data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column (format: 2024-06-30 or 30.06.2024)
            if 'date' in transformed.columns:
                try:
                    # Try parsing as YYYY-MM-DD first
                    transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years from date: {years_found}")
                except:
                    # Fallback to year extraction
                    transformed['year'] = transformed['date'].astype(str).str[:4].astype(int)
                    logger.info(f"Parsed years from string: {transformed['year'].unique()}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use employees as the primary value
            value_col = None
            if 'employees' in transformed.columns:
                value_col = 'employees'
                logger.info(f"Using employees column as value")
            else:
                logger.error("Could not find employees column!")
                return None
            
            transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
            logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store additional data in notes field
            def create_notes(row):
                try:
                    businesses = int(row.get('businesses', 0)) if pd.notna(row.get('businesses')) else None
                    turnover = row.get('turnover', None)
                    turnover_str = f"{turnover:,.0f}" if pd.notna(turnover) else 'N/A'
                    parts = [f"Construction industry (ref: 30.06)"]
                    if businesses:
                        parts.append(f"Businesses: {businesses}")
                    if turnover_str != 'N/A':
                        parts.append(f"Turnover: {turnover_str} Tsd. EUR")
                    return '; '.join(parts)
                except:
                    return 'Construction industry (ref: 30.06)'
            
            transformed['notes'] = transformed.apply(create_notes, axis=1)
            sample_notes = transformed['notes'].head(3).tolist()
            logger.info(f"Sample notes: {sample_notes}")

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming construction industry data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_total_turnover(
        self,
        df: pd.DataFrame,
        indicator_id: int = 21,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform total turnover data into database format.
        
        Table: 44231-01-02-4 - Total turnover statistics (all businesses)
        Reference: 30.06

        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        try:
            logger.info(f"Transforming {len(df)} rows of total turnover data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            transformed = df.copy()
            
            # Parse year from date column (format: 2024-06-30 or 30.06.2024)
            if 'date' in transformed.columns:
                try:
                    # Try parsing as YYYY-MM-DD first
                    transformed['year'] = pd.to_datetime(transformed['date'], errors='coerce').dt.year
                    years_found = sorted(transformed['year'].dropna().unique())
                    logger.info(f"Parsed years from date: {years_found}")
                except:
                    # Fallback to year extraction
                    transformed['year'] = transformed['date'].astype(str).str[:4].astype(int)
                    logger.info(f"Parsed years from string: {transformed['year'].unique()}")
            else:
                logger.error("No date column found!")
                return None
            
            # region_code should already be present from extractor
            if 'region_code' not in transformed.columns:
                logger.error("region_code column not found in data!")
                return None
            
            transformed['region_code'] = transformed['region_code'].astype(str).str.strip()

            # Filter to NRW regions
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(transformed)
            nrw_mask = (
                transformed['region_code'].str.startswith('05', na=False) |
                transformed['region_code'].isin(nrw_filter_codes)
            )
            transformed = transformed[nrw_mask].copy()
            filtered_count = initial_count - len(transformed)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(transformed)} NRW rows")
            
            # Use employees as the primary value
            value_col = None
            if 'employees' in transformed.columns:
                value_col = 'employees'
                logger.info(f"Using employees column as value")
            else:
                logger.error("Could not find employees column!")
                return None
            
            transformed['value'] = pd.to_numeric(transformed[value_col], errors='coerce')
            logger.info(f"Value stats: min={transformed['value'].min()}, max={transformed['value'].max()}, mean={transformed['value'].mean():.0f}")

            # Set categorical fields
            transformed['gender'] = 'total'
            transformed['nationality'] = 'total'
            transformed['age_group'] = None
            
            # Store additional data in notes field
            def create_notes(row):
                try:
                    businesses = int(row.get('businesses', 0)) if pd.notna(row.get('businesses')) else None
                    turnover = row.get('turnover', None)
                    # Turnover often shows as "-" in the data
                    turnover_str = f"{turnover:,.0f}" if pd.notna(turnover) and turnover != '-' else 'N/A'
                    parts = [f"Total turnover - all businesses (ref: 30.06)"]
                    if businesses:
                        parts.append(f"Businesses: {businesses}")
                    if turnover_str != 'N/A':
                        parts.append(f"Turnover: {turnover_str} Tsd. EUR")
                    return '; '.join(parts)
                except:
                    return 'Total turnover - all businesses (ref: 30.06)'
            
            transformed['notes'] = transformed.apply(create_notes, axis=1)
            sample_notes = transformed['notes'].head(3).tolist()
            logger.info(f"Sample notes: {sample_notes}")

            # Add indicator_id
            transformed['indicator_id'] = indicator_id

            # Add data quality flag
            transformed['data_quality_flag'] = 'V'

            # Add timestamps
            if 'extracted_at' not in transformed.columns:
                transformed['extracted_at'] = datetime.now()
            transformed['loaded_at'] = datetime.now()

            # Select required columns
            required_cols = ['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                           'age_group', 'notes', 'data_quality_flag', 'extracted_at', 'loaded_at']

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
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")

            return final_df

        except Exception as e:
            logger.error(f"Error transforming total turnover data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

