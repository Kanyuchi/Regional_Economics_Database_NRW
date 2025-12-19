"""
Business/Enterprise Data Transformer
Transforms business establishment data for database loading
"""

import pandas as pd
from typing import Optional, List
from datetime import datetime

from utils.logging import get_logger

logger = get_logger(__name__)


class BusinessTransformer:
    """Transform business and establishment data"""
    
    def __init__(self):
        """Initialize transformer"""
        logger.info("Business transformer initialized")
    
    def transform_branches_by_size(
        self,
        df: pd.DataFrame,
        indicator_id: int = 22,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform branches by size class data into database format.
        
        This transforms wide-format data (size classes as columns) into
        long format (one row per size class per region per year).
        
        Table: 52111-01-02-4
        
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
            logger.info(f"Transforming {len(df)} rows of branches data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            # Parse year
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            years_found = sorted(df['year'].dropna().unique())
            logger.info(f"Years in data: {years_found}")
            
            # Clean region codes
            df['region_code'] = df['region_code'].astype(str).str.strip()

            # Filter to NRW regions (05...)
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(df)
            nrw_mask = (
                df['region_code'].str.startswith('05', na=False) |
                df['region_code'].isin(nrw_filter_codes)
            )
            df = df[nrw_mask].copy()
            filtered_count = initial_count - len(df)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(df)} NRW rows")
            
            # Transform from wide to long format
            # Each size class becomes a separate row
            size_classes = {
                'total': 'Total - all size classes',
                'size_0_10': '0 to <10 employees',
                'size_10_50': '10 to <50 employees',
                'size_50_250': '50 to <250 employees',
                'size_250_plus': '250+ employees'
            }
            
            # Melt the dataframe
            value_vars = list(size_classes.keys())
            id_vars = ['region_code', 'region_name', 'year', 'extracted_at', 'source_table', 'data_source']
            
            df_long = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='size_class',
                value_name='value'
            )
            
            logger.info(f"Melted to {len(df_long)} rows (long format)")
            
            # Convert value to numeric
            df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
            
            # Create notes field with size class information
            df_long['notes'] = df_long['size_class'].map(size_classes)
            
            # Set categorical fields
            df_long['gender'] = 'total'
            df_long['nationality'] = 'total'
            df_long['age_group'] = None
            
            # Add indicator_id
            df_long['indicator_id'] = indicator_id
            
            # Add data quality flag
            df_long['data_quality_flag'] = 'V'
            
            # Add loaded timestamp
            df_long['loaded_at'] = datetime.now()
            
            # Select required columns
            required_cols = [
                'region_code', 'year', 'indicator_id', 'value', 
                'gender', 'nationality', 'age_group', 'notes',
                'data_quality_flag', 'extracted_at', 'loaded_at'
            ]
            
            final_df = df_long[required_cols].copy()
            
            # Remove rows with missing critical values
            critical_cols = ['value', 'year', 'region_code']
            before_count = len(final_df)
            final_df = final_df.dropna(subset=critical_cols)
            removed = before_count - len(final_df)
            if removed > 0:
                logger.info(f"Removed {removed} rows with NULL critical values")
            
            # Filter by year if requested
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")
            
            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
                logger.info(f"Size classes per region per year: {len(size_classes)}")
                
                # Sample notes
                sample_notes = final_df['notes'].unique()[:3]
                logger.info(f"Sample notes: {list(sample_notes)}")
            
            return final_df

        except Exception as e:
            logger.error(f"Error transforming branches data: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
            logger.error("Validation failed: DataFrame is empty")
            return False
        
        required_cols = ['region_code', 'year', 'indicator_id', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        if missing_cols:
            logger.error(f"Validation failed: Missing columns: {missing_cols}")
            return False
        
        # Check for NULL values in critical columns
        null_counts = df[required_cols].isnull().sum()
        if null_counts.any():
            logger.error(f"Validation failed: NULL values found: {null_counts[null_counts > 0].to_dict()}")
            return False
        
        logger.info("Data validation passed")
        return True
    
    def transform_branches_by_sector(
        self,
        df: pd.DataFrame,
        indicator_id: int = 23,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform branches by economic sector data into database format.
        
        This transforms wide-format data (sectors as columns) into
        long format (one row per sector per region per year).
        
        Table: 52111-02-01-4
        WZ 2008 classification: 18 economic sectors
        
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
            logger.info(f"Transforming {len(df)} rows of branches by sector data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")

            # Parse year
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            years_found = sorted(df['year'].dropna().unique())
            logger.info(f"Years in data: {years_found}")
            
            # Clean region codes
            df['region_code'] = df['region_code'].astype(str).str.strip()

            # Filter to NRW regions (05...)
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(df)
            nrw_mask = (
                df['region_code'].str.startswith('05', na=False) |
                df['region_code'].isin(nrw_filter_codes)
            )
            df = df[nrw_mask].copy()
            filtered_count = initial_count - len(df)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(df)} NRW rows")
            
            # Define sector mappings (WZ 2008)
            sectors = {
                'total': 'Total - all sectors (B-N, P-S)',
                'sector_B': 'B - Mining and quarrying',
                'sector_C': 'C - Manufacturing',
                'sector_D': 'D - Energy supply',
                'sector_E': 'E - Water supply',
                'sector_F': 'F - Construction',
                'sector_G': 'G - Trade, vehicle maintenance',
                'sector_H': 'H - Transport and storage',
                'sector_I': 'I - Accommodation and food services',
                'sector_J': 'J - Information and communication',
                'sector_K': 'K - Financial and insurance services',
                'sector_L': 'L - Real estate',
                'sector_M': 'M - Professional, scientific services',
                'sector_N': 'N - Other business services',
                'sector_P': 'P - Education',
                'sector_Q': 'Q - Health and social work',
                'sector_R': 'R - Arts, entertainment, recreation',
                'sector_S': 'S - Other services',
            }
            
            # Melt the dataframe from wide to long format
            value_vars = list(sectors.keys())
            id_vars = ['region_code', 'region_name', 'year', 'extracted_at', 'source_table', 'data_source']
            
            df_long = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='sector',
                value_name='value'
            )
            
            logger.info(f"Melted to {len(df_long)} rows (long format)")
            
            # Convert value to numeric
            df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
            
            # Create notes field with sector information
            df_long['notes'] = df_long['sector'].map(sectors)
            
            # Set categorical fields
            df_long['gender'] = 'total'
            df_long['nationality'] = 'total'
            df_long['age_group'] = None
            
            # Add indicator_id
            df_long['indicator_id'] = indicator_id
            
            # Add data quality flag
            df_long['data_quality_flag'] = 'V'
            
            # Add loaded timestamp
            df_long['loaded_at'] = datetime.now()
            
            # Select required columns
            required_cols = [
                'region_code', 'year', 'indicator_id', 'value', 
                'gender', 'nationality', 'age_group', 'notes',
                'data_quality_flag', 'extracted_at', 'loaded_at'
            ]
            
            final_df = df_long[required_cols].copy()
            
            # Remove rows with missing critical values
            critical_cols = ['value', 'year', 'region_code']
            before_count = len(final_df)
            final_df = final_df.dropna(subset=critical_cols)
            removed = before_count - len(final_df)
            if removed > 0:
                logger.info(f"Removed {removed} rows with NULL critical values")
            
            # Filter by year if requested
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")
            
            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
                logger.info(f"Sectors per region per year: {len(sectors)}")
                
                # Sample notes
                sample_notes = final_df['notes'].unique()[:3]
                logger.info(f"Sample notes: {list(sample_notes)}")
            
            return final_df

        except Exception as e:
            logger.error(f"Error transforming branches by sector data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def transform_business_registrations(
        self,
        df: pd.DataFrame,
        indicator_id_registrations: int = 24,
        indicator_id_deregistrations: int = 25,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform business registrations/deregistrations data into database format.
        
        This transforms wide-format data (10 category columns) into
        long format (one row per category per region per year).
        Creates separate indicators for registrations and deregistrations.
        
        Table: 52311-01-04-4
        Available period: 1998-2024 (27 years)
        
        Args:
            df: Raw data DataFrame from extractor
            indicator_id_registrations: ID for registrations indicator (default: 24)
            indicator_id_deregistrations: ID for deregistrations indicator (default: 25)
            years_filter: Optional list of years to filter
        
        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None
        
        try:
            logger.info(f"Transforming {len(df)} rows of business registrations data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            
            # Parse year
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            years_found = sorted(df['year'].dropna().unique())
            logger.info(f"Years in data: {years_found}")
            
            # Clean region codes
            df['region_code'] = df['region_code'].astype(str).str.strip()
            
            # Filter to NRW regions (05...)
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(df)
            nrw_mask = (
                df['region_code'].str.startswith('05', na=False) |
                df['region_code'].isin(nrw_filter_codes)
            )
            df = df[nrw_mask].copy()
            filtered_count = initial_count - len(df)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(df)} NRW rows")
            
            # Define category mappings
            registration_categories = {
                'reg_total': 'Total registrations',
                'reg_new_establishments': 'New establishments',
                'reg_business_foundations': 'Business foundations',
                'reg_relocations_in': 'Relocations into region',
                'reg_takeovers': 'Business takeovers'
            }
            
            deregistration_categories = {
                'dereg_total': 'Total deregistrations',
                'dereg_closures': 'Business closures',
                'dereg_business_closures': 'Business closures (establishments)',
                'dereg_relocations_out': 'Relocations out of region',
                'dereg_handovers': 'Business handovers'
            }
            
            # Process registrations
            id_vars = ['region_code', 'region_name', 'year', 'extracted_at', 'source_table', 'data_source']
            
            df_reg = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=list(registration_categories.keys()),
                var_name='category',
                value_name='value'
            )
            df_reg['notes'] = df_reg['category'].map(registration_categories)
            df_reg['indicator_id'] = indicator_id_registrations
            
            logger.info(f"Registrations melted to {len(df_reg)} rows")
            
            # Process deregistrations
            df_dereg = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=list(deregistration_categories.keys()),
                var_name='category',
                value_name='value'
            )
            df_dereg['notes'] = df_dereg['category'].map(deregistration_categories)
            df_dereg['indicator_id'] = indicator_id_deregistrations
            
            logger.info(f"Deregistrations melted to {len(df_dereg)} rows")
            
            # Combine both
            df_long = pd.concat([df_reg, df_dereg], ignore_index=True)
            logger.info(f"Combined to {len(df_long)} rows total")
            
            # Convert value to numeric
            df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
            
            # Set categorical fields
            df_long['gender'] = 'total'
            df_long['nationality'] = 'total'
            df_long['age_group'] = None
            
            # Add data quality flag
            df_long['data_quality_flag'] = 'V'
            
            # Add loaded timestamp
            df_long['loaded_at'] = datetime.now()
            
            # Select required columns
            required_cols = [
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'notes',
                'data_quality_flag', 'extracted_at', 'loaded_at'
            ]
            
            final_df = df_long[required_cols].copy()
            
            # Remove rows with missing critical values
            critical_cols = ['value', 'year', 'region_code']
            before_count = len(final_df)
            final_df = final_df.dropna(subset=critical_cols)
            removed = before_count - len(final_df)
            if removed > 0:
                logger.info(f"Removed {removed} rows with NULL critical values")
            
            # Filter by year if requested
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")
            
            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
                logger.info(f"Categories per region per year: {len(registration_categories) + len(deregistration_categories)}")
                
                # Sample notes
                sample_notes = final_df['notes'].unique()[:5]
                logger.info(f"Sample notes: {list(sample_notes)}")
            
            return final_df
        
        except Exception as e:
            logger.error(f"Error transforming business registrations data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def transform_employee_compensation(
        self,
        df: pd.DataFrame,
        indicator_id: int = 26,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform employee compensation data into database format.
        
        This transforms wide-format data (8 sectors as columns) into
        long format (one row per sector per region per year).
        
        Table: 82000-04-01-4
        Available period: 2000-2022 (23 years)
        Unit: Thousand EUR
        
        Args:
            df: Raw data DataFrame from extractor
            indicator_id: ID of the indicator in dim_indicator table (default: 26)
            years_filter: Optional list of years to filter
        
        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None
        
        try:
            logger.info(f"Transforming {len(df)} rows of employee compensation data")
            logger.info(f"Columns in raw data: {df.columns.tolist()}")
            
            # Parse year
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            years_found = sorted(df['year'].dropna().unique())
            logger.info(f"Years in data: {years_found}")
            
            # Clean region codes
            df['region_code'] = df['region_code'].astype(str).str.strip()
            
            # Filter to NRW regions (05...)
            nrw_filter_codes = ['00005', '05', '5', 'DG']
            initial_count = len(df)
            nrw_mask = (
                df['region_code'].str.startswith('05', na=False) |
                df['region_code'].isin(nrw_filter_codes)
            )
            df = df[nrw_mask].copy()
            filtered_count = initial_count - len(df)
            logger.info(f"Filtered out {filtered_count} non-NRW regions, kept {len(df)} NRW rows")
            
            # Define sector mappings
            sectors = {
                'total': 'Total - all sectors',
                'agriculture_forestry_fishing': 'A - Agriculture, forestry, fishing',
                'manufacturing_total': 'B-E - Manufacturing (excluding construction)',
                'manufacturing_processing': 'C - Manufacturing (processing industry)',
                'construction': 'F - Construction',
                'trade_transport_hospitality_it': 'G-J - Trade, transport, hospitality, IT',
                'finance_insurance_realestate': 'K-L - Finance, insurance, real estate',
                'public_services_education_health': 'O-T - Public services, education, health',
            }
            
            # Melt the dataframe from wide to long format
            value_vars = list(sectors.keys())
            id_vars = ['region_code', 'region_name', 'year', 'extracted_at', 'source_table', 'data_source']
            
            df_long = pd.melt(
                df,
                id_vars=id_vars,
                value_vars=value_vars,
                var_name='sector',
                value_name='value'
            )
            
            logger.info(f"Melted to {len(df_long)} rows (long format)")
            
            # Convert value to numeric
            df_long['value'] = pd.to_numeric(df_long['value'], errors='coerce')
            
            # Create notes field with sector information
            df_long['notes'] = df_long['sector'].map(sectors)
            
            # Set categorical fields
            df_long['gender'] = 'total'
            df_long['nationality'] = 'total'
            df_long['age_group'] = None
            
            # Add indicator_id
            df_long['indicator_id'] = indicator_id
            
            # Add data quality flag
            df_long['data_quality_flag'] = 'V'
            
            # Add loaded timestamp
            df_long['loaded_at'] = datetime.now()
            
            # Select required columns
            required_cols = [
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'notes',
                'data_quality_flag', 'extracted_at', 'loaded_at'
            ]
            
            final_df = df_long[required_cols].copy()
            
            # Remove rows with missing critical values
            critical_cols = ['value', 'year', 'region_code']
            before_count = len(final_df)
            final_df = final_df.dropna(subset=critical_cols)
            removed = before_count - len(final_df)
            if removed > 0:
                logger.info(f"Removed {removed} rows with NULL critical values")
            
            # Filter by year if requested
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")
            
            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
                logger.info(f"Sectors per region per year: {len(sectors)}")
                
                # Sample notes
                sample_notes = final_df['notes'].unique()[:5]
                logger.info(f"Sample notes: {list(sample_notes)}")
            
            return final_df
        
        except Exception as e:
            logger.error(f"Error transforming employee compensation data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def transform_corporate_insolvencies(
        self,
        raw_data: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        """
        Transform corporate insolvencies data to database format.
        
        Input columns:
        - year, region_code, region_name
        - total_applications
        - opened_proceedings
        - rejected_proceedings
        - companies_count
        - expected_claims_teur
        
        Output:
        - Indicator 27: Total applications (main indicator)
        - notes: Will store type (total/opened/rejected)
        
        Args:
            raw_data: Raw DataFrame from extractor

        Returns:
            Transformed DataFrame in long format
        """
        try:
            logger.info(f"Transforming corporate insolvencies data: {len(raw_data)} rows")
            
            # Filter for NRW regions only (code starts with '05')
            nrw_data = raw_data[raw_data['region_code'].str.startswith('05', na=False)].copy()
            logger.info(f"Filtered to NRW regions: {len(nrw_data)} rows")
            
            if nrw_data.empty:
                logger.error("No NRW regions found in data")
                return None
            
            # We'll create separate indicators for each type
            # Indicator 27: Total applications
            
            # Melt data - only the total_applications column
            value_cols = ['total_applications']
            
            melted = pd.melt(
                nrw_data,
                id_vars=['year', 'region_code', 'region_name'],
                value_vars=value_cols,
                var_name='insolvency_type',
                value_name='value'
            )
            
            logger.info(f"Melted data shape: {melted.shape}")
            
            # Assign indicator_id (27 for total applications)
            melted['indicator_id'] = 27
            
            # Map insolvency type to notes
            notes_map = {
                'total_applications': 'Corporate insolvency applications (total)'
            }
            
            melted['notes'] = melted['insolvency_type'].map(notes_map)
            
            # Convert value to numeric, handling '-' and '.' as NaN
            melted['value'] = pd.to_numeric(
                melted['value'].astype(str).str.replace('-', '').str.replace('.', '').str.strip(),
                errors='coerce'
            )
            
            # Convert year to integer
            melted['year'] = pd.to_numeric(melted['year'], errors='coerce').astype('Int64')
            
            # Clean region codes
            melted['region_code'] = melted['region_code'].astype(str).str.strip()
            
            # Create final dataframe
            final_df = melted[[
                'indicator_id',
                'year',
                'region_code',
                'region_name',
                'value',
                'notes'
            ]].copy()
            
            # Remove rows with NULL critical values
            critical_cols = ['value', 'year', 'region_code']
            before_count = len(final_df)
            final_df = final_df.dropna(subset=critical_cols)
            removed = before_count - len(final_df)
            if removed > 0:
                logger.info(f"Removed {removed} rows with NULL critical values")
            
            # Get years filter from raw_data attributes
            years_filter = raw_data.attrs.get('years_filter')
            if years_filter:
                logger.info(f"Applying year filter: {years_filter}")
                final_df = final_df[final_df['year'].isin(years_filter)]
                logger.info(f"After year filtering: {len(final_df)} rows")
            
            logger.info(f"Transformed {len(final_df)} rows successfully")
            
            if len(final_df) > 0:
                logger.info(f"Year range: {int(final_df['year'].min())} - {int(final_df['year'].max())}")
                logger.info(f"Unique regions: {final_df['region_code'].nunique()}")
                logger.info(f"Value range: {final_df['value'].min():.0f} - {final_df['value'].max():.0f}")
            
            return final_df
        
        except Exception as e:
            logger.error(f"Error transforming corporate insolvencies data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None