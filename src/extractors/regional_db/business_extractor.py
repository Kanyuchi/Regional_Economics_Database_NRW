"""
Business/Enterprise Data Extractor for Regional Database
Handles establishment statistics, business registers, etc.
"""

import pandas as pd
from typing import Optional, List
from datetime import datetime
import io

from .base_extractor import RegionalDBExtractor
from utils.logging import get_logger

logger = get_logger(__name__)


class BusinessExtractor(RegionalDBExtractor):
    """Extract business and establishment data from Regional Database"""
    
    # Table mappings
    BUSINESS_TABLES = {
        'branches_by_size': '52111-01-02-4',
        'branches_by_sector': '52111-02-01-4',
        'business_registrations': '52311-01-04-4',
        'corporate_insolvencies': '52411-02-01-4',
        'employee_compensation': '82000-04-01-4',
    }
    
    def extract_branches_by_size(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract branches by employee size class data.
        
        Table: 52111-01-02-4
        Branches categorized by employee size classes
        Available period: 2019-2023 (5 years)
        
        Args:
            regions: List of region codes
            years: List of years to extract (default: 2019-2023)

        Returns:
            DataFrame with branches data or None
        """
        table_id = self.BUSINESS_TABLES['branches_by_size']
        logger.info(f"Extracting branches by size data for table {table_id}")

        # Default to years 2019-2023 (full available period)
        if years is None:
            years = list(range(2019, 2024))  # 2019-2023 = 5 years
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"No data for year {year}")
                continue

            try:
                df_year = self._parse_branches_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
                    logger.info(f"Successfully extracted {len(df_year)} rows for year {year}")
                else:
                    logger.warning(f"No data extracted for year {year}")
                    
            except Exception as e:
                logger.error(f"Error parsing data for year {year}: {e}")
                continue

        # Combine all years
        if not all_dfs:
            logger.error("No data extracted for any year")
            return None
        
        logger.info(f"Combining data from {len(all_dfs)} years...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        logger.info(f"Total rows extracted: {len(combined_df)}")
        
        # Store years filter for transformer
        combined_df.attrs['years_filter'] = years
        
        return combined_df
    
    def _parse_branches_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for branches by size.
        
        Data structure:
        - Lines 0-3: Headers/metadata
        - Line 4: Column headers (size classes)
        - Line 5: Units
        - Line 6+: Data (year, region_code, region_name, then counts for each size class)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier

        Returns:
            Parsed DataFrame
        """
        try:
            # Skip first 6 rows (metadata + headers + units)
            skip_rows = 6
            
            # Read CSV
            df = pd.read_csv(
                io.StringIO(raw_data),
                sep=';',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str},  # Force year, code, name as strings
                encoding='utf-8'
            )
            
            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            
            # Assign column names based on structure
            # Columns: year, region_code, region_name, total, size_0_10, size_10_50, size_50_250, size_250_plus
            df.columns = [
                'year', 
                'region_code', 
                'region_name',
                'total',
                'size_0_10',
                'size_10_50',
                'size_50_250',
                'size_250_plus'
            ]
            
            # Clean region codes (remove .0 if present)
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            
            logger.info(f"Sample regions: {df['region_code'].head().tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _parse_branches_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def extract_branches_by_sector(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract branches by economic sector data (WZ 2008 classification).
        
        Table: 52111-02-01-4
        Branches categorized by economic sectors (18 sectors)
        Available period: 2006-2023 (18 years)
        
        Args:
            regions: List of region codes
            years: List of years to extract (default: 2006-2023)

        Returns:
            DataFrame with branches data or None
        """
        table_id = self.BUSINESS_TABLES['branches_by_sector']
        logger.info(f"Extracting branches by sector data for table {table_id}")

        # Default to years 2006-2023 (full available period)
        if years is None:
            years = list(range(2006, 2024))  # 2006-2023 = 18 years
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"No data for year {year}")
                continue

            try:
                df_year = self._parse_branches_sector_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
                    logger.info(f"Successfully extracted {len(df_year)} rows for year {year}")
                else:
                    logger.warning(f"No data extracted for year {year}")
                    
            except Exception as e:
                logger.error(f"Error parsing data for year {year}: {e}")
                continue

        # Combine all years
        if not all_dfs:
            logger.error("No data extracted for any year")
            return None
        
        logger.info(f"Combining data from {len(all_dfs)} years...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        logger.info(f"Total rows extracted: {len(combined_df)}")
        
        # Store years filter for transformer
        combined_df.attrs['years_filter'] = years
        
        return combined_df
    
    def _parse_branches_sector_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for branches by economic sector.
        
        Data structure:
        - Lines 0-5: Headers/metadata
        - Lines 6-8: Multi-line column headers (sector names)
        - Line 9: Units
        - Line 10+: Data (year, region_code, region_name, then counts for 18 sectors)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier

        Returns:
            Parsed DataFrame
        """
        try:
            # Skip first 10 rows (metadata + headers + units)
            skip_rows = 10
            
            # Read CSV
            df = pd.read_csv(
                io.StringIO(raw_data),
                sep=';',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str},  # Force year, code, name as strings
                encoding='utf-8'
            )
            
            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            
            # Assign column names for 18 economic sectors (WZ 2008)
            # Based on German classification system
            df.columns = [
                'year', 
                'region_code', 
                'region_name',
                'total',  # Insgesamt (B-N,P-S)
                'sector_B',  # Mining
                'sector_C',  # Manufacturing
                'sector_D',  # Energy
                'sector_E',  # Water supply
                'sector_F',  # Construction
                'sector_G',  # Trade
                'sector_H',  # Transport
                'sector_I',  # Accommodation/Food
                'sector_J',  # Information/Communication
                'sector_K',  # Financial services
                'sector_L',  # Real estate
                'sector_M',  # Professional services
                'sector_N',  # Other business services
                'sector_P',  # Education
                'sector_Q',  # Health/Social work
                'sector_R',  # Arts/Entertainment
                'sector_S',  # Other services
            ]
            
            # Clean region codes (remove .0 if present)
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            
            logger.info(f"Sample regions: {df['region_code'].head().tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _parse_branches_sector_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def extract_business_registrations(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract business registrations and deregistrations data.
        
        Table: 52311-01-04-4
        Business registrations and deregistrations with subcategories
        Available period: 1998-2024 (27 years)
        
        Categories:
        - Registrations: Total, New establishments, Business foundations, Relocations in, Takeovers
        - Deregistrations: Total, Closures, Business closures, Relocations out, Handovers
        
        Args:
            regions: List of region codes
            years: List of years to extract (default: 1998-2024)

        Returns:
            DataFrame with business registrations data or None
        """
        table_id = self.BUSINESS_TABLES['business_registrations']
        logger.info(f"Extracting business registrations data for table {table_id}")

        # Default to years 1998-2024 (full available period)
        if years is None:
            years = list(range(1998, 2025))  # 1998-2024 = 27 years
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"No data for year {year}")
                continue

            try:
                df_year = self._parse_business_registrations_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
                    logger.info(f"Successfully extracted {len(df_year)} rows for year {year}")
                else:
                    logger.warning(f"No data extracted for year {year}")
                    
            except Exception as e:
                logger.error(f"Error parsing data for year {year}: {e}")
                continue

        # Combine all years
        if not all_dfs:
            logger.error("No data extracted for any year")
            return None
        
        logger.info(f"Combining data from {len(all_dfs)} years...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        logger.info(f"Total rows extracted: {len(combined_df)}")
        
        # Store years filter for transformer
        combined_df.attrs['years_filter'] = years
        
        return combined_df
    
    def _parse_business_registrations_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for business registrations/deregistrations.
        
        Data structure:
        - Lines 0-3: Headers/metadata
        - Lines 4-6: Multi-level column headers
        - Line 7: Units (Anzahl)
        - Line 8+: Data (year, region_code, region_name, then 10 categories)
        
        Columns (13 total):
        1. Year
        2. Region code
        3. Region name
        4-8. Registrations (Total, New, Foundations, Relocations in, Takeovers)
        9-13. Deregistrations (Total, Closures, Business closures, Relocations out, Handovers)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier

        Returns:
            Parsed DataFrame
        """
        try:
            # Skip first 8 rows (metadata + multi-level headers + units)
            skip_rows = 8
            
            # Read CSV
            df = pd.read_csv(
                io.StringIO(raw_data),
                sep=';',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str},  # Force year, code, name as strings
                encoding='utf-8'
            )
            
            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            
            # Assign column names
            df.columns = [
                'year', 
                'region_code', 
                'region_name',
                # Registrations
                'reg_total',
                'reg_new_establishments',
                'reg_business_foundations',
                'reg_relocations_in',
                'reg_takeovers',
                # Deregistrations
                'dereg_total',
                'dereg_closures',
                'dereg_business_closures',
                'dereg_relocations_out',
                'dereg_handovers',
            ]
            
            # Clean region codes (remove .0 if present)
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            
            logger.info(f"Sample regions: {df['region_code'].head().tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _parse_business_registrations_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def extract_employee_compensation(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employee compensation by economic sector data.
        
        Table: 82000-04-01-4
        Employee compensation (domestic concept) by economic sectors
        Available period: 2000-2022 (23 years)
        Unit: Thousand EUR
        
        Sectors (WZ 2008):
        - Total
        - Agriculture, forestry, fishing (A)
        - Manufacturing excluding construction (B-E) Total + Processing (C)
        - Construction (F)
        - Trade, transport, hospitality, IT (G-J)
        - Finance, insurance, real estate (K-L)
        - Public services, education, health (O-T)
        
        Args:
            regions: List of region codes
            years: List of years to extract (default: 2000-2022)

        Returns:
            DataFrame with employee compensation data or None
        """
        table_id = self.BUSINESS_TABLES['employee_compensation']
        logger.info(f"Extracting employee compensation data for table {table_id}")

        # Default to years 2000-2022 (full available period)
        if years is None:
            years = list(range(2000, 2023))  # 2000-2022 = 23 years
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"No data for year {year}")
                continue

            try:
                df_year = self._parse_employee_compensation_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
                    logger.info(f"Successfully extracted {len(df_year)} rows for year {year}")
                else:
                    logger.warning(f"No data extracted for year {year}")
                    
            except Exception as e:
                logger.error(f"Error parsing data for year {year}: {e}")
                continue

        # Combine all years
        if not all_dfs:
            logger.error("No data extracted for any year")
            return None
        
        logger.info(f"Combining data from {len(all_dfs)} years...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        logger.info(f"Total rows extracted: {len(combined_df)}")
        
        # Store years filter for transformer
        combined_df.attrs['years_filter'] = years
        
        return combined_df
    
    def _parse_employee_compensation_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for employee compensation.
        
        Data structure:
        - Lines 0-9: Headers/metadata (multi-level headers)
        - Line 10+: Data (year, region_code, region_name, then 8 sector values)
        
        Columns (11 total):
        1. Year
        2. Region code
        3. Region name
        4. Total
        5. Agriculture, forestry, fishing (A)
        6. Manufacturing excluding construction (B-E)
        7. Manufacturing - Processing industry (C)
        8. Construction (F)
        9. Trade, transport, hospitality, IT (G-J)
        10. Finance, insurance, real estate (K-L)
        11. Public services, education, health (O-T)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier

        Returns:
            Parsed DataFrame
        """
        try:
            # Skip first 10 rows (metadata + multi-level headers + units)
            skip_rows = 10
            
            # Read CSV
            df = pd.read_csv(
                io.StringIO(raw_data),
                sep=';',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str},  # Force year, code, name as strings
                encoding='utf-8'
            )
            
            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            
            # Assign column names
            df.columns = [
                'year', 
                'region_code', 
                'region_name',
                'total',
                'agriculture_forestry_fishing',
                'manufacturing_total',
                'manufacturing_processing',
                'construction',
                'trade_transport_hospitality_it',
                'finance_insurance_realestate',
                'public_services_education_health',
            ]
            
            # Clean region codes (remove .0 if present)
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            
            logger.info(f"Sample regions: {df['region_code'].head().tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _parse_employee_compensation_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_corporate_insolvencies(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract corporate insolvency applications data.
        
        Table: 52411-02-01-4
        Filed corporate insolvencies - annual total
        Available period: 2000-2024
        
        Args:
            regions: List of region codes
            years: List of years to extract (default: 2000-2024)

        Returns:
            DataFrame with insolvency data or None
        """
        table_id = self.BUSINESS_TABLES['corporate_insolvencies']
        logger.info(f"Extracting corporate insolvencies data for table {table_id}")

        # Default to years 2000-2024 (full available period)
        if years is None:
            years = list(range(2000, 2025))  # 2000-2024 = 25 years
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"No data for year {year}")
                continue

            try:
                df_year = self._parse_corporate_insolvencies_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    all_dfs.append(df_year)
                    logger.info(f"Successfully extracted {len(df_year)} rows for year {year}")
                else:
                    logger.warning(f"No data extracted for year {year}")
                    
            except Exception as e:
                logger.error(f"Error parsing data for year {year}: {e}")
                continue

        # Combine all years
        if not all_dfs:
            logger.error("No data extracted for any year")
            return None
        
        logger.info(f"Combining data from {len(all_dfs)} years...")
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        logger.info(f"Total rows extracted: {len(combined_df)}")
        
        # Store years filter for transformer
        combined_df.attrs['years_filter'] = years
        
        return combined_df
    
    def _parse_corporate_insolvencies_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for corporate insolvencies.
        
        Data structure:
        - Lines 0-6: Headers/metadata
        - Line 7+: Data (year, region_code, region_name, total, opened, rejected, companies, claims)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier

        Returns:
            Parsed DataFrame
        """
        try:
            # Skip first 7 rows (metadata + multi-level headers)
            skip_rows = 7
            
            # Read CSV
            df = pd.read_csv(
                io.StringIO(raw_data),
                sep=';',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str},  # Force year, code, name as strings
                encoding='utf-8'
            )
            
            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")
            
            # Assign column names based on structure
            df.columns = [
                'year', 
                'region_code', 
                'region_name',
                'total_applications',
                'opened_proceedings',
                'rejected_proceedings',
                'companies_count',
                'expected_claims_teur'
            ]
            
            # Clean region codes (remove .0 if present)
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            
            logger.info(f"Sample regions: {df['region_code'].head().tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"Error in _parse_corporate_insolvencies_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
