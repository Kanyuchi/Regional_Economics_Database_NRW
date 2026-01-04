"""
Employment Data Extractor for Regional Database Germany
Regional Economics Database for NRW

Extracts employment and labor market indicators.
"""

import pandas as pd
from io import StringIO
from typing import Optional, List, Dict, Any
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from extractors.regional_db.base_extractor import RegionalDBExtractor
from utils.logging import get_logger


logger = get_logger(__name__)


class EmploymentExtractor(RegionalDBExtractor):
    """Extractor for employment data from Regional Database."""

    # Table IDs for employment indicators
    EMPLOYMENT_TABLES = {
        # Employees at workplace (Arbeitsort)
        'employees_workplace_gender_nationality': '13111-01-03-4',  # By gender & nationality
        'employees_workplace_sector': '13111-07-05-4',  # By economic sectors
        'employees_workplace_scope': '13111-03-02-4',  # Full-time/part-time
        'employees_workplace_qualification': '13111-11-04-4',  # Vocational qualification
        
        # Employees at residence (Wohnort)
        'employees_residence_gender_nationality': '13111-02-02-4',
        'employees_residence_scope': '13111-04-02-4',
        'employees_residence_qualification': '13111-12-03-4',
        
        # Unemployment
        'unemployed_rates': '13211-02-05-4',
        
        # Total employment by sector
        'employed_by_sector': '13312-01-05-4',
        
        # Construction industry
        'construction_industry': '44231-01-03-4',
        
        # Total turnover (all businesses)
        'total_turnover': '44231-01-02-4',
    }

    def extract_employees_workplace(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at workplace by gender and nationality.
        
        Table: 13111-01-03-4
        Reference date: June 30
        Available period: 2008-2024

        Args:
            regions: List of region codes (e.g., ['05111', '05112'])
            years: List of years to extract

        Returns:
            DataFrame with employment data or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_workplace_gender_nationality']
        logger.info(f"Extracting employees at workplace data for table {table_id}")

        # Download data (job-based for large tables)
        raw_data = self.get_table_data(table_id, format='datencsv', area='free')

        if raw_data is None:
            logger.error("Failed to download employment data")
            return None

        try:
            # Parse CSV data
            skip_rows = self._detect_data_start_row(raw_data)
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )
            
            # Assign column names based on expected structure
            # Table 13111-01-03-4 has: date, region_code, region_name, 
            # then employee counts by nationality (total/german/foreign) × gender (total/male/female)
            if len(df.columns) >= 10:
                df.columns = self._get_column_names_employment(len(df.columns))
                logger.info("Assigned column names for employment table")
            else:
                df.columns = [f'col_{i}' for i in range(len(df.columns))]
                logger.warning(f"Unexpected column count: {len(df.columns)}, using generic names")

            logger.info(f"Parsed {len(df)} rows of employment data")

            # Add metadata
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            df['reference_date_type'] = 'june_30'

            # Store years for transformer to filter
            if years:
                df.attrs['years_filter'] = years
                logger.info(f"Stored years filter for transformer: {years}")

            return df

        except Exception as e:
            logger.error(f"Error parsing employment data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _get_column_names_employment(self, num_cols: int) -> List[str]:
        """
        Get column names for employment table based on column count.
        
        Standard structure for 13111-01-03-4:
        - date, region_code, region_name
        - Then 9 value columns: total/german/foreign × total/male/female
        
        Args:
            num_cols: Number of columns in DataFrame
            
        Returns:
            List of column names
        """
        base_cols = ['date', 'region_code', 'region_name']
        
        if num_cols == 12:
            # Standard format: 3 base + 9 value columns
            value_cols = [
                'emp_total_total', 'emp_total_male', 'emp_total_female',
                'emp_german_total', 'emp_german_male', 'emp_german_female',
                'emp_foreign_total', 'emp_foreign_male', 'emp_foreign_female'
            ]
            return base_cols + value_cols
        elif num_cols == 10:
            # Simpler format
            value_cols = [
                'emp_total_total', 'emp_total_male', 'emp_total_female',
                'emp_german_total', 'emp_german_male', 'emp_german_female',
                'emp_foreign_total'
            ]
            return base_cols + value_cols
        else:
            # Generic column names
            return base_cols + [f'value_{i}' for i in range(num_cols - 3)]

    def _detect_data_start_row(self, raw_data: str) -> int:
        """
        Detect the row number where actual data starts in CSV.
        
        Args:
            raw_data: Raw CSV string

        Returns:
            Number of rows to skip to reach data
        """
        lines = raw_data.split('\n')

        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped and ';' in stripped:
                first_field = stripped.split(';')[0].strip()
                # Match date patterns like "2008-06-30" or "30.06.2008"
                if len(first_field) == 10:
                    if first_field[4] == '-' and first_field[7] == '-':
                        logger.info(f"Detected data start at row {i}: {first_field}")
                        return i
                    if first_field[2] == '.' and first_field[5] == '.':
                        logger.info(f"Detected data start at row {i} (German format): {first_field}")
                        return i

        logger.warning("Could not detect data start, using default skip=8")
        return 8

    def extract_employees_by_sector(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at workplace by economic sectors.
        
        Table: 13111-07-05-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.
        The GENESIS API returns one year at a time for this table.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with sector employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_workplace_sector']
        logger.info(f"Extracting employment by sector data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
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
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue  # Skip this year but continue with others

            try:
                # Parse this year's data
                df_year = self._parse_sector_employment_data(raw_data, table_id)
                
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def _parse_sector_employment_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for sector employment table.
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Data structure for this table:
            # Row 0-6: Headers and metadata (date in row 6: "Stichtag: YYYY-MM-DD")
            # Row 7-10: Column headers (multi-level)
            # Row 11+: Actual data
            
            # Detect the date from row 6
            lines = raw_data.split('\n')
            date_str = None
            if len(lines) > 6:
                date_line = lines[6]
                if 'Stichtag:' in date_line or 'Stand:' in date_line:
                    # Extract date from line like "Stichtag: 2011-06-30;;;;;;;;"
                    import re
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', date_line)
                    if date_match:
                        date_str = date_match.group(1)
            
            # Skip first 11 rows to get to the data
            skip_rows = 11
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            # Assign proper column names based on the structure
            df.columns = [
                'region_code',
                'region_name',
                'sector',
                'emp_total_total',
                'emp_total_male',
                'emp_total_female',
                'emp_foreign_total',
                'emp_foreign_male',
                'emp_foreign_female'
            ]

            # Add date column
            if date_str:
                df['date'] = date_str
            else:
                logger.warning("Could not detect date from file")
                df['date'] = None

            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            return df

        except Exception as e:
            logger.error(f"Error in _parse_sector_employment_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_employees_by_scope(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at workplace by employment scope (full-time/part-time).
        
        Table: 13111-03-02-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with scope employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_workplace_scope']
        logger.info(f"Extracting employment by scope data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
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
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue  # Skip this year but continue with others

            try:
                # Parse this year's data using similar structure to sector data
                df_year = self._parse_scope_employment_data(raw_data, table_id)
                
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def _parse_scope_employment_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for scope employment table.
        
        Data structure:
        - Row 0-9: Headers
        - Row 10+: Data with date in first column
        - Columns: Date, Region Code, Region Name, Scope, then 6 value columns
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Skip first 10 rows to get to the data (data starts at row 10)
            skip_rows = 10
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            # Assign proper column names (10 columns)
            # Structure: Date, Region Code, Region Name, Scope, 
            #            Total_Total, Total_Male, Total_Female,
            #            Foreign_Total, Foreign_Male, Foreign_Female
            df.columns = [
                'date',
                'region_code',
                'region_name',
                'employment_scope',
                'emp_total_total',
                'emp_total_male',
                'emp_total_female',
                'emp_foreign_total',
                'emp_foreign_male',
                'emp_foreign_female'
            ]

            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")

            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            return df

        except Exception as e:
            logger.error(f"Error in _parse_scope_employment_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_employees_by_qualification(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at workplace by vocational education qualification.
        
        Table: 13111-11-04-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with qualification employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_workplace_qualification']
        logger.info(f"Extracting employment by qualification data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
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
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue  # Skip this year but continue with others

            try:
                # Parse this year's data - structure similar to scope table
                df_year = self._parse_qualification_employment_data(raw_data, table_id)
                
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def _parse_qualification_employment_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for qualification employment table.
        
        Data structure similar to scope table:
        - Row 0-9: Headers
        - Row 10+: Data with date in first column
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Skip first 10 rows to get to the data
            skip_rows = 10
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            # Assign proper column names based on actual columns
            # Structure: Date, Region Code, Region Name, Qualification Type, then 6 value columns
            df.columns = [
                'date',
                'region_code',
                'region_name',
                'qualification_type',
                'emp_total_total',
                'emp_total_male',
                'emp_total_female',
                'emp_foreign_total',
                'emp_foreign_male',
                'emp_foreign_female'
            ]

            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")

            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            return df

        except Exception as e:
            logger.error(f"Error in _parse_qualification_employment_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_employees_residence(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at place of RESIDENCE by gender and nationality.
        
        Table: 13111-02-02-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with residence employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_residence_gender_nationality']
        logger.info(f"Extracting employment at residence data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
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
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue  # Skip this year but continue with others

            try:
                # Parse this year's data - similar to workplace employment
                df_year = self._parse_residence_employment_data(raw_data, table_id)
                
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def _parse_residence_employment_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for residence employment table.
        
        Similar structure to workplace employment (13111-01-03-4)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Skip first 10 rows to get to the data
            skip_rows = 10
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            # Assign proper column names (similar to workplace table)
            # Structure: Date, Region Code, Region Name, then 6 employment value columns
            df.columns = [
                'date',
                'region_code',
                'region_name',
                'emp_total_total',
                'emp_total_male',
                'emp_total_female',
                'emp_foreign_total',
                'emp_foreign_male',
                'emp_foreign_female'
            ]

            logger.info(f"Parsed {len(df)} rows with {len(df.columns)} columns")

            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'
            df['workplace_residence'] = 'residence'  # Mark as residence data

            return df

        except Exception as e:
            logger.error(f"Error in _parse_residence_employment_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_employees_residence_scope(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at place of RESIDENCE by employment scope (full-time/part-time).
        
        Table: 13111-04-02-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with residence scope employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_residence_scope']
        logger.info(f"Extracting employment at residence by scope data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
        logger.info(f"Will extract data for {len(years)} years: {years[0]}-{years[-1]}")

        # Extract each year separately and combine (same pattern as workplace scope)
        all_dfs = []
        
        for year in years:
            logger.info(f"Extracting year {year}...")
            
            raw_data = self.get_table_data(
                table_id, 
                format='datencsv', 
                area='free',
                startyear=year,
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue

            try:
                # Parse this year's data - similar structure to workplace scope
                df_year = self._parse_scope_employment_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    # Mark as residence data
                    df_year['workplace_residence'] = 'residence'
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def extract_employees_residence_qualification(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employees at place of RESIDENCE by vocational education qualification.
        
        Table: 13111-12-03-4
        Available period: 2008-2024 (June 30 each year)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2008-2024)

        Returns:
            DataFrame with residence qualification employment data for all requested years or None
        """
        table_id = self.EMPLOYMENT_TABLES['employees_residence_qualification']
        logger.info(f"Extracting employment at residence by qualification data for table {table_id}")

        # Default to all available years if not specified
        if years is None:
            years = list(range(2008, 2025))  # 2008-2024
        
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
                endyear=year  # Request only this specific year
            )

            if raw_data is None:
                logger.error(f"Failed to download data for year {year}")
                continue

            try:
                # Parse this year's data - reuse the qualification parser
                df_year = self._parse_qualification_employment_data(raw_data, table_id)
                
                if df_year is not None and not df_year.empty:
                    # Mark as residence data
                    df_year['workplace_residence'] = 'residence'
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
        logger.info(f"Stored years filter for transformer: {years}")
        
        return combined_df

    def extract_unemployment(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract unemployment data and rates.
        
        Table: 13211-02-05-4
        Unemployed persons by selected demographic groups and unemployment rates
        Available period: 2001-2024 (annual average)
        
        Note: This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2009-2024)

        Returns:
            DataFrame with unemployment data or None
        """
        table_id = self.EMPLOYMENT_TABLES['unemployed_rates']
        logger.info(f"Extracting unemployment data for table {table_id}")

        # Default to years 2001-2024 (full available period)
        if years is None:
            years = list(range(2001, 2025))  # 2001-2024
        
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
                logger.error(f"Failed to download data for year {year}")
                continue

            try:
                df_year = self._parse_unemployment_data(raw_data, table_id)
                
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

    def _parse_unemployment_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for unemployment table.
        
        Data structure for 13211-02-05-4:
        - Row 0: Table name
        - Row 1-5: Description headers
        - Row 6-8: Column headers (multi-line)
        - Row 9+: Data rows
        
        Columns: Year, Region Code, Region Name, then:
        - Unemployed Total, Foreign, Disabled, 15-20, 15-25, 55-65, Long-term
        - Unemployment Rate (various categories)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Fixed structure: data starts at row 9 (skip 9 header rows)
            skip_rows = 9
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None
            )

            # Determine column structure based on number of columns
            num_cols = len(df.columns)
            logger.info(f"Parsed {len(df)} rows with {num_cols} columns")
            
            # Expected 16 columns for 13211-02-05-4:
            # Year, Region Code, Region Name, then 13 value columns
            if num_cols >= 10:
                base_cols = ['date', 'region_code', 'region_name']
                value_cols = [
                    'unemployed_total',      # Arbeitslose (total)
                    'unemployed_foreign',    # Ausländer
                    'unemployed_disabled',   # schwerbehindert
                    'unemployed_15_20',      # 15 bis unter 20 Jahre
                    'unemployed_15_25',      # 15 bis unter 25 Jahre
                    'unemployed_55_65',      # 55 bis unter 65 Jahre
                    'unemployed_longterm',   # langzeitarbeitslos
                ]
                rate_cols = [
                    'rate_total',            # Arbeitslosenquote gesamt
                    'rate_male',             # Männer
                    'rate_female',           # Frauen
                ]
                
                # Assign as many columns as we have
                all_cols = base_cols + value_cols + rate_cols
                if num_cols <= len(all_cols):
                    df.columns = all_cols[:num_cols]
                else:
                    df.columns = all_cols + [f'extra_{i}' for i in range(num_cols - len(all_cols))]
                    
                logger.info(f"Assigned column names: {df.columns.tolist()[:10]}...")
            else:
                df.columns = [f'col_{i}' for i in range(num_cols)]
                logger.warning(f"Unexpected column count: {num_cols}, using generic names")

            # Clean the date column - it should just be the year
            if 'date' in df.columns:
                # The date is just the year (e.g., "2023")
                df['date'] = df['date'].astype(str).str.strip()
                
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            # Log some sample data
            logger.info(f"Sample regions: {df['region_code'].head(5).tolist() if 'region_code' in df.columns else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Error in _parse_unemployment_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_employed_by_sector(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract employed persons by economic sector (annual average).
        
        Table: 13312-01-05-4
        Employed persons by economic sectors - Annual average
        Available period: 2000-2023
        
        Note: This differs from 13111-07-05-4 which covers only employees 
        subject to social insurance. This table includes all employed persons.
        
        This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 2000-2023)

        Returns:
            DataFrame with employed persons by sector data or None
        """
        table_id = self.EMPLOYMENT_TABLES['employed_by_sector']
        logger.info(f"Extracting employed by sector data for table {table_id}")

        # Default to years 2000-2023 (full available period)
        if years is None:
            years = list(range(2000, 2024))  # 2000-2023
        
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
                logger.error(f"Failed to download data for year {year}")
                continue

            try:
                df_year = self._parse_employed_sector_data(raw_data, table_id)
                
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

    def _parse_employed_sector_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for employed persons by sector table.
        
        Data structure for 13312-01-05-4:
        - Row 0-4: Table info and headers
        - Row 5-7: Multi-level sector column headers
        - Row 8: Units (all in thousands - 1000)
        - Row 9+: Data rows
        
        WIDE FORMAT: Sectors are in COLUMNS, not rows
        Columns: Year | Region Code | Region Name | Total | Sector A | Sector B-E | Sector C | ...
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Data starts at row 9 (after 8 header rows + unit row)
            skip_rows = 9
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str}  # Force first 3 columns as strings
            )

            num_cols = len(df.columns)
            logger.info(f"Parsed {len(df)} rows with {num_cols} columns")
            
            # WIDE format: Year, Region Code, Region Name, then sector values
            # We'll use the "Total" column (column 3) as our main value
            # Columns are typically: Total, Agriculture(A), Production(B-E), Manufacturing(C), 
            #                        Construction(F), IT(J), Finance/Business(K-N), Public Services(O-U)
            if num_cols >= 11:
                df.columns = [
                    'date',                        # Year
                    'region_code',                 # Region code (DG, 01, 01001, etc.)
                    'region_name',                 # Region name
                    'employed_total',              # Total employed (all sectors)
                    'sector_agriculture',          # A: Agriculture
                    'sector_production',           # B-E: Production without construction
                    'sector_manufacturing',        # C: Manufacturing
                    'sector_construction',         # F: Construction
                    'sector_services',             # G-U: Services
                    'sector_it_finance',           # Additional services
                    'sector_public'                # Public services
                ] + [f'extra_{i}' for i in range(num_cols - 11)]
            else:
                df.columns = ['date', 'region_code', 'region_name'] + [f'value_{i}' for i in range(num_cols - 3)]
                logger.warning(f"Unexpected column count: {num_cols}, using generic names")

            # Clean region_code - ensure it's a string without decimals
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            logger.info(f"Sample regions: {df['region_code'].head(5).tolist() if 'region_code' in df.columns else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Error in _parse_employed_sector_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_construction_industry(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract construction industry statistics.
        
        Table: 44231-01-03-4
        Businesses, employees, construction industry turnover
        Reference date: 30.06
        Available period: 1995-2024 (30 years)
        
        This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 1995-2024)

        Returns:
            DataFrame with construction industry data or None
        """
        table_id = self.EMPLOYMENT_TABLES['construction_industry']
        logger.info(f"Extracting construction industry data for table {table_id}")

        # Default to years 1995-2024 (full available period)
        if years is None:
            years = list(range(1995, 2025))  # 1995-2024 = 30 years
        
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
                df_year = self._parse_construction_data(raw_data, table_id)
                
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

    def _parse_construction_data(
        self,
        raw_data: str,
        table_id: str
    ) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data for construction industry table.
        
        Data structure for 44231-01-03-4:
        - Rows 0-6: Headers
        - Row 7+: Data
        - Columns: Date, Region Code, Region Name, Businesses, Employees, Turnover (Tsd. EUR)
        
        Args:
            raw_data: Raw CSV string
            table_id: Table identifier
            
        Returns:
            DataFrame with parsed data or None
        """
        try:
            # Data starts at row 7
            skip_rows = 7
            
            df = pd.read_csv(
                StringIO(raw_data),
                delimiter=';',
                encoding='utf-8',
                skiprows=skip_rows,
                header=None,
                dtype={0: str, 1: str, 2: str}  # Force first 3 columns as strings
            )

            num_cols = len(df.columns)
            logger.info(f"Parsed {len(df)} rows with {num_cols} columns")
            
            # Expected structure: Date, Region Code, Region Name, Businesses, Employees, Turnover
            if num_cols >= 6:
                df.columns = [
                    'date',
                    'region_code',
                    'region_name',
                    'businesses',      # Number of businesses
                    'employees',       # Number of employees
                    'turnover'         # Turnover in Tsd. EUR
                ] + [f'extra_{i}' for i in range(num_cols - 6)]
            else:
                df.columns = [f'col_{i}' for i in range(num_cols)]
                logger.warning(f"Unexpected column count: {num_cols}, using generic names")

            # Clean region_code - ensure it's a string without decimals
            df['region_code'] = df['region_code'].astype(str).str.replace('.0', '', regex=False).str.strip()
            
            # Add metadata columns
            df['extracted_at'] = datetime.now()
            df['source_table'] = table_id
            df['data_source'] = 'regional_db'

            logger.info(f"Sample regions: {df['region_code'].head(5).tolist() if 'region_code' in df.columns else 'N/A'}")

            return df

        except Exception as e:
            logger.error(f"Error in _parse_construction_data: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def extract_total_turnover(
        self,
        regions: Optional[List[str]] = None,
        years: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Extract total turnover statistics (all businesses, not just construction).
        
        Table: 44231-01-02-4
        Businesses, employees, total turnover
        Reference date: 30.06
        Available period: 1995-2024 (30 years)
        
        This table requires separate API calls for each year.

        Args:
            regions: List of region codes
            years: List of years to extract (default: 1995-2024)

        Returns:
            DataFrame with total turnover data or None
        """
        table_id = self.EMPLOYMENT_TABLES['total_turnover']
        logger.info(f"Extracting total turnover data for table {table_id}")

        # Default to years 1995-2024 (full available period)
        if years is None:
            years = list(range(1995, 2025))  # 1995-2024 = 30 years
        
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
                # Reuse construction data parser (same structure)
                df_year = self._parse_construction_data(raw_data, table_id)
                
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

