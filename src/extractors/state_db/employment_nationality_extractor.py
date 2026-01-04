"""
Employment by Nationality Extractor for State Database NRW
Regional Economics Database for NRW

Extracts population data by employment status, nationality, and gender
from table 12211-9124i.

Table: 12211-9124i
Period: 1997-2019
Frequency: Annual
Source: Grundprogramm des Mikrozensus (Microcensus)
"""

import pandas as pd
from typing import Dict, List
from pathlib import Path

from .base_extractor import StateDBExtractor
from utils.logging import get_logger

logger = get_logger(__name__)


class EmploymentNationalityExtractor(StateDBExtractor):
    """
    Extractor for employment status by nationality and gender data.

    Table: 12211-9124i
    Coverage: Population at main residence by employment status,
             nationality, and gender

    Employment status categories:
    - Total
    - Employed (Erwerbstätige)
    - Unemployed (Erwerbslose)
    - Not in labor force (Nichterwerbspersonen)

    Nationality categories:
    - Total (all nationalities)
    - Germans (Deutsche)
    - Foreigners (Ausländer/-innen)

    Gender categories:
    - Male (männlich)
    - Female (weiblich)
    - Total (Insgesamt)
    """

    TABLE_CODE = "12211-9124i"
    TABLE_NAME = "Population by Employment Status, Nationality and Gender"

    # Years available (1997-2019)
    START_YEAR = 1997
    END_YEAR = 2019

    def extract_year(self, year: int) -> pd.DataFrame:
        """
        Extract employment by nationality data for a specific year.

        Args:
            year: Year to extract (1997-2019)

        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Extracting employment by nationality data for year {year}")

        # Get raw data from API
        raw_data = self.get_table_data(
            table_id=self.TABLE_CODE,
            startyear=year,
            endyear=year,
            format='datencsv',
            area='free'
        )

        if raw_data is None:
            logger.error(f"Failed to fetch data for year {year}")
            return pd.DataFrame()

        # Parse CSV data
        df = self._parse_csv_data(raw_data)

        if df.empty:
            logger.warning(f"No data found for year {year}")
            return df

        logger.info(f"Extracted {len(df)} records for year {year}")
        return df

    def _parse_csv_data(self, csv_content: str) -> pd.DataFrame:
        """
        Parse CSV content from API response.

        Args:
            csv_content: Raw CSV string from API

        Returns:
            Parsed DataFrame
        """
        lines = csv_content.strip().split('\n')

        # Find data start (skip header rows)
        data_start = 0
        for i, line in enumerate(lines):
            # Data rows start with year (4 digits)
            if line and line[0].isdigit() and len(line.split(';')[0]) == 4:
                data_start = i
                break

        if data_start == 0:
            logger.error("Could not find data rows in CSV")
            return pd.DataFrame()

        # Parse data rows
        data_lines = lines[data_start:]

        # First, let's examine the structure to determine column mapping
        if data_lines:
            sample_line = data_lines[0]
            parts = sample_line.split(';')
            logger.info(f"CSV has {len(parts)} columns")
            logger.info(f"Sample row: Year={parts[0]}, Region={parts[1]}, Gender={parts[3] if len(parts) > 3 else 'N/A'}")

        records = []
        for line in data_lines:
            if not line.strip():
                continue

            parts = line.split(';')

            # We expect: Year, Region Code, Region Name, Gender, [data columns]
            # Based on pattern: 3 nationalities × 4 employment statuses = 12 columns
            if len(parts) < 16:
                logger.debug(f"Skipping incomplete row with {len(parts)} columns: {line[:100]}")
                continue

            # Extract base information
            year = parts[0].strip()
            region_code = parts[1].strip()
            region_name = parts[2].strip()
            gender = parts[3].strip()

            # Column mapping (VERIFIED structure):
            # Columns 4-15 contain 12 metrics organized by EMPLOYMENT STATUS first,
            # then by NATIONALITY within each status:
            # Pattern: For each employment status, show [all_nationalities, german, foreigner]
            # 1. Total population: [all, german, foreigner] - columns 4, 5, 6
            # 2. Employed: [all, german, foreigner] - columns 7, 8, 9
            # 3. Unemployed: [all, german, foreigner] - columns 10, 11, 12
            # 4. Not in labor force: [all, german, foreigner] - columns 13, 14, 15

            record = {
                'year': year,
                'region_code': region_code,
                'region_name': region_name,
                'gender': gender,
                # Total population (all nationalities, german, foreigner)
                'total_population': self._parse_value(parts[4] if len(parts) > 4 else None),
                'german_total': self._parse_value(parts[5] if len(parts) > 5 else None),
                'foreigner_total': self._parse_value(parts[6] if len(parts) > 6 else None),
                # Employed (all nationalities, german, foreigner)
                'total_employed': self._parse_value(parts[7] if len(parts) > 7 else None),
                'german_employed': self._parse_value(parts[8] if len(parts) > 8 else None),
                'foreigner_employed': self._parse_value(parts[9] if len(parts) > 9 else None),
                # Unemployed (all nationalities, german, foreigner)
                'total_unemployed': self._parse_value(parts[10] if len(parts) > 10 else None),
                'german_unemployed': self._parse_value(parts[11] if len(parts) > 11 else None),
                'foreigner_unemployed': self._parse_value(parts[12] if len(parts) > 12 else None),
                # Not in labor force (all nationalities, german, foreigner)
                'total_not_in_labor_force': self._parse_value(parts[13] if len(parts) > 13 else None),
                'german_not_in_labor_force': self._parse_value(parts[14] if len(parts) > 14 else None),
                'foreigner_not_in_labor_force': self._parse_value(parts[15] if len(parts) > 15 else None)
            }

            records.append(record)

        df = pd.DataFrame(records)

        if not df.empty:
            # Convert year to integer
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
            logger.info(f"Parsed {len(df)} records from CSV")

        return df

    def _parse_value(self, value_str: str) -> float:
        """
        Parse a value string, handling special values.

        Args:
            value_str: String value from CSV

        Returns:
            Float value or None
        """
        if not value_str:
            return None

        value_str = value_str.strip()

        # Handle special values
        if value_str in ['/', '.', '-', '...', 'x', 'X']:
            return None

        try:
            # Values are in thousands
            return float(value_str.replace(',', '.'))
        except (ValueError, AttributeError):
            return None

    def extract_all_years(self) -> pd.DataFrame:
        """
        Extract data for all available years (1997-2019).

        Returns:
            DataFrame with all years combined
        """
        logger.info(f"Extracting employment by nationality data for years {self.START_YEAR}-{self.END_YEAR}")

        all_data = []

        for year in range(self.START_YEAR, self.END_YEAR + 1):
            df_year = self.extract_year(year)

            if not df_year.empty:
                all_data.append(df_year)
            else:
                logger.warning(f"No data extracted for year {year}")

        if not all_data:
            logger.error("No data extracted for any year")
            return pd.DataFrame()

        # Combine all years
        df_combined = pd.concat(all_data, ignore_index=True)

        logger.info(f"Total records extracted: {len(df_combined)}")
        logger.info(f"Years covered: {sorted(df_combined['year'].unique())}")
        logger.info(f"Regions covered: {df_combined['region_code'].nunique()}")

        return df_combined

    def save_raw_data(self, df: pd.DataFrame, output_path: Path = None) -> Path:
        """
        Save raw extracted data to CSV.

        Args:
            df: DataFrame to save
            output_path: Optional custom output path

        Returns:
            Path to saved file
        """
        if output_path is None:
            output_dir = Path('data/raw/state_db')
            output_dir.mkdir(parents=True, exist_ok=True)
            output_path = output_dir / f'employment_nationality_raw_{self.START_YEAR}_{self.END_YEAR}.csv'

        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Raw data saved to: {output_path}")

        return output_path


def main():
    """Main extraction function - test with single year first."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    extractor = EmploymentNationalityExtractor()

    # Test with single year first to verify column mapping
    print("Testing extraction with year 2019...")
    df_test = extractor.extract_year(2019)

    if not df_test.empty:
        print(f"\n✓ Test extraction successful!")
        print(f"  Records: {len(df_test):,}")
        print(f"  Regions: {df_test['region_code'].nunique()}")
        print(f"\nSample data (first 2 rows):")
        print(df_test.head(2).to_string())
        print(f"\nColumn names:")
        for col in df_test.columns:
            print(f"  - {col}")
    else:
        print("\n✗ Test extraction failed")
        sys.exit(1)


if __name__ == '__main__':
    main()
