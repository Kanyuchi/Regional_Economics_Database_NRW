"""
Migration Background Extractor for State Database NRW
Regional Economics Database for NRW

Extracts population data by migration background, employment status, and gender
from table 12211-9134i.

Table: 12211-9134i
Period: 2016-2019
Frequency: Annual
Source: Grundprogramm des Mikrozensus (Microcensus)
"""

import pandas as pd
from typing import Dict, List
from pathlib import Path

from .base_extractor import StateDBExtractor
from utils.logging import get_logger

logger = get_logger(__name__)


class MigrationBackgroundExtractor(StateDBExtractor):
    """
    Extractor for migration background and employment status data.

    Table: 12211-9134i
    Coverage: Population at main residence by migration background,
             employment status, and gender

    Migration background categories:
    - Total population
    - Without migration background
    - With migration background

    Employment status categories:
    - Total
    - Employed (Erwerbstätige)
    - Unemployed (Erwerbslose)
    - Not in labor force (Nichterwerbspersonen)

    Gender categories:
    - Male (männlich)
    - Female (weiblich)
    - Total (Insgesamt)
    """

    TABLE_CODE = "12211-9134i"
    TABLE_NAME = "Population by Migration Background and Employment Status"

    # Years available (2016-2019)
    START_YEAR = 2016
    END_YEAR = 2019

    # Column mapping (0-indexed after splitting by semicolon)
    # Columns 4-15 contain the 12 data metrics (3 migration categories × 4 employment statuses)
    COLUMN_MAPPING = {
        # Total population (all migration backgrounds)
        'total_population': 4,
        'total_employed': 5,
        'total_unemployed': 6,
        'total_not_in_labor_force': 7,

        # Without migration background
        'no_migration_bg_total': 8,
        'no_migration_bg_employed': 9,
        'no_migration_bg_unemployed': 10,
        'no_migration_bg_not_in_labor_force': 11,

        # With migration background
        'with_migration_bg_total': 12,
        'with_migration_bg_employed': 13,
        'with_migration_bg_unemployed': 14,
        'with_migration_bg_not_in_labor_force': 15
    }

    def extract_year(self, year: int) -> pd.DataFrame:
        """
        Extract migration background data for a specific year.

        Args:
            year: Year to extract (2016-2019)

        Returns:
            DataFrame with extracted data
        """
        logger.info(f"Extracting migration background data for year {year}")

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

        records = []
        for line in data_lines:
            if not line.strip():
                continue

            parts = line.split(';')

            if len(parts) < 16:
                logger.warning(f"Skipping incomplete row: {line[:100]}")
                continue

            # Extract base information
            year = parts[0].strip()
            region_code = parts[1].strip()
            region_name = parts[2].strip()
            gender = parts[3].strip()

            # Extract metrics using column mapping
            record = {
                'year': year,
                'region_code': region_code,
                'region_name': region_name,
                'gender': gender
            }

            # Extract all 12 metrics
            for metric_name, col_idx in self.COLUMN_MAPPING.items():
                value_str = parts[col_idx].strip()

                # Handle special values
                if value_str in ['/', '.', '-', '...', 'x', 'X']:
                    record[metric_name] = None
                else:
                    try:
                        # Values are in thousands
                        record[metric_name] = float(value_str.replace(',', '.'))
                    except (ValueError, AttributeError):
                        record[metric_name] = None

            records.append(record)

        df = pd.DataFrame(records)

        if not df.empty:
            # Convert year to integer
            df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')

            logger.info(f"Parsed {len(df)} records from CSV")

        return df

    def extract_all_years(self) -> pd.DataFrame:
        """
        Extract data for all available years (2016-2019).

        Returns:
            DataFrame with all years combined
        """
        logger.info(f"Extracting migration background data for years {self.START_YEAR}-{self.END_YEAR}")

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
            output_path = output_dir / f'migration_background_raw_{self.START_YEAR}_{self.END_YEAR}.csv'

        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Raw data saved to: {output_path}")

        return output_path


def main():
    """Main extraction function."""
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

    extractor = MigrationBackgroundExtractor()

    # Extract all years
    df = extractor.extract_all_years()

    if not df.empty:
        # Save raw data
        output_path = extractor.save_raw_data(df)
        print(f"\n✓ Extraction complete!")
        print(f"  Records: {len(df):,}")
        print(f"  Years: {df['year'].min()}-{df['year'].max()}")
        print(f"  Regions: {df['region_code'].nunique()}")
        print(f"  Output: {output_path}")

        # Show sample
        print(f"\nSample data (first 3 rows):")
        print(df.head(3).to_string())
    else:
        print("\n✗ Extraction failed - no data retrieved")
        sys.exit(1)


if __name__ == '__main__':
    main()
