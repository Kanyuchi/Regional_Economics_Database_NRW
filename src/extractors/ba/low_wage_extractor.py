"""
BA Low-Wage Workers Extractor
Federal Employment Agency (Bundesagentur für Arbeit)

Extracts low-wage employment data from BA Excel files.
Period: 2020-2024
Source: Sheet 8.5 - Low-wage workers by district
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional

from src.utils.logging import get_logger
from .base_extractor import BAExtractor

logger = get_logger(__name__)


class LowWageExtractor(BAExtractor):
    """
    Extractor for BA low-wage workers data.

    Processes Sheet 8.5 which contains:
    - Total full-time employees (baseline)
    - Count and percentage below three different thresholds:
      * National threshold (2024: 2,676 EUR)
      * West Germany threshold (2024: 2,745 EUR)
      * East Germany threshold (2024: 2,359 EUR)

    Note: District-level data only (no demographic breakdowns)
    """

    # Files available with district-level data (2020-2024)
    FILE_PATTERNS = {
        2024: "entgelt-dwolk-0-202412-xlsx.xlsx",
        2023: "entgelt-dwolk-0-202312-xlsx.xlsx",
        2022: "entgelt-dwolk-0-202212-xlsx.xlsx",
        2021: "entgelt-d-0-202112-xlsx.xlsx",
        2020: "entgelt-d-0-202012-xlsx.xlsx",
    }

    def __init__(self, data_dir: Path = None):
        """Initialize extractor with data directory containing BA Excel files."""
        if data_dir is None:
            data_dir = Path("/Volumes/NO NAME/Regional Economics Database for NRW/arbeitsagentur.de")

        super().__init__(data_dir)
        self.data_dir = Path(data_dir)
        logger.info(f"BA Low-Wage extractor initialized for {self.data_dir}")

    def extract_year(self, year: int) -> pd.DataFrame:
        """
        Extract low-wage workers data for a single year.

        Args:
            year: Year to extract (2020-2024)

        Returns:
            DataFrame with columns:
                - year
                - region_code
                - region_name
                - total_employees (baseline)
                - below_national_count
                - below_national_pct
                - below_west_count
                - below_west_pct
                - below_east_count (may be masked with 'X')
                - below_east_pct (may be masked with 'X')
        """
        if year not in self.FILE_PATTERNS:
            logger.error(f"Year {year} not available. District-level data only available 2020-2024")
            return pd.DataFrame()

        # Get file path
        filename = self.FILE_PATTERNS[year]
        file_path = self.data_dir / filename

        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()

        logger.info(f"Extracting {year} low-wage data from {filename}")

        try:
            # Read Sheet 8.5
            # Data starts around row 92 (after Germany, West/East, state-level data)
            df = pd.read_excel(
                file_path,
                sheet_name='8.5',
                header=None
            )

            logger.info(f"Read sheet 8.5: {len(df)} rows, {len(df.columns)} columns")

            # Find where district data starts (look for NRW district codes)
            district_start_row = None
            for idx in range(len(df)):
                val = str(df.iloc[idx, 0]).strip()
                if val.startswith('05') and len(val) == 5 and val.isdigit():
                    district_start_row = idx
                    break

            if district_start_row is None:
                logger.error("Could not find start of district data in Sheet 8.5")
                return pd.DataFrame()

            logger.info(f"District data starts at row {district_start_row}")

            # Read from district start
            df_data = df.iloc[district_start_row:].copy()

            # Filter for NRW regions and Germany
            df_filtered = self.filter_nrw_regions(df_data, region_col=0)

            # Process rows into structured records
            records = []

            for idx, row in df_filtered.iterrows():
                region_code = str(row[0]).strip()
                region_name = str(row[1]).strip() if pd.notna(row[1]) else ""

                # Skip if not a valid region code
                if not (region_code == 'D' or (region_code.startswith('05') and len(region_code) == 5)):
                    continue

                record = {
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'total_employees': self._parse_number(row[2]),
                    'below_national_count': self._parse_number(row[3]),
                    'below_national_pct': self._parse_number(row[4]),
                    'below_west_count': self._parse_number(row[5]),
                    'below_west_pct': self._parse_number(row[6]),
                    'below_east_count': self._parse_number(row[7]),
                    'below_east_pct': self._parse_number(row[8]),
                }

                records.append(record)

            df_result = pd.DataFrame(records)
            logger.info(f"Extracted {len(df_result)} records for {year}")
            logger.info(f"  Regions: {df_result['region_code'].nunique()}")

            return df_result

        except Exception as e:
            logger.error(f"Error extracting {year} data: {e}", exc_info=True)
            return pd.DataFrame()

    def extract_all_years(self) -> pd.DataFrame:
        """
        Extract low-wage data for all available years (2020-2024).

        Returns:
            Combined DataFrame with all years
        """
        logger.info("Extracting all years (2020-2024)")

        all_data = []

        for year in sorted(self.FILE_PATTERNS.keys()):
            df_year = self.extract_year(year)

            if not df_year.empty:
                all_data.append(df_year)
            else:
                logger.warning(f"No data extracted for {year}")

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            logger.info(f"Combined extraction complete: {len(df_combined):,} total records")
            logger.info(f"  Years: {sorted(df_combined['year'].unique())}")
            logger.info(f"  Regions: {df_combined['region_code'].nunique()}")
            return df_combined
        else:
            logger.error("No data extracted for any year")
            return pd.DataFrame()

    def save_raw_data(self, df: pd.DataFrame) -> Path:
        """
        Save raw extracted data to CSV.

        Args:
            df: DataFrame with extracted data

        Returns:
            Path to saved file
        """
        if df.empty:
            logger.warning("No data to save")
            return None

        # Create output directory
        output_dir = Path("data/raw/ba")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename with year range
        min_year = df['year'].min()
        max_year = df['year'].max()
        filename = f"low_wage_raw_{min_year}_{max_year}.csv"
        output_path = output_dir / filename

        # Save to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df):,} raw records to {output_path}")

        return output_path

    def _parse_number(self, value) -> Optional[float]:
        """Parse a numeric value from Excel cell, handling various formats and 'X' masking."""
        if pd.isna(value):
            return None

        # Handle 'X' masking (data suppressed for privacy)
        if isinstance(value, str) and value.strip().upper() == 'X':
            return None

        if isinstance(value, (int, float)):
            return float(value)

        # Try to parse string
        try:
            # Remove thousands separators and convert
            cleaned = str(value).replace(',', '').replace(' ', '').strip()
            if cleaned == '' or cleaned == '-' or cleaned == '.':
                return None
            return float(cleaned)
        except (ValueError, AttributeError):
            return None


def main():
    """Test the extractor."""
    import sys
    from pathlib import Path

    # Add project root to path
    project_root = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(project_root))

    extractor = LowWageExtractor()

    # Test single year
    print("Testing 2024 extraction:")
    print("=" * 80)
    df_2024 = extractor.extract_year(2024)
    print(f"Records: {len(df_2024)}")
    print(f"Columns: {df_2024.columns.tolist()}")
    print("\nSample data (first 5 rows):")
    print(df_2024.head())

    # Test all years
    print("\n" + "=" * 80)
    print("Testing all years extraction:")
    print("=" * 80)
    df_all = extractor.extract_all_years()
    print(f"Total records: {len(df_all):,}")
    print(f"Years: {sorted(df_all['year'].unique())}")

    # Save
    output_path = extractor.save_raw_data(df_all)
    print(f"\nSaved to: {output_path}")


if __name__ == '__main__':
    main()
