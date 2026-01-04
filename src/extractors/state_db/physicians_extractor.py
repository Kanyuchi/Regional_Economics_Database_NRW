"""
Physicians Extractor for State Database NRW
Extracts data on full-time physicians in hospitals by gender.

Table: 23111-12i - Full-time physicians by gender
                   - independent cities and districts - reference date (from 2005 onwards)

Available period: 2005 - 2024 (annual data)

Metrics:
- Total full-time physicians in hospitals
- Male physicians
- Female physicians

Regional Economics Database for NRW
"""

import pandas as pd
from typing import Optional
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger
from .base_extractor import StateDBExtractor

logger = get_logger(__name__)


class PhysiciansExtractor(StateDBExtractor):
    """Extractor for full-time physicians in hospitals by gender."""

    TABLE_ID = "23111-12i"
    TABLE_NAME = "Full-time Physicians in Hospitals by Gender"
    START_YEAR = 2005
    END_YEAR = 2024

    # Column indices (0-indexed after date, region_code, region_name)
    COLUMN_MAPPING = {
        'total_physicians': 3,    # Total full-time physicians
        'male_physicians': 4,     # Male physicians
        'female_physicians': 5    # Female physicians
    }

    def __init__(self):
        """Initialize the physicians extractor."""
        super().__init__()
        logger.info(f"Physicians Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_physicians_data(self, startyear: int = 2005, endyear: int = 2024) -> Optional[pd.DataFrame]:
        """Extract physicians data year-by-year."""
        logger.info("=" * 80)
        logger.info(f"EXTRACTING PHYSICIANS DATA: {self.TABLE_ID}")
        logger.info(f"Period: {startyear}-{endyear}")
        logger.info("=" * 80)

        all_dataframes = []
        successful_years = []
        failed_years = []

        for year in range(startyear, endyear + 1):
            logger.info(f"\n{'─' * 80}")
            logger.info(f"YEAR {year} ({year - startyear + 1}/{endyear - startyear + 1})")
            logger.info(f"{'─' * 80}")

            raw_data = self.get_table_data(
                table_id=self.TABLE_ID,
                format='datencsv',
                startyear=year,
                endyear=year
            )

            if raw_data is None:
                logger.warning(f"❌ No data returned for year {year}")
                failed_years.append(year)
                continue

            year_df = self._parse_physicians_data(raw_data, year)

            if year_df is not None and not year_df.empty:
                all_dataframes.append(year_df)
                successful_years.append(year)
                logger.info(f"✓ Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"❌ Failed to parse data for year {year}")
                failed_years.append(year)

        logger.info("\n" + "=" * 80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Successful years: {len(successful_years)}/{endyear - startyear + 1}")
        logger.info(f"Failed years: {len(failed_years)}")
        if failed_years:
            logger.info(f"Failed: {failed_years}")

        if not all_dataframes:
            logger.error("No data extracted for any year")
            return None

        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logger.info(f"\nCombined {len(all_dataframes)} years into {len(combined_df)} total rows")

        return combined_df

    def _parse_physicians_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """Parse raw CSV data from physicians table."""
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of physicians data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"physicians_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find data start line
            data_start_idx = None
            for i, line in enumerate(lines):
                # Look for the header line with "Anzahl" repeated
                if 'Anzahl' in line and line.count('Anzahl') >= 3:
                    data_start_idx = i + 1
                    logger.info(f"Found header at line {i}, data starts at {data_start_idx}")
                    break

            if data_start_idx is None:
                # Fallback: look for first data line
                for i, line in enumerate(lines):
                    if line.startswith('20') and ';05' in line:
                        data_start_idx = i
                        logger.warning(f"Using fallback: data starts at line {data_start_idx}")
                        break

            if data_start_idx is None:
                logger.error("Could not find data start position")
                return None

            # Parse data rows
            records = []
            for line in lines[data_start_idx:]:
                if not line.strip() or line.startswith('_'):
                    continue

                parts = line.split(';')
                if len(parts) < 6:
                    continue

                date_str = parts[0].strip()
                region_code = parts[1].strip()
                region_name = parts[2].strip()

                if not region_code.startswith('05'):
                    continue

                try:
                    extracted_year = int(date_str.split('-')[0])
                except:
                    extracted_year = year

                records.append({
                    'year': extracted_year,
                    'reference_date': date_str,
                    'region_code': region_code,
                    'region_name': region_name,
                    'total_physicians': self._clean_value(parts[3]),
                    'male_physicians': self._clean_value(parts[4]),
                    'female_physicians': self._clean_value(parts[5])
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)
            logger.info(f"Successfully parsed {len(df)} rows for year {year}")
            logger.info(f"Unique regions: {df['region_code'].nunique()}")

            nrw_row = df[df['region_code'] == '05']
            if not nrw_row.empty and pd.notna(nrw_row['total_physicians'].values[0]):
                logger.info(f"NRW total physicians: {nrw_row['total_physicians'].values[0]:,.0f}")
                if pd.notna(nrw_row['male_physicians'].values[0]):
                    logger.info(f"NRW male physicians: {nrw_row['male_physicians'].values[0]:,.0f}")
                if pd.notna(nrw_row['female_physicians'].values[0]):
                    logger.info(f"NRW female physicians: {nrw_row['female_physicians'].values[0]:,.0f}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse physicians data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _clean_value(self, value) -> Optional[float]:
        """Clean and convert a value to float."""
        if pd.isna(value):
            return None
        if isinstance(value, (int, float)):
            return float(value)
        value_str = str(value).strip()
        # Handle missing data markers
        if value_str in ['-', '.', '...', 'x', '/', '–', '', 'nan']:
            return None
        try:
            value_str = value_str.replace(' ', '').replace(',', '.')
            return float(value_str)
        except ValueError:
            return None
