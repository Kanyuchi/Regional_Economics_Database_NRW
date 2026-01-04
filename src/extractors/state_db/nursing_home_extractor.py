"""
Nursing Home Extractor for State Database NRW
Extracts data on inpatient care facilities (nursing homes), places, and staff.

Table: 22412-01i - Inpatient care facilities: Available places and staff in nursing homes
                   - independent cities and districts - reference date

Available period: 2017 - 2023 (biennial: 2017, 2019, 2021, 2023)

Metrics:
- Total nursing homes (facilities)
- Available places in nursing homes (capacity)
- Staff in nursing homes

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


class NursingHomeExtractor(StateDBExtractor):
    """Extractor for nursing home facilities, places, and staff data."""

    TABLE_ID = "22412-01i"
    TABLE_NAME = "Nursing Homes: Places and Staff"
    START_YEAR = 2017
    END_YEAR = 2023

    # Column indices (0-indexed after date, region_code, region_name)
    COLUMN_MAPPING = {
        'nursing_homes': 3,          # Total nursing homes
        'total_places': 4,           # Total available places
        'full_inpatient_places': 5,  # Full inpatient places
        'partial_inpatient_places': 6, # Partial inpatient places
        'staff_count': 10            # Staff in nursing homes
    }

    def __init__(self):
        """Initialize the nursing home extractor."""
        super().__init__()
        logger.info(f"Nursing Home Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_nursing_home_data(self, startyear: int = 2017, endyear: int = 2023) -> Optional[pd.DataFrame]:
        """Extract nursing home data year-by-year."""
        logger.info("=" * 80)
        logger.info(f"EXTRACTING NURSING HOME DATA: {self.TABLE_ID}")
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

            year_df = self._parse_nursing_home_data(raw_data, year)

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

    def _parse_nursing_home_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """Parse raw CSV data from nursing home table."""
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of nursing home data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"nursing_home_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find data start line
            data_start_idx = None
            for i, line in enumerate(lines):
                if 'Anzahl' in line and line.count('Anzahl') >= 5:
                    data_start_idx = i + 1
                    logger.info(f"Found header at line {i}, data starts at {data_start_idx}")
                    break

            if data_start_idx is None:
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
                if len(parts) < 11:
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
                    'nursing_homes': self._clean_value(parts[3]),
                    'total_places': self._clean_value(parts[4]),
                    'full_inpatient_places': self._clean_value(parts[5]),
                    'partial_inpatient_places': self._clean_value(parts[6]),
                    'staff_count': self._clean_value(parts[10])
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)
            logger.info(f"Successfully parsed {len(df)} rows for year {year}")
            logger.info(f"Unique regions: {df['region_code'].nunique()}")

            nrw_row = df[df['region_code'] == '05']
            if not nrw_row.empty:
                logger.info(f"NRW nursing homes: {nrw_row['nursing_homes'].values[0]:,.0f}")
                logger.info(f"NRW places: {nrw_row['total_places'].values[0]:,.0f}")
                logger.info(f"NRW staff: {nrw_row['staff_count'].values[0]:,.0f}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse nursing home data: {e}")
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
        if value_str in ['-', '.', '...', 'x', '/', '–', '', 'nan']:
            return None
        try:
            value_str = value_str.replace(' ', '').replace(',', '.')
            return float(value_str)
        except ValueError:
            return None
