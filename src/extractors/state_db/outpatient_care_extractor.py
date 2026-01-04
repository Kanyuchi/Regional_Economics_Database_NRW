"""
Outpatient Care Recipients Extractor for State Database NRW
Extracts people in need of care in outpatient/ambulatory services by care level.

Table: 22411-02i - People in need of care in outpatient services by care level
                   - independent cities and districts - reference date

Available period: 2017 - 2023 (biennial: 2017, 2019, 2021, 2023)

Care Levels (Pflegegrade):
- Pflegegrad 1: Slight impairment of independence
- Pflegegrad 2: Significant impairment
- Pflegegrad 3: Severe impairment
- Pflegegrad 4: Most severe impairment
- Pflegegrad 5: Most severe impairment with special care needs
- Total: Sum of all care levels

Regional Economics Database for NRW
"""

import pandas as pd
from io import StringIO
from typing import Optional, Dict, Any, List
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger
from .base_extractor import StateDBExtractor

logger = get_logger(__name__)


class OutpatientCareExtractor(StateDBExtractor):
    """
    Extractor for outpatient care recipients data by care level.

    Handles extraction of table 22411-02i which contains:
    - Care recipients in outpatient services by care level (Pflegegrad 1-5)
    - Biennial data from 2017 to 2023
    - Coverage: Districts (Kreise) and cities
    """

    # Table configuration
    TABLE_ID = "22411-02i"
    TABLE_NAME = "Outpatient Care Recipients by Care Level"
    START_YEAR = 2017
    END_YEAR = 2023

    # Care level mapping (German -> Code)
    CARE_LEVEL_MAPPING = {
        'Total': 'total',
        'Pflegegrad 1': 'level_1',
        'Pflegegrad 2': 'level_2',
        'Pflegegrad 3': 'level_3',
        'Pflegegrad 4': 'level_4',
        'Pflegegrad 5': 'level_5'
    }

    # Column indices (0-indexed after date, region_code, region_name)
    CARE_LEVEL_COLUMNS = {
        'total': 3,           # Total
        'level_1': 4,         # Pflegegrad 1
        'level_2': 5,         # Pflegegrad 2
        'level_3': 6,         # Pflegegrad 3
        'level_4': 7,         # Pflegegrad 4
        'level_5': 8          # Pflegegrad 5
    }

    def __init__(self):
        """Initialize the outpatient care extractor."""
        super().__init__()
        logger.info(f"Outpatient Care Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_outpatient_data(
        self,
        startyear: int = 2017,
        endyear: int = 2023
    ) -> Optional[pd.DataFrame]:
        """
        Extract outpatient care recipients data year-by-year.

        Args:
            startyear: Start year (default 2017)
            endyear: End year (default 2023)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("=" * 80)
        logger.info(f"EXTRACTING OUTPATIENT CARE DATA: {self.TABLE_ID}")
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

            year_df = self._parse_outpatient_data(raw_data, year)

            if year_df is not None and not year_df.empty:
                all_dataframes.append(year_df)
                successful_years.append(year)
                logger.info(f"✓ Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"❌ Failed to parse data for year {year}")
                failed_years.append(year)

        # Summary
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

    def _parse_outpatient_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from outpatient care table.

        Format: date;region_code;region_name;total;level1;level2;level3;level4;level5;...

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of outpatient care data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"outpatient_care_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find data start line (after header with 'Anzahl')
            data_start_idx = None
            for i, line in enumerate(lines):
                if 'Anzahl' in line and line.count('Anzahl') >= 5:
                    data_start_idx = i + 1
                    logger.info(f"Found header at line {i}, data starts at {data_start_idx}")
                    break

            if data_start_idx is None:
                # Fallback - look for first data line
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
                if not line.strip():
                    continue
                if line.startswith('_'):  # Footer separator
                    break

                parts = line.split(';')
                if len(parts) < 9:
                    continue

                date_str = parts[0].strip()
                region_code = parts[1].strip()
                region_name = parts[2].strip()

                # Skip if not NRW region
                if not region_code.startswith('05'):
                    continue

                # Extract year from date
                try:
                    extracted_year = int(date_str.split('-')[0])
                except:
                    extracted_year = year

                # Parse each care level
                for care_level, col_idx in self.CARE_LEVEL_COLUMNS.items():
                    if col_idx < len(parts):
                        value = parts[col_idx].strip()

                        # Get German label
                        german_label = next(
                            (k for k, v in self.CARE_LEVEL_MAPPING.items() if v == care_level),
                            care_level
                        )

                        records.append({
                            'year': extracted_year,
                            'reference_date': date_str,
                            'region_code': region_code,
                            'region_name': region_name,
                            'care_level': german_label,
                            'care_level_code': care_level,
                            'outpatient_recipients': self._clean_value(value)
                        })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)

            # Drop rows with null values
            df = df.dropna(subset=['outpatient_recipients'])

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")
            logger.info(f"Unique care levels: {df['care_level_code'].unique().tolist()}")
            logger.info(f"Unique regions: {df['region_code'].nunique()}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse outpatient care data: {e}")
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
            value_str = value_str.replace(' ', '')
            value_str = value_str.replace(',', '.')
            return float(value_str)
        except ValueError:
            return None

    def get_table_info(self) -> Dict[str, Any]:
        """Get information about the outpatient care table."""
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Outpatient care recipients by care level for NRW districts",
            "metrics": [
                "Outpatient care recipients by care level"
            ],
            "care_levels": list(self.CARE_LEVEL_MAPPING.keys()),
            "geographic_level": "District (Kreis)"
        }
