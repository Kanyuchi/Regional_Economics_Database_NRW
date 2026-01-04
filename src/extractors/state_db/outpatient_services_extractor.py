"""
Outpatient Services Extractor for State Database NRW
Extracts data on outpatient care facilities and staff.

Table: 22411-01i - Outpatient services by type of facility and staff
                   - independent cities and districts - reference date

Available period: 2017 - 2023 (biennial: 2017, 2019, 2021, 2023)

Metrics:
- Total outpatient care services (facilities)
- Single-tier facilities (with/without other social services)
- Multi-tier facilities (with/without other social services)
- Staff in outpatient services

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


class OutpatientServicesExtractor(StateDBExtractor):
    """
    Extractor for outpatient services (facilities and staff) data.

    Handles extraction of table 22411-01i which contains:
    - Number of outpatient care service facilities
    - Breakdown by facility type (single-tier vs multi-tier)
    - Staff count in outpatient services
    - Biennial data from 2017 to 2023
    - Coverage: Districts (Kreise) and cities
    """

    # Table configuration
    TABLE_ID = "22411-01i"
    TABLE_NAME = "Outpatient Services by Facility Type and Staff"
    START_YEAR = 2017
    END_YEAR = 2023

    # Column indices (0-indexed after date, region_code, region_name)
    COLUMN_MAPPING = {
        'total_services': 3,              # Total outpatient services
        'single_tier_total': 4,           # Single-tier facilities total
        'single_tier_only_care': 5,       # Single-tier without other social services
        'single_tier_with_social': 6,     # Single-tier with other social services
        'multi_tier_total': 7,            # Multi-tier facilities total
        'multi_tier_only_care': 8,        # Multi-tier without other social services
        'multi_tier_with_social': 9,      # Multi-tier with other social services
        'staff_count': 10                 # Staff in outpatient services
    }

    def __init__(self):
        """Initialize the outpatient services extractor."""
        super().__init__()
        logger.info(f"Outpatient Services Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_services_data(
        self,
        startyear: int = 2017,
        endyear: int = 2023
    ) -> Optional[pd.DataFrame]:
        """
        Extract outpatient services data year-by-year.

        Args:
            startyear: Start year (default 2017)
            endyear: End year (default 2023)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("=" * 80)
        logger.info(f"EXTRACTING OUTPATIENT SERVICES DATA: {self.TABLE_ID}")
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

            year_df = self._parse_services_data(raw_data, year)

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

    def _parse_services_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from outpatient services table.

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of outpatient services data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"outpatient_services_raw_{year}.csv"
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
                if len(parts) < 11:
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

                # Parse values
                total_services = self._clean_value(parts[3]) if len(parts) > 3 else None
                single_tier_total = self._clean_value(parts[4]) if len(parts) > 4 else None
                single_tier_only_care = self._clean_value(parts[5]) if len(parts) > 5 else None
                single_tier_with_social = self._clean_value(parts[6]) if len(parts) > 6 else None
                multi_tier_total = self._clean_value(parts[7]) if len(parts) > 7 else None
                multi_tier_only_care = self._clean_value(parts[8]) if len(parts) > 8 else None
                multi_tier_with_social = self._clean_value(parts[9]) if len(parts) > 9 else None
                staff_count = self._clean_value(parts[10]) if len(parts) > 10 else None

                records.append({
                    'year': extracted_year,
                    'reference_date': date_str,
                    'region_code': region_code,
                    'region_name': region_name,
                    'total_services': total_services,
                    'single_tier_total': single_tier_total,
                    'single_tier_only_care': single_tier_only_care,
                    'single_tier_with_social': single_tier_with_social,
                    'multi_tier_total': multi_tier_total,
                    'multi_tier_only_care': multi_tier_only_care,
                    'multi_tier_with_social': multi_tier_with_social,
                    'staff_count': staff_count
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")
            logger.info(f"Unique regions: {df['region_code'].nunique()}")
            if not df.empty:
                nrw_row = df[df['region_code'] == '05']
                if not nrw_row.empty:
                    logger.info(f"NRW total services: {nrw_row['total_services'].values[0]:,.0f}")
                    logger.info(f"NRW staff count: {nrw_row['staff_count'].values[0]:,.0f}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse outpatient services data: {e}")
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
        """Get information about the outpatient services table."""
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Outpatient care services (facilities) and staff for NRW districts",
            "metrics": [
                "Total outpatient care services",
                "Single-tier facilities",
                "Multi-tier facilities",
                "Staff in outpatient services"
            ],
            "geographic_level": "District (Kreis)"
        }
