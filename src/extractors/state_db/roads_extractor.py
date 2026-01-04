"""
Roads Extractor for State Database NRW
Extracts interregional road data by road class.

Table: 46271-01i - Total interregional roads by road class (4)
                   - independent cities and districts - reference date

Available period: 1996 - 2024

Road Classes:
- Total (Insgesamt)
- Autobahnen (Motorways)
- Bundesstraßen (Federal roads)
- Landstraßen (State roads)
- Kreisstraßen (District roads)

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


class RoadsExtractor(StateDBExtractor):
    """
    Extractor for interregional road data by road class.

    Handles extraction of table 46271-01i which contains:
    - Total road length
    - Road length by class (Autobahnen, Bundesstraßen, Landstraßen, Kreisstraßen)
    - Annual data from 1996 to 2024
    - Coverage: Cities and districts (Kreise)
    """

    # Table configuration
    TABLE_ID = "46271-01i"
    TABLE_NAME = "Interregional Roads by Road Class"
    START_YEAR = 1996
    END_YEAR = 2024

    # Column mapping
    COLUMN_MAPPING = {
        'Total': 'road_total_km',
        'Autobahnen': 'road_motorway_km',
        'Bundesstraßen': 'road_federal_km',
        'Landstraßen': 'road_state_km',
        'Kreisstraßen': 'road_district_km'
    }

    def __init__(self):
        """Initialize the roads extractor."""
        super().__init__()
        logger.info(f"Roads Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_roads_data(
        self,
        startyear: int = 1996,
        endyear: int = 2024
    ) -> Optional[pd.DataFrame]:
        """
        Extract road data year-by-year.

        Args:
            startyear: Start year (default 1996)
            endyear: End year (default 2024)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING ROADS DATA: {self.TABLE_ID}")
        logger.info(f"Period: {startyear}-{endyear}")
        logger.info("="*80)

        all_dataframes = []
        successful_years = []
        failed_years = []

        for year in range(startyear, endyear + 1):
            logger.info(f"\n{'─'*80}")
            logger.info(f"YEAR {year} ({year - startyear + 1}/{endyear - startyear + 1})")
            logger.info(f"{'─'*80}")

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

            year_df = self._parse_roads_data(raw_data, year)

            if year_df is not None and not year_df.empty:
                all_dataframes.append(year_df)
                successful_years.append(year)
                logger.info(f"✓ Successfully extracted {len(year_df)} rows for {year}")
            else:
                logger.warning(f"❌ Failed to parse data for year {year}")
                failed_years.append(year)

        # Summary
        logger.info("\n" + "="*80)
        logger.info("EXTRACTION SUMMARY")
        logger.info("="*80)
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

    def _parse_roads_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from roads table.

        The CSV format has:
        - Lines 0-4: Metadata
        - Lines 5-7: Column headers
        - Line 8: Units
        - Line 9+: Data rows

        Format: date;region_code;region_name;total;motorway;federal;state;district

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of road data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"roads_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find data start line (after units line with 'km')
            data_start_idx = None
            for i, line in enumerate(lines):
                if ';;;km;km;km;km;km' in line:
                    data_start_idx = i + 1
                    logger.info(f"Found units line at {i}, data starts at {data_start_idx}")
                    break

            if data_start_idx is None:
                # Fallback
                data_start_idx = 9
                logger.warning(f"Units line not found, using fallback position {data_start_idx}")

            # Parse data rows
            records = []
            for line in lines[data_start_idx:]:
                if not line.strip():
                    continue

                parts = line.split(';')
                if len(parts) < 8:
                    continue

                date_str = parts[0].strip()
                region_code = parts[1].strip()
                region_name = parts[2].strip()
                total = parts[3].strip()
                motorway = parts[4].strip()
                federal = parts[5].strip()
                state = parts[6].strip()
                district = parts[7].strip()

                # Skip if not NRW region
                if not region_code.startswith('05'):
                    continue

                # Extract year from date
                try:
                    extracted_year = int(date_str.split('-')[0])
                except:
                    extracted_year = year

                records.append({
                    'year': extracted_year,
                    'reference_date': date_str,
                    'region_code': region_code,
                    'region_name': region_name,
                    'road_total_km': total,
                    'road_motorway_km': motorway,
                    'road_federal_km': federal,
                    'road_state_km': state,
                    'road_district_km': district
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)

            # Convert numeric columns
            numeric_cols = ['road_total_km', 'road_motorway_km', 'road_federal_km',
                          'road_state_km', 'road_district_km']
            for col in numeric_cols:
                df[col] = df[col].apply(self._clean_value)

            # Drop rows where all road values are null (aggregated regions)
            df = df.dropna(subset=numeric_cols, how='all')

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse road data: {e}")
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
        """Get information about the roads table."""
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Interregional road length by road class for NRW districts",
            "metrics": [
                "Total road length (km)",
                "Motorway length - Autobahnen (km)",
                "Federal road length - Bundesstraßen (km)",
                "State road length - Landstraßen (km)",
                "District road length - Kreisstraßen (km)"
            ],
            "geographic_level": "District (Kreis)"
        }
