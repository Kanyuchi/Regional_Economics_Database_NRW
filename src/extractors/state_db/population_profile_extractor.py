"""
Population Profile Extractor for State Database NRW
Extracts population by gender, nationality, and age groups.

Table: 12411-9k06 - Municipal Profile: Population by gender, nationality, and age groups

Available period: 1975 - 2024

Age Groups:
- Total (Insgesamt)
- unter 6 Jahre (under 6 years)
- 6 bis unter 18 Jahre (6 to under 18 years)
- 18 bis unter 25 Jahre (18 to under 25 years)
- 25 bis unter 30 Jahre (25 to under 30 years)
- 30 bis unter 40 Jahre (30 to under 40 years)
- 40 bis unter 50 Jahre (40 to under 50 years)
- 50 bis unter 60 Jahre (50 to under 60 years)
- 60 bis unter 65 Jahre (60 to under 65 years)
- 65 Jahre und mehr (65 years and older)

Metrics per age group:
- Total population
- Male population
- Female population
- German citizens
- Foreign nationals

Note: This table provides NRW state-level aggregates only (not district-level).

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


class PopulationProfileExtractor(StateDBExtractor):
    """
    Extractor for population data by gender, nationality, and age groups.

    Handles extraction of table 12411-9k06 which contains:
    - Population by 9 age groups
    - Breakdown by gender (male/female)
    - Breakdown by nationality (German/Foreign)
    - Annual data from 1975 to 2024
    - Coverage: NRW state totals only
    """

    # Table configuration
    TABLE_ID = "12411-9k06"
    TABLE_NAME = "Population by Gender, Nationality, and Age Groups"
    START_YEAR = 1975
    END_YEAR = 2024

    # Age group mapping (German -> Code)
    AGE_GROUP_MAPPING = {
        'Total': 'total',
        'unter 6 Jahre': 'under_6',
        '6 bis unter 18 Jahre': '6_to_18',
        '18 bis unter 25 Jahre': '18_to_25',
        '25 bis unter 30 Jahre': '25_to_30',
        '30 bis unter 40 Jahre': '30_to_40',
        '40 bis unter 50 Jahre': '40_to_50',
        '50 bis unter 60 Jahre': '50_to_60',
        '60 bis unter 65 Jahre': '60_to_65',
        '65 Jahre und mehr': '65_plus'
    }

    # Column mapping
    COLUMN_MAPPING = {
        'Total': 'population_total',
        'männlich': 'population_male',
        'weiblich': 'population_female',
        'Deutsche': 'population_german',
        'Ausländer': 'population_foreign'
    }

    def __init__(self):
        """Initialize the population profile extractor."""
        super().__init__()
        logger.info(f"Population Profile Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR}")

    def extract_population_data(
        self,
        startyear: int = 1975,
        endyear: int = 2024
    ) -> Optional[pd.DataFrame]:
        """
        Extract population data year-by-year.

        Args:
            startyear: Start year (default 1975)
            endyear: End year (default 2024)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING POPULATION PROFILE DATA: {self.TABLE_ID}")
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

            year_df = self._parse_population_data(raw_data, year)

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

    def _parse_population_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from population profile table.

        The CSV format has:
        - Lines 0-4: Metadata
        - Line 5-7: Column headers
        - Line 8: Units (Anzahl)
        - Line 9+: Data rows

        Format: region_code;region_name;age_group;total;male;female;german;foreign

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of population data")

            # Save raw data
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"population_profile_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find data start line (after units line with 'Anzahl')
            data_start_idx = None
            for i, line in enumerate(lines):
                if ';;;Anzahl;Anzahl;Anzahl;Anzahl;Anzahl' in line:
                    data_start_idx = i + 1
                    logger.info(f"Found units line at {i}, data starts at {data_start_idx}")
                    break

            if data_start_idx is None:
                # Fallback
                data_start_idx = 8
                logger.warning(f"Units line not found, using fallback position {data_start_idx}")

            # Parse data rows
            records = []
            for line in lines[data_start_idx:]:
                if not line.strip():
                    continue
                if line.startswith('_'):  # Footer separator
                    break

                parts = line.split(';')
                if len(parts) < 8:
                    continue

                region_code = parts[0].strip()
                region_name = parts[1].strip()
                age_group = parts[2].strip()
                total = parts[3].strip()
                male = parts[4].strip()
                female = parts[5].strip()
                german = parts[6].strip()
                foreign = parts[7].strip()

                # Skip if not NRW region
                if not region_code.startswith('05'):
                    continue

                # Map age group to code
                age_group_code = self.AGE_GROUP_MAPPING.get(age_group, age_group.lower().replace(' ', '_'))

                records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'age_group': age_group,
                    'age_group_code': age_group_code,
                    'population_total': total,
                    'population_male': male,
                    'population_female': female,
                    'population_german': german,
                    'population_foreign': foreign
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)

            # Convert numeric columns
            numeric_cols = ['population_total', 'population_male', 'population_female',
                          'population_german', 'population_foreign']
            for col in numeric_cols:
                df[col] = df[col].apply(self._clean_value)

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse population data: {e}")
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
        """Get information about the population profile table."""
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Population by gender, nationality, and age groups for NRW state",
            "metrics": [
                "Total population",
                "Male population",
                "Female population",
                "German population",
                "Foreign population"
            ],
            "age_groups": list(self.AGE_GROUP_MAPPING.keys()),
            "geographic_level": "State (NRW only)"
        }
