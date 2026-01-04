"""
Income Tax by Bracket Extractor for State Database NRW
Extracts wage and income tax data by income size categories.

Table: 73111-030i - Wage and income tax from 2020: Taxpayers,
                    total amount of income, wage and income tax and
                    size categories according to total amount of income (17) -
                    Municipalities - Year

Available period: 2012 - 2021

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


class IncomeTaxBracketExtractor(StateDBExtractor):
    """
    Extractor for wage and income tax data by income brackets.

    Handles extraction of table 73111-030i which contains:
    - Number of taxpayers (Steuerpflichtige) by income bracket
    - Total income (Gesamtbetrag der Einkünfte) by income bracket
    - Wage and income tax (Lohn- und Einkommensteuer) by income bracket
    - 15 income size categories (brackets)
    - Annual data from 2012 to 2021
    - Coverage: Municipalities (Gemeinden)
    """

    # Table configuration
    TABLE_ID = "73111-030i"
    TABLE_NAME = "Wage and Income Tax by Income Bracket"
    START_YEAR = 2012
    END_YEAR = 2021

    # Income bracket mapping (German to standardized)
    BRACKET_MAPPING = {
        '1 - 5 000': '1_5000',
        '5 000 - 10 000': '5000_10000',
        '10 000 - 15 000': '10000_15000',
        '15 000 - 20 000': '15000_20000',
        '20 000 - 25 000': '20000_25000',
        '25 000 - 30 000': '25000_30000',
        '30 000 - 35 000': '30000_35000',
        '35 000 - 50 000': '35000_50000',
        '50 000 - 125 000': '50000_125000',
        '125 000 - 250 000': '125000_250000',
        '250 000 - 500 000': '250000_500000',
        '500 000 - 1 000 000': '500000_1000000',
        '1 000 000 und mehr': '1000000_plus',
        'insgesamt': 'total',
        'Verlustfälle': 'loss_cases'
    }

    def __init__(self):
        """Initialize the income tax bracket extractor."""
        super().__init__()
        logger.info(f"Income Tax Bracket Extractor initialized for table {self.TABLE_ID}")
        logger.info(f"Period: {self.START_YEAR}-{self.END_YEAR} ({self.END_YEAR - self.START_YEAR + 1} years)")

    def extract_income_tax_bracket_data(
        self,
        startyear: int = 2012,
        endyear: int = 2021
    ) -> Optional[pd.DataFrame]:
        """
        Extract wage and income tax data by income bracket year-by-year.

        Args:
            startyear: Start year (default 2012)
            endyear: End year (default 2021)

        Returns:
            DataFrame with extracted data for all years or None if error
        """
        logger.info("="*80)
        logger.info(f"EXTRACTING INCOME TAX BY BRACKET: {self.TABLE_ID}")
        logger.info(f"Period: {startyear}-{endyear} ({endyear - startyear + 1} years)")
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

            year_df = self._parse_bracket_data(raw_data, year)

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

    def _parse_bracket_data(self, raw_data: str, year: int) -> Optional[pd.DataFrame]:
        """
        Parse raw CSV data from income tax bracket table.

        The CSV format has:
        - Lines 0-5: Metadata
        - Line 6: Year
        - Line 7: Units (;;;Anzahl;Tsd. EUR;Tsd. EUR)
        - Line 8+: Data rows (region_code;region_name;bracket;taxpayers;income;tax)

        Args:
            raw_data: Raw CSV string from API
            year: Year being extracted

        Returns:
            Parsed DataFrame or None if error
        """
        try:
            logger.info(f"Parsing {len(raw_data):,} bytes of income tax bracket data")

            # Save raw data for inspection
            raw_dir = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "state_db"
            raw_dir.mkdir(parents=True, exist_ok=True)
            raw_file = raw_dir / f"income_tax_bracket_raw_{year}.csv"
            raw_file.write_text(raw_data, encoding='utf-8')
            logger.info(f"Saved raw data to {raw_file}")

            lines = raw_data.strip().split('\n')

            # Find the data start line (after the units line)
            data_start_idx = None
            for i, line in enumerate(lines):
                if line.startswith(';;;Anzahl'):
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

                parts = line.split(';')
                if len(parts) < 6:
                    continue

                region_code = parts[0].strip()
                region_name = parts[1].strip()
                bracket = parts[2].strip()
                taxpayers = parts[3].strip()
                total_income = parts[4].strip()
                tax_amount = parts[5].strip()

                # Skip if not NRW region
                if not region_code.startswith('05'):
                    continue

                # Standardize bracket name
                bracket_std = self.BRACKET_MAPPING.get(bracket, bracket)

                records.append({
                    'year': year,
                    'region_code': region_code,
                    'region_name': region_name,
                    'income_bracket': bracket,
                    'income_bracket_code': bracket_std,
                    'taxpayers': taxpayers,
                    'total_income_tsd_eur': total_income,
                    'tax_amount_tsd_eur': tax_amount
                })

            if not records:
                logger.error("No records parsed")
                return None

            df = pd.DataFrame(records)

            # Convert numeric columns
            for col in ['taxpayers', 'total_income_tsd_eur', 'tax_amount_tsd_eur']:
                df[col] = df[col].apply(self._clean_value)

            logger.info(f"Successfully parsed {len(df)} rows for year {year}")
            logger.info(f"Unique brackets: {df['income_bracket'].nunique()}")
            logger.info(f"Unique regions: {df['region_code'].nunique()}")

            return df

        except Exception as e:
            logger.error(f"Failed to parse bracket data: {e}")
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
        """Get information about the income tax bracket table."""
        return {
            "table_id": self.TABLE_ID,
            "table_name": self.TABLE_NAME,
            "source": "state_db",
            "source_name": "State Database NRW (Landesdatenbank)",
            "start_year": self.START_YEAR,
            "end_year": self.END_YEAR,
            "description": "Wage and income tax statistics by income bracket for NRW municipalities",
            "metrics": [
                "Number of taxpayers (Steuerpflichtige)",
                "Total income (Gesamtbetrag der Einkünfte)",
                "Wage and income tax (Lohn- und Einkommensteuer)"
            ],
            "income_brackets": list(self.BRACKET_MAPPING.keys()),
            "geographic_level": "Municipality (Gemeinde)"
        }
