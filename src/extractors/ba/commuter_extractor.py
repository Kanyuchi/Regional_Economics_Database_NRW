"""
BA Commuter Statistics Extractor
Federal Employment Agency (Bundesagentur für Arbeit)

Extracts incoming (Einpendler) and outgoing (Auspendler) commuter data.
Available period: 2002-2024
Data types: Summary (district totals) and Detailed (origin-destination flows)
"""

import pandas as pd
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import re

from src.utils.logging import get_logger

logger = get_logger(__name__)


class CommuterExtractor:
    """
    Extractor for BA commuter statistics.

    Processes CSV files containing:
    - Einpendler (incoming commuters): People who commute INTO an NRW district to work
    - Auspendler (outgoing commuters): People who commute OUT OF an NRW district to work

    File types:
    - Summary files: District-level totals only (~70 lines)
    - Detailed files: Full origin-destination breakdown (>10,000 lines)
    """

    def __init__(self, data_dir: Path = None):
        """Initialize extractor with data directory containing commuter CSV files."""
        if data_dir is None:
            data_dir = Path("/Volumes/NO NAME/Regional Economics Database for NRW/Commuters")

        self.data_dir = Path(data_dir)
        self.einpendler_dir = self.data_dir / 'Einpendler"'  # Note: folder has quote in name
        self.auspendler_dir = self.data_dir / 'Auspendler'

        logger.info(f"Commuter extractor initialized")
        logger.info(f"  Einpendler: {self.einpendler_dir}")
        logger.info(f"  Auspendler: {self.auspendler_dir}")

    def extract_file(self, file_path: Path, commuter_type: str) -> pd.DataFrame:
        """
        Extract data from a single commuter CSV file.

        Args:
            file_path: Path to CSV file
            commuter_type: 'Einpendler' or 'Auspendler'

        Returns:
            DataFrame with columns:
                - year
                - commuter_type ('incoming' or 'outgoing')
                - workplace_name
                - workplace_code
                - residence_name
                - residence_code
                - total
                - men
                - women
                - germans
                - foreigners
                - trainees
                - file_type ('summary' or 'detailed')
        """
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            return pd.DataFrame()

        try:
            logger.debug(f"Processing {file_path.name}")

            # Read first 15 lines to extract metadata
            with open(file_path, 'r', encoding='utf-8-sig') as f:
                header_lines = [f.readline() for _ in range(15)]

            # Extract year from metadata
            year = None
            for line in header_lines:
                if 'Gebietsstand' in line or 'Area status' in line or 'Berichtsmonat' in line:
                    match = re.search(r'(\d{4})', line)
                    if match:
                        year = int(match.group(1))
                        break

            if year is None:
                logger.warning(f"Could not extract year from {file_path.name}, skipping")
                return pd.DataFrame()

            # Read the CSV data (skip metadata rows, header is at row 11)
            df = pd.read_csv(
                file_path,
                sep=';',
                encoding='utf-8-sig',
                skiprows=11,
                dtype=str  # Read all as string first
            )

            # Standardize column names based on first column
            # Both file types have both "Workplace" and "Place of residence" columns,
            # so we check which one comes first
            first_col = df.columns[0] if len(df.columns) > 0 else ''

            if 'Workplace' in first_col or 'Arbeitsort' in first_col:
                # Einpendler format: Workplace (NRW) | code | Residence (origin) | code | ...
                df.columns = [
                    'workplace_name', 'workplace_code', 'residence_name', 'residence_code',
                    'total', 'men', 'women', 'germans', 'foreigners', 'trainees'
                ]
            elif 'residence' in first_col.lower() or 'wohnort' in first_col.lower():
                # Auspendler format: Residence (NRW) | code | Workplace (destination) | code | ...
                df.columns = [
                    'residence_name', 'residence_code', 'workplace_name', 'workplace_code',
                    'total', 'men', 'women', 'germans', 'foreigners', 'trainees'
                ]
            else:
                logger.warning(f"Unexpected column structure in {file_path.name}: {first_col}")
                return pd.DataFrame()

            # Clean numeric columns
            for col in ['total', 'men', 'women', 'germans', 'foreigners', 'trainees']:
                df[col] = df[col].str.replace('.', '', regex=False)  # Remove thousands separator
                df[col] = df[col].str.replace(',', '', regex=False)
                df[col] = pd.to_numeric(df[col], errors='coerce')

            # Add metadata columns
            df['year'] = year
            df['commuter_type'] = 'incoming' if commuter_type == 'Einpendler' else 'outgoing'

            # Determine file type based on row count
            df['file_type'] = 'detailed' if len(df) > 1000 else 'summary'

            # Filter out rows where both workplace and residence codes are missing
            df = df.dropna(subset=['workplace_code', 'residence_code'], how='all')

            # Filter for NRW districts (workplace for incoming, residence for outgoing)
            if commuter_type == 'Einpendler':
                # For incoming: filter by workplace (destination)
                df = df[df['workplace_code'].str.startswith('05', na=False)].copy()
            else:
                # For outgoing: filter by residence (origin)
                df = df[df['residence_code'].str.startswith('05', na=False)].copy()

            logger.info(f"  {file_path.name}: {year}, {len(df)} NRW records, {df['file_type'].iloc[0] if len(df) > 0 else 'empty'}")

            return df

        except Exception as e:
            logger.error(f"Error extracting {file_path.name}: {e}", exc_info=True)
            return pd.DataFrame()

    def extract_commuter_type(self, commuter_type: str, years: List[int] = None) -> pd.DataFrame:
        """
        Extract all files for a specific commuter type (Einpendler or Auspendler).

        Args:
            commuter_type: 'Einpendler' or 'Auspendler'
            years: Optional list of specific years to extract. If None, extracts all.

        Returns:
            Combined DataFrame with all extracted data
        """
        logger.info(f"Extracting {commuter_type} data")

        # Determine folder
        folder = self.einpendler_dir if commuter_type == 'Einpendler' else self.auspendler_dir

        if not folder.exists():
            logger.error(f"Folder not found: {folder}")
            return pd.DataFrame()

        # Get all CSV files
        csv_files = sorted(folder.glob('statistik_*.csv'))
        logger.info(f"  Found {len(csv_files)} CSV files")

        all_data = []

        for file_path in csv_files:
            df_file = self.extract_file(file_path, commuter_type)

            if not df_file.empty:
                # Filter by year if specified
                if years is not None:
                    df_file = df_file[df_file['year'].isin(years)]

                if not df_file.empty:
                    all_data.append(df_file)

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            logger.info(f"  Extracted {len(df_combined):,} total records")
            logger.info(f"  Years: {sorted(df_combined['year'].unique())}")
            logger.info(f"  Districts: {df_combined[['workplace_code', 'residence_code']].nunique().max()}")
            return df_combined
        else:
            logger.warning(f"No data extracted for {commuter_type}")
            return pd.DataFrame()

    def extract_all(self, years: List[int] = None, include_detailed: bool = True) -> pd.DataFrame:
        """
        Extract all commuter data (both Einpendler and Auspendler).

        Args:
            years: Optional list of specific years to extract. If None, extracts all (2002-2024).
            include_detailed: If True, includes detailed files. If False, only summary files.

        Returns:
            Combined DataFrame with all commuter data
        """
        logger.info("Extracting ALL commuter data (Einpendler + Auspendler)")

        # Extract both types
        df_incoming = self.extract_commuter_type('Einpendler', years)
        df_outgoing = self.extract_commuter_type('Auspendler', years)

        all_data = []

        if not df_incoming.empty:
            all_data.append(df_incoming)

        if not df_outgoing.empty:
            all_data.append(df_outgoing)

        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)

            # Filter by file type if requested
            if not include_detailed:
                logger.info("Filtering to summary files only")
                df_combined = df_combined[df_combined['file_type'] == 'summary'].copy()

            logger.info(f"Combined extraction complete: {len(df_combined):,} total records")
            logger.info(f"  Incoming: {len(df_combined[df_combined['commuter_type'] == 'incoming']):,}")
            logger.info(f"  Outgoing: {len(df_combined[df_combined['commuter_type'] == 'outgoing']):,}")
            logger.info(f"  Years: {sorted(df_combined['year'].unique())}")
            logger.info(f"  Summary records: {len(df_combined[df_combined['file_type'] == 'summary']):,}")
            logger.info(f"  Detailed records: {len(df_combined[df_combined['file_type'] == 'detailed']):,}")

            return df_combined
        else:
            logger.error("No data extracted")
            return pd.DataFrame()

    def save_raw_data(self, df: pd.DataFrame, suffix: str = '') -> Path:
        """
        Save raw extracted data to CSV.

        Args:
            df: DataFrame with extracted data
            suffix: Optional suffix for filename (e.g., 'summary_only')

        Returns:
            Path to saved file
        """
        if df.empty:
            logger.warning("No data to save")
            return None

        # Create output directory
        output_dir = Path("data/raw/ba")
        output_dir.mkdir(parents=True, exist_ok=True)

        # Generate filename
        min_year = df['year'].min()
        max_year = df['year'].max()
        filename = f"commuters_raw_{min_year}_{max_year}"
        if suffix:
            filename += f"_{suffix}"
        filename += ".csv"

        output_path = output_dir / filename

        # Save to CSV
        df.to_csv(output_path, index=False)
        logger.info(f"Saved {len(df):,} raw records to {output_path}")

        return output_path


def main():
    """Test the extractor."""
    extractor = CommuterExtractor()

    # Test extraction of summary data only (faster)
    print("=" * 80)
    print("Testing SUMMARY data extraction (2002-2024)")
    print("=" * 80)
    df_summary = extractor.extract_all(include_detailed=False)
    print(f"\nTotal summary records: {len(df_summary):,}")
    print(f"Years: {sorted(df_summary['year'].unique())}")
    print(f"Incoming: {len(df_summary[df_summary['commuter_type'] == 'incoming']):,}")
    print(f"Outgoing: {len(df_summary[df_summary['commuter_type'] == 'outgoing']):,}")

    print("\nSample incoming commuter data (2024):")
    sample_in = df_summary[(df_summary['commuter_type'] == 'incoming') & (df_summary['year'] == 2024)].head(5)
    print(sample_in[['year', 'workplace_name', 'workplace_code', 'total', 'men', 'women']].to_string())

    print("\nSample outgoing commuter data (2024):")
    sample_out = df_summary[(df_summary['commuter_type'] == 'outgoing') & (df_summary['year'] == 2024)].head(5)
    print(sample_out[['year', 'residence_name', 'residence_code', 'total', 'men', 'women']].to_string())

    # Save
    output_path = extractor.save_raw_data(df_summary, suffix='summary_only')
    print(f"\nSaved to: {output_path}")


if __name__ == '__main__':
    main()
