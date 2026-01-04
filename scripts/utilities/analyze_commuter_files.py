"""
Analyze Commuter Data Files
Scans both Einpendler and Auspendler folders to inventory all files
"""

import pandas as pd
from pathlib import Path
import re

def extract_metadata(file_path: Path) -> dict:
    """Extract metadata from a commuter CSV file."""
    try:
        # Read first 15 lines to get metadata
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            lines = [f.readline() for _ in range(15)]

        metadata = {
            'file_path': str(file_path),
            'file_name': file_path.name,
            'file_size_kb': file_path.stat().st_size / 1024,
        }

        # Extract key fields from header
        for line in lines:
            if 'Gebietsstand' in line or 'Area status' in line:
                # Extract year from date (e.g., "Juni 2024")
                match = re.search(r'(\d{4})', line)
                if match:
                    metadata['data_year'] = int(match.group(1))

            if 'Thema' in line:
                if 'Einpendler' in line:
                    metadata['commuter_type'] = 'Einpendler'
                elif 'Auspendler' in line:
                    metadata['commuter_type'] = 'Auspendler'

        # Count lines to determine if it's a detailed or summary file
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            line_count = sum(1 for _ in f)

        metadata['line_count'] = line_count

        # Classify based on line count
        if line_count > 1000:
            metadata['file_type'] = 'detailed'
        else:
            metadata['file_type'] = 'summary'

        return metadata

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None


def main():
    """Analyze all commuter files."""
    base_path = Path('/Volumes/NO NAME/Regional Economics Database for NRW/Commuters')

    # Find all CSV files in both folders
    einpendler_folder = base_path / 'Einpendler"'
    auspendler_folder = base_path / 'Auspendler'

    all_files = []

    # Process Einpendler files
    if einpendler_folder.exists():
        for file in einpendler_folder.glob('statistik_*.csv'):
            all_files.append(file)

    # Process Auspendler files
    if auspendler_folder.exists():
        for file in auspendler_folder.glob('statistik_*.csv'):
            all_files.append(file)

    print(f"Found {len(all_files)} commuter data files")
    print("=" * 100)
    print()

    # Extract metadata from all files
    metadata_list = []
    for file in sorted(all_files):
        meta = extract_metadata(file)
        if meta:
            metadata_list.append(meta)

    # Create DataFrame for analysis
    df = pd.DataFrame(metadata_list)

    # Summary statistics
    print("COMMUTER DATA INVENTORY")
    print("=" * 100)
    print()

    print("1. BY COMMUTER TYPE")
    print("-" * 100)
    type_summary = df.groupby('commuter_type').agg({
        'file_name': 'count',
        'data_year': ['min', 'max'],
        'file_type': lambda x: f"{sum(x == 'detailed')} detailed, {sum(x == 'summary')} summary"
    })
    print(type_summary)
    print()

    print("2. BY FILE TYPE")
    print("-" * 100)
    file_type_summary = df.groupby(['commuter_type', 'file_type']).agg({
        'file_name': 'count',
        'data_year': lambda x: sorted(x.tolist())
    })
    print(file_type_summary)
    print()

    print("3. COVERAGE BY YEAR")
    print("-" * 100)
    year_coverage = df.pivot_table(
        index='data_year',
        columns='commuter_type',
        values='file_type',
        aggfunc=lambda x: x.iloc[0] if len(x) == 1 else 'multiple'
    )
    print(year_coverage)
    print()

    print("4. DETAILED FILES")
    print("-" * 100)
    detailed = df[df['file_type'] == 'detailed'][['commuter_type', 'data_year', 'file_size_kb', 'line_count', 'file_name']]
    print(detailed.to_string(index=False))
    print()

    print("5. SUMMARY")
    print("-" * 100)
    print(f"Total files: {len(df)}")
    print(f"Einpendler: {len(df[df['commuter_type'] == 'Einpendler'])}")
    print(f"Auspendler: {len(df[df['commuter_type'] == 'Auspendler'])}")
    print(f"Years covered: {df['data_year'].min()}-{df['data_year'].max()}")
    print(f"Detailed files: {len(df[df['file_type'] == 'detailed'])}")
    print(f"Summary files: {len(df[df['file_type'] == 'summary'])}")
    print()

    # Check for missing data
    print("6. DATA GAPS")
    print("-" * 100)
    all_years = set(range(df['data_year'].min(), df['data_year'].max() + 1))
    einpendler_years = set(df[df['commuter_type'] == 'Einpendler']['data_year'])
    auspendler_years = set(df[df['commuter_type'] == 'Auspendler']['data_year'])

    missing_einpendler = all_years - einpendler_years
    missing_auspendler = all_years - auspendler_years

    if missing_einpendler:
        print(f"Missing Einpendler years: {sorted(missing_einpendler)}")
    else:
        print("✓ Einpendler: Complete coverage")

    if missing_auspendler:
        print(f"Missing Auspendler years: {sorted(missing_auspendler)}")
    else:
        print("✓ Auspendler: Complete coverage")
    print()

    # Save detailed inventory to CSV
    output_path = base_path / 'commuter_files_inventory.csv'
    df.to_csv(output_path, index=False)
    print(f"Detailed inventory saved to: {output_path}")


if __name__ == '__main__':
    main()
