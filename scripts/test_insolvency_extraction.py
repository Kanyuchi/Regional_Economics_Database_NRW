"""Test script to explore corporate insolvency data structure."""

import sys
import pandas as pd
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.regional_db.base_extractor import RegionalDBExtractor

def test_insolvency_extraction():
    """Test extraction of corporate insolvency data for 2024."""
    print("\n" + "="*80)
    print("TESTING CORPORATE INSOLVENCY EXTRACTION (52411-02-01-4)")
    print("="*80 + "\n")
    
    extractor = RegionalDBExtractor()
    
    try:
        # Test extraction for 2024
        table_id = "52411-02-01-4"
        year = 2024
        
        print(f"Extracting table {table_id} for year {year}...")
        
        # Use get_table_data method
        raw_data = extractor.get_table_data(
            table_id=table_id,
            format='datencsv',
            area='free',
            startyear=year,
            endyear=year
        )
        
        if raw_data is None:
            print("ERROR: No data returned from API")
            return
        
        print(f"Content length: {len(raw_data)} characters\n")
        
        # Save raw response for inspection
        raw_file = project_root / "data" / "raw" / "test_insolvency_2024.csv"
        raw_file.parent.mkdir(parents=True, exist_ok=True)
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(raw_data)
        print(f"Saved raw data to: {raw_file}\n")
        
        # Try to parse with different skip_rows values
        from io import StringIO
        
        for skip_rows in [5, 6, 7, 8]:
            print(f"\n{'-'*80}")
            print(f"Testing with skip_rows={skip_rows}")
            print(f"{'-'*80}")
            
            try:
                df = pd.read_csv(
                    StringIO(raw_data),
                    sep=';',
                    skiprows=skip_rows,
                    header=None,
                    dtype={0: str, 1: str, 2: str},
                    encoding='utf-8'
                )
                
                print(f"\nSuccessfully parsed!")
                print(f"Shape: {df.shape}")
                print(f"\nColumns ({len(df.columns)}):")
                for i, col in enumerate(df.columns):
                    print(f"  {i+1:2d}. {col}")
                
                print(f"\nFirst 3 rows:")
                print(df.head(3).to_string())
                
                print(f"\nData types:")
                print(df.dtypes)
                
                # Check for NRW regions
                if df.shape[1] > 0:
                    first_col = df.columns[0]
                    print(f"\nChecking NRW regions in column '{first_col}':")
                    nrw_check = df[first_col].astype(str).str.contains('05', na=False)
                    nrw_count = nrw_check.sum()
                    print(f"  Rows starting with '05': {nrw_count}")
                    
                    if nrw_count > 0:
                        print(f"\n  Sample NRW regions:")
                        nrw_samples = df[nrw_check].head(10)
                        print(nrw_samples[[first_col] + list(df.columns[1:3])].to_string())
                
                break  # Success, exit loop
                
            except Exception as e:
                print(f"Failed: {e}")
        
        print(f"\n{'='*80}")
        print("EXPLORATION COMPLETE")
        print(f"{'='*80}\n")
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        extractor.close()

if __name__ == "__main__":
    test_insolvency_extraction()

