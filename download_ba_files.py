"""
Download BA Employment/Wage Excel files for years 2014-2024
"""
import urllib.request
import time
from pathlib import Path

# BA file download configuration
BASE_URL = "https://statistik.arbeitsagentur.de/Statistikdaten/Detail/{year_month}/iiia6/beschaeftigung-entgelt-entgelt/{filename}"

# File naming patterns by year
FILES_TO_DOWNLOAD = [
    # Recent years (2022-2024) use "dwolk" prefix and .xlsx
    {"year": 2024, "month": 12, "filename": "entgelt-dwolk-0-202412-xlsx.xlsx"},
    {"year": 2023, "month": 12, "filename": "entgelt-dwolk-0-202312-xlsx.xlsx"},
    {"year": 2022, "month": 12, "filename": "entgelt-dwolk-0-202212-xlsx.xlsx"},
    
    # Mid years (2019-2021) use "d" prefix and .xlsx
    {"year": 2021, "month": 12, "filename": "entgelt-d-0-202112-xlsx.xlsx"},
    {"year": 2020, "month": 12, "filename": "entgelt-d-0-202012-xlsx.xlsx"},
    {"year": 2019, "month": 12, "filename": "entgelt-d-0-201912-xlsx.xlsx"},
    
    # Older years (2015-2018) use "d" prefix and .xlsm
    {"year": 2018, "month": 12, "filename": "entgelt-d-0-201812-xlsm.xlsm"},
    {"year": 2017, "month": 12, "filename": "entgelt-d-0-201712-xlsm.xlsm"},
    {"year": 2016, "month": 12, "filename": "entgelt-d-0-201612-xlsm.xlsm"},
    {"year": 2015, "month": 12, "filename": "entgelt-d-0-201512-xlsm.xlsm"},
    
    # Oldest year (2014) uses .xls format
    {"year": 2014, "month": 12, "filename": "entgelt-d-0-201412-xls.xls"},
]

def download_file(year, month, filename, output_dir):
    """Download a single BA file."""
    year_month = f"{year}{month:02d}"
    url = BASE_URL.format(year_month=year_month, filename=filename)
    output_path = Path(output_dir) / filename
    
    # Skip if already downloaded
    if output_path.exists():
        print(f"✓ Already exists: {filename}")
        return True
    
    print(f"Downloading {year}: {filename}")
    print(f"  URL: {url}")
    
    try:
        # Download with headers to avoid being blocked
        req = urllib.request.Request(
            url,
            headers={
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
        )
        
        with urllib.request.urlopen(req, timeout=60) as response:
            data = response.read()
            
        with open(output_path, 'wb') as f:
            f.write(data)
            
        print(f"✓ Downloaded: {filename} ({len(data):,} bytes)")
        return True
        
    except Exception as e:
        print(f"✗ Error downloading {filename}: {e}")
        return False

def main():
    output_dir = Path("data/raw/ba")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    print("=" * 80)
    print("BA EMPLOYMENT/WAGE DATA DOWNLOAD")
    print("Downloading files for years 2014-2024")
    print("=" * 80)
    print()
    
    success_count = 0
    fail_count = 0
    
    for file_info in FILES_TO_DOWNLOAD:
        result = download_file(
            year=file_info["year"],
            month=file_info["month"],
            filename=file_info["filename"],
            output_dir=output_dir
        )
        
        if result:
            success_count += 1
        else:
            fail_count += 1
        
        # Be polite to the server - wait between downloads
        if not (file_info == FILES_TO_DOWNLOAD[-1]):  # Don't wait after last file
            time.sleep(2)
        
        print()
    
    print("=" * 80)
    print(f"Download complete: {success_count} successful, {fail_count} failed")
    print("=" * 80)

if __name__ == "__main__":
    main()
