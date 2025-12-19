"""Debug encoding issue by inspecting raw API response"""
import sys
from pathlib import Path
import requests
import json

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.config import get_api_config

# Get credentials
config = get_api_config('regional_db')
username = config['username']
password = config['password']

# Test with a cached job for table 13111-07-05-4
print("=== Testing Encoding at Different Levels ===\n")

# Simulate retrieving a job result
url = "https://www.regionalstatistik.de/genesisws/rest/2020/data/result"

headers = {
    'accept': 'application/json; charset=UTF-8',
    'username': username,
    'password': password,
    'Content-Type': 'application/x-www-form-urlencoded'
}

# Use a test job name (you'll need to replace with an actual job)
data = {
    'name': '13111-07-05-4',  # Will create a new job
    'area': 'free',
    'compress': 'false',
    'format': 'datencsv',
    'startyear': '2024',
    'endyear': '2024',
    'job': 'false'
}

print("1. Making request to API...")
response = requests.post(url, headers=headers, data=data)

print(f"Response status: {response.status_code}")
print(f"Response apparent_encoding: {response.apparent_encoding}")
print(f"Response encoding (before fix): {response.encoding}")

# Try different encodings
print("\n2. Testing different encodings on raw bytes:")

# Get first 1000 bytes
raw_bytes = response.content[:1000]

print(f"\nRaw bytes (first 100): {raw_bytes[:100]}")

# Try different encodings
for enc in ['utf-8', 'iso-8859-1', 'windows-1252', 'latin-1']:
    try:
        decoded = raw_bytes.decode(enc)
        # Look for Öff
        if 'ff. Verwaltung' in decoded:
            if 'Öff' in decoded:
                print(f"\n✓ {enc}: Contains 'Öff' - CORRECT!")
            elif '�ff' in decoded:
                print(f"\n✗ {enc}: Contains '�ff' - WRONG encoding")
            else:
                # Check hex codes
                idx = decoded.find('ff. Verwaltung')
                if idx > 0:
                    print(f"\n? {enc}: Found at position {idx}, char before: {repr(decoded[idx-1])} (code: {ord(decoded[idx-1])})")
    except Exception as e:
        print(f"✗ {enc}: Failed - {e}")

# Now check what's in the JSON
print("\n3. Parsing JSON response:")
try:
    # Force UTF-8
    response.encoding = 'utf-8'
    result = json.loads(response.text)

    # Check if there's CSV content
    csv_content = result.get('Object', {}).get('Content', '')

    if csv_content and len(csv_content) > 100:
        print(f"CSV content length: {len(csv_content)}")

        # Look for the problematic text in CSV
        if 'ff. Verwaltung' in csv_content:
            idx = csv_content.find('ff. Verwaltung')
            sample = csv_content[max(0, idx-5):idx+30]
            print(f"\nSample from CSV: {repr(sample)}")

            if 'Öff' in sample:
                print("✓ CSV contains correct 'Öff' character")
            elif '�ff' in sample or '\xc3\x96ff' in sample:
                print("✗ CSV has encoding issue")
                # Check the actual bytes
                print(f"Characters around 'ff': {[c for c in sample[:10]]}")
                print(f"Hex codes: {[hex(ord(c)) for c in sample[:10]]}")
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
