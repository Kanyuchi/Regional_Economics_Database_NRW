# -*- coding: utf-8 -*-
"""Verify Duisburg data with proper UTF-8 output"""
import sys
from pathlib import Path
import io

# Force UTF-8 output
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

query = """
SELECT
    t.year,
    f.notes,
    f.value
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 9
AND f.geo_id = (SELECT geo_id FROM dim_geography WHERE region_code = '05112')
AND t.year = 2024
ORDER BY f.notes
"""

results = db.execute_query(query)

print("\n" + "="*80)
print(" Duisburg Employment by Sector - 2024")
print(" (UTF-8 Encoding Verification)")
print("="*80 + "\n")

for row in results:
    notes = row['notes'].replace('Sector: ', '')
    value = row['value']
    print(f"{notes:<65} {value:>12,.0f}")

print("\n" + "="*80)
print("VERIFICATION:")
print("-"*80)

# Check specific characters
test_notes = [n['notes'] for n in results if 'ff. Verwaltung' in n['notes']]
if test_notes:
    note = test_notes[0]
    if '\u00d6ff' in note:  # Öff
        print("SUCCESS: German umlauts (Ö, ü, ä) are stored correctly as UTF-8!")
        print("Note: If you see '�' in your terminal, that's just a display issue.")
        print("      The actual data in PostgreSQL is correct UTF-8.")
    else:
        print("Issue detected in encoding")

# Also check for ü in Grundstücks
test_notes2 = [n['notes'] for n in results if 'Grundst' in n['notes']]
if test_notes2:
    note = test_notes2[0]
    if '\u00fcck' in note:  # ück (with ü = U+00FC)
        print("SUCCESS: Character 'ü' in 'Grundstücks' is also correct!")

print("="*80 + "\n")

db.close()
