"""Verify the newly loaded data in indicator 9"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

print("\n" + "="*100)
print(" Verification: Recently Loaded Employment Data (Indicator 9)")
print("="*100 + "\n")

# Check data loaded in last hour
recent_query = """
SELECT
    COUNT(*) as total_records,
    COUNT(notes) as with_notes,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT notes) as unique_notes
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 9
AND f.loaded_at > NOW() - INTERVAL '1 hour'
"""

result = db.execute_query(recent_query)[0]

print("RECENTLY LOADED DATA (Last Hour):")
print("-"*100)
print(f"  Total records: {result['total_records']:,}")
print(f"  Records with notes: {result['with_notes']:,}")
print(f"  Records with NULL notes: {result['total_records'] - result['with_notes']:,}")
print(f"  Year range: {result['min_year']} - {result['max_year']}")
print(f"  Unique note values: {result['unique_notes']}")

# Sample the new data for Duisburg
print("\n" + "="*100)
print(" Sample: Duisburg Employment (Years 2020-2024, Recently Loaded)")
print("="*100 + "\n")

sample_query = """
SELECT
    g.region_name,
    t.year,
    f.value,
    f.notes,
    f.loaded_at
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 9
AND g.region_code = '05112'
AND t.year >= 2020
AND f.loaded_at > NOW() - INTERVAL '1 hour'
ORDER BY t.year DESC, f.notes
LIMIT 10
"""

samples = db.execute_query(sample_query)

if samples:
    print(f"{'Year':<6} {'Value':>12} {'Notes':<50} {'Loaded At':<20}")
    print("-"*100)
    for row in samples:
        year = row['year']
        value = f"{row['value']:,.0f}" if row['value'] else 'NULL'
        notes = (row['notes'] or 'NULL')[:48]
        loaded = row['loaded_at'].strftime('%Y-%m-%d %H:%M:%S')

        print(f"{year:<6} {value:>12} {notes:<50} {loaded:<20}")

    # Check if this is sector data (with sector in notes) or basic workplace data
    print("\n" + "="*100)
    print(" Data Type Analysis")
    print("="*100 + "\n")

    if samples[0]['notes'] and 'Sector:' in samples[0]['notes']:
        print("[INFO] This appears to be SECTOR employment data (table 13111-07-05-4)")
        print("       Each record contains sector breakdown in notes field")
    else:
        print("[INFO] This appears to be BASIC employment data (table 13111-01-03-4)")
        print("       No sector breakdown - just total employment by gender/nationality")

else:
    print("No recently loaded data found for Duisburg")

# Check total records for indicator 9
print("\n" + "="*100)
print(" Total Indicator 9 Records")
print("="*100 + "\n")

total_query = """
SELECT COUNT(*) as cnt FROM fact_demographics WHERE indicator_id = 9
"""

total = db.execute_query(total_query)[0]['cnt']
print(f"Total records for indicator 9: {total:,}")
print(f"Recently loaded (last hour): {result['total_records']:,}")
print(f"Previously loaded: {total - result['total_records']:,}")

db.close()
