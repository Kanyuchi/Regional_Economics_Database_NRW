"""Verify the newly extracted employment at workplace data"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

print("\n" + "="*100)
print(" Verification: Table 13111-01-03-4 (Employment at Workplace)")
print("="*100 + "\n")

# Check indicator 2 data
query = """
SELECT
    COUNT(*) as total_records,
    COUNT(notes) as with_notes,
    COUNT(*) - COUNT(notes) as null_notes,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT g.region_code) as unique_regions
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 2
"""

result = db.execute_query(query)[0]

print("OVERALL STATISTICS:")
print("-"*100)
print(f"  Total records: {result['total_records']:,}")
print(f"  Records with notes: {result['with_notes']:,}")
print(f"  Records with NULL notes: {result['null_notes']:,}")
print(f"  Year range: {result['min_year']} - {result['max_year']}")
print(f"  Unique regions: {result['unique_regions']}")

# Sample data for Duisburg
print("\n" + "="*100)
print(" Sample Data: Duisburg Employment (2020-2024)")
print("="*100 + "\n")

sample_query = """
SELECT
    g.region_name,
    t.year,
    f.value,
    f.gender,
    f.nationality,
    f.notes,
    f.data_quality_flag
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 2
AND g.region_code = '05112'
AND t.year >= 2020
ORDER BY t.year DESC
LIMIT 10
"""

samples = db.execute_query(sample_query)

if samples:
    print(f"{'Year':<6} {'Value':>12} {'Gender':<10} {'Nationality':<12} {'Notes':<30}")
    print("-"*100)
    for row in samples:
        year = row['year']
        value = f"{row['value']:,.0f}" if row['value'] else 'NULL'
        gender = row['gender'] or 'NULL'
        nationality = row['nationality'] or 'NULL'
        notes = row['notes'] or 'NULL'

        print(f"{year:<6} {value:>12} {gender:<10} {nationality:<12} {notes:<30}")
else:
    print("No data found for Duisburg")

# Check notes field status
print("\n" + "="*100)
print(" Notes Field Status")
print("="*100 + "\n")

if result['null_notes'] == 0:
    print("[OK] All records have notes field populated!")
else:
    print(f"[WARNING] {result['null_notes']} records have NULL notes")

# Verify against total from table 9 (sector employment)
print("\n" + "="*100)
print(" Cross-Validation: Compare with Sector Employment Total")
print("="*100 + "\n")

# Get 2024 Duisburg employment from this table (indicator 2)
workplace_query = """
SELECT f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 2
AND g.region_code = '05112'
AND t.year = 2024
"""

workplace_result = db.execute_query(workplace_query)

# Get 2024 Duisburg total from sector table (indicator 9)
sector_query = """
SELECT f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 9
AND g.region_code = '05112'
AND t.year = 2024
AND f.notes LIKE '%Insgesamt%'
"""

sector_result = db.execute_query(sector_query)

if workplace_result and sector_result:
    workplace_val = workplace_result[0]['value']
    sector_val = sector_result[0]['value']

    print(f"Duisburg 2024 Employment:")
    print(f"  From table 13111-01-03-4 (workplace basic): {workplace_val:,.0f}")
    print(f"  From table 13111-07-05-4 (sector total):    {sector_val:,.0f}")

    if abs(workplace_val - sector_val) < 100:
        print("\n[OK] Values match! Both tables show consistent employment data.")
    else:
        diff = abs(workplace_val - sector_val)
        pct = (diff / sector_val) * 100
        print(f"\n[INFO] Difference: {diff:,.0f} ({pct:.1f}%)")
        print("       This may be due to different breakdowns or reference dates.")

print("\n" + "="*100 + "\n")

db.close()
