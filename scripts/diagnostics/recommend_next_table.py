"""Recommend the next table to extract"""
import sys
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

print("\n" + "="*100)
print(" NEXT TABLE RECOMMENDATION")
print("="*100 + "\n")

# Check what's been extracted today
print("TODAY'S PROGRESS:")
print("-"*100)

today_query = """
SELECT
    indicator_id,
    COUNT(*) as records,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.loaded_at > CURRENT_DATE
GROUP BY indicator_id
ORDER BY MAX(f.loaded_at)
"""

today_results = db.execute_query(today_query)

if today_results:
    for row in today_results:
        ind_id = row['indicator_id']
        records = row['records']
        years = f"{row['min_year']}-{row['max_year']}"
        print(f"  [COMPLETED] Indicator {ind_id}: {records:,} records ({years})")
else:
    print("  No extractions today yet")

# List of planned employment tables in order
employment_tables = [
    {"id": 9, "table": "13111-07-05-4", "name": "Employment by sector", "pipeline": "etl_13111_07_05_4_employment_sector.py"},
    {"id": 2, "table": "13111-01-03-4", "name": "Employment at workplace", "pipeline": "etl_13111_01_03_4_employment.py"},
    {"id": 3, "table": "13111-03-02-4", "name": "Employment by scope (workplace)", "pipeline": "etl_13111_03_02_4_employment_scope.py"},
    {"id": 4, "table": "13111-11-04-4", "name": "Employment by qualification (workplace)", "pipeline": "etl_13111_11_04_4_employment_qualification.py"},
    {"id": 5, "table": "13111-02-02-4", "name": "Employment at residence", "pipeline": "etl_13111_02_02_4_employment_residence.py"},
    {"id": 6, "table": "13111-04-02-4", "name": "Employment at residence by scope", "pipeline": "etl_13111_04_02_4_employment_residence_scope.py"},
    {"id": 7, "table": "13111-12-03-4", "name": "Employment at residence by qualification", "pipeline": "etl_13111_12_03_4_employment_residence_qualification.py"},
]

print("\n" + "="*100)
print(" EMPLOYMENT TABLES STATUS")
print("="*100 + "\n")

# Check each table
for table_info in employment_tables:
    ind_id = table_info['id']

    # Check if extracted
    check_query = f"""
    SELECT COUNT(*) as cnt,
           COUNT(notes) as notes_cnt,
           MAX(loaded_at) as last_loaded
    FROM fact_demographics
    WHERE indicator_id = {ind_id}
    """

    result = db.execute_query(check_query)[0]
    cnt = result['cnt']
    notes_cnt = result['notes_cnt']
    last_loaded = result['last_loaded']

    table_id = table_info['table']
    name = table_info['name']
    pipeline = table_info['pipeline']

    if cnt == 0:
        status = "[NOT EXTRACTED]"
        print(f"{status:<20} {table_id:<18} - {name}")
        print(f"{'':20} Run: python pipelines/regional_db/{pipeline}")
    elif last_loaded and last_loaded.date() == db.execute_query("SELECT CURRENT_DATE as d")[0]['d']:
        status = "[DONE TODAY]"
        print(f"{status:<20} {table_id:<18} - {name} ({cnt:,} records)")
    elif notes_cnt == 0 and cnt > 0:
        status = "[NEEDS RE-EXTRACT]"
        print(f"{status:<20} {table_id:<18} - {name} ({cnt:,} records, NULL notes)")
    else:
        status = "[OK]"
        print(f"{status:<20} {table_id:<18} - {name} ({cnt:,} records)")

    print()

print("="*100)
print("\nRECOMMENDATION:")
print("-"*100)

# Find first table that needs extraction (prefer new extractions over re-extractions)
for table_info in employment_tables:
    ind_id = table_info['id']
    result = db.execute_query(f"SELECT COUNT(*) as cnt FROM fact_demographics WHERE indicator_id = {ind_id}")[0]

    if result['cnt'] == 0:
        print(f"\nNEXT: {table_info['table']} - {table_info['name']}")
        print(f"\nRun this command:")
        print(f"  python pipelines/regional_db/{table_info['pipeline']}")
        print("\nExpected:")
        print(f"  - Duration: ~15-20 minutes (17 years of data)")
        print(f"  - Records: ~800-4,000 depending on breakdowns")
        print(f"  - Years: 2008-2024")
        break
else:
    print("\nAll employment tables have been extracted!")
    print("Consider re-extracting tables with NULL notes or moving to other categories.")

print("\n" + "="*100 + "\n")

db.close()
