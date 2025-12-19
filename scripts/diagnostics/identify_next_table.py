"""Identify which tables need to be extracted/re-extracted"""
import sys
from pathlib import Path
import os

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

# Get list of all indicators with their expected pipelines
expected_extractions = {
    1: {'table': '12411-03-03-4', 'pipeline': 'etl_12411_03_03_4_population.py', 'name': 'Population'},
    2: {'table': '13111-01-03-4', 'pipeline': 'etl_13111_01_03_4_employment.py', 'name': 'Employment at workplace'},
    3: {'table': '13111-03-02-4', 'pipeline': 'etl_13111_03_02_4_employment_scope.py', 'name': 'Employment by scope (workplace)'},
    4: {'table': '13111-11-04-4', 'pipeline': 'etl_13111_11_04_4_employment_qualification.py', 'name': 'Employment by qualification (workplace)'},
    5: {'table': '13111-02-02-4', 'pipeline': 'etl_13111_02_02_4_employment_residence.py', 'name': 'Employment at residence'},
    6: {'table': '13111-04-02-4', 'pipeline': 'etl_13111_04_02_4_employment_residence_scope.py', 'name': 'Employment at residence by scope'},
    7: {'table': '13111-12-03-4', 'pipeline': 'etl_13111_12_03_4_employment_residence_qualification.py', 'name': 'Employment at residence by qualification'},
    9: {'table': '13111-07-05-4', 'pipeline': 'etl_13111_07_05_4_employment_sector.py', 'name': 'Employment by sector'},
}

# Check which indicators have data
query = """
SELECT
    indicator_id,
    COUNT(*) as record_count,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT notes) as unique_notes,
    COUNT(CASE WHEN notes IS NULL THEN 1 END) as null_notes
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY indicator_id
ORDER BY indicator_id
"""

results = db.execute_query(query)

print("\n" + "="*100)
print(" EXTRACTION STATUS - Employment Tables from Yesterday's Session")
print("="*100 + "\n")

print(f"{'ID':<4} {'Table':<18} {'Status':<20} {'Records':<10} {'Years':<12} {'Notes':<20}")
print("-"*100)

completed_today = 9  # We just re-extracted this one

for ind_id, info in expected_extractions.items():
    # Find data for this indicator
    data = next((r for r in results if r['indicator_id'] == ind_id), None)

    table = info['table']
    name = info['name'][:35]

    if data:
        records = data['record_count']
        years = f"{data['min_year']}-{data['max_year']}"
        null_notes = data['null_notes']
        unique_notes = data['unique_notes']

        if ind_id == completed_today:
            status = "[DONE TODAY]"
            notes_status = f"{unique_notes} sectors" if unique_notes > 1 else "OK"
        elif null_notes > 0:
            status = "[NEEDS RE-EXTRACT]"
            notes_status = f"{null_notes} NULL notes!"
        else:
            status = "[OK]"
            notes_status = f"{unique_notes} notes"

        print(f"{ind_id:<4} {table:<18} {status:<20} {records:>8,} {years:<12} {notes_status:<20}")
    else:
        status = "[NOT EXTRACTED]"
        print(f"{ind_id:<4} {table:<18} {status:<20} {'0':>8} {'-':<12} {'-':<20}")

print("\n" + "="*100)
print("\nRECOMMENDATION:")
print("-"*100)

# Find first table that needs work
for ind_id, info in expected_extractions.items():
    if ind_id == completed_today:
        continue

    data = next((r for r in results if r['indicator_id'] == ind_id), None)

    if not data:
        print(f"\nNEXT: Extract {info['table']} - {info['name']}")
        print(f"Run: python pipelines/regional_db/{info['pipeline']}")
        break
    elif data['null_notes'] > 0:
        print(f"\nNEXT: Re-extract {info['table']} - {info['name']}")
        print(f"Reason: {data['null_notes']} records have NULL notes (missing sector/scope/qualification info)")
        print(f"Run: python pipelines/regional_db/{info['pipeline']}")
        print(f"\nNOTE: Will need to delete old data first:")
        print(f"  DELETE FROM fact_demographics WHERE indicator_id = {ind_id};")
        break

# Check if pipeline file exists
print("\n" + "="*100)
print("PIPELINE STATUS:")
print("-"*100)

for ind_id, info in expected_extractions.items():
    pipeline_path = Path(f"pipelines/regional_db/{info['pipeline']}")
    exists = "EXISTS" if pipeline_path.exists() else "MISSING"
    print(f"  [{exists}] {info['pipeline']}")

print("\n" + "="*100 + "\n")

db.close()
