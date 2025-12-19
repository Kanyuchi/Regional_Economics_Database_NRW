"""Check which tables have been extracted"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

# Get all indicators with data
query = """
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name,
    i.source_table_id,
    COUNT(f.fact_id) as record_count,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
LEFT JOIN dim_time t ON f.time_id = t.time_id
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name, i.source_table_id
ORDER BY i.indicator_id
"""

results = db.execute_query(query)

print("\n" + "="*100)
print(" Current Data Extraction Status")
print("="*100 + "\n")

print(f"{'ID':<4} {'Table ID':<20} {'Indicator Name':<40} {'Records':<12} {'Years':<15}")
print("-"*100)

for row in results:
    indicator_id = row['indicator_id']
    table_id = row['source_table_id'] or 'N/A'
    name = row['indicator_name'][:38] if row['indicator_name'] else 'N/A'
    count = row['record_count'] or 0

    if count > 0:
        years = f"{row['min_year']}-{row['max_year']}"
        status = "[Y]"
    else:
        years = "Not extracted"
        status = "[N]"

    print(f"{status} {indicator_id:<3} {table_id:<20} {name:<40} {count:>10,} {years:<15}")

print("\n" + "="*100)

# List available pipelines
print("\nAvailable Pipelines:")
print("-"*100)

pipelines = {
    'etl_12411_03_03_4_population.py': 'Population by age, gender, nationality',
    'etl_13111_01_03_4_employment.py': 'Employment at workplace by gender/nationality',
    'etl_13111_02_02_4_employment_residence.py': 'Employment at residence by gender/nationality',
    'etl_13111_03_02_4_employment_scope.py': 'Employment by scope (full-time/part-time)',
    'etl_13111_04_02_4_employment_residence_scope.py': 'Employment at residence by scope',
    'etl_13111_07_05_4_employment_sector.py': 'Employment by economic sector (COMPLETED)',
    'etl_13111_11_04_4_employment_qualification.py': 'Employment by qualification',
    'etl_13111_12_03_4_employment_residence_qualification.py': 'Employment at residence by qualification'
}

for pipeline, description in pipelines.items():
    print(f"  * {pipeline:<60} - {description}")

print("\n" + "="*100)
print("\nRECOMMENDATION: Next table to extract")
print("-"*100)

# Suggest next table based on what's missing
extracted_ids = [r['indicator_id'] for r in results if r['record_count'] and r['record_count'] > 0]

if 1 not in extracted_ids:
    print(">> Table 12411-03-03-4: Population data (Indicator ID 1)")
    print("   Run: python pipelines/regional_db/etl_12411_03_03_4_population.py")
elif 2 not in extracted_ids:
    print(">> Table 13111-01-03-4: Employment at workplace (Indicator ID 2)")
    print("   Run: python pipelines/regional_db/etl_13111_01_03_4_employment.py")
elif 3 not in extracted_ids:
    print(">> Table 13111-03-02-4: Employment by scope (Indicator ID 3)")
    print("   Run: python pipelines/regional_db/etl_13111_03_02_4_employment_scope.py")
elif 4 not in extracted_ids:
    print(">> Table 13111-11-04-4: Employment by qualification (Indicator ID 4)")
    print("   Run: python pipelines/regional_db/etl_13111_11_04_4_employment_qualification.py")
elif 5 not in extracted_ids:
    print(">> Table 13111-02-02-4: Employment at residence (Indicator ID 5)")
    print("   Run: python pipelines/regional_db/etl_13111_02_02_4_employment_residence.py")
else:
    print(">> All main tables have been extracted!")

print("="*100 + "\n")

db.close()
