"""Check which source tables have been extracted"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

# Check data_extraction_log to see what's been extracted
log_query = """
SELECT
    source_system,
    indicator_code,
    MAX(extraction_timestamp) as last_extraction,
    SUM(records_extracted) as total_records,
    MAX(status) as last_status
FROM data_extraction_log
WHERE status = 'success'
GROUP BY source_system, indicator_code
ORDER BY indicator_code
"""

print("\n" + "="*100)
print(" Extraction History (from data_extraction_log)")
print("="*100 + "\n")

results = db.execute_query(log_query)

if results:
    print(f"{'Table/Indicator Code':<30} {'Last Extraction':<25} {'Records':<12} {'Status':<10}")
    print("-"*100)

    for row in results:
        code = row['indicator_code'] or 'N/A'
        last = row['last_extraction'].strftime('%Y-%m-%d %H:%M') if row['last_extraction'] else 'N/A'
        records = row['total_records'] or 0
        status = row['last_status'] or 'N/A'

        print(f"{code:<30} {last:<25} {records:>10,} {status:<10}")

print("\n" + "="*100)

# Also check actual data in fact_demographics
fact_query = """
SELECT
    i.source_table_id,
    i.indicator_name,
    COUNT(*) as record_count,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY i.source_table_id, i.indicator_name
ORDER BY i.source_table_id
"""

print("\n Actual Data in fact_demographics (by source table)")
print("="*100 + "\n")

results2 = db.execute_query(fact_query)

current_table = None
table_total = 0

for row in results2:
    table_id = row['source_table_id']

    if table_id != current_table:
        if current_table:
            print(f"  {'SUBTOTAL':<60} {table_total:>10,}")
            print("-"*100)
        current_table = table_id
        table_total = 0
        print(f"\nTable: {table_id}")
        print("-"*100)

    indicator = row['indicator_name'][:55] if row['indicator_name'] else 'N/A'
    count = row['record_count']
    years = f"{row['min_year']}-{row['max_year']}"

    print(f"  {indicator:<60} {count:>10,}  ({years})")
    table_total += count

if current_table:
    print(f"  {'SUBTOTAL':<60} {table_total:>10,}")

print("\n" + "="*100)

# Count unique source tables
unique_tables = db.execute_query("""
    SELECT COUNT(DISTINCT i.source_table_id) as table_count
    FROM fact_demographics f
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    WHERE i.source_table_id IS NOT NULL
""")

print(f"\nTotal unique source tables with data: {unique_tables[0]['table_count']}")
print(f"Total records in fact_demographics: {db.execute_query('SELECT COUNT(*) as cnt FROM fact_demographics')[0]['cnt']:,}")

print("="*100 + "\n")

db.close()
