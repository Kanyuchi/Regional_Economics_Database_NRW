"""Show all available tables and their status"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

print("\n" + "="*100)
print(" ALL AVAILABLE TABLES - Current Status")
print("="*100 + "\n")

# Get all indicators
all_indicators = db.execute_query("""
    SELECT
        i.indicator_id,
        i.indicator_code,
        i.indicator_name,
        i.source_table_id,
        i.category,
        COUNT(f.fact_id) as record_count,
        MAX(f.loaded_at) as last_loaded
    FROM dim_indicator i
    LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
    GROUP BY i.indicator_id, i.indicator_code, i.indicator_name, i.source_table_id, i.category
    ORDER BY i.indicator_id
""")

# Group by category
categories = {}
for row in all_indicators:
    cat = row['category'] or 'uncategorized'
    if cat not in categories:
        categories[cat] = []
    categories[cat].append(row)

# Display by category
for category, indicators in sorted(categories.items()):
    print(f"\n{category.upper().replace('_', ' ')}")
    print("-"*100)

    for ind in indicators:
        ind_id = ind['indicator_id']
        name = ind['indicator_name'][:45] if ind['indicator_name'] else 'N/A'
        table_id = ind['source_table_id'] or 'N/A'
        count = ind['record_count'] or 0
        last_loaded = ind['last_loaded']

        # Determine status
        if count == 0:
            status = "[  NOT EXTRACTED  ]"
        elif last_loaded and last_loaded.date() == db.execute_query("SELECT CURRENT_DATE as d")[0]['d']:
            status = "[ DONE TODAY     ]"
        else:
            status = "[ EXTRACTED      ]"

        print(f"{status} ID:{ind_id:>2} | {table_id:<18} | {name:<45} | {count:>6,} records")

print("\n" + "="*100)
print("\nSUMMARY:")
print("-"*100)

# Count by status
total = len(all_indicators)
extracted = sum(1 for ind in all_indicators if ind['record_count'] and ind['record_count'] > 0)
not_extracted = total - extracted

print(f"  Total indicators defined: {total}")
print(f"  Extracted: {extracted}")
print(f"  Not extracted: {not_extracted}")
print(f"  Progress: {extracted/total*100:.1f}%")

print("\n" + "="*100)
print("\nNEXT RECOMMENDATION:")
print("-"*100)

# Find first unextracted table
for ind in all_indicators:
    if ind['record_count'] == 0 or ind['record_count'] is None:
        print(f"\n  Table: {ind['source_table_id']}")
        print(f"  Name: {ind['indicator_name']}")
        print(f"  Indicator ID: {ind['indicator_id']}")
        print(f"  Category: {ind['category']}")

        # Check if pipeline exists
        pipeline_name = f"etl_{ind['source_table_id'].replace('-', '_')}_*.py" if ind['source_table_id'] else None
        if pipeline_name:
            import glob
            matches = glob.glob(f"pipelines/**/{pipeline_name}", recursive=True)
            if matches:
                print(f"\n  Pipeline found: {matches[0]}")
                print(f"  Run: python {matches[0]}")
            else:
                print(f"\n  [WARNING] No pipeline found for this table yet")
                print(f"           Need to create pipeline first")
        break

print("\n" + "="*100 + "\n")

db.close()
