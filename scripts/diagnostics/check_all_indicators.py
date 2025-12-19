"""Check all indicators in database"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

query = """
SELECT
    i.indicator_id,
    i.indicator_name,
    COUNT(f.fact_id) as record_count,
    MAX(f.loaded_at) as last_loaded
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
GROUP BY i.indicator_id, i.indicator_name
ORDER BY i.indicator_id
"""

results = db.execute_query(query)

print("\nAll Indicators and Record Counts:")
print("="*80)

for row in results:
    ind_id = row['indicator_id']
    name = row['indicator_name'][:40] if row['indicator_name'] else 'N/A'
    count = row['record_count'] or 0
    last_loaded = row['last_loaded'].strftime('%Y-%m-%d %H:%M:%S') if row['last_loaded'] else 'Never'

    print(f"{ind_id:>3} | {name:<40} | {count:>6,} | {last_loaded}")

# Also check recent extractions
print("\n" + "="*80)
print("Recent Data Loads (last hour):")
print("="*80)

recent_query = """
SELECT
    indicator_id,
    COUNT(*) as cnt,
    MAX(loaded_at) as last_loaded,
    MIN(loaded_at) as first_loaded
FROM fact_demographics
WHERE loaded_at > NOW() - INTERVAL '1 hour'
GROUP BY indicator_id
ORDER BY last_loaded DESC
"""

recent = db.execute_query(recent_query)

if recent:
    for row in recent:
        print(f"Indicator {row['indicator_id']}: {row['cnt']:,} records loaded between {row['first_loaded'].strftime('%H:%M:%S')} and {row['last_loaded'].strftime('%H:%M:%S')}")
else:
    print("No records loaded in the last hour")

db.close()
