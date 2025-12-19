"""Verify notes status for table 13111-03-02-4 (Employment by scope)"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

db = get_database('regional_economics')

print("\n" + "="*100)
print(" Verification: Table 13111-03-02-4 (Employment by Scope)")
print("="*100 + "\n")

# First, find which indicator_id is used for this table
indicator_query = """
SELECT indicator_id, indicator_name, source_table_id
FROM dim_indicator
WHERE source_table_id = '13111-03-02-4'
"""

indicators = db.execute_query(indicator_query)

if indicators:
    print("INDICATOR MAPPING:")
    print("-"*100)
    for ind in indicators:
        print(f"  Indicator ID: {ind['indicator_id']}")
        print(f"  Name: {ind['indicator_name']}")
        print(f"  Source Table: {ind['source_table_id']}")
    print()

    indicator_id = indicators[0]['indicator_id']

    # Check the data for this indicator
    stats_query = f"""
    SELECT
        COUNT(*) as total_records,
        COUNT(notes) as with_notes,
        COUNT(*) - COUNT(notes) as null_notes,
        COUNT(DISTINCT notes) as unique_notes,
        MIN(t.year) as min_year,
        MAX(t.year) as max_year,
        COUNT(DISTINCT g.region_code) as unique_regions
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = {indicator_id}
    """

    result = db.execute_query(stats_query)[0]

    print("DATA STATISTICS:")
    print("-"*100)
    print(f"  Total records: {result['total_records']:,}")
    print(f"  Records with notes: {result['with_notes']:,}")
    print(f"  Records with NULL notes: {result['null_notes']:,}")
    print(f"  Unique note values: {result['unique_notes']}")
    print(f"  Year range: {result['min_year']} - {result['max_year']}")
    print(f"  Unique regions: {result['unique_regions']}")

    # Sample some records to see what the notes look like
    print("\n" + "="*100)
    print(" Sample Records (Duisburg, Recent Years)")
    print("="*100 + "\n")

    sample_query = f"""
    SELECT
        g.region_name,
        t.year,
        f.value,
        f.notes,
        f.loaded_at
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = {indicator_id}
    AND g.region_code = '05112'
    AND t.year >= 2020
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
            notes = (row['notes'] if row['notes'] else 'NULL')[:48]
            loaded = row['loaded_at'].strftime('%Y-%m-%d %H:%M:%S')

            print(f"{year:<6} {value:>12} {notes:<50} {loaded:<20}")
    else:
        print("No data found for Duisburg")

    # Show unique note values if any exist
    if result['unique_notes'] > 0:
        print("\n" + "="*100)
        print(" All Unique Note Values")
        print("="*100 + "\n")

        unique_notes_query = f"""
        SELECT DISTINCT notes
        FROM fact_demographics
        WHERE indicator_id = {indicator_id}
        AND notes IS NOT NULL
        ORDER BY notes
        """

        unique_notes = db.execute_query(unique_notes_query)

        if unique_notes:
            for i, row in enumerate(unique_notes, 1):
                print(f"  {i}. {row['notes']}")
        else:
            print("  (No non-NULL notes found)")

    # Verdict
    print("\n" + "="*100)
    print(" VERDICT")
    print("="*100 + "\n")

    if result['null_notes'] == result['total_records']:
        print("  [NEEDS RE-EXTRACTION]")
        print(f"  ALL {result['total_records']:,} records have NULL notes!")
        print("  The scope information (full-time/part-time) is missing.")
        print("\n  Action: Delete and re-extract with fixed loader")
    elif result['null_notes'] > 0:
        print("  [PARTIALLY INCOMPLETE]")
        print(f"  {result['null_notes']:,} out of {result['total_records']:,} records have NULL notes")
        pct = (result['null_notes'] / result['total_records']) * 100
        print(f"  ({pct:.1f}% missing scope information)")
    else:
        print("  [OK]")
        print("  All records have notes field populated!")
        print("  Scope information is present.")

else:
    print("ERROR: Table 13111-03-02-4 not found in dim_indicator!")
    print("\nThis might mean:")
    print("  1. The table hasn't been configured in dim_indicator yet")
    print("  2. The source_table_id mapping is different")
    print("\nLet me check what source_table_id values exist...")

    all_tables = db.execute_query("""
        SELECT DISTINCT source_table_id
        FROM dim_indicator
        WHERE source_table_id IS NOT NULL
        ORDER BY source_table_id
    """)

    print("\nAvailable source_table_id values:")
    for t in all_tables:
        print(f"  - {t['source_table_id']}")

print("\n" + "="*100 + "\n")

db.close()
