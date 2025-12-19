"""Query employment by sector data for Duisburg"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

try:
    db = get_database('regional_economics')

    # First, find Duisburg's region code
    region_query = """
    SELECT geo_id, region_code, region_name
    FROM dim_geography
    WHERE region_name LIKE '%Duisburg%'
    AND is_active = TRUE
    """

    region_results = db.execute_query(region_query)

    if region_results:
        print("\n=== Duisburg Region Info ===")
        for r in region_results:
            print(f"Region Code: {r['region_code']}")
            print(f"Region Name: {r['region_name']}")
            print(f"Geo ID: {r['geo_id']}")
            geo_id = r['geo_id']
            region_code = r['region_code']

        # Query employment by sector data for Duisburg
        data_query = """
        SELECT
            g.region_name,
            g.region_code,
            t.year,
            t.reference_date,
            f.value,
            f.notes,
            i.indicator_name,
            f.data_quality_flag,
            f.extracted_at
        FROM fact_demographics f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_indicator i ON f.indicator_id = i.indicator_id
        WHERE f.indicator_id = 9
        AND g.region_code = :region_code
        ORDER BY t.year DESC, f.notes
        LIMIT 50
        """

        results = db.execute_query(data_query, {'region_code': region_code})

        if results:
            print(f"\n=== Employment by Sector Data for Duisburg (Latest Years) ===")
            print(f"Total records found: {len(results)}")
            print("\n")

            current_year = None
            for row in results:
                if row['year'] != current_year:
                    current_year = row['year']
                    print(f"\n--- Year {row['year']} (Reference: {row['reference_date']}) ---")

                print(f"  {row['notes']:<70} | Value: {row['value']:>10,.0f}")
        else:
            print(f"\nNo data found for Duisburg (region code: {region_code})")

        # Also provide the raw SQL for user to use directly
        print("\n\n=== SQL Query (for direct use in psql or other tools) ===")
        print(f"""
SELECT
    g.region_name,
    g.region_code,
    t.year,
    t.reference_date,
    f.value,
    f.notes,
    i.indicator_name,
    f.data_quality_flag
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id = 9
AND g.region_code = '{region_code}'
ORDER BY t.year DESC, f.notes;
""")
    else:
        print("\nDuisburg not found in dim_geography table!")

    db.close()

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
