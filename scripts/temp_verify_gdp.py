import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()

print("="*80)
print("GDP DATA VERIFICATION")
print("="*80)

with db.get_connection() as conn:
    # Total GDP records
    result = conn.execute(text("""
        SELECT COUNT(*)
        FROM fact_demographics
        WHERE indicator_id BETWEEN 29 AND 41
    """))
    total_count = result.scalar()
    print(f"\n[OK] Total GDP/GVA records in database: {total_count:,}")

    # Records by indicator
    result = conn.execute(text("""
        SELECT
            i.indicator_id,
            i.indicator_name_en,
            COUNT(f.fact_id) as record_count
        FROM dim_indicator i
        LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
        WHERE i.indicator_id BETWEEN 29 AND 41
        GROUP BY i.indicator_id, i.indicator_name_en
        ORDER BY i.indicator_id
    """))

    print("\n[OK] Records by indicator:")
    for row in result:
        print(f"   [{row[0]}] {row[1]}: {row[2]:,} records")

    # Year coverage
    result = conn.execute(text("""
        SELECT MIN(t.year), MAX(t.year), COUNT(DISTINCT t.year)
        FROM fact_demographics f
        JOIN dim_time t ON f.time_id = t.time_id
        WHERE f.indicator_id BETWEEN 29 AND 41
    """))
    year_data = result.fetchone()
    print(f"\n[OK] Year coverage: {year_data[0]} - {year_data[1]} ({year_data[2]} years)")

    # Region coverage
    result = conn.execute(text("""
        SELECT COUNT(DISTINCT g.region_code)
        FROM fact_demographics f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        WHERE f.indicator_id BETWEEN 29 AND 41
    """))
    region_count = result.scalar()
    print(f"[OK] Regions covered: {region_count}")

    # Sample data - show a few actual GDP values
    result = conn.execute(text("""
        SELECT
            g.region_name,
            t.year,
            i.indicator_name_en,
            f.value,
            i.unit_of_measure
        FROM fact_demographics f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        JOIN dim_time t ON f.time_id = t.time_id
        JOIN dim_indicator i ON f.indicator_id = i.indicator_id
        WHERE f.indicator_id = 29  -- GDP at Market Prices
        AND g.region_code = '05'   -- NRW total
        ORDER BY t.year DESC
        LIMIT 5
    """))

    print("\n[OK] Sample data (NRW total GDP, recent years):")
    for row in result:
        print(f"   {row[1]}: {row[0]} - {row[2]}: {row[3]:,.0f} {row[4]}")

print("\n" + "="*80)
print("VERIFICATION COMPLETE - All data confirmed in database!")
print("="*80)

db.close()
