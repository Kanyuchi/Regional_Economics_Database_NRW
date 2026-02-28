"""
Check Population Indicators and Data Availability
Regional Economics Database for NRW
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.database import get_database
from utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def main():
    """Check population indicator availability."""

    logger.info("=" * 80)
    logger.info("POPULATION INDICATORS - DATA AVAILABILITY CHECK")
    logger.info("=" * 80)

    db = get_database('regional_economics')

    # Query indicators
    query = """
    SELECT
        i.indicator_id,
        i.indicator_code,
        i.indicator_name_en,
        i.source_system,
        MIN(t.year) as min_year,
        MAX(t.year) as max_year,
        COUNT(DISTINCT t.year) as year_count,
        COUNT(DISTINCT f.geo_id) as region_count,
        COUNT(*) as total_records
    FROM dim_indicator i
    LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
    LEFT JOIN dim_time t ON f.time_id = t.time_id
    WHERE i.indicator_category = 'demographics'
    GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en, i.source_system
    ORDER BY i.indicator_id;
    """

    results = db.execute_query(query)

    print("\n" + "=" * 120)
    print(f"{'ID':<5} {'Code':<15} {'Name':<40} {'Source':<15} {'Years':<12} {'Regions':<8} {'Records':<10}")
    print("=" * 120)

    for row in results:
        years_range = f"{row['min_year']}-{row['max_year']}" if row['min_year'] else "No data"
        year_count = f"({row['year_count']})" if row['year_count'] else ""

        print(f"{row['indicator_id']:<5} "
              f"{row['indicator_code'] or 'N/A':<15} "
              f"{(row['indicator_name_en'] or 'N/A')[:39]:<40} "
              f"{row['source_system'] or 'N/A':<15} "
              f"{years_range:<7} {year_count:<4} "
              f"{row['region_count'] or 0:<8} "
              f"{row['total_records'] or 0:<10}")

    print("=" * 120)

    # Check specific city data
    print("\n" + "=" * 80)
    print("CHECKING SPECIFIC CITIES (Duisburg, Dortmund, Essen)")
    print("=" * 80)

    cities_query = """
    SELECT
        g.region_name,
        g.region_code,
        i.indicator_id,
        MIN(t.year) as min_year,
        MAX(t.year) as max_year,
        COUNT(*) as records
    FROM dim_geography g
    JOIN fact_demographics f ON g.geo_id = f.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    WHERE g.region_name IN ('Duisburg', 'Dortmund', 'Essen')
      AND i.indicator_category = 'demographics'
    GROUP BY g.region_name, g.region_code, i.indicator_id
    ORDER BY g.region_name, i.indicator_id;
    """

    city_results = db.execute_query(cities_query)

    if city_results:
        print(f"\n{'City':<15} {'Code':<10} {'Indicator':<12} {'Year Range':<15} {'Records':<10}")
        print("-" * 65)
        for row in city_results:
            print(f"{row['region_name']:<15} "
                  f"{row['region_code']:<10} "
                  f"{row['indicator_id']:<12} "
                  f"{row['min_year']}-{row['max_year']:<10} "
                  f"{row['records']:<10}")
    else:
        print("\n⚠️  No data found for Duisburg, Dortmund, or Essen!")

    db.close()

    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("""
Key Findings:
- Indicator 1 (Regional DB): District-level data, 2011-2024 available
- Indicators 67-71 (State DB): NRW state-level only, 1975-2024 available

For city-level historical data before 2011:
  → Not available in Regional Database (GENESIS limitation)
  → Regional DB restructured their data system in 2011

For NRW-wide historical data back to 1975:
  → Use indicators 67-71 from State Database
  → Query with geo_id for NRW state (not individual cities)
    """)


if __name__ == "__main__":
    main()
