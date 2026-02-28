"""
Database Statistics for CV/Portfolio
Regional Economics Database for NRW

Generates accurate statistics about the database for CV purposes.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.database import get_database
from utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def get_database_stats():
    """Get comprehensive database statistics."""

    db = get_database('regional_economics')

    print("\n" + "="*100)
    print("REGIONAL ECONOMICS DATABASE - COMPREHENSIVE STATISTICS")
    print("="*100)

    # 1. Count all indicators
    indicator_query = """
    SELECT
        indicator_category,
        COUNT(*) as indicator_count
    FROM dim_indicator
    WHERE is_active = TRUE
    GROUP BY indicator_category
    ORDER BY indicator_category;
    """

    indicator_results = db.execute_query(indicator_query)

    print("\n📊 INDICATORS BY CATEGORY:")
    print("-" * 80)
    total_indicators = 0
    for row in indicator_results:
        count = row['indicator_count']
        total_indicators += count
        print(f"  {row['indicator_category']:<30} {count:>5} indicators")
    print("-" * 80)
    print(f"  {'TOTAL INDICATORS':<30} {total_indicators:>5}")

    # 2. Count records in each fact table
    fact_tables = [
        'fact_demographics',
        'fact_labor_market',
        'fact_business_economy',
        'fact_public_finance',
        'fact_infrastructure',
        'fact_health'
    ]

    print("\n\n📈 RECORDS BY FACT TABLE:")
    print("-" * 80)
    total_records = 0

    for table in fact_tables:
        try:
            count_query = f"SELECT COUNT(*) as count FROM {table};"
            result = db.execute_query(count_query)
            count = result[0]['count'] if result else 0
            total_records += count
            print(f"  {table:<30} {count:>12,} records")
        except Exception as e:
            print(f"  {table:<30} {'Error':>12}")

    print("-" * 80)
    print(f"  {'TOTAL RECORDS':<30} {total_records:>12,}")

    # 3. Count geographies
    geo_query = """
    SELECT
        region_type,
        COUNT(*) as count
    FROM dim_geography
    WHERE is_active = TRUE
    GROUP BY region_type
    ORDER BY region_type;
    """

    geo_results = db.execute_query(geo_query)

    print("\n\n🗺️  GEOGRAPHIC COVERAGE:")
    print("-" * 80)
    total_regions = 0
    for row in geo_results:
        count = row['count']
        total_regions += count
        print(f"  {row['region_type']:<30} {count:>5} regions")
    print("-" * 80)
    print(f"  {'TOTAL REGIONS':<30} {total_regions:>5}")

    # 4. Time range
    time_query = """
    SELECT
        MIN(year) as min_year,
        MAX(year) as max_year,
        COUNT(DISTINCT year) as year_count
    FROM dim_time;
    """

    time_results = db.execute_query(time_query)
    time_row = time_results[0] if time_results else {}

    print("\n\n📅 TEMPORAL COVERAGE:")
    print("-" * 80)
    print(f"  Year Range:                   {time_row.get('min_year', 'N/A')} - {time_row.get('max_year', 'N/A')}")
    print(f"  Total Years:                  {time_row.get('year_count', 0)}")
    print(f"  Timespan:                     {time_row.get('max_year', 0) - time_row.get('min_year', 0) + 1} years")

    # 5. Data sources
    source_query = """
    SELECT
        source_system,
        COUNT(*) as indicator_count
    FROM dim_indicator
    GROUP BY source_system
    ORDER BY source_system;
    """

    source_results = db.execute_query(source_query)

    print("\n\n🔗 DATA SOURCES:")
    print("-" * 80)
    for row in source_results:
        print(f"  {row['source_system']:<30} {row['indicator_count']:>5} indicators")

    # 6. Most recent data
    recent_query = """
    SELECT
        i.indicator_category,
        MAX(t.year) as latest_year
    FROM fact_demographics f
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY i.indicator_category

    UNION ALL

    SELECT
        i.indicator_category,
        MAX(t.year) as latest_year
    FROM fact_labor_market f
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    JOIN dim_time t ON f.time_id = t.time_id
    GROUP BY i.indicator_category

    ORDER BY indicator_category;
    """

    recent_results = db.execute_query(recent_query)

    print("\n\n🆕 MOST RECENT DATA BY CATEGORY:")
    print("-" * 80)
    for row in recent_results:
        print(f"  {row['indicator_category']:<30} {row['latest_year']}")

    db.close()

    # Generate CV-ready statements
    print("\n\n" + "="*100)
    print("📝 CV-READY STATEMENTS:")
    print("="*100)

    print(f"""
Option 1 (Comprehensive):
    Built Regional Economics Database ({total_records:,} records, {total_indicators} indicators, {total_regions} regions)
    spanning {time_row.get('max_year', 0) - time_row.get('min_year', 0) + 1} years with exploratory analysis enabling empirical research for policy stakeholders

Option 2 (Impact-focused):
    Engineered Regional Economics Database integrating {total_indicators} economic indicators across {total_regions} NRW regions
    ({total_records:,}+ records, 1975-2024) enabling data-driven policy analysis for urban economic development

Option 3 (Technical):
    Developed end-to-end ETL pipeline for Regional Economics Database: {total_indicators} indicators, {total_records:,} records,
    50-year timespan (1975-2024) with PostgreSQL/PostGIS, Python, automated data quality validation

Option 4 (Achievement-oriented):
    Architected multi-source Regional Economics Database ({total_records:,} records from 3 government APIs) covering
    demographics, labor markets, and business economy across {total_regions} NRW regions for evidence-based policymaking

Option 5 (Concise):
    Built Regional Economics Database: {total_indicators} indicators, {total_records:,}+ records, {total_regions} regions, 50-year timespan (1975-2024)
    """)

    print("\n" + "="*100)
    print("💡 RECOMMENDED FOR CV:")
    print("="*100)
    print(f"""
    • Built Regional Economics Database ({total_records:,}+ records, {total_indicators} indicators, {total_regions} regions, 1975-2024)
      integrating demographics, labor market, and business data from 3 government APIs with automated
      ETL pipelines enabling empirical research for policy stakeholders

    OR (more concise):

    • Engineered Regional Economics Database with {total_indicators} indicators spanning 50 years ({total_records:,}+ records)
      across {total_regions} NRW regions, enabling data-driven urban economic policy analysis
    """)

    print("\n" + "="*100)


if __name__ == "__main__":
    get_database_stats()
