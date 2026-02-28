"""
Query NRW State-Level Population History (1975-2024)
Regional Economics Database for NRW

This script provides easy access to 50 years of NRW population data
from the State Database (table 12411-9k06, indicators 67-71).

Usage:
    python scripts/analysis/query_nrw_population_history.py [--query QUERY_NAME] [--output FORMAT]

Examples:
    # Show all available queries
    python scripts/analysis/query_nrw_population_history.py --list

    # Run total population query
    python scripts/analysis/query_nrw_population_history.py --query total

    # Export gender distribution to CSV
    python scripts/analysis/query_nrw_population_history.py --query gender --output csv
"""

import sys
import argparse
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.database import get_database
from utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


QUERIES = {
    "total": {
        "name": "NRW Total Population (1975-2024)",
        "description": "Year-by-year total population with growth rates",
        "sql": """
            SELECT
                t.year,
                f.value as total_population,
                LAG(f.value) OVER (ORDER BY t.year) as previous_year,
                f.value - LAG(f.value) OVER (ORDER BY t.year) as absolute_change,
                ROUND(
                    (f.value - LAG(f.value) OVER (ORDER BY t.year))::numeric /
                    NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100,
                    2
                ) as percent_change
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id = 67
              AND g.region_code = '05'
              AND f.age_group = 'total'
            ORDER BY t.year;
        """
    },
    "gender": {
        "name": "Gender Distribution (Decade Snapshots)",
        "description": "Male vs Female population at key decades",
        "sql": """
            SELECT
                t.year,
                MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
                MAX(CASE WHEN f.indicator_id = 68 THEN f.value END) as male_population,
                MAX(CASE WHEN f.indicator_id = 69 THEN f.value END) as female_population,
                ROUND(
                    MAX(CASE WHEN f.indicator_id = 68 THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
                    2
                ) as percent_male,
                ROUND(
                    MAX(CASE WHEN f.indicator_id = 69 THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
                    2
                ) as percent_female
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id IN (67, 68, 69)
              AND g.region_code = '05'
              AND f.age_group = 'total'
              AND t.year IN (1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024)
            GROUP BY t.year
            ORDER BY t.year;
        """
    },
    "foreign": {
        "name": "Foreign Population Growth (1975-2024)",
        "description": "Immigration trends over 50 years",
        "sql": """
            SELECT
                t.year,
                MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
                MAX(CASE WHEN f.indicator_id = 70 THEN f.value END) as german_population,
                MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_population,
                ROUND(
                    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
                    2
                ) as foreign_percentage
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id IN (67, 70, 71)
              AND g.region_code = '05'
              AND f.age_group = 'total'
            GROUP BY t.year
            ORDER BY t.year;
        """
    },
    "age_groups": {
        "name": "Age Group Distribution (2024)",
        "description": "Current population breakdown by age",
        "sql": """
            SELECT
                COALESCE(f.age_group, 'Unknown') as age_group,
                MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total,
                MAX(CASE WHEN f.indicator_id = 68 THEN f.value END) as male,
                MAX(CASE WHEN f.indicator_id = 69 THEN f.value END) as female,
                MAX(CASE WHEN f.indicator_id = 70 THEN f.value END) as german,
                MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_pop
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id BETWEEN 67 AND 71
              AND g.region_code = '05'
              AND t.year = 2024
            GROUP BY f.age_group
            ORDER BY
                CASE f.age_group
                    WHEN 'total' THEN 0
                    WHEN 'under_6' THEN 1
                    WHEN '6_to_18' THEN 2
                    WHEN '18_to_25' THEN 3
                    WHEN '25_to_30' THEN 4
                    WHEN '30_to_40' THEN 5
                    WHEN '40_to_50' THEN 6
                    WHEN '50_to_60' THEN 7
                    WHEN '60_to_65' THEN 8
                    WHEN '65_plus' THEN 9
                    ELSE 99
                END;
        """
    },
    "milestones": {
        "name": "Historical Milestones",
        "description": "Population at key historical moments",
        "sql": """
            SELECT
                t.year,
                CASE t.year
                    WHEN 1975 THEN 'Post Oil Crisis'
                    WHEN 1980 THEN 'Economic Recession'
                    WHEN 1990 THEN 'German Reunification'
                    WHEN 2000 THEN 'New Millennium'
                    WHEN 2008 THEN 'Financial Crisis'
                    WHEN 2015 THEN 'Refugee Crisis'
                    WHEN 2020 THEN 'COVID-19 Pandemic'
                    WHEN 2024 THEN 'Current'
                END as milestone,
                MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
                MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_population,
                ROUND(
                    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
                    2
                ) as foreign_pct
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id IN (67, 71)
              AND g.region_code = '05'
              AND f.age_group = 'total'
              AND t.year IN (1975, 1980, 1990, 2000, 2008, 2015, 2020, 2024)
            GROUP BY t.year
            ORDER BY t.year;
        """
    },
    "aging": {
        "name": "Aging Population Analysis",
        "description": "Youth vs elderly population trends",
        "sql": """
            SELECT
                t.year,
                MAX(CASE WHEN f.age_group = 'total' THEN f.value END) as total_pop,
                MAX(CASE WHEN f.age_group IN ('under_6', '6_to_18') THEN f.value END) as youth_pop,
                MAX(CASE WHEN f.age_group = '65_plus' THEN f.value END) as elderly_pop,
                ROUND(
                    MAX(CASE WHEN f.age_group IN ('under_6', '6_to_18') THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.age_group = 'total' THEN f.value END), 0) * 100,
                    2
                ) as youth_percentage,
                ROUND(
                    MAX(CASE WHEN f.age_group = '65_plus' THEN f.value END)::numeric /
                    NULLIF(MAX(CASE WHEN f.age_group = 'total' THEN f.value END), 0) * 100,
                    2
                ) as elderly_percentage
            FROM fact_demographics f
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_geography g ON f.geo_id = g.geo_id
            WHERE f.indicator_id = 67
              AND g.region_code = '05'
              AND t.year % 5 = 0
            GROUP BY t.year
            ORDER BY t.year;
        """
    }
}


def list_queries():
    """List all available queries."""
    print("\n" + "="*80)
    print("AVAILABLE QUERIES - NRW POPULATION HISTORY (1975-2024)")
    print("="*80)
    for key, info in QUERIES.items():
        print(f"\n{key}")
        print(f"  Name: {info['name']}")
        print(f"  Description: {info['description']}")
    print("\n" + "="*80)


def run_query(query_name: str, output_format: str = "table"):
    """
    Run a specific query and display results.

    Args:
        query_name: Name of the query to run
        output_format: Output format ('table', 'csv', or 'json')
    """
    if query_name not in QUERIES:
        logger.error(f"Unknown query: {query_name}")
        logger.info("Use --list to see available queries")
        return

    query_info = QUERIES[query_name]

    logger.info("=" * 80)
    logger.info(f"QUERY: {query_info['name']}")
    logger.info(f"Description: {query_info['description']}")
    logger.info("=" * 80)

    # Execute query
    db = get_database('regional_economics')
    results = db.execute_query(query_info['sql'])
    db.close()

    if not results:
        logger.warning("No results returned")
        return

    # Convert to DataFrame
    df = pd.DataFrame(results)

    # Format numbers for display
    for col in df.columns:
        if 'population' in col or col in ['total', 'male', 'female', 'german', 'foreign_pop']:
            df[col] = df[col].apply(lambda x: f"{int(x):,}" if pd.notna(x) and x != 0 else x)

    # Output results
    if output_format == "table":
        print("\n" + df.to_string(index=False))
        print(f"\nTotal rows: {len(df)}")

    elif output_format == "csv":
        output_file = PROJECT_ROOT / "analysis" / "outputs" / f"nrw_{query_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to: {output_file}")

    elif output_format == "json":
        output_file = PROJECT_ROOT / "analysis" / "outputs" / f"nrw_{query_name}_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.json"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(output_file, orient='records', indent=2)
        logger.info(f"Results saved to: {output_file}")

    print("\n" + "="*80)


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(
        description="Query NRW state-level population history (1975-2024)",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--list',
        action='store_true',
        help='List all available queries'
    )

    parser.add_argument(
        '--query',
        type=str,
        choices=list(QUERIES.keys()),
        help='Name of query to run'
    )

    parser.add_argument(
        '--output',
        type=str,
        choices=['table', 'csv', 'json'],
        default='table',
        help='Output format (default: table)'
    )

    args = parser.parse_args()

    if args.list:
        list_queries()
    elif args.query:
        run_query(args.query, args.output)
    else:
        parser.print_help()
        print("\n" + "="*80)
        print("QUICK START:")
        print("  python scripts/analysis/query_nrw_population_history.py --list")
        print("  python scripts/analysis/query_nrw_population_history.py --query total")
        print("="*80)


if __name__ == "__main__":
    main()
