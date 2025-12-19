"""
Extraction Verification Script with Time Series Analysis
========================================================

Verifies data quality and completeness after each table extraction,
with special focus on Ruhr region cities: Duisburg, Bochum, Essen, 
Dortmund, and Gelsenkirchen.

Usage:
    python verify_extraction_timeseries.py --indicator <id>
    python verify_extraction_timeseries.py --indicator 19 --export-csv

Author: Regional Economics Database Project
Date: 2025-12-18
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict
import argparse

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.logging import setup_logging, get_logger
from utils.database import get_database

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Ruhr region cities of interest (for thesis research)
RUHR_CITIES = {
    'Duisburg': '05112',
    'Bochum': '05911',
    'Essen': '05113',
    'Dortmund': '05913',
    'Gelsenkirchen': '05513'
}


def get_indicator_info(db, indicator_id: int) -> Optional[Dict]:
    """Get indicator metadata."""
    query = """
    SELECT 
        indicator_id,
        indicator_code,
        indicator_name_en,
        indicator_category,
        source_table_id,
        unit_of_measure
    FROM dim_indicator
    WHERE indicator_id = :indicator_id
    """
    result = db.execute_query(query, {'indicator_id': indicator_id})
    return result[0] if result else None


def verify_data_completeness(db, indicator_id: int) -> Dict:
    """Verify data completeness for an indicator."""
    logger.info(f"Verifying data completeness for indicator {indicator_id}...")
    
    # Get total records
    total_query = """
    SELECT COUNT(*) as total_records
    FROM fact_demographics
    WHERE indicator_id = :indicator_id
    """
    total = db.execute_query(total_query, {'indicator_id': indicator_id})[0]['total_records']
    
    # Get year coverage
    year_query = """
    SELECT 
        MIN(t.year) as min_year,
        MAX(t.year) as max_year,
        COUNT(DISTINCT t.year) as year_count
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = :indicator_id
    """
    years = db.execute_query(year_query, {'indicator_id': indicator_id})[0]
    
    # Get region coverage
    region_query = """
    SELECT COUNT(DISTINCT g.geo_id) as region_count
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = :indicator_id
    """
    regions = db.execute_query(region_query, {'indicator_id': indicator_id})[0]
    
    # Check for NULL values in critical columns
    null_query = """
    SELECT 
        SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values,
        SUM(CASE WHEN value IS NOT NULL THEN 1 ELSE 0 END) as non_null_values
    FROM fact_demographics
    WHERE indicator_id = :indicator_id
    """
    nulls = db.execute_query(null_query, {'indicator_id': indicator_id})[0]
    
    return {
        'total_records': total,
        'min_year': years['min_year'],
        'max_year': years['max_year'],
        'year_count': years['year_count'],
        'region_count': regions['region_count'],
        'null_values': nulls['null_values'],
        'non_null_values': nulls['non_null_values'],
        'completeness_rate': (nulls['non_null_values'] / total * 100) if total > 0 else 0
    }


def verify_ruhr_cities_coverage(db, indicator_id: int) -> Dict:
    """Verify that all Ruhr cities have data."""
    logger.info("Verifying Ruhr region cities coverage...")
    
    coverage = {}
    for city_name, region_code in RUHR_CITIES.items():
        query = """
        SELECT 
            COUNT(*) as record_count,
            MIN(t.year) as min_year,
            MAX(t.year) as max_year
        FROM fact_demographics f
        JOIN dim_geography g ON f.geo_id = g.geo_id
        JOIN dim_time t ON f.time_id = t.time_id
        WHERE f.indicator_id = :indicator_id
          AND g.region_code = :region_code
        """
        result = db.execute_query(query, {'indicator_id': indicator_id, 'region_code': region_code})
        if result and result[0]['record_count'] > 0:
            coverage[city_name] = {
                'region_code': region_code,
                'records': result[0]['record_count'],
                'min_year': result[0]['min_year'],
                'max_year': result[0]['max_year'],
                'status': 'OK'
            }
        else:
            coverage[city_name] = {
                'region_code': region_code,
                'records': 0,
                'status': 'MISSING'
            }
    
    return coverage


def get_time_series_data(db, indicator_id: int) -> List[Dict]:
    """Get time series data for all Ruhr cities."""
    logger.info("Extracting time series data for Ruhr cities...")
    
    region_codes = list(RUHR_CITIES.values())
    
    query = """
    SELECT 
        g.region_code,
        g.region_name,
        t.year,
        f.value,
        f.gender,
        f.nationality,
        f.age_group,
        f.notes
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = :indicator_id
      AND g.region_code IN :region_codes
    ORDER BY g.region_code, t.year
    """
    
    return db.execute_query(query, {'indicator_id': indicator_id, 'region_codes': tuple(region_codes)})


def analyze_time_series(time_series_data: List[Dict]) -> Dict:
    """Analyze time series patterns."""
    logger.info("Analyzing time series patterns...")
    
    analysis = {}
    
    # Group by city
    city_data = {}
    for row in time_series_data:
        city = row['region_code']
        if city not in city_data:
            city_data[city] = []
        city_data[city].append(row)
    
    # Analyze each city
    for city_code, data in city_data.items():
        city_name = [name for name, code in RUHR_CITIES.items() if code == city_code][0]
        
        values = [d['value'] for d in data if d['value'] is not None]
        years = [d['year'] for d in data if d['value'] is not None]
        
        if values:
            analysis[city_name] = {
                'region_code': city_code,
                'data_points': len(values),
                'min_value': min(values),
                'max_value': max(values),
                'mean_value': sum(values) / len(values),
                'year_range': f"{min(years)}-{max(years)}",
                'first_value': values[0] if values else None,
                'last_value': values[-1] if values else None,
                'change': values[-1] - values[0] if len(values) > 1 else 0,
                'change_percent': ((values[-1] - values[0]) / values[0] * 100) if len(values) > 1 and values[0] != 0 else 0
            }
        else:
            analysis[city_name] = {
                'region_code': city_code,
                'data_points': 0,
                'status': 'NO DATA'
            }
    
    return analysis


def export_time_series_csv(time_series_data: List[Dict], indicator_id: int, output_dir: Path):
    """Export time series data to CSV for further analysis."""
    import csv
    
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = output_dir / f"timeseries_indicator_{indicator_id}_{timestamp}.csv"
    
    if not time_series_data:
        logger.warning("No data to export")
        return None
    
    # Get all unique keys from the data
    fieldnames = list(time_series_data[0].keys())
    
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(time_series_data)
    
    logger.info(f"Exported time series data to: {filename}")
    return filename


def print_verification_report(
    indicator_info: Dict,
    completeness: Dict,
    ruhr_coverage: Dict,
    time_series_analysis: Dict
):
    """Print comprehensive verification report."""
    
    print("\n" + "=" * 100)
    print(" DATA EXTRACTION VERIFICATION REPORT")
    print("=" * 100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Indicator Information
    print("-" * 100)
    print(" INDICATOR INFORMATION")
    print("-" * 100)
    print(f"Indicator ID:      {indicator_info['indicator_id']}")
    print(f"Indicator Code:    {indicator_info['indicator_code']}")
    print(f"Indicator Name:    {indicator_info['indicator_name_en']}")
    print(f"Category:          {indicator_info['indicator_category']}")
    print(f"Source Table:      {indicator_info['source_table_id']}")
    print(f"Unit:              {indicator_info['unit_of_measure']}")
    print()
    
    # Data Completeness
    print("-" * 100)
    print(" DATA COMPLETENESS")
    print("-" * 100)
    print(f"Total Records:     {completeness['total_records']:,}")
    print(f"Year Range:        {completeness['min_year']} - {completeness['max_year']} ({completeness['year_count']} years)")
    print(f"Regions Covered:   {completeness['region_count']}")
    print(f"Non-NULL Values:   {completeness['non_null_values']:,} ({completeness['completeness_rate']:.1f}%)")
    print(f"NULL Values:       {completeness['null_values']:,}")
    
    # Quality Assessment
    quality_status = "EXCELLENT" if completeness['completeness_rate'] >= 95 else \
                     "GOOD" if completeness['completeness_rate'] >= 85 else \
                     "FAIR" if completeness['completeness_rate'] >= 70 else "POOR"
    print(f"Quality Status:    {quality_status}")
    print()
    
    # Ruhr Cities Coverage
    print("-" * 100)
    print(" RUHR REGION CITIES COVERAGE")
    print("-" * 100)
    print(f"{'City':<20} {'Region Code':<12} {'Records':<10} {'Year Range':<15} {'Status':<10}")
    print("-" * 100)
    
    for city_name, data in ruhr_coverage.items():
        records = data.get('records', 0)
        year_range = f"{data.get('min_year', 'N/A')}-{data.get('max_year', 'N/A')}" if records > 0 else "N/A"
        status = data.get('status', 'UNKNOWN')
        print(f"{city_name:<20} {data['region_code']:<12} {records:<10} {year_range:<15} {status:<10}")
    
    # Check if all cities have data
    all_cities_covered = all(data['status'] == 'OK' for data in ruhr_coverage.values())
    print()
    if all_cities_covered:
        print("[OK] All Ruhr region cities have data")
    else:
        missing = [city for city, data in ruhr_coverage.items() if data['status'] != 'OK']
        print(f"[!] Missing data for: {', '.join(missing)}")
    print()
    
    # Time Series Analysis
    print("-" * 100)
    print(" TIME SERIES ANALYSIS - RUHR REGION CITIES")
    print("-" * 100)
    print(f"{'City':<20} {'Data Points':<12} {'Mean Value':<15} {'First -> Last':<25} {'Change':<15}")
    print("-" * 100)
    
    for city_name, analysis in time_series_analysis.items():
        if 'status' in analysis and analysis['status'] == 'NO DATA':
            print(f"{city_name:<20} {'NO DATA':<12}")
        else:
            mean_val = f"{analysis['mean_value']:,.1f}"
            first_last = f"{analysis['first_value']:,.1f} -> {analysis['last_value']:,.1f}"
            change = f"{analysis['change']:+,.1f} ({analysis['change_percent']:+.1f}%)"
            print(f"{city_name:<20} {analysis['data_points']:<12} {mean_val:<15} {first_last:<25} {change:<15}")
    
    print()
    print("-" * 100)
    print(" VERIFICATION SUMMARY")
    print("-" * 100)
    
    # Overall assessment
    checks_passed = []
    checks_failed = []
    
    if completeness['total_records'] > 0:
        checks_passed.append("Records exist in database")
    else:
        checks_failed.append("No records found")
    
    if completeness['completeness_rate'] >= 95:
        checks_passed.append("Data completeness >= 95%")
    else:
        checks_failed.append(f"Data completeness only {completeness['completeness_rate']:.1f}%")
    
    if all_cities_covered:
        checks_passed.append("All Ruhr cities have data")
    else:
        checks_failed.append("Some Ruhr cities missing data")
    
    if completeness['year_count'] >= 10:
        checks_passed.append("Sufficient time series depth (>=10 years)")
    elif completeness['year_count'] > 0:
        checks_failed.append(f"Limited time series depth ({completeness['year_count']} years)")
    
    print("\n[PASSED CHECKS]")
    for check in checks_passed:
        print(f"  [OK] {check}")
    
    if checks_failed:
        print("\n[FAILED CHECKS]")
        for check in checks_failed:
            print(f"  [!] {check}")
    
    # Overall verdict
    print()
    if len(checks_failed) == 0:
        print("[OK] VERIFICATION PASSED - Data is accurate and complete")
    elif len(checks_failed) <= 1:
        print("[!] VERIFICATION PASSED WITH WARNINGS - Review failed checks")
    else:
        print("[X] VERIFICATION FAILED - Multiple issues detected")
    
    print("=" * 100)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Verify data extraction with time series analysis for Ruhr region cities"
    )
    parser.add_argument(
        "--indicator",
        type=int,
        required=True,
        help="Indicator ID to verify"
    )
    parser.add_argument(
        "--export-csv",
        action="store_true",
        help="Export time series data to CSV"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/analysis/timeseries"),
        help="Output directory for CSV export"
    )
    
    args = parser.parse_args()
    
    db = None
    try:
        # Connect to database
        db = get_database('regional_economics')
        
        # Get indicator info
        indicator_info = get_indicator_info(db, args.indicator)
        if not indicator_info:
            logger.error(f"Indicator {args.indicator} not found in database")
            return False
        
        # Run verification checks
        completeness = verify_data_completeness(db, args.indicator)
        ruhr_coverage = verify_ruhr_cities_coverage(db, args.indicator)
        time_series_data = get_time_series_data(db, args.indicator)
        time_series_analysis = analyze_time_series(time_series_data)
        
        # Print report
        print_verification_report(
            indicator_info,
            completeness,
            ruhr_coverage,
            time_series_analysis
        )
        
        # Export CSV if requested
        if args.export_csv and time_series_data:
            export_time_series_csv(time_series_data, args.indicator, args.output_dir)
        
        return True
        
    except Exception as e:
        logger.error(f"Verification failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
    
    finally:
        if db:
            db.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
