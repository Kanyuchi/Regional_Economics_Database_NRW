"""
Verify Business Registrations Data (Indicators 24 & 25)
Table: 52311-01-04-4
Period: 1998-2024
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


def verify_business_registrations():
    """Verify business registrations and deregistrations data"""
    
    logger.info("="*80)
    logger.info("BUSINESS REGISTRATIONS DATA VERIFICATION")
    logger.info("="*80)
    
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        # 1. Basic counts
        logger.info("\n1. BASIC STATISTICS")
        logger.info("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                i.indicator_id,
                i.indicator_code,
                i.indicator_name,
                COUNT(*) as total_records,
                COUNT(DISTINCT f.geo_id) as unique_regions,
                MIN(t.year) as min_year,
                MAX(t.year) as max_year
            FROM fact_demographics f
            JOIN dim_indicator i ON f.indicator_id = i.indicator_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE f.indicator_id IN (24, 25)
            GROUP BY i.indicator_id, i.indicator_code, i.indicator_name
            ORDER BY i.indicator_id
        """))
        
        for row in result:
            logger.info(f"\nIndicator {row[0]}: {row[1]}")
            logger.info(f"  Name: {row[2]}")
            logger.info(f"  Total records: {row[3]}")
            logger.info(f"  Unique regions: {row[4]}")
            logger.info(f"  Year range: {row[5]} - {row[6]}")
        
        # 2. Ruhr cities coverage
        logger.info("\n2. RUHR CITIES COVERAGE")
        logger.info("-" * 80)
        
        ruhr_cities = {
            '05913': 'Dortmund',
            '05113': 'Essen',
            '05112': 'Duisburg',
            '05911': 'Bochum',
            '05513': 'Gelsenkirchen'
        }
        
        for region_code, city_name in ruhr_cities.items():
            result = conn.execute(text("""
                SELECT 
                    i.indicator_code,
                    COUNT(*) as records,
                    MIN(t.year) as min_year,
                    MAX(t.year) as max_year
                FROM fact_demographics f
                JOIN dim_geography g ON f.geo_id = g.geo_id
                JOIN dim_time t ON f.time_id = t.time_id
                JOIN dim_indicator i ON f.indicator_id = i.indicator_id
                WHERE g.region_code = :region_code
                  AND f.indicator_id IN (24, 25)
                GROUP BY i.indicator_code
                ORDER BY i.indicator_code
            """), {'region_code': region_code})
            
            rows = result.fetchall()
            if rows:
                logger.info(f"\n{city_name} ({region_code}):")
                for row in rows:
                    logger.info(f"  {row[0]}: {row[1]} records ({row[2]}-{row[3]})")
            else:
                logger.warning(f"\n{city_name} ({region_code}): NO DATA FOUND")
        
        # 3. Sample data - Dortmund registrations over time
        logger.info("\n3. SAMPLE TIME SERIES - DORTMUND REGISTRATIONS")
        logger.info("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                t.year,
                f.notes,
                f.value
            FROM fact_demographics f
            JOIN dim_geography g ON f.geo_id = g.geo_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE g.region_code = '05913'
              AND f.indicator_id = 24
              AND f.notes = 'Total registrations'
            ORDER BY t.year DESC
            LIMIT 10
        """))
        
        logger.info("\nYear | Category              | Value")
        logger.info("-" * 50)
        for row in result:
            logger.info(f"{row[0]} | {row[1]:20s} | {int(row[2]):,}")
        
        # 4. Category breakdown for latest year
        logger.info("\n4. CATEGORY BREAKDOWN - DORTMUND 2024")
        logger.info("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                i.indicator_name,
                f.notes,
                f.value
            FROM fact_demographics f
            JOIN dim_geography g ON f.geo_id = g.geo_id
            JOIN dim_time t ON f.time_id = t.time_id
            JOIN dim_indicator i ON f.indicator_id = i.indicator_id
            WHERE g.region_code = '05913'
              AND f.indicator_id IN (24, 25)
              AND t.year = 2024
            ORDER BY i.indicator_id, f.notes
        """))
        
        current_indicator = None
        for row in result:
            if row[0] != current_indicator:
                logger.info(f"\n{row[0]}:")
                current_indicator = row[0]
            logger.info(f"  {row[1]:35s}: {int(row[2]):,}")
        
        # 5. Comparison across Ruhr cities - Total registrations 2024
        logger.info("\n5. RUHR CITIES COMPARISON - 2024")
        logger.info("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                g.region_name,
                f.notes,
                f.value
            FROM fact_demographics f
            JOIN dim_geography g ON f.geo_id = g.geo_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
              AND f.indicator_id = 24
              AND f.notes = 'Total registrations'
              AND t.year = 2024
            ORDER BY f.value DESC
        """))
        
        logger.info("\nCity                    | Total Registrations 2024")
        logger.info("-" * 60)
        for row in result:
            logger.info(f"{row[0]:22s} | {int(row[2]):,}")
        
        # 6. Trend analysis - Total registrations 2020-2024 (COVID impact)
        logger.info("\n6. COVID-19 IMPACT ANALYSIS (2020-2024)")
        logger.info("-" * 80)
        
        result = conn.execute(text("""
            SELECT 
                t.year,
                g.region_name,
                f.value
            FROM fact_demographics f
            JOIN dim_geography g ON f.geo_id = g.geo_id
            JOIN dim_time t ON f.time_id = t.time_id
            WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
              AND f.indicator_id = 24
              AND f.notes = 'Total registrations'
              AND t.year >= 2020
            ORDER BY t.year, g.region_name
        """))
        
        data_by_year = {}
        for row in result:
            year = row[0]
            if year not in data_by_year:
                data_by_year[year] = []
            data_by_year[year].append((row[1], int(row[2])))
        
        for year in sorted(data_by_year.keys()):
            logger.info(f"\n{year}:")
            for city, value in data_by_year[year]:
                logger.info(f"  {city:22s}: {value:,}")
        
        # 7. Data quality checks
        logger.info("\n7. DATA QUALITY CHECKS")
        logger.info("-" * 80)
        
        # Check for NULL values
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM fact_demographics 
            WHERE indicator_id IN (24, 25) 
              AND value IS NULL
        """))
        null_count = result.scalar()
        logger.info(f"NULL values: {null_count}")
        
        # Check for negative values (should be 0 for counts)
        result = conn.execute(text("""
            SELECT COUNT(*) 
            FROM fact_demographics 
            WHERE indicator_id IN (24, 25) 
              AND value < 0
        """))
        negative_count = result.scalar()
        logger.info(f"Negative values: {negative_count}")
        
        # Check notes field coverage
        result = conn.execute(text("""
            SELECT COUNT(*), COUNT(notes) 
            FROM fact_demographics 
            WHERE indicator_id IN (24, 25)
        """))
        total, with_notes = result.fetchone()
        logger.info(f"Notes field coverage: {with_notes}/{total} ({100*with_notes/total:.1f}%)")
        
        logger.info("\n" + "="*80)
        logger.info("VERIFICATION COMPLETE")
        logger.info("="*80)
        logger.info("\nStatus: Data appears complete and queryable")
        logger.info("Ready for: Thesis analysis and SQL scripts")


if __name__ == "__main__":
    verify_business_registrations()

