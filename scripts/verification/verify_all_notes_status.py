"""
Comprehensive verification of NULL notes across all fact tables in the database.

This script checks:
1. All fact tables for NULL notes values
2. Notes status by indicator/table
3. Provides recommendations for fixing NULL notes
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

def main():
    db = get_database('regional_economics')
    
    print("\n" + "="*100)
    print(" NULL NOTES VERIFICATION REPORT")
    print(f" Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*100)
    
    # ========================================================================
    # 1. Check fact_demographics table (main table with data)
    # ========================================================================
    print("\n" + "-"*100)
    print(" 1. FACT_DEMOGRAPHICS TABLE - Notes Status by Indicator")
    print("-"*100)
    
    demo_query = """
    SELECT
        i.indicator_id,
        i.source_table_id,
        i.indicator_name,
        COUNT(*) as total_records,
        COUNT(f.notes) as with_notes,
        COUNT(*) - COUNT(f.notes) as null_notes,
        ROUND(100.0 * COUNT(f.notes) / NULLIF(COUNT(*), 0), 1) as pct_with_notes,
        COUNT(DISTINCT f.notes) as unique_notes
    FROM fact_demographics f
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    GROUP BY i.indicator_id, i.source_table_id, i.indicator_name
    ORDER BY i.indicator_id
    """
    
    results = db.execute_query(demo_query)
    
    print(f"\n{'ID':<4} {'Table ID':<18} {'Indicator':<35} {'Total':>10} {'Notes':>10} {'NULL':>10} {'% Notes':>8}")
    print("-"*100)
    
    total_records = 0
    total_with_notes = 0
    total_null_notes = 0
    
    for row in results:
        indicator_id = row['indicator_id']
        table_id = row['source_table_id'] or 'N/A'
        name = (row['indicator_name'][:33] + '..') if len(row['indicator_name']) > 35 else row['indicator_name']
        total = row['total_records']
        notes = row['with_notes']
        nulls = row['null_notes']
        pct = row['pct_with_notes']
        
        total_records += total
        total_with_notes += notes
        total_null_notes += nulls
        
        # Flag rows with NULL notes
        flag = "[!]" if nulls > 0 else "[OK]"
        print(f"{indicator_id:<4} {table_id:<18} {name:<35} {total:>10,} {notes:>10,} {nulls:>10,} {pct:>7}% {flag}")
    
    print("-"*100)
    pct_total = round(100.0 * total_with_notes / total_records, 1) if total_records > 0 else 0
    print(f"{'TOTAL':<4} {'':<18} {'All Indicators':<35} {total_records:>10,} {total_with_notes:>10,} {total_null_notes:>10,} {pct_total:>7}%")
    
    # ========================================================================
    # 2. Sample of actual notes values per indicator
    # ========================================================================
    print("\n" + "-"*100)
    print(" 2. SAMPLE NOTES VALUES BY INDICATOR")
    print("-"*100)
    
    for row in results:
        indicator_id = row['indicator_id']
        table_id = row['source_table_id']
        unique_count = row['unique_notes']
        
        if row['with_notes'] > 0:
            sample_query = f"""
            SELECT DISTINCT notes
            FROM fact_demographics
            WHERE indicator_id = {indicator_id} AND notes IS NOT NULL
            LIMIT 5
            """
            samples = db.execute_query(sample_query)
            
            print(f"\n[OK] Indicator {indicator_id} ({table_id}) - {unique_count} unique note values:")
            for s in samples:
                note_preview = s['notes'][:80] + '...' if len(s['notes']) > 80 else s['notes']
                print(f"   - {note_preview}")
        else:
            print(f"\n[!] Indicator {indicator_id} ({table_id}) - NO NOTES (all NULL)")
    
    # ========================================================================
    # 3. Check other fact tables
    # ========================================================================
    print("\n" + "-"*100)
    print(" 3. OTHER FACT TABLES - Record Counts")
    print("-"*100)
    
    other_tables = [
        ('fact_labor_market', 'Labor Market'),
        ('fact_business_economy', 'Business Economy'),
        ('fact_healthcare', 'Healthcare'),
        ('fact_public_finance', 'Public Finance'),
        ('fact_infrastructure', 'Infrastructure'),
        ('fact_commuters', 'Commuters')
    ]
    
    print(f"\n{'Table Name':<30} {'Total Records':>15} {'With Notes':>15} {'NULL Notes':>15}")
    print("-"*80)
    
    for table, desc in other_tables:
        try:
            count_query = f"""
            SELECT 
                COUNT(*) as total,
                COUNT(notes) as with_notes,
                COUNT(*) - COUNT(notes) as null_notes
            FROM {table}
            """
            result = db.execute_query(count_query)
            if result:
                r = result[0]
                print(f"{table:<30} {r['total']:>15,} {r['with_notes']:>15,} {r['null_notes']:>15,}")
            else:
                print(f"{table:<30} {'0':>15} {'0':>15} {'0':>15}")
        except Exception as e:
            print(f"{table:<30} {'Error':>15} {str(e)[:30]}")
    
    # ========================================================================
    # 4. Data Extraction Log Summary
    # ========================================================================
    print("\n" + "-"*100)
    print(" 4. DATA EXTRACTION LOG - Recent Extractions")
    print("-"*100)
    
    log_query = """
    SELECT 
        indicator_code,
        status,
        records_extracted,
        extraction_timestamp,
        error_message
    FROM data_extraction_log
    WHERE status = 'success'
    ORDER BY extraction_timestamp DESC
    LIMIT 10
    """
    
    log_results = db.execute_query(log_query)
    
    print(f"\n{'Indicator Code':<30} {'Status':<12} {'Records':>12} {'Timestamp':<20}")
    print("-"*80)
    
    for log in log_results:
        code = log['indicator_code'] or 'N/A'
        status = log['status']
        records = log['records_extracted'] or 0
        timestamp = log['extraction_timestamp'].strftime('%Y-%m-%d %H:%M') if log['extraction_timestamp'] else 'N/A'
        print(f"{code:<30} {status:<12} {records:>12,} {timestamp:<20}")
    
    # ========================================================================
    # 5. Summary & Recommendations
    # ========================================================================
    print("\n" + "="*100)
    print(" SUMMARY & RECOMMENDATIONS")
    print("="*100)
    
    print(f"""
DATABASE OVERVIEW:
   - Total records in fact_demographics: {total_records:,}
   - Records WITH notes: {total_with_notes:,} ({pct_total}%)
   - Records WITHOUT notes (NULL): {total_null_notes:,} ({100 - pct_total}%)

TABLES COMPLETED (from table_registry.json):
   1. 12411-03-03-4: Population by gender, nationality, age groups
   2. 13111-01-03-4: Employees at workplace by gender/nationality
   3. 13111-07-05-4: Employees at workplace by sector
   4. 13111-03-02-4: Employees by employment scope (full/part-time)
   5. 13111-11-04-4: Employees by vocational qualification
   6. 13111-02-02-4: Employees at residence by gender/nationality
""")
    
    # Check which indicators have NULL notes issue
    null_indicators = [r for r in results if r['null_notes'] > 0]
    
    if null_indicators:
        print("[!] INDICATORS WITH NULL NOTES ISSUE:")
        for r in null_indicators:
            print(f"   - Indicator {r['indicator_id']} ({r['source_table_id']}): {r['null_notes']:,} NULL notes")
        
        print("""
RECOMMENDED ACTIONS:
   1. Review ETL pipeline for each affected indicator
   2. Check if 'notes' field should contain:
      - Age group information (for demographics)
      - Employment scope (full-time/part-time)
      - Qualification type
      - Sector information
   3. Consider backfilling notes from other columns or re-extraction
   4. Update ETL scripts to properly populate notes field
""")
    else:
        print("[OK] All indicators have notes populated - no NULL notes issue!")
    
    print("="*100 + "\n")
    
    db.close()

if __name__ == "__main__":
    main()
