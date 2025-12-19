"""
Fix Indicator Mapping Issue
============================
This script fixes the indicator ID misassignment issue where employment data
was loaded using population indicator IDs.

Actions:
1. Add new indicator entries for employment tables
2. Remap existing data in fact_demographics to correct indicators
3. Verify the fix

Created: 2025-12-18
"""

import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database
from utils.logging import setup_logging, get_logger

setup_logging()
logger = get_logger(__name__)


def get_next_indicator_id(db) -> int:
    """Get the next available indicator ID."""
    result = db.execute_query("SELECT MAX(indicator_id) as max_id FROM dim_indicator")
    return (result[0]['max_id'] or 0) + 1


def add_employment_indicators(db) -> dict:
    """
    Add new indicator entries for employment tables.
    Returns a mapping of source_table_id -> new_indicator_id
    """
    print("\n" + "="*80)
    print(" STEP 1: Adding New Employment Indicators")
    print("="*80)
    
    # Define the new indicators we need to add
    new_indicators = [
        {
            'indicator_code': 'emp_scope_workplace',
            'indicator_name': 'Beschaeftigte nach Arbeitsumfang (Arbeitsort)',
            'indicator_name_en': 'Employees by employment scope (workplace)',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_scope',
            'source_system': 'regional_db',
            'source_table_id': '13111-03-02-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance by full-time/part-time status at workplace',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': 3,  # Was wrongly using pop_female
        },
        {
            'indicator_code': 'emp_qualification_workplace',
            'indicator_name': 'Beschaeftigte nach Berufsausbildung (Arbeitsort)',
            'indicator_name_en': 'Employees by vocational qualification (workplace)',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_qualification',
            'source_system': 'regional_db',
            'source_table_id': '13111-11-04-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance by type of vocational education qualification at workplace',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': 4,  # Was wrongly using pop_age_0_17
        },
        {
            'indicator_code': 'emp_residence',
            'indicator_name': 'Beschaeftigte am Wohnort',
            'indicator_name_en': 'Employees at place of residence',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_residence',
            'source_system': 'regional_db',
            'source_table_id': '13111-02-02-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance at place of residence by gender and nationality',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': 5,  # Was wrongly using pop_age_18_64
        },
        {
            'indicator_code': 'emp_scope_residence',
            'indicator_name': 'Beschaeftigte nach Arbeitsumfang (Wohnort)',
            'indicator_name_en': 'Employees by employment scope (residence)',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_scope',
            'source_system': 'regional_db',
            'source_table_id': '13111-04-02-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance by full-time/part-time status at place of residence',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': 6,  # Was wrongly using pop_age_65_plus
        },
        {
            'indicator_code': 'emp_qualification_residence',
            'indicator_name': 'Beschaeftigte nach Berufsausbildung (Wohnort)',
            'indicator_name_en': 'Employees by vocational qualification (residence)',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_qualification',
            'source_system': 'regional_db',
            'source_table_id': '13111-12-03-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance by type of vocational education qualification at place of residence',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': 7,  # Was wrongly using pop_german
        },
        {
            'indicator_code': 'emp_sector_workplace',
            'indicator_name': 'Beschaeftigte nach Wirtschaftszweigen (Arbeitsort)',
            'indicator_name_en': 'Employees by economic sector (workplace)',
            'indicator_category': 'labor_market',
            'indicator_subcategory': 'employment_sector',
            'source_system': 'regional_db',
            'source_table_id': '13111-07-05-4',
            'unit_of_measure': 'persons',
            'description': 'Employees subject to social insurance by economic sectors (WZ 2008) at workplace',
            'update_frequency': 'annual',
            'typical_reference_date': 'June 30',
            'data_type': 'count',
            'aggregation_method': 'sum',
            'old_indicator_id': None,  # This was correctly using indicator 9, but we'll create a dedicated one
        },
    ]
    
    # Mapping of old indicator ID -> new indicator ID
    id_mapping = {}
    
    next_id = get_next_indicator_id(db)
    
    for indicator in new_indicators:
        old_id = indicator.pop('old_indicator_id')
        
        # Check if indicator already exists
        check_query = f"SELECT indicator_id FROM dim_indicator WHERE indicator_code = '{indicator['indicator_code']}'"
        existing = db.execute_query(check_query)
        
        if existing:
            new_id = existing[0]['indicator_id']
            print(f"  [EXISTS] {indicator['indicator_code']} -> ID {new_id}")
        else:
            # Insert new indicator
            columns = ', '.join(indicator.keys())
            values = ', '.join([f"'{v}'" if v is not None else 'NULL' for v in indicator.values()])
            
            insert_query = f"""
            INSERT INTO dim_indicator ({columns})
            VALUES ({values})
            RETURNING indicator_id
            """
            
            try:
                result = db.execute_query(insert_query)
                new_id = result[0]['indicator_id']
                print(f"  [ADDED] {indicator['indicator_code']} -> ID {new_id}")
            except Exception as e:
                print(f"  [ERROR] {indicator['indicator_code']}: {e}")
                continue
        
        if old_id is not None:
            id_mapping[old_id] = new_id
        
        # Store mapping by source table as well
        id_mapping[indicator['source_table_id']] = new_id
    
    return id_mapping


def remap_existing_data(db, id_mapping: dict) -> int:
    """
    Remap existing data in fact_demographics to use correct indicator IDs.
    Returns count of records updated.
    """
    print("\n" + "="*80)
    print(" STEP 2: Remapping Existing Data")
    print("="*80)
    
    total_updated = 0
    
    # Only remap for old IDs 3, 4, 5, 6, 7 (the wrongly assigned ones)
    old_ids_to_remap = [3, 4, 5, 6, 7]
    
    for old_id in old_ids_to_remap:
        if old_id not in id_mapping:
            print(f"  [SKIP] No mapping for old indicator ID {old_id}")
            continue
        
        new_id = id_mapping[old_id]
        
        # Count records to update
        count_query = f"SELECT COUNT(*) as cnt FROM fact_demographics WHERE indicator_id = {old_id}"
        result = db.execute_query(count_query)
        count = result[0]['cnt']
        
        if count > 0:
            # Update the records using execute_statement (for UPDATE/INSERT/DELETE)
            update_query = f"""
            UPDATE fact_demographics
            SET indicator_id = {new_id}
            WHERE indicator_id = {old_id}
            """
            
            try:
                rows_affected = db.execute_statement(update_query)
                print(f"  [UPDATED] Indicator {old_id} -> {new_id}: {rows_affected:,} records")
                total_updated += rows_affected
            except Exception as e:
                print(f"  [ERROR] Updating indicator {old_id}: {e}")
        else:
            print(f"  [SKIP] Indicator {old_id}: No records to update")
    
    return total_updated


def verify_fix(db):
    """Verify that the fix was applied correctly."""
    print("\n" + "="*80)
    print(" STEP 3: Verification")
    print("="*80)
    
    # Check all indicators now
    query = """
    SELECT
        i.indicator_id,
        i.indicator_code,
        i.source_table_id,
        i.indicator_category,
        COUNT(f.fact_id) as record_count
    FROM dim_indicator i
    LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
    GROUP BY i.indicator_id, i.indicator_code, i.source_table_id, i.indicator_category
    ORDER BY i.indicator_id
    """
    
    results = db.execute_query(query)
    
    print(f"\n{'ID':<4} {'Code':<30} {'Source Table':<18} {'Category':<15} {'Records':>10}")
    print("-"*85)
    
    demographics_count = 0
    labor_market_count = 0
    
    for row in results:
        ind_id = row['indicator_id']
        code = row['indicator_code'][:28] if row['indicator_code'] else 'N/A'
        source = row['source_table_id'] or 'N/A'
        cat = row['indicator_category'][:13] if row['indicator_category'] else 'N/A'
        count = row['record_count'] or 0
        
        if cat.startswith('demo'):
            demographics_count += count
        elif cat.startswith('labor'):
            labor_market_count += count
        
        status = "[OK]" if count > 0 else "[--]"
        print(f"{ind_id:<4} {code:<30} {source:<18} {cat:<15} {count:>10,} {status}")
    
    print("-"*85)
    print(f"\nSummary:")
    print(f"  Demographics records: {demographics_count:,}")
    print(f"  Labor Market records: {labor_market_count:,}")
    print(f"  Total records: {demographics_count + labor_market_count:,}")
    
    # Check for any remaining misassigned data
    misassigned_query = """
    SELECT f.indicator_id, COUNT(*) as cnt
    FROM fact_demographics f
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    WHERE i.indicator_category = 'demographics'
      AND f.age_group IS NULL
    GROUP BY f.indicator_id
    """
    
    misassigned = db.execute_query(misassigned_query)
    
    if misassigned:
        print("\n[WARNING] Potential misassigned records (demographics with no age_group):")
        for row in misassigned:
            print(f"  Indicator {row['indicator_id']}: {row['cnt']:,} records")
    else:
        print("\n[OK] No obvious misassigned records detected")


def main():
    print("\n" + "="*80)
    print(" FIX INDICATOR MAPPING ISSUE")
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*80)
    
    db = get_database('regional_economics')
    
    try:
        # Step 1: Add new employment indicators
        id_mapping = add_employment_indicators(db)
        print(f"\n  Created ID mapping: {id_mapping}")
        
        # Step 2: Remap existing data
        updated_count = remap_existing_data(db, id_mapping)
        print(f"\n  Total records remapped: {updated_count:,}")
        
        # Step 3: Verify the fix
        verify_fix(db)
        
        print("\n" + "="*80)
        print(" FIX COMPLETED SUCCESSFULLY")
        print("="*80)
        
        # Return the ID mapping for updating ETL pipelines
        return id_mapping
        
    except Exception as e:
        print(f"\n[ERROR] Fix failed: {e}")
        import traceback
        traceback.print_exc()
        return None
        
    finally:
        db.close()


if __name__ == "__main__":
    main()
