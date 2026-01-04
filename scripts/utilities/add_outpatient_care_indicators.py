"""
Setup Outpatient Care Indicators in Database
Creates dim_indicator entry for table 22411-02i (Outpatient Care Recipients by Care Level)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Outpatient care indicator based on table 22411-02i structure
# ID 76
OUTPATIENT_CARE_INDICATORS = [
    {
        'indicator_id': 76,
        'indicator_code': 'care_outpatient',
        'indicator_name': 'Pflegebedürftige in ambulanten Diensten nach Pflegegrad',
        'indicator_name_en': 'Outpatient Care Recipients by Care Level',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Services',
        'description': 'Number of care recipients in outpatient/ambulatory services by care level (Pflegegrad 1-5)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of persons receiving outpatient care services',
        'source_system': 'state_db',
        'source_table_id': '22411-02i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert outpatient care indicator definitions into database."""
    print("=" * 80)
    print("SETUP OUTPATIENT CARE INDICATORS")
    print("=" * 80)
    print(f"Creating {len(OUTPATIENT_CARE_INDICATORS)} indicator (ID 76)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in OUTPATIENT_CARE_INDICATORS:
                # Check if indicator already exists
                result = conn.execute(
                    text("SELECT indicator_id FROM dim_indicator WHERE indicator_id = :id"),
                    {"id": indicator['indicator_id']}
                )

                if result.fetchone():
                    print(f"  [{indicator['indicator_id']}] Already exists: {indicator['indicator_name']}")
                    # Update existing record
                    conn.execute(text("""
                        UPDATE dim_indicator
                        SET indicator_code = :code,
                            indicator_name = :name,
                            indicator_name_en = :name_en,
                            indicator_category = :category,
                            indicator_subcategory = :subcategory,
                            description = :description,
                            unit_of_measure = :unit,
                            unit_description = :unit_desc,
                            source_system = :source,
                            source_table_id = :table_id,
                            calculation_method = :calc_method,
                            data_type = :data_type,
                            is_active = :is_active,
                            updated_at = :updated_at
                        WHERE indicator_id = :id
                    """), {
                        'id': indicator['indicator_id'],
                        'code': indicator['indicator_code'],
                        'name': indicator['indicator_name'],
                        'name_en': indicator['indicator_name_en'],
                        'category': indicator['indicator_category'],
                        'subcategory': indicator['indicator_subcategory'],
                        'description': indicator['description'],
                        'unit': indicator['unit_of_measure'],
                        'unit_desc': indicator['unit_description'],
                        'source': indicator['source_system'],
                        'table_id': indicator['source_table_id'],
                        'calc_method': indicator['calculation_method'],
                        'data_type': indicator['data_type'],
                        'is_active': indicator['is_active'],
                        'updated_at': datetime.now()
                    })
                else:
                    # Insert new record
                    conn.execute(text("""
                        INSERT INTO dim_indicator (
                            indicator_id, indicator_code, indicator_name, indicator_name_en,
                            indicator_category, indicator_subcategory, description,
                            unit_of_measure, unit_description, source_system, source_table_id,
                            calculation_method, data_type, is_active,
                            created_at, updated_at
                        ) VALUES (
                            :id, :code, :name, :name_en,
                            :category, :subcategory, :description,
                            :unit, :unit_desc, :source, :table_id,
                            :calc_method, :data_type, :is_active,
                            :created_at, :updated_at
                        )
                    """), {
                        'id': indicator['indicator_id'],
                        'code': indicator['indicator_code'],
                        'name': indicator['indicator_name'],
                        'name_en': indicator['indicator_name_en'],
                        'category': indicator['indicator_category'],
                        'subcategory': indicator['indicator_subcategory'],
                        'description': indicator['description'],
                        'unit': indicator['unit_of_measure'],
                        'unit_desc': indicator['unit_description'],
                        'source': indicator['source_system'],
                        'table_id': indicator['source_table_id'],
                        'calc_method': indicator['calculation_method'],
                        'data_type': indicator['data_type'],
                        'is_active': indicator['is_active'],
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                    print(f"  [{indicator['indicator_id']}] Created: {indicator['indicator_name']}")

            conn.commit()

        print("\n" + "=" * 80)
        print("SUCCESS: Outpatient care indicator created/updated")
        print("=" * 80)
        print("\nYou can now run the outpatient care ETL pipeline:")
        print("  python pipelines/state_db/etl_22411_02i_outpatient_care.py --test")

    except Exception as e:
        print(f"\nERROR: Failed to setup indicators: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        db.close()

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
