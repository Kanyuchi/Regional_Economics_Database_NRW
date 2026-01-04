"""
Setup Outpatient Services Indicators in Database
Creates dim_indicator entries for table 22411-01i (Outpatient Services by Facility Type and Staff)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Outpatient services indicators based on table 22411-01i structure
# IDs 77-78
OUTPATIENT_SERVICES_INDICATORS = [
    {
        'indicator_id': 77,
        'indicator_code': 'outpatient_services_total',
        'indicator_name': 'Ambulante Pflegedienste',
        'indicator_name_en': 'Total Outpatient Care Services',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Infrastructure',
        'description': 'Total number of outpatient care service facilities (ambulante Pflegedienste)',
        'unit_of_measure': 'Facilities',
        'unit_description': 'Number of outpatient care service facilities',
        'source_system': 'state_db',
        'source_table_id': '22411-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 78,
        'indicator_code': 'outpatient_services_staff',
        'indicator_name': 'Personal in ambulanten Pflegediensten',
        'indicator_name_en': 'Staff in Outpatient Care Services',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Infrastructure',
        'description': 'Total staff employed in outpatient care services',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of staff members in outpatient care facilities',
        'source_system': 'state_db',
        'source_table_id': '22411-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert outpatient services indicator definitions into database."""
    print("=" * 80)
    print("SETUP OUTPATIENT SERVICES INDICATORS")
    print("=" * 80)
    print(f"Creating {len(OUTPATIENT_SERVICES_INDICATORS)} indicators (IDs 77-78)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in OUTPATIENT_SERVICES_INDICATORS:
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
        print("SUCCESS: Outpatient services indicators created/updated")
        print("=" * 80)
        print("\nYou can now run the outpatient services ETL pipeline:")
        print("  python pipelines/state_db/etl_22411_01i_outpatient_services.py --test")

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
