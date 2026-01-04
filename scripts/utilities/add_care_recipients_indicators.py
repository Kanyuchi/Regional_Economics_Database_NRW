"""
Setup Care Recipients Indicators in Database
Creates dim_indicator entries for table 22421-02i (Care Recipients by Care Level and Benefit Type)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Care recipients indicators based on table 22421-02i structure
# IDs 72-75
CARE_RECIPIENTS_INDICATORS = [
    {
        'indicator_id': 72,
        'indicator_code': 'care_benefit_total',
        'indicator_name': 'Leistungsempfänger nach Pflegegrad',
        'indicator_name_en': 'Total Benefit Recipients by Care Level',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Services',
        'description': 'Total number of care benefit recipients by care level (Pflegegrad 1-5)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of persons receiving care benefits',
        'source_system': 'state_db',
        'source_table_id': '22421-02i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 73,
        'indicator_code': 'care_nursing_home',
        'indicator_name': 'Pflegebedürftige in Pflegeheimen nach Pflegegrad',
        'indicator_name_en': 'Nursing Home Residents by Care Level',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Services',
        'description': 'Number of care recipients in nursing homes by care level',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of nursing home residents',
        'source_system': 'state_db',
        'source_table_id': '22421-02i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 74,
        'indicator_code': 'care_inpatient',
        'indicator_name': 'Vollstationäre Pflege nach Pflegegrad',
        'indicator_name_en': 'Inpatient Care Recipients by Care Level',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Services',
        'description': 'Number of persons receiving full inpatient care by care level',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of inpatient care recipients',
        'source_system': 'state_db',
        'source_table_id': '22421-02i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 75,
        'indicator_code': 'care_allowance',
        'indicator_name': 'Pflegegeldempfänger nach Pflegegrad',
        'indicator_name_en': 'Care Allowance Recipients by Care Level',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Care Services',
        'description': 'Number of persons receiving care allowance (home care) by care level',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of care allowance recipients (home care)',
        'source_system': 'state_db',
        'source_table_id': '22421-02i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert care recipients indicator definitions into database."""
    print("=" * 80)
    print("SETUP CARE RECIPIENTS INDICATORS")
    print("=" * 80)
    print(f"Creating {len(CARE_RECIPIENTS_INDICATORS)} indicators (IDs 72-75)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in CARE_RECIPIENTS_INDICATORS:
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
        print("SUCCESS: All care recipients indicators created/updated")
        print("=" * 80)
        print("\nYou can now run the care recipients ETL pipeline:")
        print("  python pipelines/state_db/etl_22421_02i_care_recipients.py --test")

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
