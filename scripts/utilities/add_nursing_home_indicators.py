"""Setup Nursing Home Indicators in Database"""
import sys
sys.path.append('.')
from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

NURSING_HOME_INDICATORS = [
    {'indicator_id': 79, 'indicator_code': 'nursing_homes_total',
     'indicator_name': 'Pflegeheime', 'indicator_name_en': 'Total Nursing Homes',
     'indicator_category': 'Health', 'indicator_subcategory': 'Care Infrastructure',
     'description': 'Total number of nursing home facilities (Pflegeheime)',
     'unit_of_measure': 'Facilities', 'unit_description': 'Number of nursing homes',
     'source_system': 'state_db', 'source_table_id': '22412-01i',
     'calculation_method': 'Direct extraction from State Database NRW', 'data_type': 'numeric', 'is_active': True},
    {'indicator_id': 80, 'indicator_code': 'nursing_home_places',
     'indicator_name': 'Verfügbare Plätze in Pflegeheimen', 'indicator_name_en': 'Available Places in Nursing Homes',
     'indicator_category': 'Health', 'indicator_subcategory': 'Care Infrastructure',
     'description': 'Total available places/beds in nursing homes',
     'unit_of_measure': 'Places', 'unit_description': 'Number of available places/beds',
     'source_system': 'state_db', 'source_table_id': '22412-01i',
     'calculation_method': 'Direct extraction from State Database NRW', 'data_type': 'numeric', 'is_active': True},
    {'indicator_id': 81, 'indicator_code': 'nursing_home_staff',
     'indicator_name': 'Personal in Pflegeheimen', 'indicator_name_en': 'Staff in Nursing Homes',
     'indicator_category': 'Health', 'indicator_subcategory': 'Care Infrastructure',
     'description': 'Total staff employed in nursing homes',
     'unit_of_measure': 'Persons', 'unit_description': 'Number of staff members',
     'source_system': 'state_db', 'source_table_id': '22412-01i',
     'calculation_method': 'Direct extraction from State Database NRW', 'data_type': 'numeric', 'is_active': True}
]

def main():
    print("=" * 80)
    print("SETUP NURSING HOME INDICATORS (IDs 79-81)")
    print("=" * 80)
    db = DatabaseManager()
    try:
        with db.get_connection() as conn:
            for ind in NURSING_HOME_INDICATORS:
                result = conn.execute(text("SELECT indicator_id FROM dim_indicator WHERE indicator_id = :id"), {"id": ind['indicator_id']})
                if result.fetchone():
                    print(f"  [{ind['indicator_id']}] Already exists: {ind['indicator_name']}")
                else:
                    conn.execute(text("""
                        INSERT INTO dim_indicator (indicator_id, indicator_code, indicator_name, indicator_name_en,
                            indicator_category, indicator_subcategory, description, unit_of_measure, unit_description,
                            source_system, source_table_id, calculation_method, data_type, is_active, created_at, updated_at)
                        VALUES (:indicator_id, :indicator_code, :indicator_name, :indicator_name_en, :indicator_category,
                            :indicator_subcategory, :description, :unit_of_measure, :unit_description,
                            :source_system, :source_table_id, :calculation_method, :data_type, :is_active, :created_at, :updated_at)
                    """), {**ind, 'created_at': datetime.now(), 'updated_at': datetime.now()})
                    print(f"  [{ind['indicator_id']}] Created: {ind['indicator_name']}")
            conn.commit()
        print("\n" + "=" * 80)
        print("SUCCESS")
        print("=" * 80)
    finally:
        db.close()
    return True

if __name__ == "__main__":
    sys.exit(0 if main() else 1)
