"""Setup Physicians Indicator in Database"""
import sys
sys.path.append('.')
from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

PHYSICIANS_INDICATOR = {
    'indicator_id': 83,
    'indicator_code': 'full_time_physicians_hospitals',
    'indicator_name': 'Hauptamtliche Ärzte in Krankenhäusern',
    'indicator_name_en': 'Full-time Physicians in Hospitals by Gender',
    'indicator_category': 'Health',
    'indicator_subcategory': 'Healthcare Workforce',
    'description': 'Number of full-time employed physicians in hospitals, broken down by gender (male/female/total)',
    'unit_of_measure': 'Persons',
    'unit_description': 'Number of physicians',
    'source_system': 'state_db',
    'source_table_id': '23111-12i',
    'calculation_method': 'Direct extraction from State Database NRW. Notes field contains gender breakdown (total/male/female).',
    'data_type': 'numeric',
    'is_active': True
}

def main():
    print("=" * 80)
    print("SETUP PHYSICIANS INDICATOR (ID 83)")
    print("=" * 80)
    db = DatabaseManager()
    try:
        with db.get_connection() as conn:
            ind = PHYSICIANS_INDICATOR

            result = conn.execute(
                text("SELECT indicator_id FROM dim_indicator WHERE indicator_id = :id"),
                {"id": ind['indicator_id']}
            )

            if result.fetchone():
                print(f"  [{ind['indicator_id']}] Already exists: {ind['indicator_name']}")
            else:
                conn.execute(text("""
                    INSERT INTO dim_indicator (
                        indicator_id, indicator_code, indicator_name, indicator_name_en,
                        indicator_category, indicator_subcategory, description,
                        unit_of_measure, unit_description, source_system, source_table_id,
                        calculation_method, data_type, is_active, created_at, updated_at
                    )
                    VALUES (
                        :indicator_id, :indicator_code, :indicator_name, :indicator_name_en,
                        :indicator_category, :indicator_subcategory, :description,
                        :unit_of_measure, :unit_description, :source_system, :source_table_id,
                        :calculation_method, :data_type, :is_active, :created_at, :updated_at
                    )
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
