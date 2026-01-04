"""Setup Nursing Home Recipients Indicator in Database"""
import sys
sys.path.append('.')
from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

NURSING_HOME_RECIPIENTS_INDICATOR = {
    'indicator_id': 82,
    'indicator_code': 'nursing_home_recipients_by_care_level',
    'indicator_name': 'Pflegebedürftige in Pflegeheimen',
    'indicator_name_en': 'Care Recipients in Nursing Homes by Care Level and Type',
    'indicator_category': 'Health',
    'indicator_subcategory': 'Care Recipients',
    'description': 'Number of people in need of care in nursing homes, broken down by care level (Pflegegrad 1-5) and type of care service (full/partial inpatient)',
    'unit_of_measure': 'Persons',
    'unit_description': 'Number of care recipients',
    'source_system': 'state_db',
    'source_table_id': '22412-02i',
    'calculation_method': 'Direct extraction from State Database NRW. Notes field contains category (total/care_type/care_level) with specific breakdowns.',
    'data_type': 'numeric',
    'is_active': True
}

def main():
    print("=" * 80)
    print("SETUP NURSING HOME RECIPIENTS INDICATOR (ID 82)")
    print("=" * 80)
    db = DatabaseManager()
    try:
        with db.get_connection() as conn:
            ind = NURSING_HOME_RECIPIENTS_INDICATOR

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
