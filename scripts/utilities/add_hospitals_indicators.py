"""Setup Hospitals Indicators in Database"""
import sys
sys.path.append('.')
from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

HOSPITALS_INDICATORS = [
    {
        'indicator_id': 84,
        'indicator_code': 'hospitals_by_operator',
        'indicator_name': 'Anzahl der Krankenhäuser',
        'indicator_name_en': 'Number of Hospitals by Operator Type',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Healthcare Infrastructure',
        'description': 'Number of hospitals broken down by operator type (public, private, non-profit)',
        'unit_of_measure': 'Facilities',
        'unit_description': 'Number of hospitals',
        'source_system': 'state_db',
        'source_table_id': '23111-01i',
        'calculation_method': 'Direct extraction from State Database NRW. Notes field contains operator type (total/public/private/nonprofit).',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 85,
        'indicator_code': 'hospital_beds_by_operator',
        'indicator_name': 'Aufgestellte Betten in Krankenhäusern',
        'indicator_name_en': 'Available Hospital Beds by Operator Type',
        'indicator_category': 'Health',
        'indicator_subcategory': 'Healthcare Infrastructure',
        'description': 'Number of available hospital beds (annual average) broken down by operator type (public, private, non-profit)',
        'unit_of_measure': 'Beds',
        'unit_description': 'Number of hospital beds',
        'source_system': 'state_db',
        'source_table_id': '23111-01i',
        'calculation_method': 'Direct extraction from State Database NRW. Notes field contains operator type (total/public/private/nonprofit).',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    print("=" * 80)
    print("SETUP HOSPITALS INDICATORS (IDs 84-85)")
    print("=" * 80)
    db = DatabaseManager()
    try:
        with db.get_connection() as conn:
            for ind in HOSPITALS_INDICATORS:
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
