"""
Setup Population Profile Indicators in Database
Creates dim_indicator entries for table 12411-9k06 (Population by gender, nationality, age groups)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Population profile indicators based on table 12411-9k06 structure
# IDs 67-71
POPULATION_PROFILE_INDICATORS = [
    {
        'indicator_id': 67,
        'indicator_code': 'pop_total_by_age',
        'indicator_name': 'Bevölkerung nach Altersgruppen (gesamt)',
        'indicator_name_en': 'Total Population by Age Group',
        'indicator_category': 'Demographics',
        'indicator_subcategory': 'Population by Age',
        'description': 'Total population count by age group (NRW state level)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of persons',
        'source_system': 'state_db',
        'source_table_id': '12411-9k06',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 68,
        'indicator_code': 'pop_male_by_age',
        'indicator_name': 'Männliche Bevölkerung nach Altersgruppen',
        'indicator_name_en': 'Male Population by Age Group',
        'indicator_category': 'Demographics',
        'indicator_subcategory': 'Population by Age',
        'description': 'Male population count by age group (NRW state level)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of male persons',
        'source_system': 'state_db',
        'source_table_id': '12411-9k06',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 69,
        'indicator_code': 'pop_female_by_age',
        'indicator_name': 'Weibliche Bevölkerung nach Altersgruppen',
        'indicator_name_en': 'Female Population by Age Group',
        'indicator_category': 'Demographics',
        'indicator_subcategory': 'Population by Age',
        'description': 'Female population count by age group (NRW state level)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of female persons',
        'source_system': 'state_db',
        'source_table_id': '12411-9k06',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 70,
        'indicator_code': 'pop_german_by_age',
        'indicator_name': 'Deutsche Bevölkerung nach Altersgruppen',
        'indicator_name_en': 'German Population by Age Group',
        'indicator_category': 'Demographics',
        'indicator_subcategory': 'Population by Age',
        'description': 'German citizen population count by age group (NRW state level)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of German citizens',
        'source_system': 'state_db',
        'source_table_id': '12411-9k06',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 71,
        'indicator_code': 'pop_foreign_by_age',
        'indicator_name': 'Ausländische Bevölkerung nach Altersgruppen',
        'indicator_name_en': 'Foreign Population by Age Group',
        'indicator_category': 'Demographics',
        'indicator_subcategory': 'Population by Age',
        'description': 'Foreign national population count by age group (NRW state level)',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of foreign nationals',
        'source_system': 'state_db',
        'source_table_id': '12411-9k06',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert population profile indicator definitions into database."""
    print("="*80)
    print("SETUP POPULATION PROFILE INDICATORS")
    print("="*80)
    print(f"Creating {len(POPULATION_PROFILE_INDICATORS)} indicators (IDs 67-71)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in POPULATION_PROFILE_INDICATORS:
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

        print("\n" + "="*80)
        print("SUCCESS: All population profile indicators created/updated")
        print("="*80)
        print("\nYou can now run the population profile ETL pipeline:")
        print("  python pipelines/state_db/etl_12411_9k06_population_profile.py --test")

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
