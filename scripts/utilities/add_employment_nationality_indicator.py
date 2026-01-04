"""
Add Employment by Nationality Indicator to Database
Indicator ID: 87

Adds indicator for population by employment status, nationality, and gender
from State Database NRW table 12211-9124i.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy import text
from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


# Employment by nationality indicator definition
EMPLOYMENT_NATIONALITY_INDICATOR = {
    'indicator_id': 87,
    'indicator_code': 'population_employment_nationality',
    'indicator_name': 'Bevölkerung nach Erwerbsstatus, Staatsangehörigkeit und Geschlecht',
    'indicator_name_en': 'Population by Employment Status, Nationality and Gender',
    'indicator_category': 'Demographics',
    'indicator_subcategory': 'Employment & Nationality',
    'description': '''Population at main residence by employment status, nationality, and gender (Microcensus).
Nationality: total/German/foreigner. Employment status: employed/unemployed/not in labor force/total.
Gender: male/female/total. Notes field contains nationality and employment_status combinations.''',
    'unit_of_measure': 'Thousands',
    'unit_description': 'Population in thousands (1000)',
    'source_system': 'state_db',
    'source_table_id': '12211-9124i',
    'calculation_method': 'Direct extraction from State Database NRW Microcensus. Notes field contains nationality (total/german/foreigner) and employment_status (total/employed/unemployed/not_in_labor_force). Gender dimension stored in gender field.',
    'data_type': 'numeric',
    'is_active': True
}


def add_indicator():
    """Add employment by nationality indicator to dim_indicator table."""
    db = DatabaseManager()

    logger.info(f"Adding indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}: "
                f"{EMPLOYMENT_NATIONALITY_INDICATOR['indicator_name_en']}")

    with db.get_connection() as conn:
        # Check if indicator already exists
        check_query = text("""
            SELECT indicator_id FROM dim_indicator
            WHERE indicator_id = :indicator_id
        """)

        result = conn.execute(
            check_query,
            {'indicator_id': EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}
        ).fetchone()

        if result:
            logger.warning(f"Indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']} already exists")
            print(f"⚠ Indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']} already exists in database")

            # Update existing indicator
            update_query = text("""
                UPDATE dim_indicator
                SET indicator_code = :indicator_code,
                    indicator_name = :indicator_name,
                    indicator_name_en = :indicator_name_en,
                    indicator_category = :indicator_category,
                    indicator_subcategory = :indicator_subcategory,
                    description = :description,
                    unit_of_measure = :unit_of_measure,
                    unit_description = :unit_description,
                    source_system = :source_system,
                    source_table_id = :source_table_id,
                    calculation_method = :calculation_method,
                    data_type = :data_type,
                    is_active = :is_active,
                    updated_at = CURRENT_TIMESTAMP
                WHERE indicator_id = :indicator_id
            """)

            conn.execute(update_query, EMPLOYMENT_NATIONALITY_INDICATOR)
            conn.commit()

            logger.info(f"Updated indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}")
            print(f"✓ Updated indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}")

        else:
            # Insert new indicator
            insert_query = text("""
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
                    :calculation_method, :data_type, :is_active, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
            """)

            conn.execute(insert_query, EMPLOYMENT_NATIONALITY_INDICATOR)
            conn.commit()

            logger.info(f"Inserted indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}")
            print(f"✓ Added indicator {EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']} to database")

        # Verify insertion/update
        verify_query = text("""
            SELECT indicator_id, indicator_code, indicator_name_en, source_table_id
            FROM dim_indicator
            WHERE indicator_id = :indicator_id
        """)

        result = conn.execute(
            verify_query,
            {'indicator_id': EMPLOYMENT_NATIONALITY_INDICATOR['indicator_id']}
        ).fetchone()

        if result:
            print(f"\nIndicator details:")
            print(f"  ID: {result[0]}")
            print(f"  Code: {result[1]}")
            print(f"  Name: {result[2]}")
            print(f"  Source Table: {result[3]}")
        else:
            logger.error("Verification failed - indicator not found after insert")
            print(f"✗ Verification failed")
            return False

    return True


def main():
    """Main function."""
    print("=" * 80)
    print("ADD EMPLOYMENT BY NATIONALITY INDICATOR")
    print("=" * 80)
    print()

    try:
        success = add_indicator()

        if success:
            print()
            print("=" * 80)
            print("✓ INDICATOR SETUP COMPLETE")
            print("=" * 80)
        else:
            print()
            print("=" * 80)
            print("✗ INDICATOR SETUP FAILED")
            print("=" * 80)
            sys.exit(1)

    except Exception as e:
        logger.error("Error adding indicator", exc_info=True)
        print(f"\n✗ Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
