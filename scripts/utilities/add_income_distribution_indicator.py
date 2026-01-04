"""
Add Income Distribution Indicator to Database
Indicator ID: 88

Adds indicator for population by personal net income and gender
from State Database NRW table 12211-9114i.
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


# Income distribution indicator definition
INCOME_DISTRIBUTION_INDICATOR = {
    'indicator_id': 88,
    'indicator_code': 'population_income_distribution',
    'indicator_name': 'Bevölkerung nach persönlichem Nettoeinkommen und Geschlecht',
    'indicator_name_en': 'Population by Personal Net Income and Gender',
    'indicator_category': 'Demographics',
    'indicator_subcategory': 'Income Distribution',
    'description': '''Population at main residence by personal net income brackets and gender (Microcensus).
Income brackets: under 150 EUR, 150-300, 300-500, 500-700, 700-900, 900-1100, 1100-1300, 1300-1500, 1500+ EUR.
Gender: male/female/total. Notes field contains income_bracket categories.''',
    'unit_of_measure': 'Thousands',
    'unit_description': 'Population in thousands (1000)',
    'source_system': 'state_db',
    'source_table_id': '12211-9114i',
    'calculation_method': 'Direct extraction from State Database NRW Microcensus. Notes field contains income_bracket (9 categories from under_150 to 1500_and_more). Gender dimension stored in gender field.',
    'data_type': 'numeric',
    'is_active': True
}


def add_indicator():
    """Add income distribution indicator to dim_indicator table."""
    db = DatabaseManager()

    logger.info(f"Adding indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']}: "
                f"{INCOME_DISTRIBUTION_INDICATOR['indicator_name_en']}")

    with db.get_connection() as conn:
        # Check if indicator already exists
        check_query = text("""
            SELECT indicator_id FROM dim_indicator
            WHERE indicator_id = :indicator_id
        """)

        result = conn.execute(
            check_query,
            {'indicator_id': INCOME_DISTRIBUTION_INDICATOR['indicator_id']}
        ).fetchone()

        if result:
            logger.warning(f"Indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']} already exists")
            print(f"⚠ Indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']} already exists in database")

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

            conn.execute(update_query, INCOME_DISTRIBUTION_INDICATOR)
            conn.commit()

            logger.info(f"Updated indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']}")
            print(f"✓ Updated indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']}")

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

            conn.execute(insert_query, INCOME_DISTRIBUTION_INDICATOR)
            conn.commit()

            logger.info(f"Inserted indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']}")
            print(f"✓ Added indicator {INCOME_DISTRIBUTION_INDICATOR['indicator_id']} to database")

        # Verify insertion/update
        verify_query = text("""
            SELECT indicator_id, indicator_code, indicator_name_en, source_table_id
            FROM dim_indicator
            WHERE indicator_id = :indicator_id
        """)

        result = conn.execute(
            verify_query,
            {'indicator_id': INCOME_DISTRIBUTION_INDICATOR['indicator_id']}
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
    print("ADD INCOME DISTRIBUTION INDICATOR")
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
