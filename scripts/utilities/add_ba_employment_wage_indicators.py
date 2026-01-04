"""
Add BA Employment/Wage Indicators to Database
Indicator IDs: 89-91

Adds three indicators for BA employment and wage data:
89 - Total full-time employees by demographic category
90 - Median monthly gross wage by demographic category
91 - Employee count by wage bracket and demographic category
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


# BA Employment/Wage indicator definitions
BA_INDICATORS = [
    {
        'indicator_id': 89,
        'indicator_code': 'ba_total_fulltime_employees',
        'indicator_name': 'Sozialversicherungspflichtig Vollzeitbeschäftigte der Kerngruppe',
        'indicator_name_en': 'Full-time Employees Subject to Social Insurance (Core Group)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Employment',
        'description': '''Total number of full-time employees subject to social insurance in the core group
(excluding marginal and irregular employment). Breakdowns by gender, age group, nationality,
education level, and skill level. Data from Federal Employment Agency (BA).

Reference date: December 31 of each year.

Demographic dimensions:
- Gender: male, female, total
- Age groups: under 25, 25-55, 55 and over
- Nationality: German, foreigner
- Education: no vocational degree, recognized vocational degree, academic degree
- Skill level: assistant (Helfer), specialist (Fachkraft), expert (Spezialist), highly qualified (Experte)''',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of full-time employees',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.1',
        'calculation_method': '''Direct extraction from BA Excel files (Sheet 8.1: Demographics and wage brackets).
Includes only employees at workplace (Arbeitsort).
Notes field stores education level and skill level categories.
Gender, age_group, and nationality dimensions stored in respective fields.''',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 90,
        'indicator_code': 'ba_median_wage',
        'indicator_name': 'Median des monatlichen Bruttoarbeitsentgelts',
        'indicator_name_en': 'Median Monthly Gross Wage',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wages',
        'description': '''Median monthly gross wage in EUR for full-time employees subject to social insurance
(core group). Breakdowns by gender, age group, nationality, education level, and skill level.
Data from Federal Employment Agency (BA).

Reference date: December 31 of each year.

Demographic dimensions:
- Gender: male, female, total
- Age groups: under 25, 25-55, 55 and over
- Nationality: German, foreigner
- Education: no vocational degree, recognized vocational degree, academic degree
- Skill level: assistant (Helfer), specialist (Fachkraft), expert (Spezialist), highly qualified (Experte)''',
        'unit_of_measure': 'EUR',
        'unit_description': 'Median monthly gross wage in EUR',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.1',
        'calculation_method': '''Direct extraction from BA Excel files (Sheet 8.1: Demographics and wage brackets).
Median calculated by BA from individual wage data.
Includes only employees at workplace (Arbeitsort).
Notes field stores education level and skill level categories.
Gender, age_group, and nationality dimensions stored in respective fields.''',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 91,
        'indicator_code': 'ba_wage_distribution',
        'indicator_name': 'Verteilung der Vollzeitbeschäftigten nach Entgeltklassen',
        'indicator_name_en': 'Distribution of Full-time Employees by Wage Brackets',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wages',
        'description': '''Number of full-time employees subject to social insurance in each monthly gross wage bracket.
Wage brackets: under 2,000 EUR, 2,000-3,000 EUR, 3,000-4,000 EUR, 4,000-5,000 EUR,
5,000-6,000 EUR, over 6,000 EUR. Breakdowns by gender, age group, nationality, education level,
and skill level. Data from Federal Employment Agency (BA).

Reference date: December 31 of each year.

Wage brackets stored in notes field as: wage_bracket:under_2000, wage_bracket:2000_to_3000,
wage_bracket:3000_to_4000, wage_bracket:4000_to_5000, wage_bracket:5000_to_6000, wage_bracket:over_6000

Demographic dimensions:
- Gender: male, female, total
- Age groups: under 25, 25-55, 55 and over
- Nationality: German, foreigner
- Education: no vocational degree, recognized vocational degree, academic degree
- Skill level: assistant (Helfer), specialist (Fachkraft), expert (Spezialist), highly qualified (Experte)''',
        'unit_of_measure': 'Persons',
        'unit_description': 'Number of employees in each wage bracket',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.1',
        'calculation_method': '''Direct extraction from BA Excel files (Sheet 8.1: Demographics and wage brackets).
Six wage brackets extracted from columns 4-9.
Wage bracket stored in notes field (e.g., "wage_bracket:under_2000").
Includes only employees at workplace (Arbeitsort).
Education level and skill level also stored in notes field when applicable.
Gender, age_group, and nationality dimensions stored in respective fields.''',
        'data_type': 'numeric',
        'is_active': True
    }
]


def add_indicators():
    """Add BA employment/wage indicators to dim_indicator table."""
    db = DatabaseManager()

    print("=" * 80)
    print("ADD BA EMPLOYMENT/WAGE INDICATORS")
    print("=" * 80)
    print()

    for indicator in BA_INDICATORS:
        logger.info(f"Adding indicator {indicator['indicator_id']}: "
                   f"{indicator['indicator_name_en']}")
        print(f"[{indicator['indicator_id']}] {indicator['indicator_name_en']}")
        print("-" * 80)

        with db.get_connection() as conn:
            # Check if indicator already exists
            check_query = text("""
                SELECT indicator_id FROM dim_indicator
                WHERE indicator_id = :indicator_id
            """)

            result = conn.execute(
                check_query,
                {'indicator_id': indicator['indicator_id']}
            ).fetchone()

            if result:
                logger.warning(f"Indicator {indicator['indicator_id']} already exists")
                print(f"⚠ Indicator {indicator['indicator_id']} already exists - updating")

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

                conn.execute(update_query, indicator)
                conn.commit()

                logger.info(f"Updated indicator {indicator['indicator_id']}")
                print(f"✓ Updated indicator {indicator['indicator_id']}")

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

                conn.execute(insert_query, indicator)
                conn.commit()

                logger.info(f"Inserted indicator {indicator['indicator_id']}")
                print(f"✓ Added indicator {indicator['indicator_id']}")

            # Verify insertion/update
            verify_query = text("""
                SELECT indicator_id, indicator_code, indicator_name_en, source_table_id
                FROM dim_indicator
                WHERE indicator_id = :indicator_id
            """)

            result = conn.execute(
                verify_query,
                {'indicator_id': indicator['indicator_id']}
            ).fetchone()

            if result:
                print(f"  Code: {result[1]}")
                print(f"  Source: {result[3]}")
            else:
                logger.error(f"Verification failed for indicator {indicator['indicator_id']}")
                print(f"✗ Verification failed")
                return False

        print()

    return True


def main():
    """Main function."""
    try:
        success = add_indicators()

        if success:
            print("=" * 80)
            print("✓ ALL INDICATORS SETUP COMPLETE")
            print("=" * 80)
            print()
            print("Added/Updated indicators:")
            print("  89 - Full-time Employees (by demographics)")
            print("  90 - Median Monthly Gross Wage (by demographics)")
            print("  91 - Wage Distribution (by brackets and demographics)")
            print()
        else:
            print("=" * 80)
            print("✗ INDICATOR SETUP FAILED")
            print("=" * 80)
            sys.exit(1)

    except Exception as e:
        logger.error("Error adding indicators", exc_info=True)
        print(f"\n✗ Error: {str(e)}")
        sys.exit(1)


if __name__ == '__main__':
    main()
