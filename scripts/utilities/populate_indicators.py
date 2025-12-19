"""
Populate Indicator Dimension Table
Regional Economics Database for NRW

Populates dim_indicator with demographics and labor market indicators.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.database import get_database
from utils.logging import setup_logging, get_logger


setup_logging(level="INFO")
logger = get_logger(__name__)


# Indicator Metadata
INDICATORS = [
    # Demographics Indicators
    {
        'indicator_code': 'pop_total',
        'indicator_name': 'Bevölkerung insgesamt',
        'indicator_name_en': 'Total Population',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'population',
        'source_system': 'regional_db',
        'source_table_id': '12411-03-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of persons',
        'description': 'Total population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_male',
        'indicator_name': 'Bevölkerung männlich',
        'indicator_name_en': 'Male Population',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'population',
        'source_system': 'regional_db',
        'source_table_id': '12411-03-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of male persons',
        'description': 'Male population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_female',
        'indicator_name': 'Bevölkerung weiblich',
        'indicator_name_en': 'Female Population',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'population',
        'source_system': 'regional_db',
        'source_table_id': '12411-03-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of female persons',
        'description': 'Female population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_age_0_17',
        'indicator_name': 'Bevölkerung unter 18 Jahren',
        'indicator_name_en': 'Population under 18 years',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'age_distribution',
        'source_system': 'regional_db',
        'source_table_id': '12411-04-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of persons under 18',
        'description': 'Population under 18 years by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_age_18_64',
        'indicator_name': 'Bevölkerung 18 bis unter 65 Jahren',
        'indicator_name_en': 'Population 18 to 64 years',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'age_distribution',
        'source_system': 'regional_db',
        'source_table_id': '12411-04-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of persons aged 18-64',
        'description': 'Working age population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_age_65_plus',
        'indicator_name': 'Bevölkerung 65 Jahre und älter',
        'indicator_name_en': 'Population 65 years and older',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'age_distribution',
        'source_system': 'regional_db',
        'source_table_id': '12411-04-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of persons 65+',
        'description': 'Senior population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_german',
        'indicator_name': 'Deutsche Bevölkerung',
        'indicator_name_en': 'German Population',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'nationality',
        'source_system': 'regional_db',
        'source_table_id': '12411-05-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of German nationals',
        'description': 'German population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'pop_foreign',
        'indicator_name': 'Ausländische Bevölkerung',
        'indicator_name_en': 'Foreign Population',
        'indicator_category': 'demographics',
        'indicator_subcategory': 'nationality',
        'source_system': 'regional_db',
        'source_table_id': '12411-05-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of foreign nationals',
        'description': 'Foreign population by region and year',
        'update_frequency': 'annual',
        'typical_reference_date': 'December 31',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },

    # Labor Market Indicators
    {
        'indicator_code': 'employment_total',
        'indicator_name': 'Sozialversicherungspflichtig Beschäftigte insgesamt',
        'indicator_name_en': 'Total Employees Subject to Social Insurance',
        'indicator_category': 'labor_market',
        'indicator_subcategory': 'employment',
        'source_system': 'regional_db',
        'source_table_id': '13111-01-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of employees',
        'description': 'Total employees subject to social insurance contributions',
        'update_frequency': 'quarterly',
        'typical_reference_date': 'June 30',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'employment_fulltime',
        'indicator_name': 'Vollzeitbeschäftigte',
        'indicator_name_en': 'Full-time Employees',
        'indicator_category': 'labor_market',
        'indicator_subcategory': 'employment',
        'source_system': 'regional_db',
        'source_table_id': '13111-02-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of full-time employees',
        'description': 'Full-time employees by region and year',
        'update_frequency': 'quarterly',
        'typical_reference_date': 'June 30',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
    {
        'indicator_code': 'employment_parttime',
        'indicator_name': 'Teilzeitbeschäftigte',
        'indicator_name_en': 'Part-time Employees',
        'indicator_category': 'labor_market',
        'indicator_subcategory': 'employment',
        'source_system': 'regional_db',
        'source_table_id': '13111-02-03-4',
        'unit_of_measure': 'persons',
        'unit_description': 'Number of part-time employees',
        'description': 'Part-time employees by region and year',
        'update_frequency': 'quarterly',
        'typical_reference_date': 'June 30',
        'data_type': 'count',
        'is_derived': False,
        'aggregation_method': 'sum',
        'is_active': True
    },
]


def populate_indicators():
    """Populate dim_indicator table with indicator metadata."""
    logger.info("Starting indicator dimension population")

    db = get_database('regional_economics')

    # Check if data already exists
    existing = db.execute_query("SELECT COUNT(*) as count FROM dim_indicator")
    if existing[0]['count'] > 0:
        logger.warning(f"Indicator table already has {existing[0]['count']} records")
        response = input("Do you want to clear and repopulate? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborting population")
            return

        # Clear existing data
        db.execute_statement("DELETE FROM dim_indicator")
        logger.info("Cleared existing indicator data")

    # Bulk insert
    try:
        count = db.bulk_insert('dim_indicator', INDICATORS)
        logger.info(f"Successfully inserted {count} indicator records")

        # Verify
        result = db.execute_query("""
            SELECT indicator_category, COUNT(*) as count
            FROM dim_indicator
            GROUP BY indicator_category
        """)

        logger.info("Indicator breakdown:")
        for row in result:
            logger.info(f"  {row['indicator_category']}: {row['count']}")

        # Show all indicators
        all_indicators = db.execute_query("""
            SELECT indicator_id, indicator_code, indicator_name_en
            FROM dim_indicator
            ORDER BY indicator_id
        """)

        logger.info("\nAll indicators:")
        for ind in all_indicators:
            logger.info(f"  ID {ind['indicator_id']}: {ind['indicator_code']} - {ind['indicator_name_en']}")

        return True

    except Exception as e:
        logger.error(f"Error inserting indicator data: {e}")
        return False


if __name__ == "__main__":
    success = populate_indicators()
    sys.exit(0 if success else 1)
