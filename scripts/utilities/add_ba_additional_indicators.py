"""
Add BA Additional Indicators (92-100) to Database
Covers economic sectors, occupations, and low-wage workers
Source: Federal Employment Agency (BA) - Sheets 8.3, 8.4, 8.5
"""

import sys
from pathlib import Path
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


# Define BA additional indicators (92-100)
BA_ADDITIONAL_INDICATORS = [
    # Sheet 8.3: Economic Sectors (WZ 2008) - Indicators 92-94
    {
        'indicator_id': 92,
        'indicator_code': 'ba_employment_by_sector',
        'indicator_name': 'Vollzeitbeschäftigte nach Wirtschaftszweigen (WZ 2008)',
        'indicator_name_en': 'Full-time Employees by Economic Sector (WZ 2008)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Employment by Sector',
        'description': '''Full-time employees subject to social insurance by economic sector classification (WZ 2008).

        Provides employment counts broken down by 22 economic sectors including:
        - A: Agriculture, forestry, fishing
        - B-F: Manufacturing, mining, construction
        - G-J: Trade, transport, hospitality, IT
        - K-N: Finance, insurance, business services
        - O-U: Public and other services

        Enables analysis of sectoral employment concentration and economic structure by district.

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.3''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.3',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 93,
        'indicator_code': 'ba_median_wage_by_sector',
        'indicator_name': 'Medianentgelt nach Wirtschaftszweigen (WZ 2008)',
        'indicator_name_en': 'Median Monthly Gross Wage by Economic Sector (WZ 2008)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wages by Sector',
        'description': '''Median monthly gross wage for full-time employees by economic sector (WZ 2008).

        Enables comparison of wage levels across sectors:
        - Identify high-wage sectors (e.g., finance, IT)
        - Compare manufacturing vs. service sector wages
        - Track sectoral wage disparities by district

        Unit: EUR per month (gross)
        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.3''',
        'unit_of_measure': 'EUR',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.3',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 94,
        'indicator_code': 'ba_wage_distribution_by_sector',
        'indicator_name': 'Entgeltverteilung nach Wirtschaftszweigen (WZ 2008)',
        'indicator_name_en': 'Wage Distribution by Economic Sector (WZ 2008)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wage Distribution by Sector',
        'description': '''Distribution of full-time employees across wage brackets by economic sector (WZ 2008).

        Six wage brackets:
        - Under 2,000 EUR
        - 2,000 - 3,000 EUR
        - 3,000 - 4,000 EUR
        - 4,000 - 5,000 EUR
        - 5,000 - 6,000 EUR
        - Over 6,000 EUR

        Enables analysis of:
        - Wage inequality within sectors
        - Low-wage employment by sector
        - Sectoral wage structures

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.3''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.3',
        'update_frequency': 'Annual',
        'is_active': True,
    },

    # Sheet 8.4: Occupations (KldB 2010) - Indicators 95-97
    {
        'indicator_id': 95,
        'indicator_code': 'ba_employment_by_occupation',
        'indicator_name': 'Vollzeitbeschäftigte nach Berufen (KldB 2010)',
        'indicator_name_en': 'Full-time Employees by Occupation (KldB 2010)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Employment by Occupation',
        'description': '''Full-time employees subject to social insurance by occupation classification (KldB 2010).

        Provides employment counts broken down by ~61 occupation categories including:
        - 01-09: Raw materials, production, manufacturing
        - 11-29: Construction, architecture, engineering
        - 21-29: Science, geography, IT
        - 31-34: Transport, logistics, security
        - 41-43: Business services, trade, hospitality
        - 51-54: Business organization, accounting, law
        - 61-63: Health, social services, education
        - 71-73: Humanities, culture, design

        Enables analysis of:
        - Skill demands by district
        - Occupational specialization
        - Labor market structure

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.4''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.4',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 96,
        'indicator_code': 'ba_median_wage_by_occupation',
        'indicator_name': 'Medianentgelt nach Berufen (KldB 2010)',
        'indicator_name_en': 'Median Monthly Gross Wage by Occupation (KldB 2010)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wages by Occupation',
        'description': '''Median monthly gross wage for full-time employees by occupation (KldB 2010).

        Enables comparison of wage levels across occupations:
        - Compare wages for same occupation across districts
        - Identify high-wage occupations in each region
        - Track occupational wage premiums
        - Analyze skill-based wage differentials

        Unit: EUR per month (gross)
        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.4''',
        'unit_of_measure': 'EUR',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.4',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 97,
        'indicator_code': 'ba_wage_distribution_by_occupation',
        'indicator_name': 'Entgeltverteilung nach Berufen (KldB 2010)',
        'indicator_name_en': 'Wage Distribution by Occupation (KldB 2010)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Wage Distribution by Occupation',
        'description': '''Distribution of full-time employees across wage brackets by occupation (KldB 2010).

        Six wage brackets:
        - Under 2,000 EUR
        - 2,000 - 3,000 EUR
        - 3,000 - 4,000 EUR
        - 4,000 - 5,000 EUR
        - 5,000 - 6,000 EUR
        - Over 6,000 EUR

        Enables analysis of:
        - Wage inequality within occupations
        - Low-wage occupations
        - Occupational wage structures

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.4''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.4',
        'update_frequency': 'Annual',
        'is_active': True,
    },

    # Sheet 8.5: Low-Wage Workers - Indicators 98-100
    {
        'indicator_id': 98,
        'indicator_code': 'ba_low_wage_workers_count',
        'indicator_name': 'Beschäftigte im unteren Entgeltbereich (Anzahl)',
        'indicator_name_en': 'Low-Wage Workers (Count)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Income Inequality',
        'description': '''Number of full-time employees earning below low-wage thresholds.

        Three thresholds tracked (2024 values):
        - National threshold: 2,676 EUR per month
        - West Germany threshold: 2,745 EUR per month
        - East Germany threshold: 2,359 EUR per month

        Low-wage threshold defined as two-thirds of median wage in respective region.

        Enables analysis of:
        - Working poverty risk by district
        - Regional wage disparities
        - Income inequality patterns

        Note: Threshold values updated annually based on wage developments.

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.5''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.5',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 99,
        'indicator_code': 'ba_low_wage_workers_percentage',
        'indicator_name': 'Beschäftigte im unteren Entgeltbereich (Anteil)',
        'indicator_name_en': 'Low-Wage Workers (Percentage)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Income Inequality',
        'description': '''Percentage of full-time employees earning below low-wage thresholds.

        Calculated as: (Low-wage workers / Total full-time employees) × 100

        Three thresholds tracked:
        - National threshold (2024: 2,676 EUR)
        - West Germany threshold (2024: 2,745 EUR)
        - East Germany threshold (2024: 2,359 EUR)

        Enables comparison of low-wage employment rates across districts,
        controlling for district size differences.

        Typical range: 8-15% in NRW districts (higher in service-oriented cities)

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.5''',
        'unit_of_measure': 'Percent',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.5',
        'update_frequency': 'Annual',
        'is_active': True,
    },
    {
        'indicator_id': 100,
        'indicator_code': 'ba_total_fulltime_employees_baseline',
        'indicator_name': 'Vollzeitbeschäftigte (Basislinie für Niedriglohnanalyse)',
        'indicator_name_en': 'Full-time Employees (Baseline for Low-Wage Analysis)',
        'indicator_category': 'Labor Market',
        'indicator_subcategory': 'Employment',
        'description': '''Total full-time employees subject to social insurance - baseline for low-wage analysis.

        This indicator provides the denominator for calculating low-wage percentages.
        Same population as indicator 89, but stored separately in Sheet 8.5 context.

        Use this indicator in conjunction with indicators 98-99 for low-wage analysis.

        Reference date: December 31 of each year
        Source: BA Employment/Wage Statistics, Sheet 8.5''',
        'unit_of_measure': 'Persons',
        'source_system': 'ba',
        'source_table_id': 'Sheet 8.5',
        'update_frequency': 'Annual',
        'is_active': True,
    },
]


def add_indicators():
    """Add BA additional indicators to the database."""
    db = DatabaseManager()

    insert_query = text("""
        INSERT INTO dim_indicator (
            indicator_id, indicator_code, indicator_name, indicator_name_en,
            indicator_category, indicator_subcategory, description,
            unit_of_measure, source_system, source_table_id,
            update_frequency, is_active
        )
        VALUES (
            :indicator_id, :indicator_code, :indicator_name, :indicator_name_en,
            :indicator_category, :indicator_subcategory, :description,
            :unit_of_measure, :source_system, :source_table_id,
            :update_frequency, :is_active
        )
        ON CONFLICT (indicator_id) DO UPDATE SET
            indicator_code = EXCLUDED.indicator_code,
            indicator_name = EXCLUDED.indicator_name,
            indicator_name_en = EXCLUDED.indicator_name_en,
            indicator_category = EXCLUDED.indicator_category,
            indicator_subcategory = EXCLUDED.indicator_subcategory,
            description = EXCLUDED.description,
            unit_of_measure = EXCLUDED.unit_of_measure,
            source_system = EXCLUDED.source_system,
            source_table_id = EXCLUDED.source_table_id,
            update_frequency = EXCLUDED.update_frequency,
            is_active = EXCLUDED.is_active
    """)

    logger.info("=" * 80)
    logger.info("ADDING BA ADDITIONAL INDICATORS (92-100)")
    logger.info("=" * 80)
    logger.info("")

    added_count = 0

    with db.get_connection() as conn:
        for indicator in BA_ADDITIONAL_INDICATORS:
            try:
                conn.execute(insert_query, indicator)
                logger.info(f"✓ Added indicator {indicator['indicator_id']}: {indicator['indicator_name_en']}")
                added_count += 1
            except Exception as e:
                logger.error(f"✗ Error adding indicator {indicator['indicator_id']}: {e}")
                raise

    logger.info("")
    logger.info(f"✓ Successfully added {added_count} indicators")
    logger.info("")
    logger.info("Indicator breakdown:")
    logger.info("  Sheet 8.3 (Economic Sectors): Indicators 92-94")
    logger.info("  Sheet 8.4 (Occupations): Indicators 95-97")
    logger.info("  Sheet 8.5 (Low-Wage Workers): Indicators 98-100")
    logger.info("")
    logger.info("=" * 80)


def verify_indicators():
    """Verify indicators were added successfully."""
    db = DatabaseManager()

    logger.info("Verifying indicators...")

    query = text("""
        SELECT indicator_id, indicator_code, indicator_name_en, source_table_id
        FROM dim_indicator
        WHERE indicator_id BETWEEN 92 AND 100
        ORDER BY indicator_id
    """)

    with db.get_connection() as conn:
        results = conn.execute(query).fetchall()

    if len(results) == 9:
        logger.info(f"✓ All 9 indicators verified in database")
        logger.info("")
        for row in results:
            logger.info(f"  {row[0]:3}: {row[1]:40} ({row[3]})")
    else:
        logger.error(f"✗ Expected 9 indicators, found {len(results)}")


def main():
    """Main execution."""
    print("=" * 80)
    print("BA ADDITIONAL INDICATORS SETUP")
    print("Adding indicators 92-100 to database")
    print("=" * 80)
    print()

    try:
        add_indicators()
        verify_indicators()

        print()
        print("=" * 80)
        print("✓ SETUP COMPLETE")
        print("=" * 80)
        print()
        print("Next steps:")
        print("  1. Create extractors for sheets 8.3, 8.4, 8.5")
        print("  2. Enhance database schema (add sector/occupation fields)")
        print("  3. Create transformers for new data types")
        print("  4. Run ETL pipelines")
        print()

    except Exception as e:
        logger.error(f"Setup failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
