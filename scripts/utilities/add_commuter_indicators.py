"""
Add Commuter Statistics Indicators to Database
Adds indicators 101-103 for incoming/outgoing commuter data
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


def main():
    """Add commuter statistics indicators to dim_indicator table."""
    logger.info("Adding commuter statistics indicators (101-103)")

    indicators = [
        {
            'indicator_id': 101,
            'indicator_code': 'BA_COMMUTER_INCOMING',
            'indicator_name': 'Einpendler (Incoming Commuters)',
            'indicator_name_en': 'Incoming Commuters',
            'indicator_category': 'Labor Market',
            'indicator_subcategory': 'Commuter Statistics',
            'description': 'People who commute into an NRW district to work. Includes demographic breakdowns by gender, nationality, and trainee status. Data from Federal Employment Agency (BA).',
            'unit_of_measure': 'Persons',
            'source_system': 'BA',
            'source_table_id': 'BA Pendlerstatistik',
            'update_frequency': 'Annual',
            'is_active': True,
        },
        {
            'indicator_id': 102,
            'indicator_code': 'BA_COMMUTER_OUTGOING',
            'indicator_name': 'Auspendler (Outgoing Commuters)',
            'indicator_name_en': 'Outgoing Commuters',
            'indicator_category': 'Labor Market',
            'indicator_subcategory': 'Commuter Statistics',
            'description': 'People who commute out of an NRW district to work elsewhere. Includes demographic breakdowns by gender, nationality, and trainee status. Data from Federal Employment Agency (BA).',
            'unit_of_measure': 'Persons',
            'source_system': 'BA',
            'source_table_id': 'BA Pendlerstatistik',
            'update_frequency': 'Annual',
            'is_active': True,
        },
        {
            'indicator_id': 103,
            'indicator_code': 'BA_COMMUTER_BALANCE',
            'indicator_name': 'Pendlersaldo (Net Commuter Balance)',
            'indicator_name_en': 'Net Commuter Balance',
            'indicator_category': 'Labor Market',
            'indicator_subcategory': 'Commuter Statistics',
            'description': 'Net balance of commuters (Incoming - Outgoing). Positive values indicate the district attracts more workers than it loses (jobs importer), negative values indicate more residents work elsewhere (jobs exporter). Calculated from BA commuter statistics.',
            'unit_of_measure': 'Persons',
            'source_system': 'BA',
            'source_table_id': 'BA Pendlerstatistik (derived)',
            'update_frequency': 'Annual',
            'is_active': True,
        },
    ]

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
            is_active = EXCLUDED.is_active,
            updated_at = CURRENT_TIMESTAMP
    """)

    with db.get_connection() as conn:
        for ind in indicators:
            try:
                conn.execute(insert_query, ind)
                logger.info(f"✓ Added/Updated indicator {ind['indicator_id']}: {ind['indicator_name']}")
            except Exception as e:
                logger.error(f"✗ Error adding indicator {ind['indicator_id']}: {e}")

    # Verify
    logger.info("\nVerifying indicators were added...")
    verify_query = text("""
        SELECT indicator_id, indicator_code, indicator_name, indicator_category
        FROM dim_indicator
        WHERE indicator_id BETWEEN 101 AND 103
        ORDER BY indicator_id
    """)

    with db.get_connection() as conn:
        result = conn.execute(verify_query).fetchall()

        if result:
            print("\n" + "=" * 100)
            print("COMMUTER STATISTICS INDICATORS ADDED SUCCESSFULLY")
            print("=" * 100)
            for row in result:
                print(f"{row[0]:3d} | {row[1]:30s} | {row[2]:40s} | {row[3]}")
            print("=" * 100)
        else:
            logger.error("Indicators not found in database!")

    logger.info("Commuter indicators setup complete")


if __name__ == '__main__':
    main()
