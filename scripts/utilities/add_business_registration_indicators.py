"""
Add business registration indicators to database
Indicators 24 and 25 for registrations and deregistrations
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


def add_indicators():
    """Add business registration/deregistration indicators"""
    
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        try:
            # Check if indicators already exist
            result = conn.execute(text("SELECT indicator_id, indicator_code FROM dim_indicator WHERE indicator_id IN (24, 25)"))
            existing = result.fetchall()
            
            if existing:
                logger.info(f"Found existing indicators: {existing}")
                for row in existing:
                    logger.info(f"  ID {row[0]}: {row[1]}")
            
            # Add indicator 24: Business registrations
            logger.info("\nAdding Indicator 24: Business Registrations...")
            
            conn.execute(text("""
            INSERT INTO dim_indicator (
                indicator_id,
                indicator_code,
                indicator_name,
                indicator_category,
                source_system,
                source_table_id,
                unit_of_measure,
                data_type,
                description
            ) VALUES (
                24,
                'business_registrations',
                'Business registrations by category',
                'business_economy',
                'regional_db',
                '52311-01-04-4',
                'count',
                'count',
                'Business registrations with subcategories: Total, New establishments, Business foundations, Relocations in, Takeovers. Annual totals 1998-2024.'
            )
            ON CONFLICT (indicator_id) DO UPDATE SET
                indicator_code = EXCLUDED.indicator_code,
                indicator_name = EXCLUDED.indicator_name,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
            """))
            
            logger.info("✅ Indicator 24 added/updated")
            
            # Add indicator 25: Business deregistrations
            logger.info("\nAdding Indicator 25: Business Deregistrations...")
            
            conn.execute(text("""
            INSERT INTO dim_indicator (
                indicator_id,
                indicator_code,
                indicator_name,
                indicator_category,
                source_system,
                source_table_id,
                unit_of_measure,
                data_type,
                description
            ) VALUES (
                25,
                'business_deregistrations',
                'Business deregistrations by category',
                'business_economy',
                'regional_db',
                '52311-01-04-4',
                'count',
                'count',
                'Business deregistrations with subcategories: Total, Closures, Business closures, Relocations out, Handovers. Annual totals 1998-2024.'
            )
            ON CONFLICT (indicator_id) DO UPDATE SET
                indicator_code = EXCLUDED.indicator_code,
                indicator_name = EXCLUDED.indicator_name,
                description = EXCLUDED.description,
                updated_at = CURRENT_TIMESTAMP
            """))
            
            logger.info("✅ Indicator 25 added/updated")
            
            conn.commit()
            
            # Verify
            logger.info("\nVerifying indicators...")
            result = conn.execute(text("""
                SELECT indicator_id, indicator_code, indicator_name, indicator_category, unit_of_measure 
                FROM dim_indicator 
                WHERE indicator_id IN (24, 25)
                ORDER BY indicator_id
            """))
            
            results = result.fetchall()
            for row in results:
                logger.info(f"\nIndicator ID: {row[0]}")
                logger.info(f"  Code: {row[1]}")
                logger.info(f"  Name: {row[2]}")
                logger.info(f"  Category: {row[3]}")
                logger.info(f"  Unit: {row[4]}")
            
            logger.info("\n✅ Indicators successfully added/updated")
            
        except Exception as e:
            logger.error(f"Error adding indicators: {e}")
            conn.rollback()
            import traceback
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    add_indicators()

