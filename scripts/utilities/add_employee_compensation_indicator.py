"""
Add employee compensation indicator to database
Indicator 26 for employee compensation by economic sector
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from sqlalchemy import text
from src.utils.database import DatabaseManager
from src.utils.logging import get_logger

logger = get_logger(__name__)


def add_indicator():
    """Add employee compensation indicator"""
    
    db = DatabaseManager()
    
    with db.get_connection() as conn:
        try:
            # Add indicator 26: Employee compensation
            logger.info("Adding Indicator 26: Employee Compensation...")
            
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
                    26,
                    'employee_compensation',
                    'Employee compensation by economic sector',
                    'business_economy',
                    'regional_db',
                    '82000-04-01-4',
                    'thousand EUR',
                    'currency',
                    'Employee compensation (domestic concept) by economic sectors. 8 WZ 2008 sectors (Total, Agriculture, Manufacturing, Construction, Trade/Transport, Finance/Real Estate, Public Services). Annual totals 2000-2022.'
                )
                ON CONFLICT (indicator_id) DO UPDATE SET
                    indicator_code = EXCLUDED.indicator_code,
                    indicator_name = EXCLUDED.indicator_name,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP
            """))
            
            logger.info("Indicator 26 added/updated")
            
            conn.commit()
            
            # Verify
            logger.info("\nVerifying indicator...")
            result = conn.execute(text("""
                SELECT indicator_id, indicator_code, indicator_name, indicator_category, unit_of_measure 
                FROM dim_indicator 
                WHERE indicator_id = 26
            """))
            
            row = result.fetchone()
            if row:
                logger.info(f"\nIndicator ID: {row[0]}")
                logger.info(f"  Code: {row[1]}")
                logger.info(f"  Name: {row[2]}")
                logger.info(f"  Category: {row[3]}")
                logger.info(f"  Unit: {row[4]}")
            
            logger.info("\nIndicator successfully added/updated")
            
        except Exception as e:
            logger.error(f"Error adding indicator: {e}")
            conn.rollback()
            import traceback
            logger.error(traceback.format_exc())


if __name__ == "__main__":
    add_indicator()

