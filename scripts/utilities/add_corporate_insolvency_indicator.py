"""
Add corporate insolvency indicator to dim_indicator table.
Indicator ID: 27
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.utils.database import DatabaseManager
from src.utils.logging import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


def add_indicator():
    """Add corporate insolvency indicator (ID 27) to dim_indicator table."""
    
    db = DatabaseManager()
    
    try:
        logger.info("Adding corporate insolvency indicator (ID 27)...")
        
        with db.get_session() as session:
            # Insert or update indicator
            sql = text("""
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
                    27,
                    'corporate_insolvencies',
                    'Corporate Insolvency Applications',
                    'business_economy',
                    'regional_db',
                    '52411-02-01-4',
                    'number',
                    'count',
                    'Filed corporate insolvency applications (total) - annual total. Includes both opened proceedings and proceedings rejected due to lack of assets. Data from Regional Database Germany (Regionalstatistik). Period: 2000-2024.'
                )
                ON CONFLICT (indicator_id) 
                DO UPDATE SET
                    indicator_code = EXCLUDED.indicator_code,
                    indicator_name = EXCLUDED.indicator_name,
                    indicator_category = EXCLUDED.indicator_category,
                    source_system = EXCLUDED.source_system,
                    source_table_id = EXCLUDED.source_table_id,
                    unit_of_measure = EXCLUDED.unit_of_measure,
                    data_type = EXCLUDED.data_type,
                    description = EXCLUDED.description,
                    updated_at = CURRENT_TIMESTAMP;
            """)
            
            session.execute(sql)
            session.commit()
            
            logger.info("Successfully added/updated indicator 27")
            
            # Verify
            verify_sql = text("""
                SELECT indicator_id, indicator_code, indicator_name, 
                       source_table_id, unit_of_measure
                FROM dim_indicator 
                WHERE indicator_id = 27;
            """)
            
            result = session.execute(verify_sql)
            row = result.fetchone()
            
            if row:
                logger.info(f"Verification successful:")
                logger.info(f"  ID: {row[0]}")
                logger.info(f"  Code: {row[1]}")
                logger.info(f"  Name: {row[2]}")
                logger.info(f"  Table: {row[3]}")
                logger.info(f"  Unit: {row[4]}")
            else:
                logger.error("Verification failed: Indicator 27 not found")
                
    except Exception as e:
        logger.error(f"Error adding indicator: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    finally:
        db.close()


if __name__ == "__main__":
    add_indicator()
    print("\n Indicator 27 (Corporate Insolvencies) added successfully!")

