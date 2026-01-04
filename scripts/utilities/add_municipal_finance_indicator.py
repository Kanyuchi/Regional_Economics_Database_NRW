"""
Add Municipal Finance Indicator to Database
Adds indicator 28 for municipal finances (GFK) from State Database NRW.

Table: 71517-01i

Regional Economics Database for NRW
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


def add_indicator():
    """Add municipal finance indicator to dim_indicator table."""
    
    print("\n" + "="*70)
    print("ADDING MUNICIPAL FINANCE INDICATOR (ID: 28)")
    print("="*70 + "\n")
    
    db = DatabaseManager()
    
    # Indicator configuration
    indicator = {
        'indicator_id': 28,
        'indicator_code': 'municipal_finances_gfk',
        'indicator_name': 'Municipal Finances (GFK) - Payments by Type',
        'indicator_category': 'public_finance',
        'source_system': 'state_db',
        'source_table_id': '71517-01i',
        'unit_of_measure': 'thousand EUR',
        'data_type': 'currency',
        'description': 'Municipal finance data using GFK methodology (cash accounting). '
                      'Covers payments by selected payment types for municipalities in NRW. '
                      'Source: State Database NRW (Landesdatenbank). Period: 2009-2024.'
    }
    
    # SQL for upsert (insert or update)
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
            :indicator_id,
            :indicator_code,
            :indicator_name,
            :indicator_category,
            :source_system,
            :source_table_id,
            :unit_of_measure,
            :data_type,
            :description
        )
        ON CONFLICT (indicator_id) DO UPDATE SET
            indicator_code = EXCLUDED.indicator_code,
            indicator_name = EXCLUDED.indicator_name,
            indicator_category = EXCLUDED.indicator_category,
            source_system = EXCLUDED.source_system,
            source_table_id = EXCLUDED.source_table_id,
            unit_of_measure = EXCLUDED.unit_of_measure,
            data_type = EXCLUDED.data_type,
            description = EXCLUDED.description
    """)
    
    try:
        with db.get_session() as session:
            session.execute(sql, indicator)
            session.commit()
            print(f"Successfully added/updated indicator {indicator['indicator_id']}")
            print(f"  - Code: {indicator['indicator_code']}")
            print(f"  - Name: {indicator['indicator_name']}")
            print(f"  - Category: {indicator['indicator_category']}")
            print(f"  - Source: {indicator['source_system']}")
            print(f"  - Table: {indicator['source_table_id']}")
            print(f"  - Unit: {indicator['unit_of_measure']}")
            
            # Verify
            result = session.execute(
                text("SELECT * FROM dim_indicator WHERE indicator_id = :id"),
                {'id': indicator['indicator_id']}
            ).fetchone()
            
            if result:
                print(f"\nVerification: Indicator {result[0]} exists in database")
            
    except Exception as e:
        print(f"Error adding indicator: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    print("\n" + "="*70)
    print("INDICATOR ADDED SUCCESSFULLY")
    print("="*70 + "\n")
    
    return True


if __name__ == "__main__":
    add_indicator()

