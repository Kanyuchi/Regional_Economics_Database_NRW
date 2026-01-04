"""
Setup Road Indicators in Database
Creates dim_indicator entries for table 46271-01i (Interregional Roads by Road Class)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# Road indicators based on table 46271-01i structure
# IDs 62-66
ROAD_INDICATORS = [
    {
        'indicator_id': 62,
        'indicator_code': 'road_total_km',
        'indicator_name': 'Straßen des überörtlichen Verkehrs insgesamt',
        'indicator_name_en': 'Total Interregional Roads',
        'indicator_category': 'Infrastructure',
        'indicator_subcategory': 'Roads',
        'description': 'Total length of interregional roads (all road classes combined)',
        'unit_of_measure': 'km',
        'unit_description': 'Kilometers',
        'source_system': 'state_db',
        'source_table_id': '46271-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 63,
        'indicator_code': 'road_motorway_km',
        'indicator_name': 'Autobahnen',
        'indicator_name_en': 'Motorways (Autobahnen)',
        'indicator_category': 'Infrastructure',
        'indicator_subcategory': 'Roads',
        'description': 'Length of motorways (Autobahnen) - federal highways with controlled access',
        'unit_of_measure': 'km',
        'unit_description': 'Kilometers',
        'source_system': 'state_db',
        'source_table_id': '46271-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 64,
        'indicator_code': 'road_federal_km',
        'indicator_name': 'Bundesstraßen',
        'indicator_name_en': 'Federal Roads (Bundesstraßen)',
        'indicator_category': 'Infrastructure',
        'indicator_subcategory': 'Roads',
        'description': 'Length of federal roads (Bundesstraßen) - major national roads',
        'unit_of_measure': 'km',
        'unit_description': 'Kilometers',
        'source_system': 'state_db',
        'source_table_id': '46271-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 65,
        'indicator_code': 'road_state_km',
        'indicator_name': 'Landstraßen',
        'indicator_name_en': 'State Roads (Landstraßen)',
        'indicator_category': 'Infrastructure',
        'indicator_subcategory': 'Roads',
        'description': 'Length of state roads (Landstraßen) - roads managed by the state government',
        'unit_of_measure': 'km',
        'unit_description': 'Kilometers',
        'source_system': 'state_db',
        'source_table_id': '46271-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 66,
        'indicator_code': 'road_district_km',
        'indicator_name': 'Kreisstraßen',
        'indicator_name_en': 'District Roads (Kreisstraßen)',
        'indicator_category': 'Infrastructure',
        'indicator_subcategory': 'Roads',
        'description': 'Length of district roads (Kreisstraßen) - roads managed by district/county government',
        'unit_of_measure': 'km',
        'unit_description': 'Kilometers',
        'source_system': 'state_db',
        'source_table_id': '46271-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert road indicator definitions into database."""
    print("="*80)
    print("SETUP ROAD INDICATORS")
    print("="*80)
    print(f"Creating {len(ROAD_INDICATORS)} indicators (IDs 62-66)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in ROAD_INDICATORS:
                # Check if indicator already exists
                result = conn.execute(
                    text("SELECT indicator_id FROM dim_indicator WHERE indicator_id = :id"),
                    {"id": indicator['indicator_id']}
                )

                if result.fetchone():
                    print(f"  [{indicator['indicator_id']}] Already exists: {indicator['indicator_name']}")
                    # Update existing record
                    conn.execute(text("""
                        UPDATE dim_indicator
                        SET indicator_code = :code,
                            indicator_name = :name,
                            indicator_name_en = :name_en,
                            indicator_category = :category,
                            indicator_subcategory = :subcategory,
                            description = :description,
                            unit_of_measure = :unit,
                            unit_description = :unit_desc,
                            source_system = :source,
                            source_table_id = :table_id,
                            calculation_method = :calc_method,
                            data_type = :data_type,
                            is_active = :is_active,
                            updated_at = :updated_at
                        WHERE indicator_id = :id
                    """), {
                        'id': indicator['indicator_id'],
                        'code': indicator['indicator_code'],
                        'name': indicator['indicator_name'],
                        'name_en': indicator['indicator_name_en'],
                        'category': indicator['indicator_category'],
                        'subcategory': indicator['indicator_subcategory'],
                        'description': indicator['description'],
                        'unit': indicator['unit_of_measure'],
                        'unit_desc': indicator['unit_description'],
                        'source': indicator['source_system'],
                        'table_id': indicator['source_table_id'],
                        'calc_method': indicator['calculation_method'],
                        'data_type': indicator['data_type'],
                        'is_active': indicator['is_active'],
                        'updated_at': datetime.now()
                    })
                else:
                    # Insert new record
                    conn.execute(text("""
                        INSERT INTO dim_indicator (
                            indicator_id, indicator_code, indicator_name, indicator_name_en,
                            indicator_category, indicator_subcategory, description,
                            unit_of_measure, unit_description, source_system, source_table_id,
                            calculation_method, data_type, is_active,
                            created_at, updated_at
                        ) VALUES (
                            :id, :code, :name, :name_en,
                            :category, :subcategory, :description,
                            :unit, :unit_desc, :source, :table_id,
                            :calc_method, :data_type, :is_active,
                            :created_at, :updated_at
                        )
                    """), {
                        'id': indicator['indicator_id'],
                        'code': indicator['indicator_code'],
                        'name': indicator['indicator_name'],
                        'name_en': indicator['indicator_name_en'],
                        'category': indicator['indicator_category'],
                        'subcategory': indicator['indicator_subcategory'],
                        'description': indicator['description'],
                        'unit': indicator['unit_of_measure'],
                        'unit_desc': indicator['unit_description'],
                        'source': indicator['source_system'],
                        'table_id': indicator['source_table_id'],
                        'calc_method': indicator['calculation_method'],
                        'data_type': indicator['data_type'],
                        'is_active': indicator['is_active'],
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })
                    print(f"  [{indicator['indicator_id']}] Created: {indicator['indicator_name']}")

            conn.commit()

        print("\n" + "="*80)
        print("SUCCESS: All road indicators created/updated")
        print("="*80)
        print("\nYou can now run the roads ETL pipeline:")
        print("  python pipelines/state_db/etl_46271_01i_roads.py --test")

    except Exception as e:
        print(f"\nERROR: Failed to setup indicators: {e}")
        import traceback
        traceback.print_exc()
        return False

    finally:
        db.close()

    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
