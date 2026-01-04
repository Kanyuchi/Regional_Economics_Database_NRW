"""
Setup GDP Indicators in Database
Creates dim_indicator entries for table 82711-01i (GDP and GVA by sector)

Regional Economics Database for NRW
"""

import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text
from datetime import datetime

# GDP/GVA indicators based on table 82711-01i structure
# IDs 29-40 (ID 28 is already used for Municipal Finances)
GDP_INDICATORS = [
    {
        'indicator_id': 29,
        'indicator_code': 'GDP_TOTAL',
        'indicator_name': 'BIP zu Marktpreisen',
        'indicator_name_en': 'GDP at Market Prices',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GDP',
        'description': 'Gross Domestic Product at market prices in current prices',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro (current prices)',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 30,
        'indicator_code': 'GDP_PER_EMPLOYEE',
        'indicator_name': 'BIP je Erwerbstätigen',
        'indicator_name_en': 'GDP per Employee',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'Productivity',
        'description': 'Gross Domestic Product per employee',
        'unit_of_measure': 'EUR',
        'unit_description': 'Euro per employee',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'GDP / Number of employees',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 31,
        'indicator_code': 'GVA_TOTAL',
        'indicator_name': 'Bruttowertschöpfung gesamt',
        'indicator_name_en': 'Gross Value Added Total',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA',
        'description': 'Gross Value Added at basic prices, total across all sectors',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 32,
        'indicator_code': 'GVA_AGRICULTURE_A',
        'indicator_name': 'BWS Land-/Forstwirtschaft, Fischerei (A)',
        'indicator_name_en': 'GVA Agriculture, Forestry, Fishing (A)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Agriculture, forestry and fishing (WZ 2008 Section A)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 33,
        'indicator_code': 'GVA_MANUFACTURING_BE_TOTAL',
        'indicator_name': 'BWS Produzierendes Gewerbe gesamt (B-E)',
        'indicator_name_en': 'GVA Manufacturing Total (B-E)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Manufacturing total including mining, energy (WZ 2008 Sections B-E)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 34,
        'indicator_code': 'GVA_MANUFACTURING_EXCL_CONSTRUCTION',
        'indicator_name': 'BWS Produz. Gewerbe ohne Baugewerbe',
        'indicator_name_en': 'GVA Manufacturing excl. Construction',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Manufacturing excluding construction (WZ 2008 Sections B-E excluding F)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 35,
        'indicator_code': 'GVA_MANUFACTURING_C',
        'indicator_name': 'BWS Verarbeitendes Gewerbe (C)',
        'indicator_name_en': 'GVA Manufacturing Industry (C)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Manufacturing industry only (WZ 2008 Section C)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 36,
        'indicator_code': 'GVA_CONSTRUCTION_F',
        'indicator_name': 'BWS Baugewerbe (F)',
        'indicator_name_en': 'GVA Construction (F)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Construction (WZ 2008 Section F)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 37,
        'indicator_code': 'GVA_SERVICES_GU_TOTAL',
        'indicator_name': 'BWS Dienstleistungsbereiche (G-U)',
        'indicator_name_en': 'GVA Services Total (G-U)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Services total (WZ 2008 Sections G-U)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 38,
        'indicator_code': 'GVA_TRADE_TRANSPORT_ICT_GJ',
        'indicator_name': 'BWS Handel, Verkehr, Gastgew., IKT (G-J)',
        'indicator_name_en': 'GVA Trade, Transport, Hospitality, ICT (G-J)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Trade, transport, hospitality, information/communication (WZ 2008 Sections G-J)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 39,
        'indicator_code': 'GVA_FINANCE_REALESTATE_KL',
        'indicator_name': 'BWS Finanz- und Grundstückswesen (K-L)',
        'indicator_name_en': 'GVA Finance and Real Estate (K-L)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Finance, insurance, real estate (WZ 2008 Sections K-L)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    },
    {
        'indicator_id': 40,
        'indicator_code': 'GVA_PUBLIC_OTHER_MU',
        'indicator_name': 'BWS Öffentl./sonst. Dienstl. (M-U)',
        'indicator_name_en': 'GVA Public and Other Services (M-U)',
        'indicator_category': 'Economic Output',
        'indicator_subcategory': 'GVA by Sector',
        'description': 'Gross Value Added: Business services, public services, education, health (WZ 2008 Sections M-U)',
        'unit_of_measure': 'Million EUR',
        'unit_description': 'Million Euro at basic prices',
        'source_system': 'state_db',
        'source_table_id': '82711-01i',
        'calculation_method': 'Direct extraction from State Database NRW',
        'data_type': 'numeric',
        'is_active': True
    }
]

def main():
    """Insert GDP indicator definitions into database."""
    print("="*80)
    print("SETUP GDP INDICATORS")
    print("="*80)
    print(f"Creating {len(GDP_INDICATORS)} indicators (IDs 29-40)")
    print("")

    db = DatabaseManager()

    try:
        with db.get_connection() as conn:
            for indicator in GDP_INDICATORS:
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
        print("SUCCESS: All GDP indicators created/updated")
        print("="*80)
        print("\nYou can now run the GDP ETL pipeline:")
        print("  python pipelines/state_db/etl_82711_01i_gdp.py --test")

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
