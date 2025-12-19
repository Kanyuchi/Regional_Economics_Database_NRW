"""
Populate Geography Dimension Table
Regional Economics Database for NRW

Populates dim_geography with NRW districts and regions.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from utils.database import get_database
from utils.logging import setup_logging, get_logger


setup_logging(level="INFO")
logger = get_logger(__name__)


# NRW Geography Data
# Based on official regional codes (Regionalschlüssel)
NRW_GEOGRAPHY = [
    # Country level
    {'region_code': '00', 'region_name': 'Deutschland', 'region_name_en': 'Germany', 'region_type': 'country', 'parent_region_code': None},

    # State level
    {'region_code': '05', 'region_name': 'Nordrhein-Westfalen', 'region_name_en': 'North Rhine-Westphalia', 'region_type': 'state', 'parent_region_code': '00'},

    # Administrative Districts (Regierungsbezirke)
    {'region_code': '051', 'region_name': 'Düsseldorf', 'region_name_en': 'Düsseldorf', 'region_type': 'administrative_district', 'parent_region_code': '05'},
    {'region_code': '053', 'region_name': 'Köln', 'region_name_en': 'Cologne', 'region_type': 'administrative_district', 'parent_region_code': '05'},
    {'region_code': '055', 'region_name': 'Münster', 'region_name_en': 'Münster', 'region_type': 'administrative_district', 'parent_region_code': '05'},
    {'region_code': '057', 'region_name': 'Detmold', 'region_name_en': 'Detmold', 'region_type': 'administrative_district', 'parent_region_code': '05'},
    {'region_code': '059', 'region_name': 'Arnsberg', 'region_name_en': 'Arnsberg', 'region_type': 'administrative_district', 'parent_region_code': '05'},

    # Urban Districts (Kreisfreie Städte) - Regierungsbezirk Düsseldorf
    {'region_code': '05111', 'region_name': 'Düsseldorf', 'region_name_en': 'Düsseldorf', 'region_type': 'urban_district', 'parent_region_code': '051'},
    {'region_code': '05112', 'region_name': 'Duisburg', 'region_name_en': 'Duisburg', 'region_type': 'urban_district', 'parent_region_code': '051', 'ruhr_area': True},
    {'region_code': '05113', 'region_name': 'Essen', 'region_name_en': 'Essen', 'region_type': 'urban_district', 'parent_region_code': '051', 'ruhr_area': True},
    {'region_code': '05114', 'region_name': 'Krefeld', 'region_name_en': 'Krefeld', 'region_type': 'urban_district', 'parent_region_code': '051', 'lower_rhine_region': True},
    {'region_code': '05116', 'region_name': 'Mönchengladbach', 'region_name_en': 'Mönchengladbach', 'region_type': 'urban_district', 'parent_region_code': '051', 'lower_rhine_region': True},
    {'region_code': '05117', 'region_name': 'Mülheim an der Ruhr', 'region_name_en': 'Mülheim an der Ruhr', 'region_type': 'urban_district', 'parent_region_code': '051', 'ruhr_area': True},
    {'region_code': '05119', 'region_name': 'Oberhausen', 'region_name_en': 'Oberhausen', 'region_type': 'urban_district', 'parent_region_code': '051', 'ruhr_area': True},
    {'region_code': '05120', 'region_name': 'Remscheid', 'region_name_en': 'Remscheid', 'region_type': 'urban_district', 'parent_region_code': '051'},
    {'region_code': '05122', 'region_name': 'Solingen', 'region_name_en': 'Solingen', 'region_type': 'urban_district', 'parent_region_code': '051'},
    {'region_code': '05124', 'region_name': 'Wuppertal', 'region_name_en': 'Wuppertal', 'region_type': 'urban_district', 'parent_region_code': '051'},

    # Rural Districts (Kreise) - Regierungsbezirk Düsseldorf
    {'region_code': '05154', 'region_name': 'Kleve', 'region_name_en': 'Kleve', 'region_type': 'district', 'parent_region_code': '051', 'lower_rhine_region': True},
    {'region_code': '05158', 'region_name': 'Mettmann', 'region_name_en': 'Mettmann', 'region_type': 'district', 'parent_region_code': '051'},
    {'region_code': '05162', 'region_name': 'Rhein-Kreis Neuss', 'region_name_en': 'Rhein-Kreis Neuss', 'region_type': 'district', 'parent_region_code': '051'},
    {'region_code': '05166', 'region_name': 'Viersen', 'region_name_en': 'Viersen', 'region_type': 'district', 'parent_region_code': '051', 'lower_rhine_region': True},
    {'region_code': '05170', 'region_name': 'Wesel', 'region_name_en': 'Wesel', 'region_type': 'district', 'parent_region_code': '051', 'lower_rhine_region': True, 'ruhr_area': True},

    # Urban Districts - Regierungsbezirk Köln
    {'region_code': '05315', 'region_name': 'Bonn', 'region_name_en': 'Bonn', 'region_type': 'urban_district', 'parent_region_code': '053'},
    {'region_code': '05334', 'region_name': 'Köln', 'region_name_en': 'Cologne', 'region_type': 'urban_district', 'parent_region_code': '053'},
    {'region_code': '05358', 'region_name': 'Leverkusen', 'region_name_en': 'Leverkusen', 'region_type': 'urban_district', 'parent_region_code': '053'},

    # Rural Districts - Regierungsbezirk Köln
    {'region_code': '05378', 'region_name': 'Rheinisch-Bergischer Kreis', 'region_name_en': 'Rheinisch-Bergischer Kreis', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05374', 'region_name': 'Oberbergischer Kreis', 'region_name_en': 'Oberbergischer Kreis', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05382', 'region_name': 'Rhein-Sieg-Kreis', 'region_name_en': 'Rhein-Sieg-Kreis', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05354', 'region_name': 'Städteregion Aachen', 'region_name_en': 'Städteregion Aachen', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05335', 'region_name': 'Düren', 'region_name_en': 'Düren', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05362', 'region_name': 'Rhein-Erft-Kreis', 'region_name_en': 'Rhein-Erft-Kreis', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05366', 'region_name': 'Euskirchen', 'region_name_en': 'Euskirchen', 'region_type': 'district', 'parent_region_code': '053'},
    {'region_code': '05370', 'region_name': 'Heinsberg', 'region_name_en': 'Heinsberg', 'region_type': 'district', 'parent_region_code': '053'},

    # Urban Districts - Regierungsbezirk Münster
    {'region_code': '05515', 'region_name': 'Bottrop', 'region_name_en': 'Bottrop', 'region_type': 'urban_district', 'parent_region_code': '055', 'ruhr_area': True},
    {'region_code': '05512', 'region_name': 'Gelsenkirchen', 'region_name_en': 'Gelsenkirchen', 'region_type': 'urban_district', 'parent_region_code': '055', 'ruhr_area': True},
    {'region_code': '05513', 'region_name': 'Münster', 'region_name_en': 'Münster', 'region_type': 'urban_district', 'parent_region_code': '055'},

    # Rural Districts - Regierungsbezirk Münster
    {'region_code': '05554', 'region_name': 'Borken', 'region_name_en': 'Borken', 'region_type': 'district', 'parent_region_code': '055'},
    {'region_code': '05558', 'region_name': 'Coesfeld', 'region_name_en': 'Coesfeld', 'region_type': 'district', 'parent_region_code': '055'},
    {'region_code': '05562', 'region_name': 'Recklinghausen', 'region_name_en': 'Recklinghausen', 'region_type': 'district', 'parent_region_code': '055', 'ruhr_area': True},
    {'region_code': '05566', 'region_name': 'Steinfurt', 'region_name_en': 'Steinfurt', 'region_type': 'district', 'parent_region_code': '055'},
    {'region_code': '05570', 'region_name': 'Warendorf', 'region_name_en': 'Warendorf', 'region_type': 'district', 'parent_region_code': '055'},

    # Urban Districts - Regierungsbezirk Detmold
    {'region_code': '05711', 'region_name': 'Bielefeld', 'region_name_en': 'Bielefeld', 'region_type': 'urban_district', 'parent_region_code': '057'},

    # Rural Districts - Regierungsbezirk Detmold
    {'region_code': '05754', 'region_name': 'Gütersloh', 'region_name_en': 'Gütersloh', 'region_type': 'district', 'parent_region_code': '057'},
    {'region_code': '05758', 'region_name': 'Herford', 'region_name_en': 'Herford', 'region_type': 'district', 'parent_region_code': '057'},
    {'region_code': '05762', 'region_name': 'Höxter', 'region_name_en': 'Höxter', 'region_type': 'district', 'parent_region_code': '057'},
    {'region_code': '05766', 'region_name': 'Lippe', 'region_name_en': 'Lippe', 'region_type': 'district', 'parent_region_code': '057'},
    {'region_code': '05770', 'region_name': 'Minden-Lübbecke', 'region_name_en': 'Minden-Lübbecke', 'region_type': 'district', 'parent_region_code': '057'},
    {'region_code': '05774', 'region_name': 'Paderborn', 'region_name_en': 'Paderborn', 'region_type': 'district', 'parent_region_code': '057'},

    # Urban Districts - Regierungsbezirk Arnsberg
    {'region_code': '05911', 'region_name': 'Bochum', 'region_name_en': 'Bochum', 'region_type': 'urban_district', 'parent_region_code': '059', 'ruhr_area': True},
    {'region_code': '05913', 'region_name': 'Dortmund', 'region_name_en': 'Dortmund', 'region_type': 'urban_district', 'parent_region_code': '059', 'ruhr_area': True},
    {'region_code': '05914', 'region_name': 'Hagen', 'region_name_en': 'Hagen', 'region_type': 'urban_district', 'parent_region_code': '059', 'ruhr_area': True},
    {'region_code': '05915', 'region_name': 'Hamm', 'region_name_en': 'Hamm', 'region_type': 'urban_district', 'parent_region_code': '059'},
    {'region_code': '05916', 'region_name': 'Herne', 'region_name_en': 'Herne', 'region_type': 'urban_district', 'parent_region_code': '059', 'ruhr_area': True},

    # Rural Districts - Regierungsbezirk Arnsberg
    {'region_code': '05954', 'region_name': 'Ennepe-Ruhr-Kreis', 'region_name_en': 'Ennepe-Ruhr-Kreis', 'region_type': 'district', 'parent_region_code': '059', 'ruhr_area': True},
    {'region_code': '05958', 'region_name': 'Hochsauerlandkreis', 'region_name_en': 'Hochsauerlandkreis', 'region_type': 'district', 'parent_region_code': '059'},
    {'region_code': '05962', 'region_name': 'Märkischer Kreis', 'region_name_en': 'Märkischer Kreis', 'region_type': 'district', 'parent_region_code': '059'},
    {'region_code': '05966', 'region_name': 'Olpe', 'region_name_en': 'Olpe', 'region_type': 'district', 'parent_region_code': '059'},
    {'region_code': '05970', 'region_name': 'Siegen-Wittgenstein', 'region_name_en': 'Siegen-Wittgenstein', 'region_type': 'district', 'parent_region_code': '059'},
    {'region_code': '05974', 'region_name': 'Soest', 'region_name_en': 'Soest', 'region_type': 'district', 'parent_region_code': '059'},
    {'region_code': '05978', 'region_name': 'Unna', 'region_name_en': 'Unna', 'region_type': 'district', 'parent_region_code': '059', 'ruhr_area': True},
]


def populate_geography():
    """Populate dim_geography table with NRW geography data."""
    logger.info("Starting geography dimension population")

    db = get_database('regional_economics')

    # Check if data already exists
    existing = db.execute_query("SELECT COUNT(*) as count FROM dim_geography")
    if existing[0]['count'] > 0:
        logger.warning(f"Geography table already has {existing[0]['count']} records")
        response = input("Do you want to clear and repopulate? (yes/no): ")
        if response.lower() != 'yes':
            logger.info("Aborting population")
            return

        # Clear existing data
        db.execute_statement("DELETE FROM dim_geography")
        logger.info("Cleared existing geography data")

    # Prepare records
    records = []
    for geo in NRW_GEOGRAPHY:
        record = {
            'region_code': geo['region_code'],
            'region_name': geo['region_name'],
            'region_name_en': geo.get('region_name_en'),
            'region_type': geo['region_type'],
            'parent_region_code': geo.get('parent_region_code'),
            'ruhr_area': geo.get('ruhr_area', False),
            'lower_rhine_region': geo.get('lower_rhine_region', False),
            'is_active': True
        }
        records.append(record)

    # Bulk insert
    try:
        count = db.bulk_insert('dim_geography', records)
        logger.info(f"Successfully inserted {count} geography records")

        # Verify
        result = db.execute_query("""
            SELECT region_type, COUNT(*) as count
            FROM dim_geography
            GROUP BY region_type
        """)

        logger.info("Geography breakdown:")
        for row in result:
            logger.info(f"  {row['region_type']}: {row['count']}")

        return True

    except Exception as e:
        logger.error(f"Error inserting geography data: {e}")
        return False


if __name__ == "__main__":
    success = populate_geography()
    sys.exit(0 if success else 1)
