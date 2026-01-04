import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()

with db.get_connection() as conn:
    result = conn.execute(text("""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = 'dim_indicator'
        ORDER BY ordinal_position
    """))

    print("dim_indicator table columns:")
    for row in result:
        print(f"  - {row[0]} ({row[1]})")

db.close()
