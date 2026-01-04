import sys
sys.path.append('.')

from src.utils.database import DatabaseManager
from sqlalchemy import text

db = DatabaseManager()

with db.get_connection() as conn:
    result = conn.execute(text("SELECT MAX(indicator_id) FROM dim_indicator"))
    max_id = result.scalar()
    print(f"Max indicator ID: {max_id}")

    result = conn.execute(text("""
        SELECT indicator_id, indicator_name
        FROM dim_indicator
        ORDER BY indicator_id DESC
        LIMIT 10
    """))

    print("\nRecent indicators:")
    for row in result:
        print(f"  {row[0]}: {row[1]}")

db.close()
