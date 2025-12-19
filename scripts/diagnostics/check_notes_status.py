"""Quick script to check notes status in database for indicator_id = 9"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from utils.database import get_database

try:
    db = get_database('regional_economics')

    # Check notes status
    query = """
    SELECT
        COUNT(*) as total_records,
        COUNT(notes) as with_notes,
        COUNT(*) - COUNT(notes) as null_notes,
        COUNT(DISTINCT notes) as unique_notes
    FROM fact_demographics
    WHERE indicator_id = 9
    """

    results = db.execute_query(query)

    if results:
        row = results[0]
        print("\n=== Notes Status for Indicator ID = 9 ===")
        print(f"Total records: {row['total_records']}")
        print(f"Records with notes: {row['with_notes']}")
        print(f"Records with NULL notes: {row['null_notes']}")
        print(f"Unique note values: {row['unique_notes']}")

        # Show sample notes
        sample_query = """
        SELECT DISTINCT notes
        FROM fact_demographics
        WHERE indicator_id = 9 AND notes IS NOT NULL
        LIMIT 10
        """

        samples = db.execute_query(sample_query)
        if samples:
            print("\n=== Sample Notes Values ===")
            for i, s in enumerate(samples, 1):
                print(f"{i}. {s['notes']}")
        else:
            print("\nNo non-NULL notes found in database!")

    db.close()

except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
