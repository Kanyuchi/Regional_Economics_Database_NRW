"""Check what indicators are currently in the database."""

from src.utils.database import DatabaseManager

def check_indicators():
    """List all indicators in the database."""
    db = DatabaseManager()
    
    with db.get_session() as session:
        result = session.execute(
            "SELECT indicator_id, indicator_code, indicator_name, source_table_id FROM dim_indicator ORDER BY indicator_id"
        )
        
        print("\n=== CURRENT INDICATORS IN DATABASE ===\n")
        print(f"{'ID':>3} | {'Code':<30} | {'Name':<50} | {'Table ID'}")
        print("-" * 120)
        
        for row in result:
            print(f"{row[0]:>3} | {row[1]:<30} | {row[2][:50]:<50} | {row[3]}")
        
        # Count total
        count_result = session.execute("SELECT COUNT(*) FROM dim_indicator")
        total = count_result.fetchone()[0]
        print(f"\n{'='*120}")
        print(f"TOTAL INDICATORS: {total}")

if __name__ == "__main__":
    check_indicators()

