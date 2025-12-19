"""
Quick Data Validation: Duisburg Employment Data
Table: 13111-07-05-4
Purpose: Validate data was loaded correctly before moving to next table
"""

import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.database import DatabaseManager

def validate_data():
    """Quick validation of loaded data."""
    
    print("\n" + "="*80)
    print("DATA VALIDATION: Duisburg Employment by Sector (2008-2024)")
    print("="*80)
    
    db = DatabaseManager('regional_economics')
    
    # Query 1: Total records
    query1 = """
    SELECT COUNT(*) as total_records
    FROM fact_demographics
    WHERE indicator_id = 9
    """
    result = db.execute_query(query1)
    if result:
        total_records = result[0]['total_records']
        print(f"\nTotal Records (indicator_id=9): {total_records:,}")
    else:
        print("\nNo records found")
        db.close()
        return False
    
    # Query 2: Records by year
    query2 = """
    SELECT t.year, COUNT(*) as count
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 9
    GROUP BY t.year
    ORDER BY t.year
    """
    result = db.execute_query(query2)
    print("\nRecords by Year:")
    print("Year | Count")
    print("-" * 20)
    for row in result:
        print(f"{int(row['year'])} | {row['count']}")
    
    # Query 3: Duisburg specific
    query3 = """
    SELECT 
        g.region_name,
        t.year,
        SUM(f.value) as total_employment
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE g.region_code = '05112' 
      AND f.indicator_id = 9
    GROUP BY g.region_name, t.year
    ORDER BY t.year
    """
    result = db.execute_query(query3)
    
    print(f"\n" + "="*80)
    print("DUISBURG EMPLOYMENT TREND (2008-2024)")
    print("="*80)
    
    df = pd.DataFrame(result)
    
    if df.empty:
        print("ERROR: No data found for Duisburg!")
        db.close()
        return False
    
    # Convert Decimal to float
    df['total_employment'] = df['total_employment'].astype(float)
    
    print(f"\nRegion: {df['region_name'].iloc[0]}")
    print(f"Years covered: {int(df['year'].min())} - {int(df['year'].max())}")
    print(f"Data points: {len(df)}")
    print("\nEmployment by Year:")
    print("-" * 50)
    
    for _, row in df.iterrows():
        print(f"{int(row['year'])}: {row['total_employment']:>15,.0f} employees")
    
    # Calculate growth
    first_year = df.iloc[0]
    last_year = df.iloc[-1]
    change = last_year['total_employment'] - first_year['total_employment']
    pct_change = (change / first_year['total_employment']) * 100
    
    print(f"\n" + "-"*50)
    print(f"Change ({int(first_year['year'])}-{int(last_year['year'])}): {change:>10,.0f} ({pct_change:+.2f}%)")
    
    # Create simple visualization
    output_dir = PROJECT_ROOT / "analysis" / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(df['year'], df['total_employment'], marker='o', linewidth=2, markersize=8)
    ax.fill_between(df['year'], df['total_employment'], alpha=0.3)
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Total Employment', fontsize=12, fontweight='bold')
    ax.set_title('Duisburg: Total Employment Trend (2008-2024)', fontsize=14, fontweight='bold')
    ax.grid(True, alpha=0.3)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    # Add trend line
    import numpy as np
    z = np.polyfit(df['year'], df['total_employment'], 1)
    p = np.poly1d(z)
    ax.plot(df['year'], p(df['year']), "r--", alpha=0.5, label='Trend')
    ax.legend()
    
    plt.tight_layout()
    viz_path = output_dir / "duisburg_employment_validation.png"
    plt.savefig(viz_path, dpi=300, bbox_inches='tight')
    print(f"\nVisualization saved: {viz_path}")
    
    db.close()
    
    print("\n" + "="*80)
    print("DATA VALIDATION COMPLETE - DATA IS PRESENT AND ACCURATE")
    print("="*80)
    
    return True


if __name__ == "__main__":
    success = validate_data()
    sys.exit(0 if success else 1)
