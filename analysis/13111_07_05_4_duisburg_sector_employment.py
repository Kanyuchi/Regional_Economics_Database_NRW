"""
Time Series Analysis: Duisburg Employment by Economic Sector
Table: 13111-07-05-4
Period: 2008-2024

Validates data loaded into database and provides comprehensive analysis
of employment trends across economic sectors in Duisburg.
"""

import sys
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.database import DatabaseManager

# Set style for professional visualizations
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

def get_duisburg_employment_data():
    """
    Retrieve Duisburg employment by sector data from database.
    
    Returns:
        DataFrame with employment data
    """
    db = DatabaseManager('regional_economics')
    
    query = """
    SELECT 
        t.year,
        g.region_name,
        g.region_code,
        i.indicator_name,
        f.value,
        f.notes,
        f.extracted_at,
        f.data_quality_flag
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    WHERE g.region_name LIKE '%Duisburg%'
      AND f.indicator_id = 9
      AND t.year >= 2008 AND t.year <= 2024
    ORDER BY t.year, f.notes
    """
    
    df = db.execute_query(query)
    db.close()
    
    # Convert to DataFrame with proper column names
    columns = ['year', 'region_name', 'region_code', 'indicator_name', 
               'value', 'notes', 'extracted_at', 'data_quality_flag']
    
    data = pd.DataFrame(df, columns=columns)
    
    return data


def extract_sector_from_notes(notes):
    """Extract sector name from notes field."""
    if pd.isna(notes):
        return 'Unknown'
    if 'Sector:' in notes:
        return notes.replace('Sector:', '').strip()
    return notes.strip()


def create_analysis_report(df):
    """
    Create comprehensive analysis report with multiple visualizations.
    
    Args:
        df: DataFrame with employment data
    """
    if df.empty:
        print("ERROR: No data found for Duisburg!")
        return
    
    print("="*80)
    print("TIME SERIES ANALYSIS: DUISBURG EMPLOYMENT BY ECONOMIC SECTOR")
    print("="*80)
    print(f"Table: 13111-07-05-4")
    print(f"Region: {df['region_name'].iloc[0]}")
    print(f"Region Code: {df['region_code'].iloc[0]}")
    print(f"Period: {int(df['year'].min())} - {int(df['year'].max())}")
    print(f"Total Records: {len(df)}")
    print(f"Data Quality: {df['data_quality_flag'].value_counts().to_dict()}")
    print("="*80)
    
    # Extract sector information
    df['sector'] = df['notes'].apply(extract_sector_from_notes)
    
    # Data quality summary
    print("\nDATA QUALITY SUMMARY")
    print("-" * 80)
    print(f"Years with data: {df['year'].nunique()}")
    print(f"Unique sectors: {df['sector'].nunique()}")
    print(f"Missing values: {df['value'].isna().sum()}")
    print(f"Data extracted: {df['extracted_at'].iloc[0]}")
    
    # Sector summary
    print("\nSECTORS TRACKED")
    print("-" * 80)
    sectors = df['sector'].unique()
    for i, sector in enumerate(sorted(sectors), 1):
        records = len(df[df['sector'] == sector])
        print(f"{i:2d}. {sector[:70]:<70} ({records} records)")
    
    # Create output directory
    output_dir = PROJECT_ROOT / "analysis" / "outputs" / "13111_07_05_4_duisburg"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Save raw data
    csv_path = output_dir / f"duisburg_sector_employment_2008_2024.csv"
    df.to_csv(csv_path, index=False)
    print(f"\nRaw data saved to: {csv_path}")
    
    # Create visualizations
    create_visualizations(df, output_dir)
    
    # Statistical summary
    create_statistical_summary(df, output_dir)
    
    print("\n" + "="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)


def create_visualizations(df, output_dir):
    """Create comprehensive visualizations."""
    
    print("\nCREATING VISUALIZATIONS")
    print("-" * 80)
    
    # 1. Overall employment trend
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Aggregate total employment per year
    yearly_total = df.groupby('year')['value'].sum().reset_index()
    
    ax.plot(yearly_total['year'], yearly_total['value'], 
            marker='o', linewidth=2.5, markersize=8, color='#2E86AB')
    ax.fill_between(yearly_total['year'], yearly_total['value'], 
                     alpha=0.3, color='#2E86AB')
    
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Total Employment', fontsize=12, fontweight='bold')
    ax.set_title('Duisburg: Total Employment Trend (2008-2024)\nAll Economic Sectors', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.grid(True, alpha=0.3)
    
    # Add annotations for min/max
    max_year = yearly_total.loc[yearly_total['value'].idxmax()]
    min_year = yearly_total.loc[yearly_total['value'].idxmin()]
    
    ax.annotate(f'Peak: {max_year["value"]:,.0f}\n({int(max_year["year"])})',
                xy=(max_year['year'], max_year['value']),
                xytext=(10, 10), textcoords='offset points',
                fontsize=10, bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.7))
    
    plt.tight_layout()
    viz1_path = output_dir / "1_overall_employment_trend.png"
    plt.savefig(viz1_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {viz1_path.name}")
    
    # 2. Top 5 sectors over time
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Get top 5 sectors by average employment
    top_sectors = df.groupby('sector')['value'].mean().nlargest(5).index
    
    for sector in top_sectors:
        sector_data = df[df['sector'] == sector].sort_values('year')
        ax.plot(sector_data['year'], sector_data['value'], 
                marker='o', linewidth=2, markersize=6, label=sector[:50])
    
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Employment', fontsize=12, fontweight='bold')
    ax.set_title('Duisburg: Top 5 Economic Sectors by Employment (2008-2024)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='best', fontsize=9, framealpha=0.9)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    viz2_path = output_dir / "2_top5_sectors_trend.png"
    plt.savefig(viz2_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {viz2_path.name}")
    
    # 3. Heatmap of employment by sector and year
    # Prepare pivot table
    pivot_data = df.pivot_table(values='value', index='sector', 
                                  columns='year', aggfunc='sum')
    
    # Select top 10 sectors for readability
    top_10_sectors = df.groupby('sector')['value'].mean().nlargest(10).index
    pivot_top10 = pivot_data.loc[top_10_sectors]
    
    fig, ax = plt.subplots(figsize=(16, 10))
    sns.heatmap(pivot_top10, annot=False, fmt='.0f', cmap='YlOrRd', 
                cbar_kws={'label': 'Employment'}, linewidths=0.5, ax=ax)
    
    ax.set_title('Duisburg: Employment Heatmap - Top 10 Sectors (2008-2024)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Economic Sector', fontsize=12, fontweight='bold')
    
    # Truncate long sector names for y-axis
    y_labels = [label.get_text()[:40] + '...' if len(label.get_text()) > 40 
                else label.get_text() for label in ax.get_yticklabels()]
    ax.set_yticklabels(y_labels, fontsize=9)
    
    plt.tight_layout()
    viz3_path = output_dir / "3_sector_employment_heatmap.png"
    plt.savefig(viz3_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {viz3_path.name}")
    
    # 4. Growth rate analysis (2008 vs 2024)
    if 2008 in df['year'].values and 2024 in df['year'].values:
        df_2008 = df[df['year'] == 2008].set_index('sector')['value']
        df_2024 = df[df['year'] == 2024].set_index('sector')['value']
        
        # Calculate growth rate
        growth = ((df_2024 - df_2008) / df_2008 * 100).dropna()
        growth = growth.sort_values(ascending=False).head(15)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        colors = ['green' if x > 0 else 'red' for x in growth.values]
        
        y_pos = range(len(growth))
        ax.barh(y_pos, growth.values, color=colors, alpha=0.7, edgecolor='black')
        ax.set_yticks(y_pos)
        ax.set_yticklabels([s[:50] for s in growth.index], fontsize=9)
        ax.set_xlabel('Growth Rate (%)', fontsize=12, fontweight='bold')
        ax.set_title('Duisburg: Employment Growth Rate by Sector (2008-2024)\nTop 15 Sectors', 
                     fontsize=14, fontweight='bold', pad=20)
        ax.axvline(x=0, color='black', linestyle='-', linewidth=0.8)
        ax.grid(True, alpha=0.3, axis='x')
        
        plt.tight_layout()
        viz4_path = output_dir / "4_growth_rate_2008_2024.png"
        plt.savefig(viz4_path, dpi=300, bbox_inches='tight')
        plt.close()
        print(f"  Saved: {viz4_path.name}")
    
    # 5. Sector composition over time (stacked area chart)
    fig, ax = plt.subplots(figsize=(16, 10))
    
    # Aggregate similar sectors into broader categories
    sector_categories = {
        'Produzierendes Gewerbe': ['Produzierendes Gewerbe', 'Verarbeitendes Gewerbe', 'Baugewerbe'],
        'Dienstleistungen': ['Dienstleistungsbereiche', 'Handel, Gastgewerbe, Verkehr'],
        'Gesundheit/Soziales': ['Gesundheits- und Sozialwesen', 'Erziehung und Unterricht'],
        'Information/Finanzen': ['Information und Kommunikation', 'Finanz- und Versicherungsdienstleistungen'],
    }
    
    # Create aggregated data
    df_agg = df.copy()
    df_agg['category'] = 'Sonstige'
    
    for category, keywords in sector_categories.items():
        mask = df_agg['sector'].str.contains('|'.join(keywords), case=False, na=False)
        df_agg.loc[mask, 'category'] = category
    
    pivot_cat = df_agg.pivot_table(values='value', index='year', 
                                    columns='category', aggfunc='sum', fill_value=0)
    
    ax.stackplot(pivot_cat.index, *[pivot_cat[col] for col in pivot_cat.columns],
                 labels=pivot_cat.columns, alpha=0.8)
    
    ax.set_xlabel('Year', fontsize=12, fontweight='bold')
    ax.set_ylabel('Employment', fontsize=12, fontweight='bold')
    ax.set_title('Duisburg: Employment Composition by Sector Category (2008-2024)', 
                 fontsize=14, fontweight='bold', pad=20)
    ax.legend(loc='upper left', fontsize=10, framealpha=0.9)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    viz5_path = output_dir / "5_sector_composition_stacked.png"
    plt.savefig(viz5_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {viz5_path.name}")
    
    print(f"\nAll visualizations saved to: {output_dir}")


def create_statistical_summary(df, output_dir):
    """Create statistical summary report."""
    
    print("\nGENERATING STATISTICAL SUMMARY")
    print("-" * 80)
    
    summary_path = output_dir / "statistical_summary.txt"
    
    with open(summary_path, 'w', encoding='utf-8') as f:
        f.write("="*80 + "\n")
        f.write("STATISTICAL SUMMARY: DUISBURG EMPLOYMENT BY SECTOR (2008-2024)\n")
        f.write("="*80 + "\n\n")
        
        # Overall statistics
        f.write("OVERALL STATISTICS\n")
        f.write("-"*80 + "\n")
        yearly_total = df.groupby('year')['value'].sum()
        f.write(f"Total employment (2024): {yearly_total.iloc[-1]:,.0f}\n")
        f.write(f"Total employment (2008): {yearly_total.iloc[0]:,.0f}\n")
        f.write(f"Change (2008-2024): {yearly_total.iloc[-1] - yearly_total.iloc[0]:,.0f}\n")
        f.write(f"Growth rate: {((yearly_total.iloc[-1] / yearly_total.iloc[0]) - 1) * 100:.2f}%\n")
        f.write(f"Average annual employment: {yearly_total.mean():,.0f}\n")
        f.write(f"Standard deviation: {yearly_total.std():,.0f}\n\n")
        
        # Year-over-year changes
        f.write("YEAR-OVER-YEAR CHANGES\n")
        f.write("-"*80 + "\n")
        yoy_change = yearly_total.diff()
        yoy_pct = yearly_total.pct_change() * 100
        
        for year in yearly_total.index[1:]:
            f.write(f"{int(year)}: {yoy_change.loc[year]:+,.0f} ({yoy_pct.loc[year]:+.2f}%)\n")
        
        f.write("\n")
        
        # Sector-specific statistics
        f.write("TOP 10 SECTORS BY AVERAGE EMPLOYMENT\n")
        f.write("-"*80 + "\n")
        
        sector_stats = df.groupby('sector')['value'].agg(['mean', 'std', 'min', 'max'])
        sector_stats = sector_stats.sort_values('mean', ascending=False).head(10)
        
        for i, (sector, row) in enumerate(sector_stats.iterrows(), 1):
            f.write(f"\n{i}. {sector[:70]}\n")
            f.write(f"   Average: {row['mean']:,.0f}\n")
            f.write(f"   Std Dev: {row['std']:,.0f}\n")
            f.write(f"   Range: {row['min']:,.0f} - {row['max']:,.0f}\n")
        
        # Data quality notes
        f.write("\n" + "="*80 + "\n")
        f.write("DATA QUALITY NOTES\n")
        f.write("="*80 + "\n")
        f.write(f"Total records analyzed: {len(df)}\n")
        f.write(f"Years covered: {int(df['year'].min())} - {int(df['year'].max())}\n")
        f.write(f"Sectors tracked: {df['sector'].nunique()}\n")
        f.write(f"Data quality flag: {df['data_quality_flag'].iloc[0]}\n")
        f.write(f"Last extraction: {df['extracted_at'].iloc[0]}\n")
        f.write(f"\nAnalysis generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    print(f"Statistical summary saved to: {summary_path}")


def main():
    """Main execution function."""
    print("\n" + "="*80)
    print("LOADING DATA FROM DATABASE...")
    print("="*80)
    
    try:
        df = get_duisburg_employment_data()
        
        if df.empty:
            print("\nERROR: No data found for Duisburg in the database!")
            print("   Table: 13111-07-05-4 (Employment by Sector)")
            print("   Please verify:")
            print("   1. Data was successfully loaded")
            print("   2. Region name contains 'Duisburg'")
            print("   3. Indicator ID = 9")
            return
        
        create_analysis_report(df)
        
    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
