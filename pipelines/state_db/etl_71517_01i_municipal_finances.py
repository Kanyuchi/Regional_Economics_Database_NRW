"""
ETL Pipeline: Municipal Finances (GFK) - State Database NRW
Table: 71517-01i

Finances (GFK) of municipalities and municipal associations:
Payments by selected payment types - Municipalities - Year

Available period: 2009 - 2024

This pipeline uses an existing job ID for data retrieval rather than
submitting a new extraction request.

Regional Economics Database for NRW
"""

import sys
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.state_db import MunicipalFinanceExtractor, StateDBJobCache
from src.transformers.municipal_finance_transformer import MunicipalFinanceTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)

# Table metadata
TABLE_ID = "71517-01i"
TABLE_NAME = "Municipal Finances (GFK)"
INDICATOR_ID = 28
START_YEAR = 2009
END_YEAR = 2024

# Note: We'll extract fresh data for the full range
# Set to None to extract new data, or provide job ID to use existing job
EXISTING_JOB_ID = None  # Set to job ID string if you want to use existing job


def main():
    """
    Run the ETL pipeline for municipal finances.
    
    Uses existing job ID for retrieval - does NOT submit new extraction request.
    """
    print("\n" + "="*80)
    print("ETL PIPELINE: MUNICIPAL FINANCES (GFK)")
    print(f"Table: {TABLE_ID} | Period: {START_YEAR}-{END_YEAR}")
    print("Source: State Database NRW (Landesdatenbank)")
    print("="*80 + "\n")
    
    start_time = datetime.now()
    year_range = f"{START_YEAR}-{END_YEAR}"
    
    # Initialize components
    print("[1/5] Initializing extractor, transformer, and loader...")
    extractor = MunicipalFinanceExtractor()
    transformer = MunicipalFinanceTransformer()
    loader = DataLoader()
    print("      Components initialized\n")
    
    # EXTRACT - Either use existing job or create new extraction
    if EXISTING_JOB_ID:
        print(f"[2/5] Using existing job ID: {EXISTING_JOB_ID}")
        print("      Retrieving data from existing job...")
        StateDBJobCache.add_existing_job(TABLE_ID, EXISTING_JOB_ID, year_range)
        raw_df = extractor.retrieve_municipal_finances(EXISTING_JOB_ID)
    else:
        print(f"[2/5] Submitting new extraction request for {START_YEAR}-{END_YEAR}")
        print("      This will create an async job - please wait...")
        raw_df = extractor.extract_municipal_finances(
            startyear=START_YEAR,
            endyear=END_YEAR
        )
    
    if raw_df is None or raw_df.empty:
        print("\nERROR: Failed to retrieve data from State Database")
        print("       The job may have expired or not be ready yet.")
        print("       Check the job status on the Landesdatenbank portal.")
        extractor.close()
        return False
    
    print(f"      Retrieved {len(raw_df):,} rows from API")
    print(f"      Columns: {raw_df.columns.tolist()[:5]}..." if len(raw_df.columns) > 5 else f"      Columns: {raw_df.columns.tolist()}")
    print()
    
    # TRANSFORM
    print("[3/5] Transforming data to database format...")
    transformed_df = transformer.transform_municipal_finances(
        raw_df,
        indicator_id=INDICATOR_ID
    )
    
    if transformed_df is None or transformed_df.empty:
        print("\nERROR: Transformation failed")
        extractor.close()
        return False
    
    print(f"      Transformed {len(transformed_df):,} records")
    print(f"      Unique regions: {transformed_df['region_code'].nunique()}")
    if 'year' in transformed_df.columns:
        years = sorted(transformed_df['year'].dropna().unique())
        print(f"      Years covered: {years[0]} - {years[-1]}")
    print(f"      Payment types: {transformed_df['notes'].nunique()}")
    print()
    
    # LOAD
    print("[4/5] Loading data to database...")
    records_loaded = loader.load_demographics_data(transformed_df)
    
    if records_loaded is None or records_loaded == 0:
        print("\nERROR: No records were loaded to database")
        extractor.close()
        return False
    
    print(f"      Loaded {records_loaded:,} records to fact_demographics")
    
    # Mark job as loaded in cache
    StateDBJobCache.mark_loaded(TABLE_ID, year_range)
    
    # Cleanup
    extractor.close()
    
    # Summary
    elapsed = datetime.now() - start_time
    print("\n[5/5] Verification and summary")
    print("="*80)
    print("ETL PIPELINE COMPLETED SUCCESSFULLY")
    print("="*80)
    print(f"Table:           {TABLE_ID} - {TABLE_NAME}")
    print(f"Source:          State Database NRW")
    print(f"Indicator ID:    {INDICATOR_ID}")
    print(f"Period:          {START_YEAR}-{END_YEAR}")
    print(f"Records loaded:  {records_loaded:,}")
    print(f"Elapsed time:    {elapsed}")
    print("="*80 + "\n")
    
    # Verification query
    print("Running verification query...")
    verify_data()
    
    return True


def verify_data():
    """Run verification query to confirm data was loaded correctly."""
    from src.utils.database import DatabaseManager
    
    db = DatabaseManager()
    
    try:
        with db.get_session() as session:
            # Count records for indicator 28
            result = session.execute(text("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT fd.geo_id) as unique_regions,
                    MIN(dt.year) as min_year,
                    MAX(dt.year) as max_year,
                    COUNT(DISTINCT fd.notes) as payment_types
                FROM fact_demographics fd
                JOIN dim_time dt ON fd.time_id = dt.time_id
                WHERE fd.indicator_id = :indicator_id
            """), {'indicator_id': INDICATOR_ID}).fetchone()
            
            if result and result[0] > 0:
                print(f"\nVerification Results for Indicator {INDICATOR_ID}:")
                print(f"  Total records:    {result[0]:,}")
                print(f"  Unique regions:   {result[1]}")
                print(f"  Year range:       {result[2]} - {result[3]}")
                print(f"  Payment types:    {result[4]}")
            else:
                print(f"\nNo records found for indicator {INDICATOR_ID}")
                
    except Exception as e:
        print(f"\nVerification error: {e}")


def extract_only():
    """
    Extract data only (for testing/debugging).
    Does not transform or load.
    """
    print("\n[EXTRACT ONLY MODE]\n")
    
    extractor = MunicipalFinanceExtractor()
    
    print(f"Retrieving data using job ID: {EXISTING_JOB_ID}")
    raw_df = extractor.retrieve_municipal_finances(EXISTING_JOB_ID)
    
    if raw_df is not None:
        print(f"\nExtracted {len(raw_df)} rows")
        print(f"Columns: {raw_df.columns.tolist()}")
        print(f"\nFirst 5 rows:")
        print(raw_df.head())
        
        # Save to CSV for inspection
        output_file = project_root / "data" / "processed" / "municipal_finances_extracted.csv"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        raw_df.to_csv(output_file, index=False)
        print(f"\nSaved to: {output_file}")
    else:
        print("Extraction failed")
    
    extractor.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Municipal Finances ETL Pipeline")
    parser.add_argument('--extract-only', action='store_true', 
                       help='Only extract data (for testing)')
    args = parser.parse_args()
    
    if args.extract_only:
        extract_only()
    else:
        main()

