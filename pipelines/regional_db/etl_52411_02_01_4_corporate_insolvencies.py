"""
ETL Pipeline for Corporate Insolvency Applications

Table: 52411-02-01-4
Description: Filed corporate insolvencies - annual total
Period: 2000-2024 (25 years)
Indicator: 27 (Corporate Insolvency Applications)

Regional Economics Database for NRW
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.extractors.regional_db.business_extractor import BusinessExtractor
from src.transformers.business_transformer import BusinessTransformer
from src.loaders.db_loader import DataLoader
from src.utils.database import DatabaseManager
from src.utils.logging import get_logger
from sqlalchemy import text

logger = get_logger(__name__)


def main():
    """Main ETL pipeline for corporate insolvencies."""
    
    logger.info("="*80)
    logger.info("CORPORATE INSOLVENCY APPLICATIONS ETL PIPELINE")
    logger.info("Table: 52411-02-01-4")
    logger.info("Period: 2000-2024 (25 years)")
    logger.info("="*80)
    
    # Initialize components
    extractor = None
    loader = None
    
    try:
        # STEP 1: EXTRACTION
        logger.info("\n" + "="*80)
        logger.info("STEP 1: EXTRACTION")
        logger.info("="*80)
        
        extractor = BusinessExtractor()
        years = list(range(2000, 2025))  # 2000-2024
        
        logger.info(f"Extracting corporate insolvency data for {len(years)} years...")
        raw_data = extractor.extract_corporate_insolvencies(years=years)
        
        if raw_data is None or raw_data.empty:
            logger.error("Extraction failed: No data returned")
            return
        
        logger.info(f"Extraction successful: {len(raw_data)} rows")
        logger.info(f"Columns: {list(raw_data.columns)}")
        
        # STEP 2: TRANSFORMATION
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMATION")
        logger.info("="*80)
        
        transformer = BusinessTransformer()
        transformed_data = transformer.transform_corporate_insolvencies(raw_data)
        
        if transformed_data is None or transformed_data.empty:
            logger.error("Transformation failed: No data returned")
            return
        
        logger.info(f"Transformation successful: {len(transformed_data)} rows")
        logger.info(f"Indicators: {transformed_data['indicator_id'].unique().tolist()}")
        logger.info(f"Year range: {int(transformed_data['year'].min())} - {int(transformed_data['year'].max())}")
        logger.info(f"Unique regions: {transformed_data['region_code'].nunique()}")
        
        # STEP 3: LOADING
        logger.info("\n" + "="*80)
        logger.info("STEP 3: LOADING")
        logger.info("="*80)
        
        loader = DataLoader()
        
        logger.info(f"Loading {len(transformed_data)} records to database...")
        loader.load_demographics_data(transformed_data)
        
        logger.info("Loading successful")
        
        # STEP 4: VERIFICATION
        logger.info("\n" + "="*80)
        logger.info("STEP 4: VERIFICATION")
        logger.info("="*80)
        
        db = DatabaseManager()
        with db.get_session() as session:
            # Count records for indicator 27
            result = session.execute(text("""
                SELECT COUNT(*) 
                FROM fact_demographics 
                WHERE indicator_id = 27
            """))
            count = result.fetchone()[0]
            logger.info(f"\nCorporate Insolvencies (Indicator 27): {count} records")
            
            # Get year range
            result = session.execute(text("""
                SELECT MIN(dt.year), MAX(dt.year)
                FROM fact_demographics fd
                JOIN dim_time dt ON fd.time_id = dt.time_id
                WHERE fd.indicator_id = 27
            """))
            min_year, max_year = result.fetchone()
            logger.info(f"Year range: {min_year} - {max_year}")
            
            # Count regions
            result = session.execute(text("""
                SELECT COUNT(DISTINCT geo_id)
                FROM fact_demographics
                WHERE indicator_id = 27
            """))
            region_count = result.fetchone()[0]
            logger.info(f"Unique regions: {region_count}")
        
        db.close()
        
        logger.info("\n" + "="*80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Corporate Insolvencies (ID 27): {count} records")
        logger.info(f"Period: {min_year}-{max_year} ({max_year - min_year + 1} years)")
        logger.info(f"Regions: {region_count} NRW districts")
        logger.info("="*80)
        logger.info("\n*** REGIONAL DATABASE GERMANY: 17/17 TABLES COMPLETE! ***")
        logger.info("="*80)
        
        logger.info("\nNext steps:")
        logger.info("1. Run verification script")
        logger.info("2. Create SQL analysis script")
        logger.info("3. Update documentation")
        logger.info("4. Begin State Database NRW extraction")
        logger.info("="*80)
        
    except Exception as e:
        logger.error(f"\nERROR in ETL pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    
    finally:
        # Clean up
        if extractor:
            extractor.close()
        if loader:
            loader.close()


if __name__ == "__main__":
    main()

