"""
ETL Pipeline for Employee Compensation by Economic Sector
Table: 82000-04-01-4
Period: 2000-2022 (23 years)
Unit: Thousand EUR

Sectors (8 total):
- Total
- Agriculture, forestry, fishing (A)
- Manufacturing excluding construction (B-E)
- Manufacturing - Processing industry (C)
- Construction (F)
- Trade, transport, hospitality, IT (G-J)
- Finance, insurance, real estate (K-L)
- Public services, education, health (O-T)
"""

import sys
sys.path.append('.')

from src.extractors.regional_db.business_extractor import BusinessExtractor
from src.transformers.business_transformer import BusinessTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Execute ETL pipeline for employee compensation"""
    
    logger.info("="*80)
    logger.info("ETL Pipeline: Employee Compensation (82000-04-01-4)")
    logger.info("Period: 2000-2022 (23 years)")
    logger.info("="*80)
    
    extractor = None
    loader = None
    
    try:
        # ========================================
        # STEP 1: EXTRACT
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 1: EXTRACTING DATA")
        logger.info("="*80)
        
        extractor = BusinessExtractor()
        
        # Extract all 23 years (2000-2022)
        years = list(range(2000, 2023))
        logger.info(f"Extracting data for {len(years)} years: {years[0]}-{years[-1]}")
        
        raw_data = extractor.extract_employee_compensation(years=years)
        
        if raw_data is None or raw_data.empty:
            logger.error("Extraction failed - no data retrieved")
            return False
        
        logger.info(f"Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        
        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)
        
        transformer = BusinessTransformer()
        
        # Transform data into database format
        transformed_data = transformer.transform_employee_compensation(
            raw_data,
            indicator_id=26,
            years_filter=years
        )
        
        if transformed_data is None or transformed_data.empty:
            logger.error("Transformation failed - no data output")
            return False
        
        logger.info(f"Transformation successful: {len(transformed_data)} transformed rows")
        
        # Validate
        if not transformer.validate_data(transformed_data):
            logger.error("Data validation failed")
            return False
        
        # ========================================
        # STEP 3: LOAD
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 3: LOADING TO DATABASE")
        logger.info("="*80)
        
        loader = DataLoader()
        
        # Load data
        success = loader.load_demographics_data(transformed_data)
        
        if not success:
            logger.error("Loading failed")
            return False
        
        logger.info("Loading successful")
        
        # ========================================
        # STEP 4: VERIFICATION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 4: VERIFICATION")
        logger.info("="*80)
        
        # Query database directly to verify
        from sqlalchemy import text
        from src.utils.database import DatabaseManager
        db = DatabaseManager()
        
        with db.get_connection() as conn:
            # Count records
            result = conn.execute(text("SELECT COUNT(*) FROM fact_demographics WHERE indicator_id = 26"))
            record_count = result.scalar() or 0
            logger.info(f"\nEmployee Compensation (Indicator 26): {record_count} records")
            
            # Check year range
            result = conn.execute(text("""
                SELECT MIN(t.year), MAX(t.year), COUNT(DISTINCT g.geo_id)
                FROM fact_demographics f 
                JOIN dim_time t ON f.time_id = t.time_id 
                JOIN dim_geography g ON f.geo_id = g.geo_id
                WHERE f.indicator_id = 26
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"Year range: {year_range[0]} - {year_range[1]}")
                logger.info(f"Unique regions: {year_range[2]}")
        
        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Employee Compensation (ID 26): {record_count} records")
        logger.info(f"Period: 2000-2022 (23 years)")
        logger.info(f"Sectors: 8 (Total + 7 WZ 2008 sectors)")
        logger.info("\n" + "="*80)
        logger.info("REGIONAL DATABASE GERMANY: 17/17 TABLES COMPLETE! 🎉")
        logger.info("="*80)
        logger.info("\nNext steps:")
        logger.info("1. Run verification script")
        logger.info("2. Create SQL analysis script")
        logger.info("3. Begin State Database NRW extraction")
        logger.info("="*80)
        
        return True
        
    except Exception as e:
        logger.error(f"ETL pipeline failed with error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
        
    finally:
        # Cleanup
        if extractor:
            extractor.close()
        if loader:
            loader.close()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

