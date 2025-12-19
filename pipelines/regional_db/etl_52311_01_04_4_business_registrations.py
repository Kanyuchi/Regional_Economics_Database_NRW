"""
ETL Pipeline for Business Registrations/Deregistrations
Table: 52311-01-04-4
Period: 1998-2024 (27 years)

Categories:
- Registrations: Total, New establishments, Business foundations, Relocations in, Takeovers
- Deregistrations: Total, Closures, Business closures, Relocations out, Handovers
"""

import sys
sys.path.append('.')

from src.extractors.regional_db.business_extractor import BusinessExtractor
from src.transformers.business_transformer import BusinessTransformer
from src.loaders.db_loader import DataLoader
from src.utils.logging import get_logger

logger = get_logger(__name__)


def main():
    """Execute ETL pipeline for business registrations/deregistrations"""
    
    logger.info("="*80)
    logger.info("ETL Pipeline: Business Registrations/Deregistrations (52311-01-04-4)")
    logger.info("Period: 1998-2024 (27 years)")
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
        
        # Extract all 27 years (1998-2024)
        years = list(range(1998, 2025))
        logger.info(f"Extracting data for {len(years)} years: {years[0]}-{years[-1]}")
        
        raw_data = extractor.extract_business_registrations(years=years)
        
        if raw_data is None or raw_data.empty:
            logger.error("Extraction failed - no data retrieved")
            return False
        
        logger.info(f"✅ Extraction successful: {len(raw_data)} raw rows")
        logger.info(f"Columns: {raw_data.columns.tolist()}")
        
        # ========================================
        # STEP 2: TRANSFORM
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("STEP 2: TRANSFORMING DATA")
        logger.info("="*80)
        
        transformer = BusinessTransformer()
        
        # Transform data into database format
        # Creates two indicators: 24 (registrations) and 25 (deregistrations)
        transformed_data = transformer.transform_business_registrations(
            raw_data,
            indicator_id_registrations=24,
            indicator_id_deregistrations=25,
            years_filter=years
        )
        
        if transformed_data is None or transformed_data.empty:
            logger.error("Transformation failed - no data output")
            return False
        
        logger.info(f"✅ Transformation successful: {len(transformed_data)} transformed rows")
        
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
        
        logger.info("✅ Loading successful")
        
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
            # Count registrations (indicator 24)
            result = conn.execute(text("SELECT COUNT(*) FROM fact_demographics WHERE indicator_id = 24"))
            reg_count = result.scalar() or 0
            logger.info(f"\nRegistrations (Indicator 24): {reg_count} records")
            
            # Count deregistrations (indicator 25)
            result = conn.execute(text("SELECT COUNT(*) FROM fact_demographics WHERE indicator_id = 25"))
            dereg_count = result.scalar() or 0
            logger.info(f"Deregistrations (Indicator 25): {dereg_count} records")
            
            total_records = reg_count + dereg_count
            logger.info(f"\nTotal records loaded: {total_records}")
            
            # Check year range
            result = conn.execute(text("""
                SELECT MIN(t.year), MAX(t.year) 
                FROM fact_demographics f 
                JOIN dim_time t ON f.time_id = t.time_id 
                WHERE f.indicator_id IN (24, 25)
            """))
            year_range = result.fetchone()
            if year_range and year_range[0]:
                logger.info(f"Year range: {year_range[0]} - {year_range[1]}")
        
        # ========================================
        # COMPLETION
        # ========================================
        logger.info("\n" + "="*80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)
        logger.info(f"Registrations (ID 24): {reg_count} records")
        logger.info(f"Deregistrations (ID 25): {dereg_count} records")
        logger.info(f"Total: {total_records} records")
        logger.info(f"Period: 1998-2024 (27 years)")
        logger.info("\nNext steps:")
        logger.info("1. Run verification: python scripts/verification/verify_extraction_timeseries.py --indicator 24")
        logger.info("2. Run verification: python scripts/verification/verify_extraction_timeseries.py --indicator 25")
        logger.info("3. Create SQL analysis script")
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

