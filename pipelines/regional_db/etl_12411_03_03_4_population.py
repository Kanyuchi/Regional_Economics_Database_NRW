"""
ETL Pipeline: Table 12411-03-03-4
=================================
Population by gender, nationality, and age groups (Reference date: Dec 31)

Source: Regional Database Germany (Regionalstatistik)
Table ID: 12411-03-03-4
Category: Demographics
Available Period: 2011-2024

Description:
    Extracts population data by:
    - Gender (male/female/total)
    - Nationality (German/foreign/total)
    - Age groups (21 categories from under 3 to 90+)
    - Geographic level: Districts and independent cities

Usage:
    python pipelines/regional_db/etl_12411_03_03_4_population.py

Status: ✅ COMPLETED (2025-12-16)
Records Loaded: 17,864
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.logging import setup_logging, get_logger
from extractors.regional_db.demographics_extractor import DemographicsExtractor
from transformers.demographics_transformer import DemographicsTransformer
from loaders.db_loader import DataLoader

# Pipeline metadata
PIPELINE_INFO = {
    "table_id": "12411-03-03-4",
    "table_name": "Population by gender, nationality, and age groups",
    "source": "regional_db",
    "category": "demographics",
    "available_period": "2011-2024",
    "reference_date": "December 31",
    "geographic_level": "districts",
}

# Setup logging
setup_logging()
logger = get_logger(__name__)


def run_pipeline(years: list = None) -> bool:
    """
    Run the complete ETL pipeline for table 12411-03-03-4.
    
    Args:
        years: Optional list of years to extract. If None, uses all available years.
    
    Returns:
        True if pipeline completed successfully, False otherwise.
    """
    start_time = datetime.now()
    
    logger.info("=" * 60)
    logger.info(f"ETL Pipeline: {PIPELINE_INFO['table_id']}")
    logger.info(f"Table: {PIPELINE_INFO['table_name']}")
    logger.info(f"Started: {start_time.isoformat()}")
    logger.info("=" * 60)
    
    extractor = None
    loader = None
    
    try:
        # =====================================================
        # STEP 1: EXTRACT
        # =====================================================
        logger.info("Step 1: Extracting data from Regional Database")
        extractor = DemographicsExtractor()
        
        # Use specified years or all available
        if years is None:
            years = list(range(2011, 2025))  # 2011-2024
        
        raw_data = extractor.extract_population_total(years=years)
        
        if raw_data is None:
            logger.error("No data extracted. Aborting pipeline.")
            return False
        
        logger.info(f"Extracted {len(raw_data)} characters of raw data")
        
        # =====================================================
        # STEP 2: TRANSFORM
        # =====================================================
        logger.info("Step 2: Transforming data")
        transformer = DemographicsTransformer()
        
        transformed_data = transformer.transform_population_data(raw_data)
        
        if transformed_data is None or transformed_data.empty:
            logger.error("Transformation failed. Aborting pipeline.")
            return False
        
        logger.info(f"Transformed {len(transformed_data)} rows")
        
        # =====================================================
        # STEP 3: LOAD
        # =====================================================
        logger.info("Step 3: Loading data into database")
        loader = DataLoader()
        
        records_loaded = loader.load_demographics_data(transformed_data)
        
        logger.info(f"Successfully loaded {records_loaded} records")
        
        # =====================================================
        # COMPLETE
        # =====================================================
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"Pipeline Completed Successfully!")
        logger.info(f"Duration: {duration:.1f} seconds")
        logger.info(f"Records Loaded: {records_loaded}")
        logger.info("=" * 60)
        
        return True
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
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
    import argparse
    
    parser = argparse.ArgumentParser(
        description=f"ETL Pipeline for {PIPELINE_INFO['table_id']}: {PIPELINE_INFO['table_name']}"
    )
    parser.add_argument(
        "--years",
        type=int,
        nargs="+",
        help="Specific years to extract (default: all available 2011-2024)"
    )
    parser.add_argument(
        "--info",
        action="store_true",
        help="Show pipeline information and exit"
    )
    
    args = parser.parse_args()
    
    if args.info:
        print("\nPipeline Information:")
        print("-" * 40)
        for key, value in PIPELINE_INFO.items():
            print(f"  {key}: {value}")
        print()
        sys.exit(0)
    
    success = run_pipeline(years=args.years)
    sys.exit(0 if success else 1)

