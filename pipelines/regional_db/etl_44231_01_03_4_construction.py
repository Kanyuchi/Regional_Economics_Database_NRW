"""
ETL Pipeline: Table 44231-01-03-4
=================================
Businesses, employees, construction industry turnover - Reference date 30.06

Source: Regional Database Germany (Regionalstatistik)
Table ID: 44231-01-03-4
Category: Business Economy / Construction
Available Period: 1995-2024 (30 years)

Description:
    Extracts construction industry statistics including:
    - Number of businesses
    - Number of employees
    - Turnover in Tsd. EUR
    - Geographic level: Districts and independent cities
    - Reference: 30.06 each year

Usage:
    python pipelines/regional_db/etl_44231_01_03_4_construction.py
    python pipelines/regional_db/etl_44231_01_03_4_construction.py --years 2020 2021 2022 2023 2024

Status: READY TO RUN
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.logging import setup_logging, get_logger
from extractors.regional_db.employment_extractor import EmploymentExtractor
from transformers.employment_transformer import EmploymentTransformer
from loaders.db_loader import DataLoader

# Pipeline metadata
PIPELINE_INFO = {
    "table_id": "44231-01-03-4",
    "table_name": "Businesses, employees, construction industry turnover",
    "source": "regional_db",
    "category": "business_economy",
    "subcategory": "construction",
    "available_period": "1995-2024",
    "reference_date": "30.06",
    "geographic_level": "districts",
    "indicator_id": 20,  # construction_industry
}

# Setup logging
setup_logging()
logger = get_logger(__name__)


def run_pipeline(years: list = None) -> bool:
    """
    Run the complete ETL pipeline for table 44231-01-03-4.
    
    Args:
        years: Optional list of years to extract. If None, uses 1995-2024 (all 30 years).
    
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
        extractor = EmploymentExtractor()
        
        # Use specified years or default (1995-2024 = 30 years)
        if years is None:
            years = list(range(1995, 2025))  # 1995-2024
        
        logger.info(f"Extracting {len(years)} years of data: {years[0]}-{years[-1]}")
        
        raw_data = extractor.extract_construction_industry(years=years)
        
        if raw_data is None or raw_data.empty:
            logger.error("No data extracted. Aborting pipeline.")
            return False
        
        logger.info(f"Extracted {len(raw_data)} rows of raw data")
        
        # =====================================================
        # STEP 2: TRANSFORM
        # =====================================================
        logger.info("Step 2: Transforming data")
        transformer = EmploymentTransformer()
        
        # Get years filter from raw data attributes if set
        years_filter = raw_data.attrs.get('years_filter', years)
        
        transformed_data = transformer.transform_construction_industry(
            raw_data,
            indicator_id=PIPELINE_INFO['indicator_id'],
            years_filter=years_filter
        )
        
        if transformed_data is None or transformed_data.empty:
            logger.error("Transformation failed. Aborting pipeline.")
            return False
        
        logger.info(f"Transformed {len(transformed_data)} rows")
        
        # Validate data
        if not transformer.validate_data(transformed_data):
            logger.error("Data validation failed. Aborting pipeline.")
            return False
        
        # =====================================================
        # STEP 3: LOAD
        # =====================================================
        logger.info("Step 3: Loading data into database")
        loader = DataLoader()
        
        records_loaded = loader.load_demographics_data(transformed_data)
        
        if records_loaded == 0:
            logger.warning("No records were loaded")
            return False
        
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
        help="Specific years to extract (default: all 1995-2024)"
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
