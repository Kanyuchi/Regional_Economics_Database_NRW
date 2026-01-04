"""
ETL Pipeline: Table [TABLE_ID]
=================================
[TABLE_DESCRIPTION]

Source: [SOURCE_NAME]
Table ID: [TABLE_ID]
Category: [CATEGORY]
Available Period: [PERIOD]

Description:
    [DETAILED_DESCRIPTION]

Usage:
    python pipelines/[source]/etl_[TABLE_ID_UNDERSCORED]_[SHORT_NAME].py

Status: 🔄 PENDING
Records Loaded: 0

================================================================================
HOW TO USE THIS TEMPLATE:
================================================================================
1. Copy this file to: pipelines/[source]/etl_[TABLE_ID_UNDERSCORED]_[SHORT_NAME].py
   Example: pipelines/regional_db/etl_13111_01_03_4_employment.py

2. Replace all [PLACEHOLDERS] with actual values

3. Create/modify extractor in: src/extractors/[source]/[category]_extractor.py

4. Create/modify transformer in: src/transformers/[category]_transformer.py

5. Update the table_registry.json with the new table info

6. Test the pipeline: python pipelines/[source]/etl_[TABLE_ID]...py

7. Update status in table_registry.json to "completed"
================================================================================
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from utils.logging import setup_logging, get_logger

# TODO: Import the specific extractor, transformer, and loader
# from extractors.[source].[category]_extractor import [Category]Extractor
# from transformers.[category]_transformer import [Category]Transformer
# from loaders.db_loader import DataLoader

# Pipeline metadata - UPDATE THESE VALUES
PIPELINE_INFO = {
    "table_id": "[TABLE_ID]",
    "table_name": "[TABLE_DESCRIPTION]",
    "source": "[source]",  # regional_db, state_db, or ba
    "category": "[category]",  # demographics, labor_market, economic_activity, etc.
    "available_period": "[PERIOD]",
    "reference_date": "[REFERENCE_DATE]",  # e.g., "December 31", "June 30"
    "geographic_level": "districts",  # usually "districts" for this project
}

# Setup logging
setup_logging()
logger = get_logger(__name__)


def run_pipeline(years: list = None) -> bool:
    """
    Run the complete ETL pipeline for table [TABLE_ID].
    
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
        logger.info("Step 1: Extracting data from [SOURCE]")
        
        # TODO: Initialize extractor
        # extractor = [Category]Extractor()
        
        # TODO: Extract data
        # raw_data = extractor.extract_[data_type](years=years)
        raw_data = None  # PLACEHOLDER
        
        if raw_data is None:
            logger.error("No data extracted. Aborting pipeline.")
            return False
        
        logger.info(f"Extracted {len(raw_data)} characters of raw data")
        
        # =====================================================
        # STEP 2: TRANSFORM
        # =====================================================
        logger.info("Step 2: Transforming data")
        
        # TODO: Initialize transformer
        # transformer = [Category]Transformer()
        
        # TODO: Transform data
        # transformed_data = transformer.transform_[data_type](raw_data)
        transformed_data = None  # PLACEHOLDER
        
        if transformed_data is None:
            logger.error("Transformation failed. Aborting pipeline.")
            return False
        
        logger.info(f"Transformed {len(transformed_data)} rows")
        
        # =====================================================
        # STEP 3: LOAD
        # =====================================================
        logger.info("Step 3: Loading data into database")
        
        # TODO: Initialize loader
        # loader = DataLoader()
        
        # TODO: Load data
        # records_loaded = loader.load_[category]_data(transformed_data)
        records_loaded = 0  # PLACEHOLDER
        
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
        help="Specific years to extract (default: all available)"
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

