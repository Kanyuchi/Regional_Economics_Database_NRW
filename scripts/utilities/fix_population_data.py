"""
Fix Population Data - Remove Incorrect Data and Reload
Regional Economics Database for NRW

This script:
1. Removes incorrectly loaded population data (with individual age groups)
2. Re-runs the ETL pipeline to load corrected data (only 'total' age group)

Usage:
    python scripts/utilities/fix_population_data.py
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))
sys.path.insert(0, str(PROJECT_ROOT / "pipelines" / "regional_db"))

from utils.logging import setup_logging, get_logger
from utils.database import get_database
from sqlalchemy import text

# Setup logging
setup_logging()
logger = get_logger(__name__)


def clear_population_data(indicator_id: int = 1) -> int:
    """
    Clear incorrect population data from fact_demographics table.

    Args:
        indicator_id: ID of the population indicator to clear (default: 1)

    Returns:
        Number of records deleted
    """
    logger.info(f"Clearing population data for indicator_id={indicator_id}")

    try:
        db = get_database('regional_economics')

        # First, count records to be deleted
        count_query = """
        SELECT COUNT(*) as count
        FROM fact_demographics
        WHERE indicator_id = :indicator_id
        """

        with db.get_connection() as conn:
            result = conn.execute(text(count_query), {'indicator_id': indicator_id})
            count = result.fetchone()[0]

            logger.info(f"Found {count} records to delete")

            if count == 0:
                logger.info("No records to delete")
                return 0

            # Delete the records
            delete_query = """
            DELETE FROM fact_demographics
            WHERE indicator_id = :indicator_id
            """

            conn.execute(text(delete_query), {'indicator_id': indicator_id})
            conn.commit()

            logger.info(f"Successfully deleted {count} records")

        db.close()
        return count

    except Exception as e:
        logger.error(f"Error clearing population data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0


def run_population_etl() -> bool:
    """
    Run the population ETL pipeline to reload correct data.

    Returns:
        True if successful, False otherwise
    """
    logger.info("Running population ETL pipeline")

    try:
        # Import the ETL module
        from etl_12411_03_03_4_population import run_pipeline

        # Run the pipeline
        success = run_pipeline()

        if success:
            logger.info("ETL pipeline completed successfully")
        else:
            logger.error("ETL pipeline failed")

        return success

    except Exception as e:
        logger.error(f"Error running ETL pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def main():
    """Main execution function."""
    logger.info("=" * 80)
    logger.info("FIX POPULATION DATA - Remove Incorrect Data and Reload")
    logger.info("=" * 80)

    # Step 1: Clear incorrect data
    logger.info("\nStep 1: Clearing incorrect population data...")
    deleted_count = clear_population_data(indicator_id=1)

    if deleted_count == 0:
        logger.warning("No records were deleted. Check if data exists in the database.")
    else:
        logger.info(f"✓ Deleted {deleted_count} incorrect records")

    # Step 2: Reload correct data
    logger.info("\nStep 2: Reloading population data with corrected transformer...")
    success = run_population_etl()

    if success:
        logger.info("\n" + "=" * 80)
        logger.info("✓ POPULATION DATA FIX COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        logger.info("\nNext steps:")
        logger.info("1. Verify data with your MCP query")
        logger.info("2. Check that population values are now correct (~500K for Duisburg)")
        logger.info("3. Confirm no duplicate age group entries")
    else:
        logger.error("\n" + "=" * 80)
        logger.error("✗ POPULATION DATA FIX FAILED")
        logger.error("=" * 80)
        logger.error("\nCheck the logs above for errors")
        sys.exit(1)


if __name__ == "__main__":
    main()
