"""
Nursing Home Data Transformer
Regional Economics Database for NRW

Transforms raw nursing home data (facilities, places, staff) into database-ready format.
Handles data from State Database NRW table 22412-01i.

Indicators:
- 79: Total Nursing Homes (facilities count)
- 80: Available Places in Nursing Homes (capacity)
- 81: Staff in Nursing Homes
"""

import pandas as pd
from typing import Optional, List
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class NursingHomeTransformer:
    """Transforms nursing home data (facilities, places, staff) for database loading."""

    INDICATOR_NURSING_HOMES = 79
    INDICATOR_PLACES = 80
    INDICATOR_STAFF = 81

    METRIC_MAPPING = {
        'nursing_homes': INDICATOR_NURSING_HOMES,
        'total_places': INDICATOR_PLACES,
        'staff_count': INDICATOR_STAFF
    }

    METRIC_NAMES = {
        'nursing_homes': 'Total Nursing Homes',
        'total_places': 'Available Places in Nursing Homes',
        'staff_count': 'Staff in Nursing Homes'
    }

    def __init__(self):
        logger.info("Nursing Home transformer initialized")

    def transform_nursing_home_data(self, df: pd.DataFrame, years_filter: Optional[List[int]] = None) -> Optional[pd.DataFrame]:
        """Transform nursing home data for database loading."""
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("=" * 80)
        logger.info("TRANSFORMING NURSING HOME DATA")
        logger.info("=" * 80)
        logger.info(f"Input rows: {len(df)}")

        try:
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            id_cols = ['year', 'reference_date', 'region_code', 'region_name']
            metric_cols = ['nursing_homes', 'total_places', 'staff_count']

            melted = df.melt(id_vars=id_cols, value_vars=metric_cols, var_name='metric', value_name='value')
            logger.info(f"Melted to {len(melted)} rows")

            before_count = len(melted)
            melted = melted.dropna(subset=['value'])
            logger.info(f"Dropped {before_count - len(melted)} rows with NULL values")

            melted['indicator_id'] = melted['metric'].map(self.METRIC_MAPPING)
            melted['metric_name'] = melted['metric'].map(self.METRIC_NAMES)
            melted['notes'] = 'metric:' + melted['metric'].astype(str) + '|' + melted['metric_name'].astype(str)

            melted['gender'] = 'total'
            melted['nationality'] = None
            melted['age_group'] = None
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            result = melted[['region_code', 'year', 'indicator_id', 'value', 'gender', 'nationality',
                            'age_group', 'migration_background', 'data_quality_flag', 'notes',
                            'extracted_at', 'loaded_at', 'metric']]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")

            return result

        except Exception as e:
            logger.error(f"Transformation error: {e}")
            return None

    def validate_data(self, df: pd.DataFrame) -> bool:
        if df is None or df.empty:
            logger.error("Validation failed: Empty DataFrame")
            return False
        logger.info("Validation passed")
        return True
