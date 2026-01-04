"""
Roads Data Transformer
Regional Economics Database for NRW

Transforms raw interregional road data into database-ready format.
Handles data from State Database NRW table 46271-01i.
"""

import pandas as pd
import numpy as np
from typing import Optional, Dict, Any, List
from datetime import datetime

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class RoadsTransformer:
    """Transforms interregional road data by road class for database loading."""

    # Indicator IDs for road metrics
    INDICATOR_ROAD_TOTAL = 62
    INDICATOR_ROAD_MOTORWAY = 63
    INDICATOR_ROAD_FEDERAL = 64
    INDICATOR_ROAD_STATE = 65
    INDICATOR_ROAD_DISTRICT = 66

    # Metric column to indicator mapping
    METRIC_MAPPING = {
        'road_total_km': INDICATOR_ROAD_TOTAL,
        'road_motorway_km': INDICATOR_ROAD_MOTORWAY,
        'road_federal_km': INDICATOR_ROAD_FEDERAL,
        'road_state_km': INDICATOR_ROAD_STATE,
        'road_district_km': INDICATOR_ROAD_DISTRICT
    }

    # Human-readable road type names
    ROAD_TYPE_NAMES = {
        'road_total_km': 'Total Roads',
        'road_motorway_km': 'Autobahnen (Motorways)',
        'road_federal_km': 'Bundesstraßen (Federal Roads)',
        'road_state_km': 'Landstraßen (State Roads)',
        'road_district_km': 'Kreisstraßen (District Roads)'
    }

    def __init__(self):
        """Initialize the roads transformer."""
        logger.info("Roads transformer initialized")

    def transform_roads_data(
        self,
        df: pd.DataFrame,
        years_filter: Optional[List[int]] = None
    ) -> Optional[pd.DataFrame]:
        """
        Transform roads data from wide to long format.

        The raw data has structure:
        - year, reference_date, region_code, region_name,
          road_total_km, road_motorway_km, road_federal_km, road_state_km, road_district_km

        We transform to:
        - year, region_code, indicator_id, value, notes (containing road type)

        Args:
            df: Raw data DataFrame from extractor
            years_filter: Optional list of years to filter

        Returns:
            Transformed DataFrame ready for database loading
        """
        if df is None or df.empty:
            logger.warning("Empty DataFrame provided for transformation")
            return None

        logger.info("="*80)
        logger.info("TRANSFORMING ROADS DATA")
        logger.info("="*80)
        logger.info(f"Input rows: {len(df)}")
        logger.info(f"Columns: {df.columns.tolist()}")

        try:
            # Filter years if specified
            if years_filter:
                df = df[df['year'].isin(years_filter)]
                logger.info(f"Filtered to years {years_filter}: {len(df)} rows")

            # Melt from wide to long format (one row per road type)
            id_cols = ['year', 'region_code', 'region_name']
            if 'reference_date' in df.columns:
                id_cols.append('reference_date')

            metric_cols = list(self.METRIC_MAPPING.keys())

            melted = df.melt(
                id_vars=id_cols,
                value_vars=metric_cols,
                var_name='road_type',
                value_name='value'
            )

            logger.info(f"Melted to {len(melted)} rows")

            # Drop rows with no value
            before_count = len(melted)
            melted = melted.dropna(subset=['value'])
            logger.info(f"Dropped {before_count - len(melted)} rows with NULL values")

            # Map road types to indicator IDs
            melted['indicator_id'] = melted['road_type'].map(self.METRIC_MAPPING)

            # Create notes field with road type information (parseable format)
            melted['road_type_name'] = melted['road_type'].map(self.ROAD_TYPE_NAMES)
            melted['notes'] = 'road_type:' + melted['road_type'].astype(str) + \
                             '|' + melted['road_type_name'].astype(str)

            # Add standard columns
            melted['gender'] = 'total'
            melted['nationality'] = 'total'
            melted['age_group'] = None
            melted['migration_background'] = None
            melted['data_quality_flag'] = 'V'  # Verified
            melted['extracted_at'] = datetime.now()
            melted['loaded_at'] = datetime.now()

            # Select and order columns for database
            result = melted[[
                'region_code', 'year', 'indicator_id', 'value',
                'gender', 'nationality', 'age_group', 'migration_background',
                'data_quality_flag', 'notes', 'extracted_at', 'loaded_at',
                'road_type', 'road_type_name'
            ]]

            logger.info(f"Final transformed rows: {len(result)}")
            logger.info(f"Indicator IDs: {sorted(result['indicator_id'].unique())}")
            logger.info(f"Year range: {result['year'].min()} - {result['year'].max()}")
            logger.info(f"Unique road types: {result['road_type'].nunique()}")

            return result

        except Exception as e:
            logger.error(f"Transformation error: {e}")
            import traceback
            traceback.print_exc()
            return None

    def validate_data(self, df: pd.DataFrame) -> bool:
        """
        Validate transformed data before loading.

        Args:
            df: Transformed DataFrame

        Returns:
            True if valid, False otherwise
        """
        if df is None or df.empty:
            logger.error("Validation failed: Empty DataFrame")
            return False

        # Check required columns
        required_cols = ['region_code', 'year', 'indicator_id', 'value']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.error(f"Validation failed: Missing columns {missing_cols}")
            return False

        # Check for null indicator IDs
        null_indicators = df['indicator_id'].isnull().sum()
        if null_indicators > 0:
            logger.warning(f"Found {null_indicators} rows with NULL indicator_id")

        # Check that we have all 5 indicators
        indicators = sorted(df['indicator_id'].unique())
        expected = [
            self.INDICATOR_ROAD_TOTAL,
            self.INDICATOR_ROAD_MOTORWAY,
            self.INDICATOR_ROAD_FEDERAL,
            self.INDICATOR_ROAD_STATE,
            self.INDICATOR_ROAD_DISTRICT
        ]
        if not all(ind in indicators for ind in expected):
            logger.warning(f"Not all expected indicators present: {indicators}")

        # Check value ranges (road lengths should be positive)
        negative_values = (df['value'] < 0).sum()
        if negative_values > 0:
            logger.warning(f"Found {negative_values} rows with negative values")

        logger.info("Validation passed")
        return True

    def get_road_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Get summary statistics by road type.

        Args:
            df: Raw data DataFrame

        Returns:
            Summary DataFrame
        """
        metric_cols = list(self.METRIC_MAPPING.keys())

        summary_data = []
        for col in metric_cols:
            if col in df.columns:
                total = df[col].sum()
                mean = df[col].mean()
                summary_data.append({
                    'road_type': col,
                    'road_type_name': self.ROAD_TYPE_NAMES.get(col, col),
                    'total_km': total,
                    'avg_km_per_region': mean,
                    'num_regions': df[col].notna().sum()
                })

        return pd.DataFrame(summary_data)

    def get_regional_summary(self, df: pd.DataFrame, year: int = 2024) -> pd.DataFrame:
        """
        Get summary for a specific year by region.

        Args:
            df: Raw data DataFrame
            year: Year to summarize

        Returns:
            Regional summary DataFrame
        """
        year_df = df[df['year'] == year].copy()

        if year_df.empty:
            logger.warning(f"No data for year {year}")
            return pd.DataFrame()

        return year_df[[
            'region_code', 'region_name',
            'road_total_km', 'road_motorway_km', 'road_federal_km',
            'road_state_km', 'road_district_km'
        ]].sort_values('road_total_km', ascending=False)
