"""
Tests for ICT Indicators Transformer
Regional Economics Database for NRW

Tests the ICTIndicatorsTransformer class functionality.
"""

import pytest
import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from transformers.ict_indicators_transformer import ICTIndicatorsTransformer


class TestICTIndicatorsTransformer:
    """Test suite for ICT Indicators Transformer."""

    def test_transformer_initialization(self):
        """Test that the transformer initializes correctly."""
        transformer = ICTIndicatorsTransformer()

        assert hasattr(transformer, 'indicator_mapping')
        assert isinstance(transformer.indicator_mapping, dict)

    def test_transform_ict_data_with_valid_input(self, sample_raw_ict_data):
        """Test transformation with valid raw data."""
        transformer = ICTIndicatorsTransformer()

        # Transform data
        transformed = transformer.transform_ict_data(
            sample_raw_ict_data,
            indicator_id_base=50
        )

        assert transformed is not None
        assert not transformed.empty
        assert 'year' in transformed.columns
        assert 'region_code' in transformed.columns
        assert 'indicator_id' in transformed.columns
        assert 'value' in transformed.columns
        assert 'indicator_name' in transformed.columns

        # Check that data was converted to long format
        # Original: 6 rows × 6 columns → Long: 6 regions × 3 indicators = 18 rows
        assert len(transformed) > len(sample_raw_ict_data)

    def test_transform_ict_data_with_empty_dataframe(self):
        """Test transformation with empty DataFrame."""
        transformer = ICTIndicatorsTransformer()

        empty_df = pd.DataFrame()
        transformed = transformer.transform_ict_data(empty_df)

        assert transformed is None

    def test_transform_ict_data_with_none(self):
        """Test transformation with None input."""
        transformer = ICTIndicatorsTransformer()

        transformed = transformer.transform_ict_data(None)

        assert transformed is None

    def test_transform_ict_data_with_year_filter(self, sample_raw_ict_data):
        """Test transformation with year filter."""
        transformer = ICTIndicatorsTransformer()

        # Filter to only 2020
        transformed = transformer.transform_ict_data(
            sample_raw_ict_data,
            indicator_id_base=50,
            years_filter=[2020]
        )

        assert transformed is not None
        # Should only contain 2020 data
        assert all(transformed['year'] == 2020)

    def test_transform_ict_data_indicator_ids(self, sample_raw_ict_data):
        """Test that indicator IDs are assigned correctly."""
        transformer = ICTIndicatorsTransformer()

        transformed = transformer.transform_ict_data(
            sample_raw_ict_data,
            indicator_id_base=100
        )

        assert transformed is not None
        # Check that indicator IDs start from base
        assert transformed['indicator_id'].min() >= 100

    def test_create_fact_records(self, sample_transformed_ict_data, sample_geo_mapping, sample_time_mapping):
        """Test creation of fact table records."""
        transformer = ICTIndicatorsTransformer()

        fact_df = transformer.create_fact_records(
            sample_transformed_ict_data,
            sample_geo_mapping,
            sample_time_mapping
        )

        assert fact_df is not None
        assert not fact_df.empty
        assert 'geo_id' in fact_df.columns
        assert 'time_id' in fact_df.columns
        assert 'indicator_id' in fact_df.columns
        assert 'value' in fact_df.columns

        # Check that mappings were applied
        assert all(fact_df['geo_id'].notna())
        assert all(fact_df['time_id'].notna())

    def test_create_fact_records_with_unmapped_regions(self, sample_transformed_ict_data, sample_time_mapping):
        """Test fact record creation with unmapped regions."""
        transformer = ICTIndicatorsTransformer()

        # Empty geography mapping
        empty_geo_mapping = {}

        fact_df = transformer.create_fact_records(
            sample_transformed_ict_data,
            empty_geo_mapping,
            sample_time_mapping
        )

        # Should return empty or None since no regions can be mapped
        assert fact_df is None or fact_df.empty

    def test_validate_data_with_valid_data(self, sample_transformed_ict_data):
        """Test validation with valid data."""
        transformer = ICTIndicatorsTransformer()

        is_valid = transformer.validate_data(sample_transformed_ict_data)

        assert is_valid is True

    def test_validate_data_with_missing_columns(self):
        """Test validation with missing required columns."""
        transformer = ICTIndicatorsTransformer()

        # Create DataFrame with missing columns
        invalid_df = pd.DataFrame({
            'year': [2020, 2021],
            'region_code': ['05111', '05112']
            # Missing: indicator_id, value
        })

        is_valid = transformer.validate_data(invalid_df)

        assert is_valid is False

    def test_validate_data_with_null_values(self, sample_transformed_ict_data):
        """Test validation with null values in key columns."""
        transformer = ICTIndicatorsTransformer()

        # Introduce null values
        df_with_nulls = sample_transformed_ict_data.copy()
        df_with_nulls.loc[0, 'value'] = None

        is_valid = transformer.validate_data(df_with_nulls)

        assert is_valid is False

    def test_validate_data_with_empty_dataframe(self):
        """Test validation with empty DataFrame."""
        transformer = ICTIndicatorsTransformer()

        is_valid = transformer.validate_data(pd.DataFrame())

        assert is_valid is False

    def test_validate_data_with_none(self):
        """Test validation with None input."""
        transformer = ICTIndicatorsTransformer()

        is_valid = transformer.validate_data(None)

        assert is_valid is False

    def test_get_indicator_metadata(self, sample_transformed_ict_data):
        """Test extraction of indicator metadata."""
        transformer = ICTIndicatorsTransformer()

        metadata = transformer.get_indicator_metadata(
            sample_transformed_ict_data,
            indicator_id_base=50
        )

        assert isinstance(metadata, list)
        assert len(metadata) > 0

        # Check structure of first metadata entry
        if metadata:
            first_entry = metadata[0]
            assert 'indicator_id' in first_entry
            assert 'indicator_code' in first_entry
            assert 'indicator_name' in first_entry
            assert 'category' in first_entry
            assert 'source' in first_entry

    def test_get_indicator_metadata_with_empty_data(self):
        """Test metadata extraction with empty data."""
        transformer = ICTIndicatorsTransformer()

        metadata = transformer.get_indicator_metadata(pd.DataFrame())

        assert isinstance(metadata, list)
        assert len(metadata) == 0

    def test_value_cleaning_german_format(self):
        """Test that German number formats are cleaned correctly."""
        transformer = ICTIndicatorsTransformer()

        # Create test data with German number format
        test_data = pd.DataFrame({
            'year': [2020, 2020],
            'region_code': ['05111', '05112'],
            'region_name': ['Düsseldorf', 'Duisburg'],
            'indicator_1': ['1.234,56', '5.678,90']
        })

        transformed = transformer.transform_ict_data(test_data, indicator_id_base=50)

        assert transformed is not None
        # Values should be converted to float
        assert all(transformed['value'].notna())
        assert all(isinstance(v, (int, float)) for v in transformed['value'])

    def test_value_cleaning_missing_markers(self):
        """Test that missing value markers are handled correctly."""
        transformer = ICTIndicatorsTransformer()

        # Create test data with missing value markers
        test_data = pd.DataFrame({
            'year': [2020, 2020, 2020],
            'region_code': ['05111', '05112', '05113'],
            'region_name': ['Düsseldorf', 'Duisburg', 'Essen'],
            'indicator_1': ['100.5', '-', 'x']
        })

        transformed = transformer.transform_ict_data(test_data, indicator_id_base=50)

        # Should only have one valid value
        assert transformed is not None
        assert len(transformed) == 1  # Only the first row has a valid value
