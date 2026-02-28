"""
Tests for Database Loader - ICT Data
Regional Economics Database for NRW

Tests the ICT data loading functionality in DataLoader class.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from loaders.db_loader import DataLoader


class TestDataLoaderICT:
    """Test suite for ICT data loading."""

    @patch('loaders.db_loader.get_database')
    def test_loader_initialization(self, mock_get_database):
        """Test that the loader initializes correctly."""
        mock_db = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader('regional_economics')

        mock_get_database.assert_called_once_with('regional_economics')
        assert loader.db == mock_db

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_with_valid_data(self, mock_get_database, sample_transformed_ict_data):
        """Test loading valid ICT data."""
        # Setup mock database
        mock_db = Mock()
        mock_db.bulk_insert = Mock(return_value=len(sample_transformed_ict_data))
        mock_db.execute_query = Mock(return_value=[
            {'geo_id': 1, 'region_code': '05111'},
            {'geo_id': 2, 'region_code': '05112'},
            {'geo_id': 3, 'region_code': '05113'}
        ])
        mock_db.execute_statement = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        # Mock mappings
        time_mapping = {2020: 1, 2021: 2}

        # Load data
        count = loader.load_ict_data(
            sample_transformed_ict_data,
            time_mapping=time_mapping
        )

        # Verify bulk insert was called
        assert mock_db.bulk_insert.called
        assert count > 0

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_with_empty_dataframe(self, mock_get_database):
        """Test loading empty DataFrame."""
        mock_db = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        count = loader.load_ict_data(pd.DataFrame())

        assert count == 0

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_with_none(self, mock_get_database):
        """Test loading None data."""
        mock_db = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        count = loader.load_ict_data(None)

        assert count == 0

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_with_unmapped_regions(self, mock_get_database, sample_transformed_ict_data):
        """Test loading data with unmapped regions."""
        # Setup mock database with empty geography mapping
        mock_db = Mock()
        mock_db.execute_query = Mock(return_value=[])  # Empty mapping
        mock_db.execute_statement = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        count = loader.load_ict_data(sample_transformed_ict_data)

        # Should return 0 since no regions can be mapped
        assert count == 0

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_custom_table_name(self, mock_get_database, sample_transformed_ict_data, sample_time_mapping):
        """Test loading to custom table name."""
        mock_db = Mock()
        mock_db.bulk_insert = Mock(return_value=len(sample_transformed_ict_data))
        mock_db.execute_query = Mock(return_value=[
            {'geo_id': 1, 'region_code': '05111'},
            {'geo_id': 2, 'region_code': '05112'},
            {'geo_id': 3, 'region_code': '05113'}
        ])
        mock_db.execute_statement = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        # Load to custom table with time mapping
        count = loader.load_ict_data(
            sample_transformed_ict_data,
            time_mapping=sample_time_mapping,
            table_name='custom_ict_table'
        )

        # Verify bulk_insert was called with correct table name
        mock_db.bulk_insert.assert_called_once()
        call_args = mock_db.bulk_insert.call_args
        assert call_args[0][0] == 'custom_ict_table'

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_with_database_error(self, mock_get_database, sample_transformed_ict_data):
        """Test handling of database errors during loading."""
        mock_db = Mock()
        mock_db.execute_query = Mock(side_effect=Exception("Database connection failed"))
        mock_db.execute_statement = Mock()
        mock_get_database.return_value = mock_db

        loader = DataLoader()

        count = loader.load_ict_data(sample_transformed_ict_data)

        # Should return 0 and handle error gracefully
        assert count == 0

    @patch('loaders.db_loader.get_database')
    def test_load_ict_data_records_structure(self, mock_get_database, sample_transformed_ict_data):
        """Test that records are structured correctly for database insertion."""
        mock_db = Mock()
        mock_db.execute_query = Mock(return_value=[
            {'geo_id': 1, 'region_code': '05111'},
            {'geo_id': 2, 'region_code': '05112'},
            {'geo_id': 3, 'region_code': '05113'}
        ])
        mock_db.execute_statement = Mock()

        # Capture the records passed to bulk_insert
        inserted_records = []

        def capture_bulk_insert(table_name, records):
            inserted_records.extend(records)
            return len(records)

        mock_db.bulk_insert = Mock(side_effect=capture_bulk_insert)
        mock_get_database.return_value = mock_db

        loader = DataLoader()
        time_mapping = {2020: 1, 2021: 2}

        loader.load_ict_data(sample_transformed_ict_data, time_mapping=time_mapping)

        # Verify records structure
        assert len(inserted_records) > 0
        first_record = inserted_records[0]

        # Check required fields
        assert 'geo_id' in first_record
        assert 'time_id' in first_record
        assert 'indicator_id' in first_record
        assert 'value' in first_record
        assert 'loaded_at' in first_record
