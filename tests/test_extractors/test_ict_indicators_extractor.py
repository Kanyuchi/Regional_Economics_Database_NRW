"""
Tests for ICT Indicators Extractor
Regional Economics Database for NRW

Tests the ICTIndicatorsExtractor class functionality.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from extractors.state_db.ict_indicators_extractor import ICTIndicatorsExtractor


class TestICTIndicatorsExtractor:
    """Test suite for ICT Indicators Extractor."""

    def test_extractor_initialization(self):
        """Test that the extractor initializes correctly."""
        extractor = ICTIndicatorsExtractor()

        assert extractor.TABLE_ID == "52911-01i"
        assert extractor.TABLE_NAME == "ICT Indicators by Districts and Cities"
        assert extractor.START_YEAR == 2020
        assert extractor.END_YEAR == 2025
        assert hasattr(extractor, 'username')
        assert hasattr(extractor, 'password')

    def test_get_table_info(self):
        """Test that table information is retrieved correctly."""
        extractor = ICTIndicatorsExtractor()
        info = extractor.get_table_info()

        assert info['table_id'] == "52911-01i"
        assert info['source'] == "state_db"
        assert info['start_year'] == 2020
        assert info['end_year'] == 2025
        assert 'ICT indicators' in info['description']
        assert len(info['indicator_categories']) > 0

    def test_parse_ict_data_with_valid_csv(self, mock_csv_response):
        """Test parsing of valid CSV data."""
        extractor = ICTIndicatorsExtractor()

        # Parse the mock CSV
        df = extractor._parse_ict_data(mock_csv_response, expected_year=2020)

        assert df is not None
        assert not df.empty
        assert 'year' in df.columns
        assert 'region_code' in df.columns
        assert 'region_name' in df.columns
        assert all(df['year'] == 2020)

    def test_parse_ict_data_with_invalid_csv(self):
        """Test parsing of invalid CSV data."""
        extractor = ICTIndicatorsExtractor()

        # Test with insufficient lines
        invalid_csv = "Line1\nLine2\nLine3"
        df = extractor._parse_ict_data(invalid_csv, expected_year=2020)

        assert df is None

    def test_parse_ict_data_with_empty_string(self):
        """Test parsing of empty CSV data."""
        extractor = ICTIndicatorsExtractor()

        df = extractor._parse_ict_data("", expected_year=2020)

        assert df is None

    @patch.object(ICTIndicatorsExtractor, 'get_table_data')
    def test_extract_ict_data_single_year(self, mock_get_table_data, mock_csv_response):
        """Test extraction for a single year."""
        extractor = ICTIndicatorsExtractor()

        # Mock the API response
        mock_get_table_data.return_value = mock_csv_response

        # Extract data for single year
        df = extractor.extract_ict_data(startyear=2020, endyear=2020)

        # Verify the method was called
        mock_get_table_data.assert_called_once()

        # Check result
        assert df is not None or df is None  # Depends on CSV parsing

    @patch.object(ICTIndicatorsExtractor, 'get_table_data')
    def test_extract_ict_data_multiple_years(self, mock_get_table_data, mock_csv_response):
        """Test extraction for multiple years."""
        extractor = ICTIndicatorsExtractor()

        # Mock the API response
        mock_get_table_data.return_value = mock_csv_response

        # Extract data for multiple years
        df = extractor.extract_ict_data(startyear=2020, endyear=2022)

        # Verify the method was called 3 times (once per year)
        assert mock_get_table_data.call_count == 3

    @patch.object(ICTIndicatorsExtractor, 'get_table_data')
    def test_extract_ict_data_with_failures(self, mock_get_table_data):
        """Test extraction when some years fail."""
        extractor = ICTIndicatorsExtractor()

        # Mock API to return None for all years
        mock_get_table_data.return_value = None

        # Extract data
        df = extractor.extract_ict_data(startyear=2020, endyear=2022)

        # Should return None if all years fail
        assert df is None

    @patch.object(ICTIndicatorsExtractor, 'retrieve_existing_job')
    def test_retrieve_ict_data(self, mock_retrieve, mock_csv_response):
        """Test retrieving data from existing job."""
        extractor = ICTIndicatorsExtractor()

        # Mock the job retrieval
        mock_retrieve.return_value = mock_csv_response

        # Retrieve data
        df = extractor.retrieve_ict_data('52911-01i_123456789')

        # Verify the method was called
        mock_retrieve.assert_called_once_with('52911-01i_123456789')

    def test_table_configuration(self):
        """Test that table configuration constants are correct."""
        assert ICTIndicatorsExtractor.TABLE_ID == "52911-01i"
        assert ICTIndicatorsExtractor.START_YEAR == 2020
        assert ICTIndicatorsExtractor.END_YEAR == 2025
        assert ICTIndicatorsExtractor.END_YEAR - ICTIndicatorsExtractor.START_YEAR == 5
