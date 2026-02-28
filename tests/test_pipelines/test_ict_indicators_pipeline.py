"""
Tests for ICT Indicators Pipeline
Regional Economics Database for NRW

Integration tests for the complete ICT indicators ETL pipeline.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'src'))

from pipelines.ict_indicators_pipeline import run_ict_indicators_etl


class TestICTIndicatorsPipeline:
    """Test suite for ICT Indicators ETL Pipeline."""

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsTransformer')
    @patch('pipelines.ict_indicators_pipeline.DataLoader')
    def test_pipeline_success_with_load(
        self,
        mock_loader_class,
        mock_transformer_class,
        mock_extractor_class,
        sample_raw_ict_data,
        sample_transformed_ict_data
    ):
        """Test successful pipeline execution with database loading."""
        # Setup mocks
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=sample_raw_ict_data)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        mock_transformer = Mock()
        mock_transformer.transform_ict_data = Mock(return_value=sample_transformed_ict_data)
        mock_transformer.validate_data = Mock(return_value=True)
        mock_transformer.get_indicator_metadata = Mock(return_value=[])
        mock_transformer_class.return_value = mock_transformer

        mock_loader = Mock()
        mock_loader.load_indicator_metadata = Mock(return_value=0)
        mock_loader.load_ict_data = Mock(return_value=len(sample_transformed_ict_data))
        mock_loader.close = Mock()
        mock_loader_class.return_value = mock_loader

        # Run pipeline
        success = run_ict_indicators_etl(
            startyear=2020,
            endyear=2021,
            indicator_id_base=50,
            load_to_db=True
        )

        # Verify success
        assert success is True

        # Verify all components were called
        mock_extractor.extract_ict_data.assert_called_once()
        mock_transformer.transform_ict_data.assert_called_once()
        mock_transformer.validate_data.assert_called_once()
        mock_loader.load_ict_data.assert_called_once()

        # Verify cleanup
        mock_extractor.close.assert_called_once()
        mock_loader.close.assert_called_once()

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsTransformer')
    def test_pipeline_success_without_load(
        self,
        mock_transformer_class,
        mock_extractor_class,
        sample_raw_ict_data,
        sample_transformed_ict_data
    ):
        """Test successful pipeline execution without database loading."""
        # Setup mocks
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=sample_raw_ict_data)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        mock_transformer = Mock()
        mock_transformer.transform_ict_data = Mock(return_value=sample_transformed_ict_data)
        mock_transformer.validate_data = Mock(return_value=True)
        mock_transformer_class.return_value = mock_transformer

        # Run pipeline without loading
        success = run_ict_indicators_etl(
            startyear=2020,
            endyear=2021,
            indicator_id_base=50,
            load_to_db=False
        )

        # Verify success
        assert success is True

        # Verify extraction and transformation were called
        mock_extractor.extract_ict_data.assert_called_once()
        mock_transformer.transform_ict_data.assert_called_once()

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    def test_pipeline_failure_no_extraction(self, mock_extractor_class):
        """Test pipeline failure when extraction returns no data."""
        # Setup mock to return None
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=None)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        # Run pipeline
        success = run_ict_indicators_etl(startyear=2020, endyear=2021)

        # Verify failure
        assert success is False

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsTransformer')
    def test_pipeline_failure_no_transformation(
        self,
        mock_transformer_class,
        mock_extractor_class,
        sample_raw_ict_data
    ):
        """Test pipeline failure when transformation returns no data."""
        # Setup mocks
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=sample_raw_ict_data)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        mock_transformer = Mock()
        mock_transformer.transform_ict_data = Mock(return_value=None)
        mock_transformer_class.return_value = mock_transformer

        # Run pipeline
        success = run_ict_indicators_etl(startyear=2020, endyear=2021)

        # Verify failure
        assert success is False

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsTransformer')
    def test_pipeline_failure_validation(
        self,
        mock_transformer_class,
        mock_extractor_class,
        sample_raw_ict_data,
        sample_transformed_ict_data
    ):
        """Test pipeline failure when validation fails."""
        # Setup mocks
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=sample_raw_ict_data)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        mock_transformer = Mock()
        mock_transformer.transform_ict_data = Mock(return_value=sample_transformed_ict_data)
        mock_transformer.validate_data = Mock(return_value=False)  # Validation fails
        mock_transformer_class.return_value = mock_transformer

        # Run pipeline
        success = run_ict_indicators_etl(startyear=2020, endyear=2021)

        # Verify failure
        assert success is False

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsTransformer')
    @patch('pipelines.ict_indicators_pipeline.DataLoader')
    def test_pipeline_failure_no_records_loaded(
        self,
        mock_loader_class,
        mock_transformer_class,
        mock_extractor_class,
        sample_raw_ict_data,
        sample_transformed_ict_data
    ):
        """Test pipeline failure when no records are loaded."""
        # Setup mocks
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(return_value=sample_raw_ict_data)
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor.close = Mock()
        mock_extractor_class.return_value = mock_extractor

        mock_transformer = Mock()
        mock_transformer.transform_ict_data = Mock(return_value=sample_transformed_ict_data)
        mock_transformer.validate_data = Mock(return_value=True)
        mock_transformer.get_indicator_metadata = Mock(return_value=[])
        mock_transformer_class.return_value = mock_transformer

        mock_loader = Mock()
        mock_loader.load_indicator_metadata = Mock(return_value=0)
        mock_loader.load_ict_data = Mock(return_value=0)  # No records loaded
        mock_loader.close = Mock()
        mock_loader_class.return_value = mock_loader

        # Run pipeline
        success = run_ict_indicators_etl(
            startyear=2020,
            endyear=2021,
            load_to_db=True
        )

        # Verify failure
        assert success is False

    @patch('pipelines.ict_indicators_pipeline.ICTIndicatorsExtractor')
    def test_pipeline_exception_handling(self, mock_extractor_class):
        """Test that pipeline handles exceptions gracefully."""
        # Setup mock to raise exception
        mock_extractor = Mock()
        mock_extractor.extract_ict_data = Mock(side_effect=Exception("Test error"))
        mock_extractor.get_table_info = Mock(return_value={
            'table_id': '52911-01i',
            'table_name': 'ICT Indicators',
            'source_name': 'State Database NRW',
            'start_year': 2020,
            'end_year': 2025,
            'geographic_coverage': 'NRW Districts'
        })
        mock_extractor_class.return_value = mock_extractor

        # Run pipeline
        success = run_ict_indicators_etl(startyear=2020, endyear=2021)

        # Verify failure
        assert success is False
