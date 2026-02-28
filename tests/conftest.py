"""
Pytest Configuration and Fixtures
Regional Economics Database for NRW

Shared test fixtures and configuration for all tests.
"""

import pytest
import pandas as pd
from datetime import datetime
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))


@pytest.fixture
def sample_raw_ict_data():
    """
    Create sample raw ICT data as it would come from the API.

    Returns:
        DataFrame with sample raw ICT data
    """
    data = {
        'year': [2020, 2020, 2020, 2021, 2021, 2021],
        'region_code': ['05111', '05112', '05113', '05111', '05112', '05113'],
        'region_name': [
            'Düsseldorf', 'Duisburg', 'Essen',
            'Düsseldorf', 'Duisburg', 'Essen'
        ],
        'Broadband_Coverage': ['95.5', '88.2', '91.3', '97.1', '90.5', '93.8'],
        'ICT_Employment': ['12500', '8300', '10200', '13100', '8700', '10800'],
        'Digital_Infrastructure_Index': ['85.3', '72.1', '78.9', '87.2', '74.5', '81.2']
    }

    return pd.DataFrame(data)


@pytest.fixture
def sample_transformed_ict_data():
    """
    Create sample transformed ICT data in long format.

    Returns:
        DataFrame with sample transformed ICT data
    """
    data = {
        'year': [2020, 2020, 2020, 2021, 2021, 2021] * 2,
        'region_code': ['05111', '05112', '05113', '05111', '05112', '05113'] * 2,
        'region_name': ['Düsseldorf', 'Duisburg', 'Essen'] * 4,
        'indicator_id': [50] * 6 + [51] * 6,
        'indicator_name': ['Broadband availability'] * 6 + ['ICT employment'] * 6,
        'value': [95.5, 88.2, 91.3, 97.1, 90.5, 93.8, 12500, 8300, 10200, 13100, 8700, 10800],
        'notes': ['ICT Indicator: Broadband availability'] * 6 + ['ICT Indicator: ICT employment'] * 6,
        'extracted_at': [datetime.now()] * 12
    }

    return pd.DataFrame(data)


@pytest.fixture
def sample_geo_mapping():
    """
    Create sample geography mapping.

    Returns:
        Dict mapping region codes to geo_ids
    """
    return {
        '05111': 1,  # Düsseldorf
        '05112': 2,  # Duisburg
        '05113': 3,  # Essen
        '05114': 4,  # Krefeld
        '05115': 5,  # Mönchengladbach
    }


@pytest.fixture
def sample_time_mapping():
    """
    Create sample time dimension mapping.

    Returns:
        Dict mapping years to time_ids
    """
    return {
        2019: 1,
        2020: 2,
        2021: 3,
        2022: 4,
        2023: 5,
        2024: 6,
        2025: 7
    }


@pytest.fixture
def mock_csv_response():
    """
    Create mock CSV response as it would come from the State Database API.

    Returns:
        String with CSV data including headers
    """
    csv_data = """Table: 52911-01i
ICT Indicators
By districts and independent cities
NRW, Germany

Metadata line 5
Metadata line 6
ICT Indicators;ICT Indicators;ICT Indicators;Broadband;Broadband;Employment;Employment
Description;Description;Description;Coverage;Speed;ICT Jobs;Tech Jobs
Units;Units;Units;Percent;Mbps;Number;Number
2020;05111;Düsseldorf;95,5;250;12500;15000
2020;05112;Duisburg;88,2;150;8300;9500
2020;05113;Essen;91,3;200;10200;12000
"""
    return csv_data


@pytest.fixture
def mock_api_response_success():
    """
    Create mock successful API response.

    Returns:
        Dict representing API JSON response
    """
    return {
        "Ident": {
            "Service": "data",
            "Method": "table"
        },
        "Status": {
            "Code": 0,
            "Content": "Data retrieved successfully",
            "Type": "Success"
        },
        "Object": {
            "Content": "mocked_csv_data_here"
        },
        "Copyright": "© Federal Statistical Office, Wiesbaden 2025"
    }


@pytest.fixture
def mock_api_response_job():
    """
    Create mock API response for async job submission.

    Returns:
        Dict representing API JSON response for job creation
    """
    return {
        "Ident": {
            "Service": "data",
            "Method": "table"
        },
        "Status": {
            "Code": 99,
            "Content": "The table will be generated through batch processing. The table can be viewed as result with the following name soon: 52911-01i_123456789",
            "Type": "Information"
        },
        "Parameter": {
            "name": "52911-01i",
            "job": "true"
        },
        "Copyright": "© Federal Statistical Office, Wiesbaden 2025"
    }


@pytest.fixture
def temp_output_dir(tmp_path):
    """
    Create temporary output directory for test files.

    Args:
        tmp_path: Pytest tmp_path fixture

    Returns:
        Path to temporary directory
    """
    output_dir = tmp_path / "test_output"
    output_dir.mkdir()
    return output_dir
