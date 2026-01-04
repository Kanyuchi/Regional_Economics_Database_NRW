"""
Base Extractor for State Database NRW (Landesdatenbank NRW)
Regional Economics Database for NRW

Handles API interactions with the State Database GENESIS web services.
API structure is similar to Regional Database Germany but uses header-based authentication.

Endpoint: https://www.landesdatenbank.nrw.de/ldbnrwws/rest/2020/
"""

import time
import json
import requests
from typing import Dict, Any, Optional, List
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config import get_config
from utils.logging import get_logger

logger = get_logger(__name__)


class StateDBExtractor:
    """Base extractor for State Database NRW API."""

    # Base URL for State Database NRW REST API
    API_BASE_URL = "https://www.landesdatenbank.nrw.de/ldbnrwws/rest/2020"

    def __init__(self):
        """Initialize the State Database extractor."""
        self.config = get_config()
        self.source_config = self.config.get_source_config('state_db')

        # API configuration - credentials from config
        self.username = self.source_config['username']
        self.password = self.source_config['password']
        self.timeout = self.source_config.get('timeout', 120)
        self.retry_attempts = self.source_config.get('retry_attempts', 3)

        # Rate limiting
        self.rate_limit = self.source_config.get('rate_limit', {})
        self.requests_per_minute = self.rate_limit.get('requests_per_minute', 30)
        self.last_request_time = 0
        self.min_request_interval = 60.0 / self.requests_per_minute

        # Create session with retry logic
        self.session = self._create_session()

        logger.info("State Database NRW extractor initialized")

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry logic.
        
        Note: State DB uses header-based auth, not basic auth.

        Returns:
            Configured requests Session
        """
        session = requests.Session()

        # Configure retry strategy
        retry_strategy = Retry(
            total=self.retry_attempts,
            backoff_factor=2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        return session

    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Get authentication headers for State Database API.
        
        Returns:
            Dictionary with auth headers
        """
        return {
            'username': self.username,
            'password': self.password,
            'accept': 'application/json; charset=UTF-8',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def _rate_limit_wait(self) -> None:
        """Implement rate limiting by waiting between requests."""
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time

        if time_since_last_request < self.min_request_interval:
            wait_time = self.min_request_interval - time_since_last_request
            logger.debug(f"Rate limiting: waiting {wait_time:.2f} seconds")
            time.sleep(wait_time)

        self.last_request_time = time.time()

    def _make_request(
        self,
        endpoint: str,
        method: str = 'GET',
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None
    ) -> Optional[requests.Response]:
        """
        Make API request with rate limiting and error handling.

        Args:
            endpoint: API endpoint (relative to API_BASE_URL)
            method: HTTP method (GET or POST)
            params: Query parameters for GET requests
            data: Form data for POST requests

        Returns:
            Response object or None if error
        """
        self._rate_limit_wait()

        # Construct full URL
        url = f"{self.API_BASE_URL}/{endpoint.lstrip('/')}"
        headers = self._get_auth_headers()

        logger.info(f"Request URL: {url}")
        logger.info(f"Request method: {method}")

        try:
            if method.upper() == 'GET':
                response = self.session.get(
                    url, 
                    params=params, 
                    headers=headers,
                    timeout=self.timeout
                )
            elif method.upper() == 'POST':
                response = self.session.post(
                    url, 
                    data=data, 
                    headers=headers,
                    timeout=self.timeout
                )
            else:
                logger.error(f"Unsupported HTTP method: {method}")
                return None

            logger.info(f"Response status: {response.status_code}")
            return response

        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.timeout}s")
            return None
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None

    def get_table_data(
        self,
        table_id: str,
        format: str = 'datencsv',
        area: str = 'free',
        startyear: Optional[int] = None,
        endyear: Optional[int] = None,
        **filters
    ) -> Optional[str]:
        """
        Get table data from State Database NRW.
        
        Submits an async job request and retrieves the result.
        
        Args:
            table_id: Table identifier (e.g., '71517-01i')
            format: Output format ('datencsv', 'xlsx', etc.)
            area: Data area ('free', 'all', etc.)
            startyear: Start year for data range
            endyear: End year for data range
            **filters: Additional filters
            
        Returns:
            Raw CSV data string or None if error
        """
        logger.info(f"Requesting table {table_id} from State Database NRW")
        
        # Default year range if not specified
        if startyear is None:
            startyear = 2009
        if endyear is None:
            endyear = 2024
        
        # Prepare form data matching API requirements
        data = {
            'name': table_id,
            'area': area,
            'compress': 'false',
            'transpose': 'false',
            'format': format,
            'job': 'true',  # Async processing for large datasets
            'stand': '01.01.2024 01:00',
            'language': 'en',
            'contents': '',
            'startyear': str(startyear),
            'endyear': str(endyear),
            'timeslices': '',
            'regionalvariable': '',
            'regionalkey': filters.get('regionalkey', ''),
            'classifyingvariable1': '',
            'classifyingkey1': '',
            'classifyingvariable2': '',
            'classifyingkey2': '',
            'classifyingvariable3': '',
            'classifyingkey3': '',
            'classifyingvariable4': '',
            'classifyingkey4': '',
            'classifyingvariable5': '',
            'classifyingkey5': ''
        }
        
        # Submit job request
        response = self._make_request('data/table', method='POST', data=data)
        
        if response is None:
            return None
        
        # Parse response
        try:
            result = json.loads(response.text)
            status_code = result.get('Status', {}).get('Code')
            status_content = result.get('Status', {}).get('Content', '')
            
            if status_code == 99:
                # Async job created - extract job name
                job_name = status_content.split(': ')[-1]
                logger.info(f"Async job created: {job_name}")
                
                # Wait and retrieve result
                return self._retrieve_job_result(job_name)
                
            elif status_code == 0:
                # Direct response - extract data
                logger.info("Direct response received")
                csv_data = result.get('Object', {}).get('Content')
                if csv_data:
                    logger.info(f"Extracted {len(csv_data):,} characters of CSV data")
                    return csv_data
                else:
                    logger.warning("Direct response has no Object.Content")
                    return None
            else:
                logger.error(f"Unexpected status code: {status_code}")
                logger.error(f"Response: {status_content}")
                return None
                
        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON response: {response.text[:500]}")
            return None

    def retrieve_existing_job(self, job_name: str) -> Optional[str]:
        """
        Retrieve data from an existing job by job name.
        
        Use this when you already have a job ID from a previous request.
        
        Args:
            job_name: Full job name (e.g., '71517-01i_149084252')
            
        Returns:
            Raw CSV data string or None if error
        """
        logger.info(f"Retrieving existing job: {job_name}")
        return self._retrieve_job_result(job_name, max_attempts=10, wait_time=5)

    def _retrieve_job_result(
        self, 
        job_name: str, 
        max_attempts: int = 10, 
        wait_time: int = 5
    ) -> Optional[str]:
        """
        Retrieve result from async job.
        
        Polls the API until the job is complete or max attempts reached.

        Args:
            job_name: Job identifier (e.g., '71517-01i_149084252')
            max_attempts: Maximum number of polling attempts
            wait_time: Seconds to wait between attempts

        Returns:
            CSV data from job or None
        """
        logger.info(f"Retrieving result for job: {job_name}")

        for attempt in range(max_attempts):
            # Wait between retries (except first attempt)
            if attempt > 0:
                logger.info(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

            # Request job result
            data = {
                'name': job_name,
                'area': 'free',
                'compress': 'false',
                'language': 'en'
            }
            
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Requesting job result...")
            response = self._make_request('data/result', method='POST', data=data)

            if response is None:
                logger.warning(f"No response received (attempt {attempt + 1})")
                continue

            try:
                result = json.loads(response.text)
                status_code = result.get('Status', {}).get('Code')
                status_message = result.get('Status', {}).get('Content', '')

                logger.info(f"Job status: code={status_code}, message='{status_message[:100]}...'")

                if status_code == 0:
                    # Job complete - extract CSV data
                    csv_data = result.get('Object', {}).get('Content')
                    if csv_data:
                        logger.info(f"SUCCESS! Retrieved {len(csv_data):,} bytes after {attempt + 1} attempt(s)")
                        return csv_data
                    else:
                        obj = result.get('Object')
                        logger.error(f"Job complete but no content. Object type: {type(obj)}")
                        return None

                elif status_code in [98, 104]:
                    # Job still processing
                    logger.info(f"Job still processing (code {status_code})")
                    continue
                    
                elif status_code == 22:
                    # Job not found or expired
                    logger.error(f"Job not found or expired: {status_message}")
                    return None
                    
                else:
                    logger.warning(f"Unexpected status code {status_code}: {status_message}")
                    continue

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse job result: {e}")
                logger.error(f"Response preview: {response.text[:500]}")
                return None

        logger.error(f"Job did not complete after {max_attempts} attempts")
        return None

    def list_tables(self, search: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        List available tables from State Database NRW.

        Args:
            search: Optional search term

        Returns:
            List of table information dictionaries or None
        """
        logger.info(f"Listing tables (search: {search})")

        data = {
            'selection': search if search else '*',
            'area': 'free',
            'language': 'en'
        }

        response = self._make_request('catalogue/tables', method='POST', data=data)

        if response is None:
            return None

        try:
            result = json.loads(response.text)
            tables = result.get('List', [])
            logger.info(f"Found {len(tables)} tables")
            return tables
        except Exception as e:
            logger.error(f"Error parsing table list: {e}")
            return None

    def close(self) -> None:
        """Close the session."""
        if self.session:
            self.session.close()
            logger.info("Session closed")
