"""
Base Extractor for Regional Database Germany (Regionalstatistik)
Regional Economics Database for NRW

Handles API interactions with the GENESIS web services.
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
from .job_cache import JobCache


logger = get_logger(__name__)


class RegionalDBExtractor:
    """Base extractor for Regional Database Germany API."""

    def __init__(self):
        """Initialize the Regional Database extractor."""
        self.config = get_config()
        self.source_config = self.config.get_source_config('regional_db')

        # API configuration
        self.base_url = self.source_config['base_url']
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

        logger.info("Regional Database extractor initialized")

    def _create_session(self) -> requests.Session:
        """
        Create requests session with retry logic.

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
            endpoint: API endpoint (relative to base_url)
            method: HTTP method (GET or POST)
            params: Query parameters
            data: Request body data

        Returns:
            Response object or None if request failed
        """
        # Apply rate limiting
        self._rate_limit_wait()

        # Build full URL
        url = f"{self.base_url}{endpoint}"

        # Add authentication
        if params is None:
            params = {}

        try:
            logger.debug(f"Making {method} request to {endpoint}")

            # For Regional Database API, credentials go in custom headers
            headers = {
                'accept': 'application/json; charset=UTF-8',
                'username': self.username,
                'password': self.password,
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            # Debug logging
            logger.info(f"Request URL: {url}")
            logger.info(f"Request method: {method}")
            logger.info(f"Username: '{self.username}' (len={len(self.username)})")
            logger.info(f"Password: '{self.password[:5]}...' (len={len(self.password)})")
            if data:
                logger.info(f"Request data keys: {list(data.keys())}")
                logger.info(f"Request data sample: name={data.get('name')}, area={data.get('area')}, format={data.get('format')}, startyear='{data.get('startyear')}', endyear='{data.get('endyear')}'")

            if method.upper() == 'GET':
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
            else:
                # Use the exact pattern from working test_api_direct.py
                response = requests.post(
                    url,
                    headers=headers,
                    data=data if data else params,
                    timeout=self.timeout
                )

            logger.info(f"Response status: {response.status_code}")
            response.raise_for_status()

            # CRITICAL: Explicitly set encoding to UTF-8 to handle German special characters
            # The GENESIS API returns UTF-8 data but doesn't always declare it correctly in headers
            response.encoding = 'utf-8'

            logger.debug(f"Request successful: {response.status_code}")

            return response

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {endpoint}: {e}")
            return None

    def get_table_metadata(self, table_id: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific table.

        Args:
            table_id: Table identifier (e.g., '12411-03-03-4')

        Returns:
            Dictionary containing table metadata or None
        """
        logger.info(f"Fetching metadata for table: {table_id}")

        response = self._make_request(
            'metadata/table',
            params={'name': table_id, 'format': 'json'}
        )

        if response is None:
            return None

        try:
            metadata = response.json()
            logger.info(f"Metadata retrieved for {table_id}")
            return metadata
        except Exception as e:
            logger.error(f"Error parsing metadata: {e}")
            return None

    def get_table_data(
        self,
        table_id: str,
        format: str = 'datencsv',
        area: str = 'free',
        **filters
    ) -> Optional[str]:
        """
        Download table data from Regional Database.
        Uses async job processing - submits job and retrieves result.
        
        IMPORTANT: This method checks for cached jobs first to avoid
        creating duplicate API requests. Always uses cached job if available.

        Args:
            table_id: Table identifier
            format: Data format ('datencsv' for data CSV)
            area: Geographic area ('free' for all, or specific region code)
            **filters: Additional filter parameters (startyear, endyear, etc.)

        Returns:
            Raw data as string or None
        """
        # Prepare year range for cache key
        default_start_year = filters.get('startyear', '2011')
        default_end_year = filters.get('endyear', '2024')
        year_range = f"{default_start_year}-{default_end_year}"
        
        # CHECK FOR CACHED JOB FIRST - avoid creating duplicate API requests
        cached_job = JobCache.get_job(table_id, year_range)
        if cached_job:
            logger.info(f"Found cached job: {cached_job}")
            logger.info("Attempting to retrieve data from cached job...")
            
            result = self._retrieve_job_result(cached_job)
            if result:
                logger.info(f"Successfully retrieved data from cached job!")
                JobCache.mark_retrieved(table_id, year_range)
                return result
            else:
                logger.warning(f"Cached job {cached_job} retrieval failed")
                # Don't create new job - let user decide
                logger.error("Please check the GENESIS portal for job status")
                logger.error("If job is 'Ready', try running the retrieval again")
                logger.error("If job failed, clear the cache and retry: JobCache.clear_job()")
                return None
        
        logger.info(f"No cached job found for {table_id} ({year_range})")
        logger.info(f"Creating new job request for table: {table_id}")

        # Prepare form data with all required fields
        data = {
            'name': table_id,
            'area': area,
            'compress': 'false',
            'transpose': 'false',
            'format': format,
            'job': 'true',  # Async processing
            'stand': '01.01.2024 01:00',  # Fixed past date - API rejects future timestamps
            'language': 'en',
            'contents': '',
            'startyear': default_start_year,  # Use wide range, filter in transformer
            'endyear': default_end_year,      # Use wide range, filter in transformer
            'timeslices': filters.get('timeslices', ''),
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

        # Submit async job
        response = self._make_request('data/table', method='POST', data=data)

        if response is None:
            return None

        # Parse job response
        try:
            result = json.loads(response.text)
            status_code = result.get('Status', {}).get('Code')

            if status_code == 99:
                # Async job created - CACHE IT immediately
                job_name = result.get('Status', {}).get('Content', '').split(': ')[-1]
                logger.info(f"Async job created: {job_name}")
                
                # Save to cache BEFORE attempting retrieval
                JobCache.save_job(table_id, job_name, year_range)

                # Wait and retrieve result
                csv_data = self._retrieve_job_result(job_name)
                if csv_data:
                    JobCache.mark_retrieved(table_id, year_range)
                return csv_data
                
            elif status_code == 0:
                # Direct response - extract CSV data from Object.Content
                logger.info("Direct response received")
                csv_data = result.get('Object', {}).get('Content')
                if csv_data:
                    logger.info(f"Extracted {len(csv_data):,} characters of CSV data from direct response")
                    return csv_data
                else:
                    logger.warning("Direct response has no Object.Content, trying raw text")
                    return response.text
            else:
                logger.error(f"Unexpected status code: {status_code}")
                logger.error(f"Response: {result.get('Status', {}).get('Content')}")
                return None

        except json.JSONDecodeError:
            logger.error(f"Failed to parse JSON response: {response.text[:500]}")
            return None

    def _retrieve_job_result(self, job_name: str, max_attempts: int = 5, wait_time: int = 5) -> Optional[str]:
        """
        Retrieve result from async job.
        
        For jobs that are already "Ready" on the GENESIS portal, this should
        succeed on the first attempt. We use minimal retries since the job
        is typically already processed.

        Args:
            job_name: Job identifier (e.g., '12411-03-03-4_923273039')
            max_attempts: Maximum number of polling attempts (reduced - jobs are usually ready)
            wait_time: Seconds to wait between attempts

        Returns:
            CSV data from job or None
        """
        logger.info(f"Retrieving result for job: {job_name}")

        for attempt in range(max_attempts):
            # First attempt immediately, then wait between retries
            if attempt > 0:
                logger.info(f"Waiting {wait_time}s before retry...")
                time.sleep(wait_time)

            # Use data/result endpoint with job name
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

                logger.info(f"Job status: code={status_code}, message='{status_message[:100]}...' (attempt {attempt + 1}/{max_attempts})")

                if status_code == 0:
                    # Job complete - extract CSV data from Object.Content
                    csv_data = result.get('Object', {}).get('Content')
                    if csv_data:
                        logger.info(f"SUCCESS! Retrieved {len(csv_data):,} bytes after {attempt + 1} attempt(s)")
                        return csv_data
                    else:
                        # Check if Object is a list or different structure
                        obj = result.get('Object')
                        logger.error(f"Job complete but no content. Object type: {type(obj)}")
                        logger.error(f"Object keys: {obj.keys() if isinstance(obj, dict) else 'N/A'}")
                        return None

                elif status_code in [98, 104]:
                    # Job still processing (98) or result not available yet (104)
                    logger.info(f"Job still processing (code {status_code})")
                    continue
                    
                elif status_code == 22:
                    # Job not found or expired
                    logger.error(f"Job not found or expired: {status_message}")
                    return None
                    
                else:
                    logger.warning(f"Unexpected status code {status_code}: {status_message}")
                    # Continue trying for unexpected codes
                    continue

            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse job result: {e}")
                logger.error(f"Response preview: {response.text[:500]}")
                return None

        logger.error(f"Job did not complete after {max_attempts} attempts")
        logger.info("Tip: Job may still be processing. Check GENESIS portal and try again later.")
        return None

    def list_tables(self, search: Optional[str] = None) -> Optional[List[Dict[str, Any]]]:
        """
        List available tables from Regional Database.

        Args:
            search: Optional search term

        Returns:
            List of table information dictionaries or None
        """
        logger.info(f"Listing tables (search: {search})")

        params = {'format': 'json'}
        if search:
            params['selection'] = search

        response = self._make_request('catalogue/tables', params=params)

        if response is None:
            return None

        try:
            tables = response.json()
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
