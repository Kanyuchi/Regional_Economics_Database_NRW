"""
Job Cache for State Database NRW API
Prevents duplicate job requests by caching job IDs.

Regional Economics Database for NRW
"""

import json
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.logging import get_logger

logger = get_logger(__name__)


class StateDBJobCache:
    """
    Cache for State Database NRW API job IDs.
    
    The GENESIS API creates async jobs for large data requests.
    This cache stores job IDs so we can retrieve existing jobs
    instead of creating new ones on every ETL run.
    
    Stored separately from Regional DB cache in state_db_job_cache.json.
    """
    
    CACHE_FILE = Path(__file__).parent.parent.parent.parent / "data" / "reference" / "state_db_job_cache.json"
    
    @classmethod
    def load(cls) -> Dict[str, Any]:
        """
        Load existing job cache from file.
        
        Returns:
            Dictionary with job cache data
        """
        try:
            if cls.CACHE_FILE.exists():
                with open(cls.CACHE_FILE, 'r', encoding='utf-8') as f:
                    cache = json.load(f)
                    logger.debug(f"Loaded State DB job cache with {len(cache.get('jobs', {}))} entries")
                    return cache
        except Exception as e:
            logger.warning(f"Could not load State DB job cache: {e}")
        
        return {
            "source": "state_db",
            "description": "State Database NRW (Landesdatenbank) job cache",
            "jobs": {}, 
            "created_at": datetime.now().isoformat()
        }
    
    @classmethod
    def save(cls, cache: Dict[str, Any]) -> None:
        """
        Save job cache to file.
        
        Args:
            cache: Cache dictionary to save
        """
        try:
            cls.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            cache["updated_at"] = datetime.now().isoformat()
            with open(cls.CACHE_FILE, 'w', encoding='utf-8') as f:
                json.dump(cache, f, indent=2)
            logger.debug(f"Saved State DB job cache to {cls.CACHE_FILE}")
        except Exception as e:
            logger.error(f"Could not save State DB job cache: {e}")
    
    @classmethod
    def get_job(cls, table_id: str, year_range: Optional[str] = None) -> Optional[str]:
        """
        Get cached job ID for a table.
        
        Args:
            table_id: Table identifier (e.g., '71517-01i')
            year_range: Optional year range key (e.g., '2009-2024')
        
        Returns:
            Job ID if found and not expired, None otherwise
        """
        cache = cls.load()
        cache_key = f"{table_id}_{year_range}" if year_range else table_id
        
        job_info = cache.get("jobs", {}).get(cache_key)
        if job_info:
            job_id = job_info.get("job_id")
            status = job_info.get("status", "unknown")
            
            # Don't return jobs that were already successfully loaded
            if status == "data_loaded":
                logger.info(f"Job {job_id} already loaded to database, skipping")
                return None
            
            logger.info(f"Found cached State DB job: {job_id} (status: {status})")
            return job_id
        
        return None
    
    @classmethod
    def save_job(cls, table_id: str, job_id: str, year_range: Optional[str] = None) -> None:
        """
        Save a job ID to cache.
        
        Args:
            table_id: Table identifier
            job_id: Job ID from API response
            year_range: Optional year range key
        """
        cache = cls.load()
        cache_key = f"{table_id}_{year_range}" if year_range else table_id
        
        cache["jobs"][cache_key] = {
            "job_id": job_id,
            "table_id": table_id,
            "year_range": year_range,
            "created_at": datetime.now().isoformat(),
            "status": "created"
        }
        cls.save(cache)
        logger.info(f"Cached State DB job {job_id} for {cache_key}")
    
    @classmethod
    def update_status(cls, table_id: str, status: str, year_range: Optional[str] = None) -> None:
        """
        Update job status in cache.
        
        Args:
            table_id: Table identifier
            status: New status ('created', 'ready', 'retrieved', 'data_loaded', 'failed')
            year_range: Optional year range key
        """
        cache = cls.load()
        cache_key = f"{table_id}_{year_range}" if year_range else table_id
        
        if cache_key in cache.get("jobs", {}):
            cache["jobs"][cache_key]["status"] = status
            cache["jobs"][cache_key]["status_updated_at"] = datetime.now().isoformat()
            cls.save(cache)
            logger.debug(f"Updated State DB job status for {cache_key}: {status}")
    
    @classmethod
    def mark_retrieved(cls, table_id: str, year_range: Optional[str] = None) -> None:
        """Mark a job as successfully retrieved (CSV data downloaded)."""
        cls.update_status(table_id, "retrieved", year_range)
    
    @classmethod
    def mark_loaded(cls, table_id: str, year_range: Optional[str] = None) -> None:
        """Mark a job as successfully loaded to database."""
        cls.update_status(table_id, "data_loaded", year_range)
    
    @classmethod
    def mark_failed(cls, table_id: str, year_range: Optional[str] = None) -> None:
        """Mark a job as failed."""
        cls.update_status(table_id, "failed", year_range)
    
    @classmethod
    def clear_job(cls, table_id: str, year_range: Optional[str] = None) -> None:
        """
        Remove a job from cache.
        
        Args:
            table_id: Table identifier
            year_range: Optional year range key
        """
        cache = cls.load()
        cache_key = f"{table_id}_{year_range}" if year_range else table_id
        
        if cache_key in cache.get("jobs", {}):
            del cache["jobs"][cache_key]
            cls.save(cache)
            logger.info(f"Cleared State DB job cache for {cache_key}")
    
    @classmethod
    def list_jobs(cls) -> Dict[str, Any]:
        """
        List all cached jobs.
        
        Returns:
            Dictionary of all cached jobs
        """
        cache = cls.load()
        return cache.get("jobs", {})
    
    @classmethod
    def add_existing_job(cls, table_id: str, job_id: str, year_range: Optional[str] = None, 
                         status: str = "ready") -> None:
        """
        Manually add an existing job ID to cache.
        
        Use this for jobs that were created before caching was implemented,
        or for jobs created via manual API calls.
        
        Args:
            table_id: Table identifier (e.g., '71517-01i')
            job_id: Full job ID from API (e.g., '71517-01i_149084252')
            year_range: Optional year range key (e.g., '2009-2024')
            status: Initial status (default 'ready')
        """
        cache = cls.load()
        cache_key = f"{table_id}_{year_range}" if year_range else table_id
        
        cache["jobs"][cache_key] = {
            "job_id": job_id,
            "table_id": table_id,
            "year_range": year_range,
            "created_at": datetime.now().isoformat(),
            "status": status,
            "note": "Manually added existing job"
        }
        cls.save(cache)
        logger.info(f"Added existing State DB job {job_id} for {cache_key}")

