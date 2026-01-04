"""
Federal Employment Agency (BA) Extractors
Regional Economics Database for NRW

Extractors for employment and wage data from the German Federal Employment Agency.
"""

from pathlib import Path
from .base_extractor import BAExtractor
from .employment_wage_extractor import EmploymentWageExtractor

__all__ = ['BAExtractor', 'EmploymentWageExtractor']
