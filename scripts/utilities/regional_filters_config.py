#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Regional Filters Configuration
Description: This file contains the regional filter definitions for GDP/GVA analysis.
"""

# Regional filters by region code
REGIONAL_FILTERS_BY_CODE = {
    'nrw': ['00005', '05', '5'],
    'duisburg': ['05112'],
    'metropole_ruhr': ['05112', '05113', '05911', '05914', '05915', '05916'],
    'niederrhein': ['05154', '05166', '05170'],
    'regierungsbezirk_duesseldorf': ['05111', '05112', '05113', '05114', '05116', '05117', '05119', '05120'],
    'regierungsbezirke': ['051', '053', '055', '057', '059']
}

# Mapping of region codes to human-readable names (will be populated from the database)
REGION_CODE_TO_NAME = {}

# Function to get region name from code
def get_region_name(code):
    """
    Get the region name for a given region code
    
    Args:
        code (str): Region code
        
    Returns:
        str: Region name if found, otherwise the code itself
    """
    return REGION_CODE_TO_NAME.get(code, code)

# Function to get filter name in a human-readable format
def get_filter_display_name(filter_key):
    """
    Get a human-readable display name for a filter key
    
    Args:
        filter_key (str): Filter key from REGIONAL_FILTERS_BY_CODE
        
    Returns:
        str: Human-readable display name
    """
    display_names = {
        'nrw': 'North Rhine-Westphalia',
        'duisburg': 'Duisburg',
        'metropole_ruhr': 'Metropole Ruhr',
        'niederrhein': 'Niederrhein',
        'regierungsbezirk_duesseldorf': 'Regierungsbezirk Düsseldorf',
        'regierungsbezirke': 'All Regierungsbezirke'
    }
    return display_names.get(filter_key, filter_key.replace('_', ' ').title()) 