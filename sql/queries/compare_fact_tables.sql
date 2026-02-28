-- ================================================================================
-- COMPARE FACT TABLES WITH DIMENSIONAL STRUCTURE
-- ================================================================================
-- This query compares all fact tables that follow the standard dimensional model
-- (geo_id, time_id, indicator_id, value)
-- ================================================================================

SELECT
    'fact_demographics' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_demographics

UNION ALL

SELECT
    'fact_ict_indicators' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_ict_indicators

UNION ALL

SELECT
    'fact_labor_market' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_labor_market

UNION ALL

SELECT
    'fact_infrastructure' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_infrastructure

UNION ALL

SELECT
    'fact_healthcare' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_healthcare

UNION ALL

SELECT
    'fact_business_economy' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_business_economy

UNION ALL

SELECT
    'fact_public_finance' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_public_finance

ORDER BY total_records DESC;


-- ================================================================================
-- ALTERNATIVE: Simple record count for ALL fact tables
-- ================================================================================

\echo ''
\echo 'Simple Record Counts (All Fact Tables):'
\echo '----------------------------------------'

SELECT 'fact_demographics' as table_name, COUNT(*) as records FROM fact_demographics
UNION ALL
SELECT 'fact_ict_indicators', COUNT(*) FROM fact_ict_indicators
UNION ALL
SELECT 'fact_labor_market', COUNT(*) FROM fact_labor_market
UNION ALL
SELECT 'fact_infrastructure', COUNT(*) FROM fact_infrastructure
UNION ALL
SELECT 'fact_healthcare', COUNT(*) FROM fact_healthcare
UNION ALL
SELECT 'fact_business_economy', COUNT(*) FROM fact_business_economy
UNION ALL
SELECT 'fact_public_finance', COUNT(*) FROM fact_public_finance
UNION ALL
SELECT 'fact_commuters', COUNT(*) FROM fact_commuters
ORDER BY records DESC;
