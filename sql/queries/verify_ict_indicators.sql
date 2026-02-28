-- ================================================================================
-- ICT INDICATORS DATA VERIFICATION QUERIES
-- ================================================================================
-- Table: 52911-01i - ICT Indicators for Nordrhein-Westfalen
-- Created: 2026-01-10
-- Purpose: Verify successful loading of ICT indicators data into PostgreSQL
-- ================================================================================

-- =============================================================================
-- 1. METADATA VERIFICATION (dim_indicator table)
-- =============================================================================

-- 1.1 Count ICT indicators
SELECT
    'Total ICT Indicators' as metric,
    COUNT(*) as count
FROM dim_indicator
WHERE source_table_id = '52911-01i';

-- 1.2 View all ICT indicator metadata
SELECT
    indicator_id,
    indicator_code,
    indicator_name,
    indicator_category,
    source_table_id,
    source_system,
    unit_of_measure,
    created_at
FROM dim_indicator
WHERE source_table_id = '52911-01i'
ORDER BY indicator_id;

-- 1.3 Verify indicator ID range
SELECT
    MIN(indicator_id) as min_id,
    MAX(indicator_id) as max_id,
    COUNT(*) as total_indicators
FROM dim_indicator
WHERE source_table_id = '52911-01i';

-- 1.4 Check for any missing required fields
SELECT
    indicator_id,
    indicator_name,
    CASE WHEN indicator_category IS NULL THEN 'MISSING' ELSE indicator_category END as category,
    CASE WHEN source_system IS NULL THEN 'MISSING' ELSE source_system END as source,
    CASE WHEN unit_of_measure IS NULL THEN 'MISSING' ELSE unit_of_measure END as unit
FROM dim_indicator
WHERE source_table_id = '52911-01i'
    AND (indicator_category IS NULL
         OR source_system IS NULL
         OR unit_of_measure IS NULL);


-- =============================================================================
-- 2. FACT DATA VERIFICATION (fact_ict_indicators table)
-- =============================================================================

-- 2.1 Count total records
SELECT
    'Total ICT Fact Records' as metric,
    COUNT(*) as count
FROM fact_ict_indicators
WHERE indicator_id >= 104;

-- 2.2 Summary statistics
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions,
    COUNT(DISTINCT time_id) as unique_time_periods,
    MIN(value) as min_value,
    MAX(value) as max_value,
    ROUND(AVG(value)::numeric, 2) as avg_value,
    MIN(loaded_at) as first_loaded,
    MAX(loaded_at) as last_loaded
FROM fact_ict_indicators
WHERE indicator_id >= 104;

-- 2.3 Check for NULL values in critical columns
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN geo_id IS NULL THEN 1 ELSE 0 END) as null_geo_id,
    SUM(CASE WHEN time_id IS NULL THEN 1 ELSE 0 END) as null_time_id,
    SUM(CASE WHEN indicator_id IS NULL THEN 1 ELSE 0 END) as null_indicator_id,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_value,
    SUM(CASE WHEN unit IS NULL THEN 1 ELSE 0 END) as null_unit
FROM fact_ict_indicators
WHERE indicator_id >= 104;

-- 2.4 View sample records with all dimension data (first 10)
SELECT
    f.fact_id,
    g.region_code,
    g.region_name,
    t.year,
    i.indicator_id,
    i.indicator_code,
    i.indicator_name,
    f.value,
    f.unit,
    f.data_quality_flag,
    f.loaded_at
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY i.indicator_id, t.year
LIMIT 10;


-- =============================================================================
-- 3. DATA QUALITY CHECKS
-- =============================================================================

-- 3.1 Check for duplicate records (same region, time, indicator)
SELECT
    geo_id,
    time_id,
    indicator_id,
    COUNT(*) as duplicate_count
FROM fact_ict_indicators
WHERE indicator_id >= 104
GROUP BY geo_id, time_id, indicator_id
HAVING COUNT(*) > 1;

-- 3.2 Verify value ranges are reasonable (should be percentages 0-100)
SELECT
    i.indicator_name,
    f.value,
    f.unit,
    CASE
        WHEN f.value < 0 THEN '⚠️ NEGATIVE VALUE'
        WHEN f.value > 100 AND f.unit = 'Prozent' THEN '⚠️ EXCEEDS 100%'
        ELSE '✅ OK'
    END as validation_status
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
    AND (f.value < 0 OR (f.value > 100 AND f.unit = 'Prozent'));

-- 3.3 Check for orphaned references (should return no rows)
-- Check for fact records pointing to non-existent indicators
SELECT
    f.indicator_id,
    COUNT(*) as orphaned_records
FROM fact_ict_indicators f
LEFT JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id >= 104
    AND i.indicator_id IS NULL
GROUP BY f.indicator_id;

-- 3.4 Verify all indicators have corresponding fact data
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name,
    COUNT(f.fact_id) as fact_record_count
FROM dim_indicator i
LEFT JOIN fact_ict_indicators f ON i.indicator_id = f.indicator_id
WHERE i.source_table_id = '52911-01i'
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name
HAVING COUNT(f.fact_id) = 0;


-- =============================================================================
-- 4. COMPREHENSIVE DATA VIEW
-- =============================================================================

-- 4.1 Full ICT indicators dataset with all context
SELECT
    g.region_code,
    g.region_name,
    g.region_type,
    t.year,
    i.indicator_id,
    i.indicator_code,
    i.indicator_name,
    f.value,
    f.unit,
    i.indicator_category,
    i.source_table_id,
    f.data_quality_flag,
    f.notes,
    f.extracted_at,
    f.loaded_at
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY t.year, i.indicator_id;

-- 4.2 ICT indicators grouped by year (for trend analysis)
SELECT
    t.year,
    COUNT(DISTINCT i.indicator_id) as indicator_count,
    COUNT(f.fact_id) as total_records,
    ROUND(MIN(f.value)::numeric, 2) as min_value,
    ROUND(MAX(f.value)::numeric, 2) as max_value,
    ROUND(AVG(f.value)::numeric, 2) as avg_value
FROM fact_ict_indicators f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
GROUP BY t.year
ORDER BY t.year;


-- =============================================================================
-- 5. TOP/BOTTOM ICT INDICATORS (2020 data)
-- =============================================================================

-- 5.1 Top 10 ICT indicators by value (highest adoption/usage)
SELECT
    i.indicator_name,
    f.value,
    f.unit,
    t.year
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE i.source_table_id = '52911-01i'
    AND t.year = 2020
ORDER BY f.value DESC
LIMIT 10;

-- 5.2 Bottom 10 ICT indicators by value (lowest adoption/usage)
SELECT
    i.indicator_name,
    f.value,
    f.unit,
    t.year
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE i.source_table_id = '52911-01i'
    AND t.year = 2020
ORDER BY f.value ASC
LIMIT 10;


-- =============================================================================
-- 6. SPECIFIC INDICATOR ANALYSIS
-- =============================================================================

-- 6.1 Internet access indicators
SELECT
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
    AND (i.indicator_name ILIKE '%internet%'
         OR i.indicator_name ILIKE '%internetverb%')
ORDER BY f.value DESC;

-- 6.2 E-commerce indicators
SELECT
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
    AND (i.indicator_name ILIKE '%verkauf%'
         OR i.indicator_name ILIKE '%bestell%'
         OR i.indicator_name ILIKE '%e-commerce%'
         OR i.indicator_name ILIKE '%online%')
ORDER BY f.value DESC;

-- 6.3 Website and digital presence indicators
SELECT
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
    AND (i.indicator_name ILIKE '%website%'
         OR i.indicator_name ILIKE '%social media%'
         OR i.indicator_name ILIKE '%online-präsenz%')
ORDER BY f.value DESC;


-- =============================================================================
-- 7. EXPORT-READY VIEWS
-- =============================================================================

-- 7.1 Business-friendly view (for reports)
CREATE OR REPLACE VIEW view_ict_indicators_summary AS
SELECT
    t.year,
    g.region_name,
    i.indicator_name,
    f.value || ' ' || f.unit as formatted_value,
    f.value as raw_value,
    i.indicator_category,
    f.data_quality_flag,
    f.loaded_at
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i';

-- 7.2 Test the view
SELECT * FROM view_ict_indicators_summary
ORDER BY year, indicator_name
LIMIT 10;


-- =============================================================================
-- 8. COMPARISON WITH OTHER DATA
-- =============================================================================

-- 8.1 Compare ICT data volume with other indicator categories
SELECT
    i.indicator_category,
    i.source_table_id,
    COUNT(DISTINCT i.indicator_id) as indicator_count,
    COUNT(f.fact_id) as fact_record_count
FROM dim_indicator i
LEFT JOIN fact_ict_indicators f ON i.indicator_id = f.indicator_id
WHERE i.indicator_category IS NOT NULL
GROUP BY i.indicator_category, i.source_table_id
ORDER BY fact_record_count DESC;


-- =============================================================================
-- 9. QUICK STATUS CHECK (Run this first!)
-- =============================================================================

-- Master status query - run this first to get overview
SELECT
    '✅ ICT Indicators Metadata' as check_category,
    COUNT(*) as count,
    'dim_indicator table' as location
FROM dim_indicator
WHERE source_table_id = '52911-01i'

UNION ALL

SELECT
    '✅ ICT Fact Records' as check_category,
    COUNT(*) as count,
    'fact_ict_indicators table' as location
FROM fact_ict_indicators
WHERE indicator_id >= 104

UNION ALL

SELECT
    '✅ Years Covered' as check_category,
    COUNT(DISTINCT t.year) as count,
    'Unique years in data' as location
FROM fact_ict_indicators f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id >= 104

UNION ALL

SELECT
    '✅ Regions Covered' as check_category,
    COUNT(DISTINCT g.region_name) as count,
    'Unique regions in data' as location
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id >= 104;


-- =============================================================================
-- NOTES:
-- =============================================================================
-- - All queries use indicator_id >= 104 to filter ICT indicators
-- - source_table_id = '52911-01i' identifies ICT data in dim_indicator
-- - Data represents STATE-LEVEL aggregates for Nordrhein-Westfalen
-- - Values are percentages (Prozent) unless otherwise noted
-- - Run section 9 first for quick overview
-- =============================================================================
