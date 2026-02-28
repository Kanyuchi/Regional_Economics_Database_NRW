-- ================================================================================
-- QUICK ICT INDICATORS VERIFICATION
-- ================================================================================
-- Run this script for a fast overview of ICT data loading success
-- ================================================================================

\echo ''
\echo '================================================================================'
\echo 'ICT INDICATORS DATA VERIFICATION - QUICK CHECK'
\echo '================================================================================'
\echo ''

-- Quick Status Check
\echo '1. OVERALL STATUS'
\echo '----------------'
SELECT
    '✅ ICT Indicators Metadata' as check_category,
    COUNT(*) as count
FROM dim_indicator
WHERE source_table_id = '52911-01i'

UNION ALL

SELECT
    '✅ ICT Fact Records' as check_category,
    COUNT(*) as count
FROM fact_ict_indicators
WHERE indicator_id >= 104

UNION ALL

SELECT
    '✅ Years Covered' as check_category,
    COUNT(DISTINCT t.year) as count
FROM fact_ict_indicators f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id >= 104

UNION ALL

SELECT
    '✅ Regions Covered' as check_category,
    COUNT(DISTINCT g.region_name) as count
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id >= 104;

\echo ''
\echo '2. DATA QUALITY SUMMARY'
\echo '-----------------------'
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    MIN(value) as min_value,
    MAX(value) as max_value,
    ROUND(AVG(value)::numeric, 2) as avg_value
FROM fact_ict_indicators
WHERE indicator_id >= 104;

\echo ''
\echo '3. SAMPLE DATA (First 5 Records)'
\echo '---------------------------------'
SELECT
    g.region_name,
    t.year,
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY i.indicator_id
LIMIT 5;

\echo ''
\echo '4. TOP 5 ICT INDICATORS (Highest Values)'
\echo '-----------------------------------------'
SELECT
    i.indicator_name,
    f.value || '%' as percentage
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY f.value DESC
LIMIT 5;

\echo ''
\echo '5. DATA QUALITY CHECK'
\echo '---------------------'
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN '✅ No duplicate records found'
        ELSE '⚠️ ' || COUNT(*) || ' duplicate records found'
    END as duplicate_check
FROM (
    SELECT geo_id, time_id, indicator_id, COUNT(*) as cnt
    FROM fact_ict_indicators
    WHERE indicator_id >= 104
    GROUP BY geo_id, time_id, indicator_id
    HAVING COUNT(*) > 1
) duplicates;

\echo ''
\echo '================================================================================'
\echo 'Verification Complete!'
\echo 'For detailed analysis, run: psql -d regional_db -f verify_ict_indicators.sql'
\echo '================================================================================'
