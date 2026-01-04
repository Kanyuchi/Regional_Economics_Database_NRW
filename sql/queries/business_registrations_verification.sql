/*
===================================================================================
BUSINESS REGISTRATIONS & DEREGISTRATIONS - VERIFICATION QUERIES
===================================================================================
Table: 52311-01-04-4
Indicators: 24 (Registrations), 25 (Deregistrations)
Period: 1998-2024 (27 years)

Run these queries in DBeaver to verify data accessibility and correctness
===================================================================================
*/

-- ===================================================================================
-- 1. BASIC DATA CHECK
-- ===================================================================================
-- Verify records exist for both indicators
SELECT 
    i.indicator_id,
    i.indicator_code,
    i.indicator_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT f.geo_id) as unique_regions,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (24, 25)
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name
ORDER BY i.indicator_id;


-- ===================================================================================
-- 2. RUHR CITIES - QUICK CHECK
-- ===================================================================================
-- Verify all 5 Ruhr cities have data
SELECT 
    g.region_code,
    g.region_name,
    COUNT(*) as total_records,
    MIN(t.year) as first_year,
    MAX(t.year) as last_year
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
GROUP BY g.region_code, g.region_name
ORDER BY g.region_name;


-- ===================================================================================
-- 3. DORTMUND - TIME SERIES (Total Registrations)
-- ===================================================================================
-- See registration trends over time for Dortmund
SELECT 
    t.year,
    f.value as total_registrations,
    ROUND(f.value - LAG(f.value) OVER (ORDER BY t.year), 0) as year_over_year_change,
    ROUND(((f.value - LAG(f.value) OVER (ORDER BY t.year)) / LAG(f.value) OVER (ORDER BY t.year)) * 100, 1) as pct_change
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code = '05913'
  AND f.indicator_id = 24
  AND f.notes = 'Total registrations'
ORDER BY t.year DESC
LIMIT 15;


-- ===================================================================================
-- 4. CATEGORY BREAKDOWN - DORTMUND 2024
-- ===================================================================================
-- See all subcategories for Dortmund in 2024
SELECT 
    i.indicator_name,
    f.notes as category,
    f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE g.region_code = '05913'
  AND f.indicator_id IN (24, 25)
  AND t.year = 2024
ORDER BY i.indicator_id, f.notes;


-- ===================================================================================
-- 5. RUHR CITIES COMPARISON - 2024
-- ===================================================================================
-- Compare total registrations across all Ruhr cities
SELECT 
    g.region_name as city,
    f.value as total_registrations_2024
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id = 24
  AND f.notes = 'Total registrations'
  AND t.year = 2024
ORDER BY f.value DESC;


-- ===================================================================================
-- 6. RUHR CITIES COMPARISON - 2020-2024 (COVID Impact)
-- ===================================================================================
-- See registration trends during COVID period
SELECT 
    t.year,
    g.region_name as city,
    f.value as total_registrations
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id = 24
  AND f.notes = 'Total registrations'
  AND t.year >= 2020
ORDER BY t.year DESC, f.value DESC;


-- ===================================================================================
-- 7. REGISTRATION VS DEREGISTRATION - NET CHANGE
-- ===================================================================================
-- Calculate net business change (registrations - deregistrations) for Ruhr cities 2024
WITH registrations AS (
    SELECT 
        g.region_code,
        g.region_name,
        f.value as registrations
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
      AND f.indicator_id = 24
      AND f.notes = 'Total registrations'
      AND t.year = 2024
),
deregistrations AS (
    SELECT 
        g.region_code,
        f.value as deregistrations
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
      AND f.indicator_id = 25
      AND f.notes = 'Total deregistrations'
      AND t.year = 2024
)
SELECT 
    r.region_name as city,
    r.registrations,
    d.deregistrations,
    (r.registrations - d.deregistrations) as net_change,
    ROUND(((r.registrations - d.deregistrations) / r.registrations) * 100, 1) as net_change_pct
FROM registrations r
JOIN deregistrations d ON r.region_code = d.region_code
ORDER BY net_change DESC;


-- ===================================================================================
-- 8. LONG-TERM TREND - DORTMUND (1998 vs 2024)
-- ===================================================================================
-- Compare first and last year for Dortmund
WITH first_year AS (
    SELECT 
        'Registrations' as type,
        f.notes as category,
        f.value as value_1998
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code = '05913'
      AND f.indicator_id = 24
      AND t.year = 1998
),
last_year AS (
    SELECT 
        'Registrations' as type,
        f.notes as category,
        f.value as value_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code = '05913'
      AND f.indicator_id = 24
      AND t.year = 2024
)
SELECT 
    f.category,
    f.value_1998,
    l.value_2024,
    (l.value_2024 - f.value_1998) as absolute_change,
    ROUND(((l.value_2024 - f.value_1998) / f.value_1998) * 100, 1) as percent_change
FROM first_year f
JOIN last_year l ON f.category = l.category
ORDER BY f.category;


-- ===================================================================================
-- 9. ALL NRW CITIES - TOP 10 BY REGISTRATIONS (2024)
-- ===================================================================================
-- See which NRW cities have the most business registrations
SELECT 
    g.region_name as city,
    g.region_code,
    f.value as total_registrations_2024
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 24
  AND f.notes = 'Total registrations'
  AND t.year = 2024
  AND g.region_code LIKE '05%'  -- NRW codes start with 05
ORDER BY f.value DESC
LIMIT 10;


-- ===================================================================================
-- 10. DATA QUALITY CHECK
-- ===================================================================================
-- Verify data quality (no NULLs, no negatives)
SELECT 
    'NULL values' as check_type,
    COUNT(*) as count
FROM fact_demographics 
WHERE indicator_id IN (24, 25) 
  AND value IS NULL

UNION ALL

SELECT 
    'Negative values' as check_type,
    COUNT(*) as count
FROM fact_demographics 
WHERE indicator_id IN (24, 25) 
  AND value < 0

UNION ALL

SELECT 
    'Missing notes' as check_type,
    COUNT(*) as count
FROM fact_demographics 
WHERE indicator_id IN (24, 25) 
  AND (notes IS NULL OR notes = '')

UNION ALL

SELECT 
    'Total records' as check_type,
    COUNT(*) as count
FROM fact_demographics 
WHERE indicator_id IN (24, 25);


-- ===================================================================================
-- 11. YEAR-BY-YEAR AVAILABILITY CHECK
-- ===================================================================================
-- Verify data exists for all years 1998-2024
SELECT 
    t.year,
    COUNT(DISTINCT CASE WHEN f.indicator_id = 24 THEN f.geo_id END) as regions_with_registrations,
    COUNT(DISTINCT CASE WHEN f.indicator_id = 25 THEN f.geo_id END) as regions_with_deregistrations
FROM dim_time t
LEFT JOIN fact_demographics f ON t.time_id = f.time_id AND f.indicator_id IN (24, 25)
WHERE t.year BETWEEN 1998 AND 2024
GROUP BY t.year
ORDER BY t.year DESC;


-- ===================================================================================
-- 12. CATEGORY DISTRIBUTION - All Ruhr Cities 2024
-- ===================================================================================
-- See breakdown of registration types across Ruhr cities
SELECT 
    f.notes as category,
    SUM(CASE WHEN g.region_code = '05913' THEN f.value ELSE 0 END) as dortmund,
    SUM(CASE WHEN g.region_code = '05113' THEN f.value ELSE 0 END) as essen,
    SUM(CASE WHEN g.region_code = '05112' THEN f.value ELSE 0 END) as duisburg,
    SUM(CASE WHEN g.region_code = '05911' THEN f.value ELSE 0 END) as bochum,
    SUM(CASE WHEN g.region_code = '05513' THEN f.value ELSE 0 END) as gelsenkirchen,
    SUM(f.value) as total
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id = 24
  AND t.year = 2024
GROUP BY f.notes
ORDER BY total DESC;


/*
===================================================================================
QUICK TESTING QUERIES
===================================================================================
*/

-- Simple count check
SELECT COUNT(*) FROM fact_demographics WHERE indicator_id IN (24, 25);

-- Quick Dortmund check
SELECT * FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code = '05913'
  AND f.indicator_id = 24
  AND t.year = 2024
ORDER BY f.notes;

-- Latest year data for all Ruhr cities
SELECT 
    g.region_name,
    i.indicator_code,
    f.notes,
    f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
  AND t.year = 2024
ORDER BY g.region_name, i.indicator_id, f.notes;

