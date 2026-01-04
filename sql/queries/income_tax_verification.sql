-- ============================================================================
-- Income Tax Data Verification Script
-- Table: 73111-010i (State Database NRW)
-- Indicators: 56 (Taxpayers), 57 (Total Income), 58 (Wage/Income Tax)
-- ============================================================================

-- ============================================================================
-- 1. BASIC DATA OVERVIEW
-- ============================================================================

-- Total record count by indicator
SELECT
    i.indicator_id,
    i.indicator_name,
    i.indicator_name_en,
    i.unit_description,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 56 AND 58
GROUP BY i.indicator_id, i.indicator_name, i.indicator_name_en, i.unit_description
ORDER BY i.indicator_id;

-- Year coverage
SELECT
    MIN(t.year) as first_year,
    MAX(t.year) as last_year,
    COUNT(DISTINCT t.year) as years_covered,
    ARRAY_AGG(DISTINCT t.year ORDER BY t.year) as all_years
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58;

-- Geographic coverage
SELECT
    COUNT(DISTINCT g.geo_id) as regions_count,
    COUNT(DISTINCT CASE WHEN LENGTH(g.region_code) = 2 THEN g.geo_id END) as state_level,
    COUNT(DISTINCT CASE WHEN LENGTH(g.region_code) = 3 THEN g.geo_id END) as regierungsbezirk_level,
    COUNT(DISTINCT CASE WHEN LENGTH(g.region_code) = 5 THEN g.geo_id END) as kreis_level
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 56 AND 58;


-- ============================================================================
-- 2. NRW STATE TOTALS - TIME SERIES
-- ============================================================================

-- NRW totals over time (all three indicators)
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 56 THEN f.value END) as taxpayers,
    MAX(CASE WHEN f.indicator_id = 57 THEN f.value END) as total_income_tsd_eur,
    MAX(CASE WHEN f.indicator_id = 58 THEN f.value END) as tax_amount_tsd_eur,
    -- Calculated metrics
    ROUND(MAX(CASE WHEN f.indicator_id = 57 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 56 THEN f.value END), 0), 2) as avg_income_per_taxpayer_tsd,
    ROUND(MAX(CASE WHEN f.indicator_id = 58 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 57 THEN f.value END), 0) * 100, 2) as effective_tax_rate_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58
  AND g.region_code = '05'  -- NRW state total
GROUP BY t.year
ORDER BY t.year;


-- ============================================================================
-- 3. REGIONAL COMPARISON (LATEST YEAR)
-- ============================================================================

-- All regions for latest year (2021)
SELECT
    g.region_code,
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 56 THEN f.value END) as taxpayers,
    MAX(CASE WHEN f.indicator_id = 57 THEN f.value END) as total_income_tsd_eur,
    MAX(CASE WHEN f.indicator_id = 58 THEN f.value END) as tax_amount_tsd_eur,
    -- Average income per taxpayer (in EUR, converting from thousands)
    ROUND(MAX(CASE WHEN f.indicator_id = 57 THEN f.value END) * 1000 /
          NULLIF(MAX(CASE WHEN f.indicator_id = 56 THEN f.value END), 0), 0) as avg_income_eur
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58
  AND t.year = 2021
  AND LENGTH(g.region_code) = 5  -- Kreis level only
GROUP BY g.region_code, g.region_name
ORDER BY taxpayers DESC;


-- ============================================================================
-- 4. RUHR REGION ANALYSIS
-- ============================================================================

-- Ruhr cities comparison (major cities in Ruhrgebiet)
SELECT
    g.region_code,
    g.region_name,
    t.year,
    MAX(CASE WHEN f.indicator_id = 56 THEN f.value END) as taxpayers,
    MAX(CASE WHEN f.indicator_id = 58 THEN f.value END) as tax_amount_tsd_eur,
    ROUND(MAX(CASE WHEN f.indicator_id = 58 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 56 THEN f.value END), 0), 2) as tax_per_taxpayer_tsd
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58
  AND g.region_name IN ('Duisburg', 'Essen', 'Dortmund', 'Bochum', 'Gelsenkirchen',
                        'Oberhausen', 'Mülheim an der Ruhr', 'Bottrop', 'Herne')
  AND t.year IN (1998, 2010, 2021)  -- Compare start, middle, end
GROUP BY g.region_code, g.region_name, t.year
ORDER BY g.region_name, t.year;


-- ============================================================================
-- 5. GROWTH ANALYSIS
-- ============================================================================

-- Taxpayer growth by region (1998 vs 2021)
WITH first_year AS (
    SELECT
        g.region_code,
        g.region_name,
        f.value as taxpayers_1998
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 56
      AND t.year = 1998
      AND LENGTH(g.region_code) = 5
),
last_year AS (
    SELECT
        g.region_code,
        f.value as taxpayers_2021
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 56
      AND t.year = 2021
      AND LENGTH(g.region_code) = 5
)
SELECT
    fy.region_code,
    fy.region_name,
    fy.taxpayers_1998,
    ly.taxpayers_2021,
    ly.taxpayers_2021 - fy.taxpayers_1998 as absolute_change,
    ROUND((ly.taxpayers_2021 - fy.taxpayers_1998) / fy.taxpayers_1998 * 100, 1) as pct_change
FROM first_year fy
JOIN last_year ly ON fy.region_code = ly.region_code
ORDER BY pct_change DESC;


-- ============================================================================
-- 6. TAX REVENUE ANALYSIS
-- ============================================================================

-- Top 10 regions by tax revenue (2021)
SELECT
    g.region_code,
    g.region_name,
    f.value as tax_amount_tsd_eur,
    ROUND(f.value / 1000, 2) as tax_amount_mio_eur,
    ROUND(f.value / 1000000, 3) as tax_amount_bil_eur
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 58  -- Wage and income tax
  AND t.year = 2021
  AND LENGTH(g.region_code) = 5  -- Kreis level
ORDER BY f.value DESC
LIMIT 10;


-- ============================================================================
-- 7. DATA QUALITY CHECKS
-- ============================================================================

-- Check for NULL values
SELECT
    i.indicator_name_en,
    COUNT(*) as total_records,
    COUNT(f.value) as non_null_values,
    COUNT(*) - COUNT(f.value) as null_values
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id BETWEEN 56 AND 58
GROUP BY i.indicator_name_en;

-- Check for negative values (should be none for tax data)
SELECT
    i.indicator_name_en,
    COUNT(*) as negative_count
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id BETWEEN 56 AND 58
  AND f.value < 0
GROUP BY i.indicator_name_en;

-- Verify all three indicators have matching region/year combinations
SELECT
    g.region_code,
    t.year,
    COUNT(DISTINCT f.indicator_id) as indicator_count
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58
GROUP BY g.region_code, t.year
HAVING COUNT(DISTINCT f.indicator_id) < 3
ORDER BY g.region_code, t.year;


-- ============================================================================
-- 8. SUMMARY STATISTICS
-- ============================================================================

-- Summary statistics for 2021
SELECT
    i.indicator_name_en,
    COUNT(f.value) as n,
    ROUND(MIN(f.value), 0) as min_value,
    ROUND(AVG(f.value), 0) as avg_value,
    ROUND(MAX(f.value), 0) as max_value,
    ROUND(STDDEV(f.value), 0) as std_dev
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 56 AND 58
  AND t.year = 2021
GROUP BY i.indicator_id, i.indicator_name_en
ORDER BY i.indicator_id;
