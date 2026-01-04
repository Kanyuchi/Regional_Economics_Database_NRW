-- ============================================================================
-- Income Tax by Income Bracket Verification Script
-- Table: 73111-030i (State Database NRW)
-- Indicators: 59 (Taxpayers), 60 (Total Income), 61 (Tax) - all by bracket
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
WHERE i.indicator_id BETWEEN 59 AND 61
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
WHERE f.indicator_id BETWEEN 59 AND 61;

-- Geographic and bracket coverage
SELECT
    COUNT(DISTINCT f.geo_id) as regions_count,
    COUNT(DISTINCT SPLIT_PART(f.notes, '|', 1)) as bracket_codes,
    COUNT(DISTINCT t.year) as years
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 59 AND 61;


-- ============================================================================
-- 2. INCOME BRACKET DISTRIBUTION (NRW 2021)
-- ============================================================================

-- Full income distribution for NRW state total
SELECT
    SPLIT_PART(f.notes, '|', 2) as income_bracket,
    MAX(CASE WHEN f.indicator_id = 59 THEN f.value END) as taxpayers,
    MAX(CASE WHEN f.indicator_id = 60 THEN f.value END) / 1000 as income_mio_eur,
    MAX(CASE WHEN f.indicator_id = 61 THEN f.value END) / 1000 as tax_mio_eur,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 61 THEN f.value END) /
        NULLIF(MAX(CASE WHEN f.indicator_id = 60 THEN f.value END), 0) * 100, 2
    ) as effective_tax_rate_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 59 AND 61
  AND g.region_code = '05'  -- NRW state total
  AND t.year = 2021
GROUP BY SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 2)
        WHEN '1 - 5 000' THEN 1
        WHEN '5 000 - 10 000' THEN 2
        WHEN '10 000 - 15 000' THEN 3
        WHEN '15 000 - 20 000' THEN 4
        WHEN '20 000 - 25 000' THEN 5
        WHEN '25 000 - 30 000' THEN 6
        WHEN '30 000 - 35 000' THEN 7
        WHEN '35 000 - 50 000' THEN 8
        WHEN '50 000 - 125 000' THEN 9
        WHEN '125 000 - 250 000' THEN 10
        WHEN '250 000 - 500 000' THEN 11
        WHEN '500 000 - 1 000 000' THEN 12
        WHEN '1 000 000 und mehr' THEN 13
        ELSE 99
    END;


-- ============================================================================
-- 3. TIME SERIES - HIGH INCOME TAXPAYERS
-- ============================================================================

-- Millionaires (€1M+ income) over time in NRW
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 59 THEN f.value END) as millionaire_count,
    MAX(CASE WHEN f.indicator_id = 60 THEN f.value END) / 1000 as total_income_mio,
    MAX(CASE WHEN f.indicator_id = 61 THEN f.value END) / 1000 as tax_paid_mio
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 59 AND 61
  AND g.region_code = '05'
  AND f.notes LIKE '%1000000_plus%'
GROUP BY t.year
ORDER BY t.year;


-- ============================================================================
-- 4. REGIONAL COMPARISON - HIGHEST INCOME REGIONS
-- ============================================================================

-- Top 10 regions by share of high earners (€125k+) in 2021
WITH bracket_totals AS (
    SELECT
        g.region_code,
        g.region_name,
        SUM(CASE WHEN f.indicator_id = 59 THEN f.value ELSE 0 END) as total_taxpayers
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 59
      AND t.year = 2021
      AND LENGTH(g.region_code) = 5
    GROUP BY g.region_code, g.region_name
),
high_earners AS (
    SELECT
        g.region_code,
        SUM(CASE WHEN f.indicator_id = 59 THEN f.value ELSE 0 END) as high_income_taxpayers
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 59
      AND t.year = 2021
      AND LENGTH(g.region_code) = 5
      AND (f.notes LIKE '%125000_250000%'
           OR f.notes LIKE '%250000_500000%'
           OR f.notes LIKE '%500000_1000000%'
           OR f.notes LIKE '%1000000_plus%')
    GROUP BY g.region_code
)
SELECT
    bt.region_name,
    bt.total_taxpayers,
    he.high_income_taxpayers,
    ROUND(he.high_income_taxpayers::numeric / bt.total_taxpayers * 100, 2) as high_earner_pct
FROM bracket_totals bt
JOIN high_earners he ON bt.region_code = he.region_code
WHERE bt.total_taxpayers > 0
ORDER BY high_earner_pct DESC
LIMIT 10;


-- ============================================================================
-- 5. RUHR REGION ANALYSIS
-- ============================================================================

-- Income distribution comparison: Düsseldorf vs Duisburg (2021)
SELECT
    g.region_name,
    SPLIT_PART(f.notes, '|', 2) as bracket,
    MAX(CASE WHEN f.indicator_id = 59 THEN f.value END) as taxpayers,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 61 THEN f.value END) /
        NULLIF(MAX(CASE WHEN f.indicator_id = 60 THEN f.value END), 0) * 100, 2
    ) as effective_rate_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 59 AND 61
  AND g.region_name IN ('Düsseldorf', 'Duisburg')
  AND t.year = 2021
GROUP BY g.region_name, SPLIT_PART(f.notes, '|', 2)
ORDER BY g.region_name,
    CASE SPLIT_PART(f.notes, '|', 2)
        WHEN '1 - 5 000' THEN 1
        WHEN '50 000 - 125 000' THEN 9
        WHEN '1 000 000 und mehr' THEN 13
        ELSE 5
    END;


-- ============================================================================
-- 6. DATA QUALITY CHECKS
-- ============================================================================

-- Check for NULL values
SELECT
    i.indicator_name_en,
    COUNT(*) as total_records,
    COUNT(f.value) as non_null_values,
    COUNT(*) - COUNT(f.value) as null_values
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id BETWEEN 59 AND 61
GROUP BY i.indicator_name_en;

-- Verify all 13 brackets present for each region/year
SELECT
    t.year,
    COUNT(DISTINCT SPLIT_PART(f.notes, '|', 1)) as bracket_count
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 59
GROUP BY t.year
ORDER BY t.year;

-- Check bracket totals match aggregated totals from 73111-010i
-- (Sum of bracket taxpayers should roughly equal total taxpayers)
SELECT
    t.year,
    SUM(f.value) as bracket_sum_taxpayers
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 59  -- Taxpayers by bracket
  AND g.region_code = '05'  -- NRW
GROUP BY t.year
ORDER BY t.year;


-- ============================================================================
-- 7. SUMMARY STATISTICS
-- ============================================================================

-- Summary by year
SELECT
    t.year,
    COUNT(DISTINCT g.geo_id) as regions,
    COUNT(DISTINCT SPLIT_PART(f.notes, '|', 1)) as brackets,
    SUM(CASE WHEN f.indicator_id = 59 THEN 1 ELSE 0 END) as taxpayer_records,
    SUM(CASE WHEN f.indicator_id = 60 THEN 1 ELSE 0 END) as income_records,
    SUM(CASE WHEN f.indicator_id = 61 THEN 1 ELSE 0 END) as tax_records
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 59 AND 61
GROUP BY t.year
ORDER BY t.year;
