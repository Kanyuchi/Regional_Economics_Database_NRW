-- ============================================================================
-- Verification SQL Script for Outpatient Services Data
-- Table: 22411-01i (State Database NRW)
-- Indicators: 77 (Services), 78 (Staff)
-- Period: 2017, 2019, 2021, 2023 (biennial data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id BETWEEN 77 AND 78;

-- 2. Records by indicator
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name_en,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 77 AND 78
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en
ORDER BY i.indicator_id;

-- 3. Year coverage verification
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 77 AND 78
GROUP BY t.year
ORDER BY t.year;

-- 4. NRW Outpatient Services and Staff Over Time
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services,
    MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 77 THEN f.value END), 0), 1) as staff_per_service
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 77 AND 78
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 5. Services Growth Over Time (NRW)
SELECT
    t.year,
    f.value as services,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 77
  AND g.region_code = '05'
ORDER BY t.year;

-- 6. Top 10 Districts by Outpatient Services (2023)
SELECT
    g.region_name,
    g.region_code,
    MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services,
    MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 77 THEN f.value END), 0), 1) as staff_per_service
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 77 AND 78
  AND t.year = 2023
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
ORDER BY services DESC NULLS LAST
LIMIT 10;

-- 7. Staff Growth vs Services Growth 2017-2023 (NRW)
WITH base_year AS (
    SELECT
        MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services_2017,
        MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff_2017
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id BETWEEN 77 AND 78
      AND g.region_code = '05'
      AND t.year = 2017
),
current_year AS (
    SELECT
        MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services_2023,
        MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff_2023
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id BETWEEN 77 AND 78
      AND g.region_code = '05'
      AND t.year = 2023
)
SELECT
    services_2017,
    services_2023,
    ROUND((services_2023 - services_2017) / services_2017 * 100, 2) as services_growth_pct,
    staff_2017,
    staff_2023,
    ROUND((staff_2023 - staff_2017) / staff_2017 * 100, 2) as staff_growth_pct
FROM base_year, current_year;

-- 8. Ruhr Area Cities Outpatient Services Comparison (2023)
SELECT
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services,
    MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 78 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 77 THEN f.value END), 0), 1) as staff_per_service
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 77 AND 78
  AND t.year = 2023
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
GROUP BY g.region_name, g.region_code
ORDER BY services DESC NULLS LAST;

-- 9. Services per 10,000 Care Recipients (comparing indicators 76 and 77)
-- Shows outpatient service coverage
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 76 THEN f.value END) as outpatient_recipients,
    MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) as services,
    ROUND(MAX(CASE WHEN f.indicator_id = 77 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 76 THEN f.value END), 0) * 10000, 1) as services_per_10k_recipients
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (76, 77)
  AND g.region_code = '05'
  AND SPLIT_PART(f.notes, '|', 1) IN ('care_level:total', 'metric:total_services')
GROUP BY t.year
ORDER BY t.year;

-- 10. Data Quality Check - Records by Region Level
SELECT
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END as region_level,
    COUNT(*) as record_count,
    COUNT(DISTINCT g.region_code) as unique_regions
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 77 AND 78
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
