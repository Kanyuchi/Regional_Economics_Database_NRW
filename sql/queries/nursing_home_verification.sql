-- ============================================================================
-- Verification SQL Script for Nursing Home Data
-- Table: 22412-01i (State Database NRW)
-- Indicators: 79 (Nursing Homes), 80 (Places), 81 (Staff)
-- Period: 2017, 2019, 2021, 2023 (biennial data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id BETWEEN 79 AND 81;

-- 2. Records by indicator
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name_en,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 79 AND 81
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en
ORDER BY i.indicator_id;

-- 3. Year coverage verification
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 79 AND 81
GROUP BY t.year
ORDER BY t.year;

-- 4. NRW Nursing Home Statistics Over Time
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as nursing_homes,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 79 THEN f.value END), 0), 1) as places_per_home,
    ROUND(MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0), 2) as staff_per_place
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 5. Nursing Homes Growth Over Time (NRW)
SELECT
    t.year,
    f.value as nursing_homes,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 79
  AND g.region_code = '05'
ORDER BY t.year;

-- 6. Places Growth Over Time (NRW)
SELECT
    t.year,
    f.value as total_places,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 80
  AND g.region_code = '05'
ORDER BY t.year;

-- 7. Top 10 Districts by Nursing Homes (2023)
SELECT
    g.region_name,
    g.region_code,
    MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as nursing_homes,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 79 THEN f.value END), 0), 1) as places_per_home,
    ROUND(MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0), 2) as staff_per_place
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND t.year = 2023
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
ORDER BY nursing_homes DESC NULLS LAST
LIMIT 10;

-- 8. 2017-2023 Growth Comparison (NRW)
WITH base_year AS (
    SELECT
        MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as homes_2017,
        MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as places_2017,
        MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff_2017
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id BETWEEN 79 AND 81
      AND g.region_code = '05'
      AND t.year = 2017
),
current_year AS (
    SELECT
        MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as homes_2023,
        MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as places_2023,
        MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff_2023
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id BETWEEN 79 AND 81
      AND g.region_code = '05'
      AND t.year = 2023
)
SELECT
    homes_2017,
    homes_2023,
    ROUND((homes_2023 - homes_2017) / homes_2017 * 100, 2) as homes_growth_pct,
    places_2017,
    places_2023,
    ROUND((places_2023 - places_2017) / places_2017 * 100, 2) as places_growth_pct,
    staff_2017,
    staff_2023,
    ROUND((staff_2023 - staff_2017) / staff_2017 * 100, 2) as staff_growth_pct
FROM base_year, current_year;

-- 9. Ruhr Area Cities Nursing Home Comparison (2023)
SELECT
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as nursing_homes,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 79 THEN f.value END), 0), 1) as places_per_home,
    ROUND(MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0), 2) as staff_per_place
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND t.year = 2023
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
GROUP BY g.region_name, g.region_code
ORDER BY nursing_homes DESC NULLS LAST;

-- 10. Average Facility Size by Regierungsbezirk (2023)
SELECT
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 79 THEN f.value END) as nursing_homes,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    ROUND(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 79 THEN f.value END), 0), 1) as avg_places_per_home
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND t.year = 2023
  AND LENGTH(g.region_code) = 3  -- Regierungsbezirk level
GROUP BY g.region_name, g.region_code
ORDER BY avg_places_per_home DESC NULLS LAST;

-- 11. Staffing Ratio Trends Over Time (NRW)
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0), 3) as staff_per_place
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 12. Districts with Highest Staff-to-Place Ratios (2023)
SELECT
    g.region_name,
    g.region_code,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as total_places,
    MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) as staff,
    ROUND(MAX(CASE WHEN f.indicator_id = 81 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0), 3) as staff_per_place
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 79 AND 81
  AND t.year = 2023
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
HAVING MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) IS NOT NULL
ORDER BY staff_per_place DESC NULLS LAST
LIMIT 10;

-- 13. Data Quality Check - Records by Region Level
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
WHERE f.indicator_id BETWEEN 79 AND 81
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
