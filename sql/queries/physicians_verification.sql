-- ============================================================================
-- Verification SQL Script for Physicians Data
-- Table: 23111-12i (State Database NRW)
-- Indicator: 83 (Full-time Physicians in Hospitals by Gender)
-- Period: 2005 - 2024 (annual data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id = 83;

-- 2. Records by year
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 83
GROUP BY t.year
ORDER BY t.year;

-- 3. NRW Total Physicians Over Time (2005-2024)
SELECT
    t.year,
    MAX(f.value) as total_physicians
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND g.region_code = '05'
  AND f.notes = 'gender:total|Total'
GROUP BY t.year
ORDER BY t.year;

-- 4. NRW Physicians by Gender Over Time
SELECT
    t.year,
    MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total,
    MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) as male,
    MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 5. Growth Trends 2005-2024 (NRW)
WITH base_year AS (
    SELECT
        MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total_2005,
        MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) as male_2005,
        MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female_2005
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 83
      AND g.region_code = '05'
      AND t.year = 2005
),
current_year AS (
    SELECT
        MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total_2024,
        MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) as male_2024,
        MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female_2024
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 83
      AND g.region_code = '05'
      AND t.year = 2024
)
SELECT
    total_2005,
    total_2024,
    total_2024 - total_2005 as total_growth,
    ROUND((total_2024 - total_2005) / total_2005 * 100, 2) as total_growth_pct,
    male_2005,
    male_2024,
    male_2024 - male_2005 as male_growth,
    ROUND((male_2024 - male_2005) / male_2005 * 100, 2) as male_growth_pct,
    female_2005,
    female_2024,
    female_2024 - female_2005 as female_growth,
    ROUND((female_2024 - female_2005) / female_2005 * 100, 2) as female_growth_pct
FROM base_year, current_year;

-- 6. Top 10 Districts by Total Physicians (2024)
SELECT
    g.region_name,
    g.region_code,
    MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total_physicians,
    MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female_physicians,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND t.year = 2024
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
HAVING MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) IS NOT NULL
ORDER BY total_physicians DESC
LIMIT 10;

-- 7. Female Physician Share by District (2024) - Top 10
SELECT
    g.region_name,
    MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total_physicians,
    MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female_physicians,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND t.year = 2024
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
HAVING MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) IS NOT NULL
ORDER BY female_pct DESC
LIMIT 10;

-- 8. Physician Growth by Regierungsbezirk (2005-2024)
SELECT
    g.region_name,
    MAX(CASE WHEN t.year = 2005 THEN f.value END) as physicians_2005,
    MAX(CASE WHEN t.year = 2024 THEN f.value END) as physicians_2024,
    ROUND((MAX(CASE WHEN t.year = 2024 THEN f.value END) -
           MAX(CASE WHEN t.year = 2005 THEN f.value END)) /
          NULLIF(MAX(CASE WHEN t.year = 2005 THEN f.value END), 0) * 100, 2) as growth_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND f.notes = 'gender:total|Total'
  AND LENGTH(g.region_code) = 3  -- Regierungsbezirk level
  AND t.year IN (2005, 2024)
GROUP BY g.region_name, g.region_code
ORDER BY growth_pct DESC;

-- 9. Gender Balance Trend (2005 vs 2024)
SELECT
    '2005' as year,
    ROUND(MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as male_pct,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND g.region_code = '05'
  AND t.year = 2005

UNION ALL

SELECT
    '2024' as year,
    ROUND(MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as male_pct,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND g.region_code = '05'
  AND t.year = 2024;

-- 10. Ruhr Area Cities Comparison (2024)
SELECT
    g.region_name,
    MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) as total_physicians,
    MAX(CASE WHEN f.notes = 'gender:male|Male' THEN f.value END) as male_physicians,
    MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) as female_physicians,
    ROUND(MAX(CASE WHEN f.notes = 'gender:female|Female' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END), 0) * 100, 1) as female_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND t.year = 2024
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
GROUP BY g.region_name, g.region_code
HAVING MAX(CASE WHEN f.notes = 'gender:total|Total' THEN f.value END) IS NOT NULL
ORDER BY total_physicians DESC;

-- 11. Year-over-Year Growth Analysis (recent years)
SELECT
    t.year,
    f.value as total_physicians,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    f.value - LAG(f.value) OVER (ORDER BY t.year) as yoy_change,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 83
  AND g.region_code = '05'
  AND f.notes = 'gender:total|Total'
  AND t.year >= 2015
ORDER BY t.year;

-- 12. Data Quality Check - Records by Region Level
SELECT
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END as region_level,
    COUNT(*) as record_count,
    COUNT(DISTINCT g.region_code) as unique_regions,
    COUNT(DISTINCT t.year) as years_covered
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 83
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
