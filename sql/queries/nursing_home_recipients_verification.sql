-- ============================================================================
-- Verification SQL Script for Nursing Home Recipients Data
-- Table: 22412-02i (State Database NRW)
-- Indicator: 82 (Care Recipients in Nursing Homes by Care Level and Type)
-- Period: 2017, 2019, 2021, 2023 (biennial data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id = 82;

-- 2. Records by year
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 82
GROUP BY t.year
ORDER BY t.year;

-- 3. NRW Total Recipients Over Time
SELECT
    t.year,
    MAX(f.value) as total_recipients
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND g.region_code = '05'
  AND f.notes = 'category:total'
GROUP BY t.year
ORDER BY t.year;

-- 4. NRW Recipients by Care Type Over Time
SELECT
    t.year,
    MAX(CASE WHEN f.notes LIKE 'care_type:full_inpatient%' THEN f.value END) as full_inpatient,
    MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) as partial_inpatient,
    ROUND(MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes LIKE 'care_type:full_inpatient%' THEN f.value END) +
                 MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END), 0) * 100, 1)
          as partial_inpatient_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND g.region_code = '05'
  AND f.notes LIKE 'care_type:%'
GROUP BY t.year
ORDER BY t.year;

-- 5. NRW Recipients by Care Level Over Time (2023)
SELECT
    CASE
        WHEN f.notes LIKE '%Care Level 5%' THEN 'Care Level 5 (Highest)'
        WHEN f.notes LIKE '%Care Level 4%' THEN 'Care Level 4'
        WHEN f.notes LIKE '%Care Level 3%' THEN 'Care Level 3'
        WHEN f.notes LIKE '%Care Level 2%' THEN 'Care Level 2'
        WHEN f.notes LIKE '%Care Level 1%' THEN 'Care Level 1 (Lowest)'
        WHEN f.notes LIKE '%Not Yet Assigned%' THEN 'Not Yet Assigned'
    END as care_level,
    f.value as recipients_2023,
    ROUND(f.value / NULLIF((SELECT MAX(value)
                            FROM fact_demographics f2
                            JOIN dim_time t2 ON f2.time_id = t2.time_id
                            JOIN dim_geography g2 ON f2.geo_id = g2.geo_id
                            WHERE f2.indicator_id = 82
                              AND g2.region_code = '05'
                              AND f2.notes = 'category:total'
                              AND t2.year = 2023), 0) * 100, 1) as pct_of_total
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND g.region_code = '05'
  AND f.notes LIKE 'care_level:%'
  AND t.year = 2023
ORDER BY f.value DESC;

-- 6. Growth in Total Recipients 2017-2023
WITH base_year AS (
    SELECT MAX(f.value) as recipients_2017
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 82
      AND g.region_code = '05'
      AND f.notes = 'category:total'
      AND t.year = 2017
),
current_year AS (
    SELECT MAX(f.value) as recipients_2023
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 82
      AND g.region_code = '05'
      AND f.notes = 'category:total'
      AND t.year = 2023
)
SELECT
    recipients_2017,
    recipients_2023,
    ROUND((recipients_2023 - recipients_2017) / recipients_2017 * 100, 2) as growth_pct,
    recipients_2023 - recipients_2017 as absolute_growth
FROM base_year, current_year;

-- 7. Top 10 Districts by Total Recipients (2023)
SELECT
    g.region_name,
    g.region_code,
    f.value as total_recipients
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND t.year = 2023
  AND f.notes = 'category:total'
  AND LENGTH(g.region_code) = 5  -- District level only
ORDER BY f.value DESC NULLS LAST
LIMIT 10;

-- 8. Partial Inpatient Care Share by District (2023) - Top 10
SELECT
    g.region_name,
    MAX(CASE WHEN f.notes LIKE 'care_type:full_inpatient%' THEN f.value END) as full_inpatient,
    MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) as partial_inpatient,
    ROUND(MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes LIKE 'care_type:full_inpatient%' THEN f.value END) +
                 MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END), 0) * 100, 1)
          as partial_inpatient_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND t.year = 2023
  AND f.notes LIKE 'care_type:%'
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
ORDER BY partial_inpatient_pct DESC NULLS LAST
LIMIT 10;

-- 9. Care Level Distribution - Comparison with Outpatient Care (2023)
-- Shows care intensity differences between nursing homes and outpatient care
SELECT
    'Nursing Homes' as care_setting,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 5%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 82
                  AND notes = 'category:total'), 0) * 100, 1) as level_5_pct,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 4%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 82
                  AND notes = 'category:total'), 0) * 100, 1) as level_4_pct,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 3%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 82
                  AND notes = 'category:total'), 0) * 100, 1) as level_3_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND g.region_code = '05'
  AND t.year = 2023
  AND f.notes LIKE 'care_level:care_level%'

UNION ALL

SELECT
    'Outpatient Care' as care_setting,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 5%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 76
                  AND notes = 'care_level:total|Total'), 0) * 100, 1) as level_5_pct,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 4%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 76
                  AND notes = 'care_level:total|Total'), 0) * 100, 1) as level_4_pct,
    ROUND(MAX(CASE WHEN f.notes LIKE '%Care Level 3%' THEN f.value END) /
          NULLIF((SELECT MAX(value) FROM fact_demographics WHERE indicator_id = 76
                  AND notes = 'care_level:total|Total'), 0) * 100, 1) as level_3_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 76
  AND g.region_code = '05'
  AND t.year = 2023
  AND f.notes LIKE 'care_level:level%';

-- 10. Capacity Utilization: Recipients vs Available Places (2023)
SELECT
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 82 AND f.notes = 'category:total' THEN f.value END) as recipients,
    MAX(CASE WHEN f.indicator_id = 80 THEN f.value END) as available_places,
    ROUND(MAX(CASE WHEN f.indicator_id = 82 AND f.notes = 'category:total' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 80 THEN f.value END), 0) * 100, 1) as occupancy_rate_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (80, 82)
  AND t.year = 2023
  AND g.region_code = '05'
GROUP BY g.region_name, g.region_code;

-- 11. Ruhr Area Cities Comparison (2023)
SELECT
    g.region_name,
    MAX(CASE WHEN f.notes = 'category:total' THEN f.value END) as total_recipients,
    MAX(CASE WHEN f.notes LIKE 'care_type:full_inpatient%' THEN f.value END) as full_inpatient,
    MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) as partial_inpatient,
    ROUND(MAX(CASE WHEN f.notes LIKE 'care_type:partial_inpatient%' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.notes = 'category:total' THEN f.value END), 0) * 100, 1)
          as partial_inpatient_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 82
  AND t.year = 2023
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
GROUP BY g.region_name, g.region_code
ORDER BY total_recipients DESC NULLS LAST;

-- 12. Data Quality Check - Records by Region Level
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
WHERE f.indicator_id = 82
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
