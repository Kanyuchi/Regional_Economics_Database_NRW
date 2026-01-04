-- ============================================================================
-- Verification SQL Script for Care Recipients Data
-- Table: 22421-02i (State Database NRW)
-- Indicators: 72-75 (Benefit Total, Nursing Home, Inpatient, Care Allowance)
-- Period: 2017, 2019, 2021, 2023 (biennial data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id BETWEEN 72 AND 75;

-- 2. Records by indicator
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name_en,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 72 AND 75
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en
ORDER BY i.indicator_id;

-- 3. Year coverage verification
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 72 AND 75
GROUP BY t.year
ORDER BY t.year;

-- 4. Care levels breakdown
SELECT
    SPLIT_PART(f.notes, '|', 1) as care_level_code,
    SPLIT_PART(f.notes, '|', 2) as care_level_name,
    COUNT(*) as records_per_care_level
FROM fact_demographics f
WHERE f.indicator_id = 72  -- Total benefit recipients
GROUP BY SPLIT_PART(f.notes, '|', 1), SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 1)
        WHEN 'care_level:total' THEN 0
        WHEN 'care_level:level_1' THEN 1
        WHEN 'care_level:level_2' THEN 2
        WHEN 'care_level:level_3' THEN 3
        WHEN 'care_level:level_4' THEN 4
        WHEN 'care_level:level_5' THEN 5
        WHEN 'care_level:not_assigned' THEN 6
        ELSE 99
    END;

-- 5. NRW Total Care Recipients by Care Level - Latest Year (2023)
SELECT
    SPLIT_PART(f.notes, '|', 2) as care_level,
    MAX(CASE WHEN f.indicator_id = 72 THEN f.value END) as benefit_total,
    MAX(CASE WHEN f.indicator_id = 73 THEN f.value END) as nursing_home,
    MAX(CASE WHEN f.indicator_id = 74 THEN f.value END) as inpatient_care,
    MAX(CASE WHEN f.indicator_id = 75 THEN f.value END) as care_allowance,
    ROUND(MAX(CASE WHEN f.indicator_id = 75 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 72 THEN f.value END), 0) * 100, 2) as home_care_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 72 AND 75
  AND t.year = 2023
  AND g.region_code = '05'  -- NRW state level
GROUP BY SPLIT_PART(f.notes, '|', 1), SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 1)
        WHEN 'care_level:total' THEN 0
        WHEN 'care_level:level_1' THEN 1
        WHEN 'care_level:level_2' THEN 2
        WHEN 'care_level:level_3' THEN 3
        WHEN 'care_level:level_4' THEN 4
        WHEN 'care_level:level_5' THEN 5
        WHEN 'care_level:not_assigned' THEN 6
        ELSE 99
    END;

-- 6. Care Recipients Growth Over Time (NRW State Total)
SELECT
    t.year,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) as total_recipients,
    LAG(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END))
        OVER (ORDER BY t.year) as prev_year,
    ROUND((SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) -
           LAG(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END))
               OVER (ORDER BY t.year)) /
          NULLIF(LAG(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END))
               OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 72  -- Benefit recipients total
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 7. Top 10 Districts by Total Care Recipients (2023)
SELECT
    g.region_name,
    g.region_code,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) as total_recipients,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 75 THEN f.value END) as care_allowance,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 75 THEN f.value END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 72 THEN f.value END), 0) * 100, 2) as home_care_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 72 AND 75
  AND t.year = 2023
  AND LENGTH(g.region_code) = 5  -- District level only
GROUP BY g.region_name, g.region_code
ORDER BY total_recipients DESC NULLS LAST
LIMIT 10;

-- 8. Care Level Distribution Comparison 2017 vs 2023 (NRW)
SELECT
    SPLIT_PART(f.notes, '|', 2) as care_level,
    SUM(CASE WHEN t.year = 2017 THEN f.value END) as recipients_2017,
    SUM(CASE WHEN t.year = 2023 THEN f.value END) as recipients_2023,
    ROUND((SUM(CASE WHEN t.year = 2023 THEN f.value END) -
           SUM(CASE WHEN t.year = 2017 THEN f.value END)) /
          NULLIF(SUM(CASE WHEN t.year = 2017 THEN f.value END), 0) * 100, 2) as change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 72  -- Benefit recipients total
  AND g.region_code = '05'
  AND t.year IN (2017, 2023)
GROUP BY SPLIT_PART(f.notes, '|', 1), SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 1)
        WHEN 'care_level:total' THEN 0
        WHEN 'care_level:level_1' THEN 1
        WHEN 'care_level:level_2' THEN 2
        WHEN 'care_level:level_3' THEN 3
        WHEN 'care_level:level_4' THEN 4
        WHEN 'care_level:level_5' THEN 5
        WHEN 'care_level:not_assigned' THEN 6
        ELSE 99
    END;

-- 9. Home Care vs Institutional Care Ratio Over Time (NRW, All Care Levels)
SELECT
    t.year,
    SUM(CASE WHEN f.indicator_id = 72 THEN f.value END) as benefit_total,
    SUM(CASE WHEN f.indicator_id = 73 THEN f.value END) as nursing_home,
    SUM(CASE WHEN f.indicator_id = 74 THEN f.value END) as inpatient,
    SUM(CASE WHEN f.indicator_id = 75 THEN f.value END) as care_allowance,
    ROUND(SUM(CASE WHEN f.indicator_id = 75 THEN f.value END) /
          NULLIF(SUM(CASE WHEN f.indicator_id = 72 THEN f.value END), 0) * 100, 2) as home_care_pct,
    ROUND((SUM(CASE WHEN f.indicator_id = 73 THEN f.value END) +
           SUM(CASE WHEN f.indicator_id = 74 THEN f.value END)) /
          NULLIF(SUM(CASE WHEN f.indicator_id = 72 THEN f.value END), 0) * 100, 2) as institutional_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 72 AND 75
  AND g.region_code = '05'
  AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total'
GROUP BY t.year
ORDER BY t.year;

-- 10. Ruhr Area Cities Care Recipients Comparison (2023)
SELECT
    g.region_name,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 72 THEN f.value END) as total_recipients,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 75 THEN f.value END) as care_allowance,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 75 THEN f.value END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'care_level:total' AND f.indicator_id = 72 THEN f.value END), 0) * 100, 2) as home_care_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 72 AND 75
  AND t.year = 2023
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
GROUP BY g.region_name, g.region_code
ORDER BY total_recipients DESC NULLS LAST;

-- 11. Data Quality Check - Records by Region Level
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
WHERE f.indicator_id BETWEEN 72 AND 75
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
