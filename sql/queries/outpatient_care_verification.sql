-- ============================================================================
-- Verification SQL Script for Outpatient Care Recipients Data
-- Table: 22411-02i (State Database NRW)
-- Indicator: 76 (Outpatient Care Recipients by Care Level)
-- Period: 2017, 2019, 2021, 2023 (biennial data)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id = 76;

-- 2. Year coverage verification
SELECT
    t.year,
    COUNT(*) as records_per_year
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 76
GROUP BY t.year
ORDER BY t.year;

-- 3. Care levels breakdown
SELECT
    SPLIT_PART(f.notes, '|', 1) as care_level_code,
    SPLIT_PART(f.notes, '|', 2) as care_level_name,
    COUNT(*) as records_per_care_level
FROM fact_demographics f
WHERE f.indicator_id = 76
GROUP BY SPLIT_PART(f.notes, '|', 1), SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 1)
        WHEN 'care_level:total' THEN 0
        WHEN 'care_level:level_1' THEN 1
        WHEN 'care_level:level_2' THEN 2
        WHEN 'care_level:level_3' THEN 3
        WHEN 'care_level:level_4' THEN 4
        WHEN 'care_level:level_5' THEN 5
        ELSE 99
    END;

-- 4. NRW Total Outpatient Care Recipients by Care Level - Latest Year (2023)
SELECT
    SPLIT_PART(f.notes, '|', 2) as care_level,
    f.value as recipients,
    ROUND(f.value / (SELECT SUM(f2.value) FROM fact_demographics f2
                     JOIN dim_time t2 ON f2.time_id = t2.time_id
                     JOIN dim_geography g2 ON f2.geo_id = g2.geo_id
                     WHERE f2.indicator_id = 76
                       AND g2.region_code = '05'
                       AND t2.year = 2023
                       AND SPLIT_PART(f2.notes, '|', 1) != 'care_level:total') * 100, 2) as pct_of_subtotal
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 76
  AND t.year = 2023
  AND g.region_code = '05'
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 1)
        WHEN 'care_level:total' THEN 0
        WHEN 'care_level:level_1' THEN 1
        WHEN 'care_level:level_2' THEN 2
        WHEN 'care_level:level_3' THEN 3
        WHEN 'care_level:level_4' THEN 4
        WHEN 'care_level:level_5' THEN 5
        ELSE 99
    END;

-- 5. Outpatient Care Growth Over Time (NRW State Total)
SELECT
    t.year,
    f.value as total_recipients,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 2) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 76
  AND g.region_code = '05'
  AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total'
ORDER BY t.year;

-- 6. Top 10 Districts by Outpatient Care Recipients (2023)
SELECT
    g.region_name,
    g.region_code,
    f.value as outpatient_recipients
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 76
  AND t.year = 2023
  AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total'
  AND LENGTH(g.region_code) = 5  -- District level only
ORDER BY f.value DESC NULLS LAST
LIMIT 10;

-- 7. Care Level Distribution Comparison 2017 vs 2023 (NRW)
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
WHERE f.indicator_id = 76
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
        ELSE 99
    END;

-- 8. Compare Outpatient Care (76) vs Total Care Recipients (72)
-- Shows outpatient share of total care recipients
SELECT
    t.year,
    SUM(CASE WHEN f.indicator_id = 76 AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) as outpatient,
    SUM(CASE WHEN f.indicator_id = 72 AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) as total_recipients,
    ROUND(SUM(CASE WHEN f.indicator_id = 76 AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END) /
          NULLIF(SUM(CASE WHEN f.indicator_id = 72 AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total' THEN f.value END), 0) * 100, 2) as outpatient_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (72, 76)
  AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- 9. Ruhr Area Cities Outpatient Care Comparison (2023)
SELECT
    g.region_name,
    f.value as outpatient_recipients
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 76
  AND t.year = 2023
  AND SPLIT_PART(f.notes, '|', 1) = 'care_level:total'
  AND g.region_code IN ('05111', '05112', '05113', '05114', '05116',  -- Ruhr cities
                        '05117', '05119', '05120', '05124', '05913',
                        '05914', '05915', '05916')
ORDER BY f.value DESC NULLS LAST;

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
WHERE f.indicator_id = 76
GROUP BY
    CASE
        WHEN LENGTH(g.region_code) = 2 THEN 'State'
        WHEN LENGTH(g.region_code) = 3 THEN 'Regierungsbezirk'
        WHEN LENGTH(g.region_code) = 5 THEN 'Kreis'
        ELSE 'Other'
    END
ORDER BY region_level;
