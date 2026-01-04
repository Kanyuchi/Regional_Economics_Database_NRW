-- ============================================================================
-- Verification SQL Script for Population Profile Data (NRW State Level)
-- Table: 12411-9k06 (State Database NRW)
-- Indicators: 67-71 (Total, Male, Female, German, Foreign by Age Group)
-- Period: 1975-2024 (50 years)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id BETWEEN 67 AND 71;

-- 2. Records by indicator
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name_en,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 67 AND 71
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en
ORDER BY i.indicator_id;

-- 3. Year range verification
SELECT
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT t.year) as years_count
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 67 AND 71;

-- 4. Age groups breakdown
SELECT
    SPLIT_PART(f.notes, '|', 2) as age_group,
    COUNT(*) as records_per_age_group
FROM fact_demographics f
WHERE f.indicator_id = 67  -- Total population
GROUP BY SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 2)
        WHEN 'Total' THEN 0
        WHEN 'unter 6 Jahre' THEN 1
        WHEN '6 bis unter 18 Jahre' THEN 2
        WHEN '18 bis unter 25 Jahre' THEN 3
        WHEN '25 bis unter 30 Jahre' THEN 4
        WHEN '30 bis unter 40 Jahre' THEN 5
        WHEN '40 bis unter 50 Jahre' THEN 6
        WHEN '50 bis unter 60 Jahre' THEN 7
        WHEN '60 bis unter 65 Jahre' THEN 8
        WHEN '65 Jahre und mehr' THEN 9
        ELSE 99
    END;

-- 5. NRW Population by Age Group - Latest Year (2024)
SELECT
    SPLIT_PART(f.notes, '|', 2) as age_group,
    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total,
    MAX(CASE WHEN f.indicator_id = 68 THEN f.value END) as male,
    MAX(CASE WHEN f.indicator_id = 69 THEN f.value END) as female,
    MAX(CASE WHEN f.indicator_id = 70 THEN f.value END) as german,
    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_pop,
    ROUND(MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100, 2) as foreign_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 67 AND 71
  AND t.year = 2024
GROUP BY SPLIT_PART(f.notes, '|', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, '|', 2)
        WHEN 'Total' THEN 0
        WHEN 'unter 6 Jahre' THEN 1
        WHEN '6 bis unter 18 Jahre' THEN 2
        WHEN '18 bis unter 25 Jahre' THEN 3
        WHEN '25 bis unter 30 Jahre' THEN 4
        WHEN '30 bis unter 40 Jahre' THEN 5
        WHEN '40 bis unter 50 Jahre' THEN 6
        WHEN '50 bis unter 60 Jahre' THEN 7
        WHEN '60 bis unter 65 Jahre' THEN 8
        WHEN '65 Jahre und mehr' THEN 9
        ELSE 99
    END;

-- 6. NRW Total Population Over Time (Total only)
SELECT
    t.year,
    f.value as total_population,
    LAG(f.value) OVER (ORDER BY t.year) as prev_year,
    ROUND((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
          NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100, 3) as yoy_change_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 67
  AND SPLIT_PART(f.notes, '|', 1) = 'age_group:total'
ORDER BY t.year;

-- 7. Foreign Population Share Over Time
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 67 AND SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) as total_pop,
    MAX(CASE WHEN f.indicator_id = 71 AND SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) as foreign_pop,
    ROUND(MAX(CASE WHEN f.indicator_id = 71 AND SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 67 AND SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END), 0) * 100, 2) as foreign_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (67, 71)
GROUP BY t.year
ORDER BY t.year;

-- 8. Population Age Structure Over Time (Decade snapshots)
SELECT
    t.year,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:under_6' THEN f.value END) /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END), 0) * 100, 2) as pct_under_6,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:6_to_18' THEN f.value END) /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END), 0) * 100, 2) as pct_6_to_18,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) IN ('age_group:18_to_25', 'age_group:25_to_30', 'age_group:30_to_40', 'age_group:40_to_50', 'age_group:50_to_60') THEN f.value ELSE 0 END) /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END), 0) * 100, 2) as pct_working_age,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:65_plus' THEN f.value END) /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END), 0) * 100, 2) as pct_65_plus
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 67
  AND t.year IN (1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024)
GROUP BY t.year
ORDER BY t.year;

-- 9. Gender Ratio Over Time (Female/Male)
SELECT
    t.year,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' AND f.indicator_id = 68 THEN f.value END) as male,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' AND f.indicator_id = 69 THEN f.value END) as female,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' AND f.indicator_id = 69 THEN f.value END)::numeric /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' AND f.indicator_id = 68 THEN f.value END), 0) * 100, 2) as females_per_100_males
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (68, 69)
GROUP BY t.year
ORDER BY t.year;

-- 10. Data Quality Check - Sum of age groups equals total
SELECT
    t.year,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) as reported_total,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) != 'age_group:total' THEN f.value END) as sum_of_age_groups,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) -
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) != 'age_group:total' THEN f.value END) as difference
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 67
GROUP BY t.year
HAVING ABS(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 1) = 'age_group:total' THEN f.value END) -
       SUM(CASE WHEN SPLIT_PART(f.notes, '|', 1) != 'age_group:total' THEN f.value END)) > 1
ORDER BY t.year;
