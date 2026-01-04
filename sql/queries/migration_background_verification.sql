-- ===============================================================================
-- Migration Background and Employment Status Verification Queries
-- Table: 12211-9134i | Indicator: 86 | Period: 2016-2019
-- ===============================================================================

-- Query 1: Record count summary
SELECT
    'Migration Background' as dataset,
    COUNT(*) as total_records,
    MIN(t.year) as earliest_year,
    MAX(t.year) as latest_year,
    COUNT(DISTINCT f.geo_id) as regions_covered
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 86;

-- Query 2: NRW population trend by migration background (2016-2019)
SELECT
    t.year,
    SPLIT_PART(f.notes, '|', 1) as migration_category,
    SUM(CASE WHEN f.gender = 'Total' AND SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as population_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 86
    AND g.region_code = '05'
    AND SPLIT_PART(f.notes, '|', 2) = 'employment_status:total'
    AND f.gender = 'Total'
GROUP BY t.year, SPLIT_PART(f.notes, '|', 1)
ORDER BY t.year, migration_category;

-- Query 3: Employment rates by migration background (2019)
SELECT
    SPLIT_PART(f.notes, '|', 1) as migration_category,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' AND f.gender = 'Total' THEN f.value ELSE 0 END) as total_pop,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' AND f.gender = 'Total' THEN f.value ELSE 0 END) as employed,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' AND f.gender = 'Total' THEN f.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' AND f.gender = 'Total' THEN f.value ELSE 0 END), 0) * 100, 1) as employment_rate_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 86
    AND g.region_code = '05'
    AND t.year = 2019
GROUP BY SPLIT_PART(f.notes, '|', 1)
ORDER BY total_pop DESC;

-- Query 4: Gender breakdown by migration background (2019)
SELECT
    f.gender,
    SPLIT_PART(f.notes, '|', 1) as migration_category,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as population_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 86
    AND g.region_code = '05'
    AND t.year = 2019
    AND f.gender != 'Total'
GROUP BY f.gender, SPLIT_PART(f.notes, '|', 1)
ORDER BY migration_category, f.gender;
