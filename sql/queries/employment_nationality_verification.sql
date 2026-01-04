-- ===============================================================================
-- Employment by Nationality Verification Queries
-- Table: 12211-9124i | Indicator: 87 | Period: 1997-2019
-- ===============================================================================

-- Query 1: Record count summary
SELECT
    'Employment by Nationality' as dataset,
    COUNT(*) as total_records,
    MIN(t.year) as earliest_year,
    MAX(t.year) as latest_year,
    COUNT(DISTINCT f.geo_id) as regions_covered
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 87;

-- Query 2: NRW employment trend by nationality (1997-2019)
SELECT
    t.year,
    SPLIT_PART(f.notes, '|', 1) as nationality_category,
    SUM(CASE WHEN f.gender = 'Total' AND SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as population_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
    AND g.region_code = '05'
    AND SPLIT_PART(f.notes, '|', 2) = 'employment_status:total'
    AND f.gender = 'Total'
GROUP BY t.year, SPLIT_PART(f.notes, '|', 1)
ORDER BY t.year, nationality_category;

-- Query 3: Employment rates by nationality (2019)
SELECT
    SPLIT_PART(f.notes, '|', 1) as nationality_category,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' AND f.gender = 'Total' THEN f.value ELSE 0 END) as total_pop,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' AND f.gender = 'Total' THEN f.value ELSE 0 END) as employed,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' AND f.gender = 'Total' THEN f.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' AND f.gender = 'Total' THEN f.value ELSE 0 END), 0) * 100, 1) as employment_rate_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
    AND g.region_code = '05'
    AND t.year = 2019
GROUP BY SPLIT_PART(f.notes, '|', 1)
ORDER BY total_pop DESC;

-- Query 4: Gender breakdown by nationality (2019)
SELECT
    f.gender,
    SPLIT_PART(f.notes, '|', 1) as nationality_category,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as population_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
    AND g.region_code = '05'
    AND t.year = 2019
    AND f.gender != 'Total'
GROUP BY f.gender, SPLIT_PART(f.notes, '|', 1)
ORDER BY nationality_category, f.gender;

-- Query 5: Foreign population employment trend (1997 vs 2019)
SELECT
    t.year,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as foreigner_total,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' THEN f.value ELSE 0 END) as foreigner_employed,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' THEN f.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END), 0) * 100, 1) as employment_rate_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
    AND g.region_code = '05'
    AND SPLIT_PART(f.notes, '|', 1) = 'nationality:foreigner'
    AND f.gender = 'Total'
    AND t.year IN (1997, 2019)
GROUP BY t.year
ORDER BY t.year;

-- Query 6: Top 10 cities by foreigner employment rate (2019)
SELECT
    g.region_name,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) as foreigner_total,
    SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' THEN f.value ELSE 0 END) as foreigner_employed,
    ROUND(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:employed' THEN f.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END), 0) * 100, 1) as employment_rate_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
    AND SPLIT_PART(f.notes, '|', 1) = 'nationality:foreigner'
    AND f.gender = 'Total'
    AND t.year = 2019
    AND g.geo_type = 'City'
GROUP BY g.region_name
HAVING SUM(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'employment_status:total' THEN f.value ELSE 0 END) > 10
ORDER BY employment_rate_pct DESC
LIMIT 10;

-- Query 7: Data completeness check
SELECT
    t.year,
    COUNT(DISTINCT g.region_code) as regions_with_data,
    COUNT(*) as total_records
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 87
GROUP BY t.year
ORDER BY t.year;
