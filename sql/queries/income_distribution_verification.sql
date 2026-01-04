-- ===============================================================================
-- Income Distribution Verification Queries
-- Table: 12211-9114i | Indicator: 88 | Period: 2005-2019
-- ===============================================================================

-- Query 1: Record count summary
SELECT
    'Income Distribution' as dataset,
    COUNT(*) as total_records,
    MIN(t.year) as earliest_year,
    MAX(t.year) as latest_year,
    COUNT(DISTINCT f.geo_id) as regions_covered
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 88;

-- Query 2: Income distribution trend for NRW (2005 vs 2019)
SELECT
    t.year,
    SPLIT_PART(f.notes, ':', 2) as income_bracket,
    SUM(CASE WHEN f.gender = 'Total' THEN f.value ELSE 0 END) as population_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 88
    AND g.region_code = '05'
    AND t.year IN (2005, 2019)
    AND f.gender = 'Total'
GROUP BY t.year, SPLIT_PART(f.notes, ':', 2)
ORDER BY t.year,
    CASE SPLIT_PART(f.notes, ':', 2)
        WHEN 'under_150' THEN 1
        WHEN '150_to_300' THEN 2
        WHEN '300_to_500' THEN 3
        WHEN '500_to_700' THEN 4
        WHEN '700_to_900' THEN 5
        WHEN '900_to_1100' THEN 6
        WHEN '1100_to_1300' THEN 7
        WHEN '1300_to_1500' THEN 8
        WHEN '1500_and_more' THEN 9
    END;

-- Query 3: Gender income gap - population by income bracket (2019)
SELECT
    SPLIT_PART(f.notes, ':', 2) as income_bracket,
    SUM(CASE WHEN f.gender = 'Male' THEN f.value ELSE 0 END) as male_population,
    SUM(CASE WHEN f.gender = 'Female' THEN f.value ELSE 0 END) as female_population,
    ROUND(SUM(CASE WHEN f.gender = 'Male' THEN f.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN f.gender = 'Female' THEN f.value ELSE 0 END), 0), 2) as male_to_female_ratio
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 88
    AND g.region_code = '05'
    AND t.year = 2019
    AND f.gender IN ('Male', 'Female')
GROUP BY SPLIT_PART(f.notes, ':', 2)
ORDER BY
    CASE SPLIT_PART(f.notes, ':', 2)
        WHEN 'under_150' THEN 1
        WHEN '150_to_300' THEN 2
        WHEN '300_to_500' THEN 3
        WHEN '500_to_700' THEN 4
        WHEN '700_to_900' THEN 5
        WHEN '900_to_1100' THEN 6
        WHEN '1100_to_1300' THEN 7
        WHEN '1300_to_1500' THEN 8
        WHEN '1500_and_more' THEN 9
    END;

-- Query 4: High income earners trend (1500+ EUR)
SELECT
    t.year,
    SUM(CASE WHEN f.gender = 'Total' THEN f.value ELSE 0 END) as high_earners_thousands,
    ROUND(SUM(CASE WHEN f.gender = 'Total' THEN f.value ELSE 0 END) * 100.0 /
          (SELECT SUM(f2.value)
           FROM fact_demographics f2
           JOIN dim_time t2 ON f2.time_id = t2.time_id
           JOIN dim_geography g2 ON f2.geo_id = g2.geo_id
           WHERE f2.indicator_id = 88
               AND g2.region_code = '05'
               AND t2.year = t.year
               AND f2.gender = 'Total'
               AND SPLIT_PART(f2.notes, ':', 2) = '1500_and_more'), 1) as pct_of_total
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 88
    AND g.region_code = '05'
    AND SPLIT_PART(f.notes, ':', 2) = '1500_and_more'
    AND f.gender = 'Total'
GROUP BY t.year
ORDER BY t.year;

-- Query 5: Low income population trend (under 500 EUR)
SELECT
    t.year,
    SUM(CASE WHEN SPLIT_PART(f.notes, ':', 2) IN ('under_150', '150_to_300', '300_to_500')
             AND f.gender = 'Total' THEN f.value ELSE 0 END) as low_income_thousands
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 88
    AND g.region_code = '05'
GROUP BY t.year
ORDER BY t.year;

-- Query 6: Income distribution by gender - percentage breakdown (2019)
WITH gender_totals AS (
    SELECT
        f.gender,
        SUM(f.value) as total_population
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 88
        AND g.region_code = '05'
        AND t.year = 2019
        AND f.gender IN ('Male', 'Female')
    GROUP BY f.gender
)
SELECT
    f.gender,
    SPLIT_PART(f.notes, ':', 2) as income_bracket,
    SUM(f.value) as population_thousands,
    ROUND(SUM(f.value) * 100.0 / gt.total_population, 1) as pct_of_gender
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN gender_totals gt ON f.gender = gt.gender
WHERE f.indicator_id = 88
    AND g.region_code = '05'
    AND t.year = 2019
    AND f.gender IN ('Male', 'Female')
GROUP BY f.gender, SPLIT_PART(f.notes, ':', 2), gt.total_population
ORDER BY f.gender,
    CASE SPLIT_PART(f.notes, ':', 2)
        WHEN 'under_150' THEN 1
        WHEN '150_to_300' THEN 2
        WHEN '300_to_500' THEN 3
        WHEN '500_to_700' THEN 4
        WHEN '700_to_900' THEN 5
        WHEN '900_to_1100' THEN 6
        WHEN '1100_to_1300' THEN 7
        WHEN '1300_to_1500' THEN 8
        WHEN '1500_and_more' THEN 9
    END;

-- Query 7: Data completeness check
SELECT
    t.year,
    COUNT(DISTINCT g.region_code) as regions_with_data,
    COUNT(*) as total_records
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 88
GROUP BY t.year
ORDER BY t.year;
