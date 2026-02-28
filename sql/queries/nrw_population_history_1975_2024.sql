-- ============================================================================
-- NRW State-Level Population History (1975-2024)
-- Table: 12411-9k06 (State Database NRW)
-- Indicators: 67-71 (Total, Male, Female, German, Foreign by Age Group)
-- Period: 50 years of historical data
-- ============================================================================

-- Query 1: NRW Total Population Over Time (All 50 Years)
-- Shows overall population trend with year-over-year changes
SELECT
    t.year,
    f.value as total_population,
    LAG(f.value) OVER (ORDER BY t.year) as previous_year,
    f.value - LAG(f.value) OVER (ORDER BY t.year) as absolute_change,
    ROUND(
        (f.value - LAG(f.value) OVER (ORDER BY t.year))::numeric /
        NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100,
        2
    ) as percent_change
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id = 67  -- Total population
  AND g.region_code = '05'  -- NRW state
  AND f.age_group = 'total'  -- Total across all age groups
ORDER BY t.year;


-- Query 2: Gender Distribution Over Time (Decade Snapshots)
-- Shows male vs female population at key points
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
    MAX(CASE WHEN f.indicator_id = 68 THEN f.value END) as male_population,
    MAX(CASE WHEN f.indicator_id = 69 THEN f.value END) as female_population,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 68 THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
        2
    ) as percent_male,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 69 THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
        2
    ) as percent_female
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (67, 68, 69)  -- Total, Male, Female
  AND g.region_code = '05'
  AND f.age_group = 'total'
  AND t.year IN (1975, 1980, 1985, 1990, 1995, 2000, 2005, 2010, 2015, 2020, 2024)
GROUP BY t.year
ORDER BY t.year;


-- Query 3: Foreign Population Growth (1975-2024)
-- Shows immigration trends over 50 years
SELECT
    t.year,
    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
    MAX(CASE WHEN f.indicator_id = 70 THEN f.value END) as german_population,
    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_population,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 71 THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
        2
    ) as foreign_percentage
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (67, 70, 71)  -- Total, German, Foreign
  AND g.region_code = '05'
  AND f.age_group = 'total'
GROUP BY t.year
ORDER BY t.year;


-- Query 4: Age Group Distribution (Latest Year - 2024)
-- Shows population breakdown by age groups for most recent year
SELECT
    COALESCE(f.age_group, 'Unknown') as age_group,
    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total,
    MAX(CASE WHEN f.indicator_id = 68 THEN f.value END) as male,
    MAX(CASE WHEN f.indicator_id = 69 THEN f.value END) as female,
    MAX(CASE WHEN f.indicator_id = 70 THEN f.value END) as german,
    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_pop,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 67 THEN f.value END)::numeric /
        NULLIF(
            SUM(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END))
            FILTER (WHERE f.age_group != 'total')
            OVER (),
            0
        ) * 100,
        2
    ) as percent_of_total
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 67 AND 71
  AND g.region_code = '05'
  AND t.year = 2024
GROUP BY f.age_group
ORDER BY
    CASE f.age_group
        WHEN 'total' THEN 0
        WHEN 'under_6' THEN 1
        WHEN '6_to_18' THEN 2
        WHEN '18_to_25' THEN 3
        WHEN '25_to_30' THEN 4
        WHEN '30_to_40' THEN 5
        WHEN '40_to_50' THEN 6
        WHEN '50_to_60' THEN 7
        WHEN '60_to_65' THEN 8
        WHEN '65_plus' THEN 9
        ELSE 99
    END;


-- Query 5: Major Historical Milestones
-- Compare population at key historical moments
SELECT
    t.year,
    CASE t.year
        WHEN 1975 THEN 'Post Oil Crisis'
        WHEN 1980 THEN 'Economic Recession'
        WHEN 1990 THEN 'German Reunification'
        WHEN 2000 THEN 'New Millennium'
        WHEN 2008 THEN 'Financial Crisis'
        WHEN 2015 THEN 'Refugee Crisis'
        WHEN 2020 THEN 'COVID-19 Pandemic'
        WHEN 2024 THEN 'Current'
        ELSE NULL
    END as milestone,
    MAX(CASE WHEN f.indicator_id = 67 THEN f.value END) as total_population,
    MAX(CASE WHEN f.indicator_id = 71 THEN f.value END) as foreign_population,
    ROUND(
        MAX(CASE WHEN f.indicator_id = 71 THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.indicator_id = 67 THEN f.value END), 0) * 100,
        2
    ) as foreign_pct
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id IN (67, 71)
  AND g.region_code = '05'
  AND f.age_group = 'total'
  AND t.year IN (1975, 1980, 1990, 2000, 2008, 2015, 2020, 2024)
GROUP BY t.year
ORDER BY t.year;


-- Query 6: Long-term Growth Analysis (5-year periods)
-- Shows population change over 5-year periods
WITH five_year_data AS (
    SELECT
        t.year,
        f.value as population
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 67
      AND g.region_code = '05'
      AND f.age_group = 'total'
      AND t.year % 5 = 0  -- Every 5 years
)
SELECT
    year as period_start,
    year + 5 as period_end,
    population as start_population,
    LEAD(population) OVER (ORDER BY year) as end_population,
    LEAD(population) OVER (ORDER BY year) - population as absolute_change,
    ROUND(
        (LEAD(population) OVER (ORDER BY year) - population)::numeric /
        NULLIF(population, 0) * 100,
        2
    ) as percent_change
FROM five_year_data
WHERE LEAD(population) OVER (ORDER BY year) IS NOT NULL
ORDER BY year;


-- Query 7: Aging Population Analysis
-- Compare youth vs elderly population over time
SELECT
    t.year,
    MAX(CASE WHEN f.age_group = 'total' THEN f.value END) as total_pop,
    MAX(CASE WHEN f.age_group IN ('under_6', '6_to_18') THEN f.value END) as youth_pop,
    MAX(CASE WHEN f.age_group = '65_plus' THEN f.value END) as elderly_pop,
    ROUND(
        MAX(CASE WHEN f.age_group IN ('under_6', '6_to_18') THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.age_group = 'total' THEN f.value END), 0) * 100,
        2
    ) as youth_percentage,
    ROUND(
        MAX(CASE WHEN f.age_group = '65_plus' THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.age_group = 'total' THEN f.value END), 0) * 100,
        2
    ) as elderly_percentage,
    ROUND(
        MAX(CASE WHEN f.age_group = '65_plus' THEN f.value END)::numeric /
        NULLIF(MAX(CASE WHEN f.age_group IN ('under_6', '6_to_18') THEN f.value END), 0),
        2
    ) as elderly_youth_ratio
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 67
  AND g.region_code = '05'
  AND t.year % 5 = 0  -- Every 5 years
GROUP BY t.year
ORDER BY t.year;


-- ============================================================================
-- USAGE NOTES:
-- ============================================================================
--
-- Run these queries using:
--   psql $DATABASE_URL -f sql/queries/nrw_population_history_1975_2024.sql
--
-- Or from Python:
--   from utils.database import get_database
--   db = get_database('regional_economics')
--   results = db.execute_query(query_string)
--
-- Key Points:
-- - Indicator 67: Total Population by Age Group
-- - Indicator 68: Male Population by Age Group
-- - Indicator 69: Female Population by Age Group
-- - Indicator 70: German Population by Age Group
-- - Indicator 71: Foreign Population by Age Group
--
-- - All data is for NRW state level (region_code = '05')
-- - Time range: 1975-2024 (50 years)
-- - For city-level data, use Regional DB (indicator 1) which has 2011-2024
-- ============================================================================
