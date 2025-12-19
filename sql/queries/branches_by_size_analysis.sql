-- ============================================================================
-- BRANCHES BY EMPLOYEE SIZE CLASS ANALYSIS
-- Table: 52111-01-02-4 (Indicator ID: 22)
-- Database: regional_economics
-- Created: December 18, 2025
-- ============================================================================
-- Note: Data available 2019-2023 (5 years, recent period only)
-- Size classes: 0-10, 10-50, 50-250, 250+, and Total
-- ============================================================================

-- ============================================================================
-- 1. BASIC OVERVIEW
-- ============================================================================

-- 1.1 Total records and coverage
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT g.geo_id) as regions,
    MIN(t.year) as first_year,
    MAX(t.year) as last_year,
    COUNT(DISTINCT t.year) as years_covered,
    COUNT(DISTINCT f.notes) as size_classes
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22;

-- 1.2 Data summary by year and size class
SELECT 
    t.year,
    f.notes as size_class,
    COUNT(*) as regions,
    SUM(f.value) as total_branches,
    ROUND(AVG(f.value)::numeric, 0) as avg_per_region
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
GROUP BY t.year, f.notes
ORDER BY t.year, 
    CASE 
        WHEN f.notes = 'Total - all size classes' THEN 1
        WHEN f.notes = '0 to <10 employees' THEN 2
        WHEN f.notes = '10 to <50 employees' THEN 3
        WHEN f.notes = '50 to <250 employees' THEN 4
        WHEN f.notes = '250+ employees' THEN 5
    END;

-- ============================================================================
-- 2. RUHR REGION CITIES - BY SIZE CLASS
-- ============================================================================

-- 2.1 Full time series for all 5 Ruhr cities
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.notes as size_class,
    f.value as branches
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year, 
    CASE 
        WHEN f.notes = 'Total - all size classes' THEN 1
        WHEN f.notes = '0 to <10 employees' THEN 2
        WHEN f.notes = '10 to <50 employees' THEN 3
        WHEN f.notes = '50 to <250 employees' THEN 4
        WHEN f.notes = '250+ employees' THEN 5
    END;

-- 2.2 Total branches per city over time
SELECT 
    g.region_name as city,
    t.year,
    f.value as total_branches
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND f.notes = 'Total - all size classes'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;

-- ============================================================================
-- 3. SIZE CLASS DISTRIBUTION ANALYSIS
-- ============================================================================

-- 3.1 Size class composition for Ruhr cities (latest year)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN f.notes = 'Total - all size classes' THEN f.value END) as total,
    MAX(CASE WHEN f.notes = '0 to <10 employees' THEN f.value END) as micro,
    MAX(CASE WHEN f.notes = '10 to <50 employees' THEN f.value END) as small,
    MAX(CASE WHEN f.notes = '50 to <250 employees' THEN f.value END) as medium,
    MAX(CASE WHEN f.notes = '250+ employees' THEN f.value END) as large,
    ROUND((MAX(CASE WHEN f.notes = '0 to <10 employees' THEN f.value END) / 
           NULLIF(MAX(CASE WHEN f.notes = 'Total - all size classes' THEN f.value END), 0) * 100)::numeric, 1) 
           as micro_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND t.year = 2023
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name
ORDER BY total DESC;

-- 3.2 Size class shares evolution (first vs last year)
WITH first_year AS (
    SELECT 
        g.region_name,
        f.notes,
        f.value as branches_2019
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 22
      AND t.year = 2019
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
),
last_year AS (
    SELECT 
        g.region_name,
        f.notes,
        f.value as branches_2023
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 22
      AND t.year = 2023
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    fy.region_name as city,
    fy.notes as size_class,
    fy.branches_2019,
    ly.branches_2023,
    (ly.branches_2023 - fy.branches_2019) as absolute_change,
    ROUND(((ly.branches_2023 - fy.branches_2019) / 
           NULLIF(fy.branches_2019, 0) * 100)::numeric, 1) as percent_change
FROM first_year fy
JOIN last_year ly ON fy.region_name = ly.region_name AND fy.notes = ly.notes
ORDER BY fy.region_name, 
    CASE 
        WHEN fy.notes = 'Total - all size classes' THEN 1
        WHEN fy.notes = '0 to <10 employees' THEN 2
        WHEN fy.notes = '10 to <50 employees' THEN 3
        WHEN fy.notes = '50 to <250 employees' THEN 4
        WHEN fy.notes = '250+ employees' THEN 5
    END;

-- ============================================================================
-- 4. YEAR-OVER-YEAR CHANGES
-- ============================================================================

-- 4.1 Year-over-year growth for total branches (Ruhr cities)
WITH yearly_data AS (
    SELECT 
        g.region_name,
        t.year,
        f.value as branches,
        LAG(f.value) OVER (PARTITION BY g.region_name ORDER BY t.year) as prev_year
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 22
      AND f.notes = 'Total - all size classes'
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    region_name as city,
    year,
    branches,
    prev_year,
    (branches - prev_year) as yoy_change,
    ROUND(((branches - prev_year) / NULLIF(prev_year, 0) * 100)::numeric, 1) as yoy_percent
FROM yearly_data
WHERE prev_year IS NOT NULL
ORDER BY region_name, year;

-- ============================================================================
-- 5. COMPARATIVE ANALYSIS
-- ============================================================================

-- 5.1 NRW average vs Ruhr cities (latest year)
WITH nrw_avg AS (
    SELECT 
        f.notes,
        AVG(f.value) as nrw_average
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 22
      AND t.year = 2023
      AND g.region_code LIKE '05%'
    GROUP BY f.notes
),
ruhr_cities AS (
    SELECT 
        g.region_name,
        f.notes,
        f.value
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 22
      AND t.year = 2023
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    rc.region_name as city,
    rc.notes as size_class,
    rc.value as city_value,
    ROUND(na.nrw_average::numeric, 0) as nrw_avg,
    ROUND(((rc.value - na.nrw_average) / NULLIF(na.nrw_average, 0) * 100)::numeric, 1) as vs_nrw_pct
FROM ruhr_cities rc
JOIN nrw_avg na ON rc.notes = na.notes
ORDER BY rc.region_name, 
    CASE 
        WHEN rc.notes = 'Total - all size classes' THEN 1
        WHEN rc.notes = '0 to <10 employees' THEN 2
        WHEN rc.notes = '10 to <50 employees' THEN 3
        WHEN rc.notes = '50 to <250 employees' THEN 4
        WHEN rc.notes = '250+ employees' THEN 5
    END;

-- ============================================================================
-- 6. COVID-19 IMPACT ANALYSIS (2019-2021)
-- ============================================================================

-- 6.1 COVID impact on total branches
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 2019 THEN f.value END) as branches_2019,
    MAX(CASE WHEN t.year = 2020 THEN f.value END) as branches_2020,
    MAX(CASE WHEN t.year = 2021 THEN f.value END) as branches_2021,
    ROUND(((MAX(CASE WHEN t.year = 2021 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2019 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2019 THEN f.value END), 0) * 100)::numeric, 1) as change_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND f.notes = 'Total - all size classes'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2019 AND 2021
GROUP BY g.region_name
ORDER BY change_pct;

-- 6.2 COVID impact by size class (Dortmund example)
SELECT 
    f.notes as size_class,
    MAX(CASE WHEN t.year = 2019 THEN f.value END) as y2019,
    MAX(CASE WHEN t.year = 2020 THEN f.value END) as y2020,
    MAX(CASE WHEN t.year = 2021 THEN f.value END) as y2021,
    MAX(CASE WHEN t.year = 2022 THEN f.value END) as y2022,
    MAX(CASE WHEN t.year = 2023 THEN f.value END) as y2023,
    ROUND(((MAX(CASE WHEN t.year = 2023 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2019 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2019 THEN f.value END), 0) * 100)::numeric, 1) 
           as change_2019_2023_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND g.region_code = '05913'  -- Dortmund
GROUP BY f.notes
ORDER BY CASE 
    WHEN f.notes = 'Total - all size classes' THEN 1
    WHEN f.notes = '0 to <10 employees' THEN 2
    WHEN f.notes = '10 to <50 employees' THEN 3
    WHEN f.notes = '50 to <250 employees' THEN 4
    WHEN f.notes = '250+ employees' THEN 5
END;

-- ============================================================================
-- 7. EXPORT-READY QUERIES
-- ============================================================================

-- 7.1 Full dataset for Ruhr cities (copy to CSV)
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.notes as size_class,
    f.value as branches,
    f.extracted_at
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year, 
    CASE 
        WHEN f.notes = 'Total - all size classes' THEN 1
        WHEN f.notes = '0 to <10 employees' THEN 2
        WHEN f.notes = '10 to <50 employees' THEN 3
        WHEN f.notes = '50 to <250 employees' THEN 4
        WHEN f.notes = '250+ employees' THEN 5
    END;

-- 7.2 Pivot table: Cities by year (Total branches only)
SELECT 
    t.year,
    MAX(CASE WHEN g.region_code = '05112' THEN f.value END) as duisburg,
    MAX(CASE WHEN g.region_code = '05911' THEN f.value END) as bochum,
    MAX(CASE WHEN g.region_code = '05113' THEN f.value END) as essen,
    MAX(CASE WHEN g.region_code = '05913' THEN f.value END) as dortmund,
    MAX(CASE WHEN g.region_code = '05513' THEN f.value END) as gelsenkirchen
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND f.notes = 'Total - all size classes'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY t.year
ORDER BY t.year;

-- 7.3 Pivot table: Size classes by city (latest year)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN f.notes = 'Total - all size classes' THEN f.value END) as total,
    MAX(CASE WHEN f.notes = '0 to <10 employees' THEN f.value END) as size_0_10,
    MAX(CASE WHEN f.notes = '10 to <50 employees' THEN f.value END) as size_10_50,
    MAX(CASE WHEN f.notes = '50 to <250 employees' THEN f.value END) as size_50_250,
    MAX(CASE WHEN f.notes = '250+ employees' THEN f.value END) as size_250_plus
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 22
  AND t.year = 2023
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name
ORDER BY total DESC;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
