-- ============================================================================
-- CONSTRUCTION INDUSTRY DATA ANALYSIS QUERIES
-- Table: 44231-01-03-4 (Indicator ID: 20)
-- Database: regional_economics
-- Created: December 18, 2025
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
    COUNT(DISTINCT t.year) as years_covered
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20;

-- 1.2 Data summary by year
SELECT 
    t.year,
    COUNT(*) as regions,
    ROUND(AVG(f.value)::numeric, 0) as avg_employees,
    MIN(f.value) as min_employees,
    MAX(f.value) as max_employees,
    SUM(f.value) as total_employees
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
GROUP BY t.year
ORDER BY t.year;

-- ============================================================================
-- 2. RUHR REGION CITIES - TIME SERIES
-- ============================================================================

-- 2.1 Full time series for all 5 Ruhr cities
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.value as construction_employees,
    f.notes
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;

-- 2.2 Ruhr cities summary statistics
SELECT 
    g.region_name as city,
    g.region_code,
    COUNT(*) as years_of_data,
    MIN(t.year) as first_year,
    MAX(t.year) as last_year,
    ROUND(AVG(f.value)::numeric, 0) as mean_employees,
    MIN(f.value) as min_employees,
    MAX(f.value) as max_employees
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name, g.region_code
ORDER BY mean_employees DESC;

-- ============================================================================
-- 3. TREND ANALYSIS - FIRST VS LAST YEAR
-- ============================================================================

-- 3.1 Change from 1995 to 2024 for Ruhr cities
WITH first_year AS (
    SELECT g.region_name, g.region_code, f.value as employees_1995
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND t.year = 1995
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
),
last_year AS (
    SELECT g.region_name, g.region_code, f.value as employees_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND t.year = 2024
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    fy.region_name as city,
    fy.region_code,
    fy.employees_1995,
    ly.employees_2024,
    (ly.employees_2024 - fy.employees_1995) as absolute_change,
    ROUND(((ly.employees_2024 - fy.employees_1995) / fy.employees_1995 * 100)::numeric, 1) as percent_change
FROM first_year fy
JOIN last_year ly ON fy.region_code = ly.region_code
ORDER BY percent_change;

-- 3.2 Change for ALL NRW regions (top 10 declines)
WITH first_year AS (
    SELECT g.region_name, g.region_code, f.value as employees_1995
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20 AND t.year = 1995
),
last_year AS (
    SELECT g.region_name, g.region_code, f.value as employees_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20 AND t.year = 2024
)
SELECT 
    fy.region_name,
    fy.employees_1995,
    ly.employees_2024,
    (ly.employees_2024 - fy.employees_1995) as absolute_change,
    ROUND(((ly.employees_2024 - fy.employees_1995) / NULLIF(fy.employees_1995, 0) * 100)::numeric, 1) as percent_change
FROM first_year fy
JOIN last_year ly ON fy.region_code = ly.region_code
WHERE fy.employees_1995 > 0
ORDER BY percent_change
LIMIT 10;

-- ============================================================================
-- 4. YEAR-OVER-YEAR CHANGES
-- ============================================================================

-- 4.1 Year-over-year change for a specific city (Dortmund)
WITH yearly_data AS (
    SELECT 
        t.year,
        f.value as employees,
        LAG(f.value) OVER (ORDER BY t.year) as prev_year_employees
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND g.region_code = '05913'  -- Dortmund
)
SELECT 
    year,
    employees,
    prev_year_employees,
    (employees - prev_year_employees) as yoy_change,
    ROUND(((employees - prev_year_employees) / NULLIF(prev_year_employees, 0) * 100)::numeric, 1) as yoy_percent
FROM yearly_data
WHERE prev_year_employees IS NOT NULL
ORDER BY year;

-- 4.2 Peak year for each Ruhr city
WITH ranked AS (
    SELECT 
        g.region_name,
        t.year,
        f.value as employees,
        RANK() OVER (PARTITION BY g.geo_id ORDER BY f.value DESC) as rank
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT region_name as city, year as peak_year, employees as peak_employees
FROM ranked
WHERE rank = 1
ORDER BY peak_employees DESC;

-- ============================================================================
-- 5. COMPARATIVE ANALYSIS
-- ============================================================================

-- 5.1 Ruhr cities vs NRW total by decade
SELECT 
    g.region_name,
    CASE 
        WHEN t.year BETWEEN 1995 AND 1999 THEN '1995-1999'
        WHEN t.year BETWEEN 2000 AND 2009 THEN '2000-2009'
        WHEN t.year BETWEEN 2010 AND 2019 THEN '2010-2019'
        WHEN t.year >= 2020 THEN '2020-2024'
    END as decade,
    ROUND(AVG(f.value)::numeric, 0) as avg_employees
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name, decade
ORDER BY g.region_name, decade;

-- 5.2 Share of NRW construction employment by city (latest year)
WITH nrw_total AS (
    SELECT SUM(f.value) as total
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND t.year = 2024
      AND g.region_code LIKE '05%'
      AND LENGTH(g.region_code) = 5  -- Only districts, not NRW total
)
SELECT 
    g.region_name as city,
    f.value as employees,
    ROUND((f.value / nt.total * 100)::numeric, 2) as share_of_nrw_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
CROSS JOIN nrw_total nt
WHERE f.indicator_id = 20
  AND t.year = 2024
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY employees DESC;

-- ============================================================================
-- 6. SPECIFIC QUERIES FOR THESIS RESEARCH
-- ============================================================================

-- 6.1 Pre/Post German Reunification comparison (1995 vs 2000)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 1995 THEN f.value END) as emp_1995,
    MAX(CASE WHEN t.year = 2000 THEN f.value END) as emp_2000,
    MAX(CASE WHEN t.year = 2000 THEN f.value END) - MAX(CASE WHEN t.year = 1995 THEN f.value END) as change_95_00
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year IN (1995, 2000)
GROUP BY g.region_name
ORDER BY change_95_00;

-- 6.2 Financial Crisis impact (2007-2010)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 2007 THEN f.value END) as emp_2007,
    MAX(CASE WHEN t.year = 2008 THEN f.value END) as emp_2008,
    MAX(CASE WHEN t.year = 2009 THEN f.value END) as emp_2009,
    MAX(CASE WHEN t.year = 2010 THEN f.value END) as emp_2010,
    ROUND(((MAX(CASE WHEN t.year = 2010 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2007 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2007 THEN f.value END), 0) * 100)::numeric, 1) as change_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2007 AND 2010
GROUP BY g.region_name
ORDER BY change_pct;

-- 6.3 COVID-19 impact (2019-2021)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 2019 THEN f.value END) as emp_2019,
    MAX(CASE WHEN t.year = 2020 THEN f.value END) as emp_2020,
    MAX(CASE WHEN t.year = 2021 THEN f.value END) as emp_2021,
    ROUND(((MAX(CASE WHEN t.year = 2021 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2019 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2019 THEN f.value END), 0) * 100)::numeric, 1) as change_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2019 AND 2021
GROUP BY g.region_name
ORDER BY change_pct;

-- 6.4 Recovery analysis - decline periods
WITH yearly AS (
    SELECT 
        g.region_name,
        t.year,
        f.value as employees,
        LAG(f.value) OVER (PARTITION BY g.geo_id ORDER BY t.year) as prev_year
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    region_name as city,
    COUNT(*) FILTER (WHERE employees < prev_year) as years_of_decline,
    COUNT(*) FILTER (WHERE employees > prev_year) as years_of_growth,
    COUNT(*) FILTER (WHERE employees = prev_year) as years_unchanged,
    COUNT(*) - 1 as total_years  -- minus 1 for first year with no comparison
FROM yearly
WHERE prev_year IS NOT NULL
GROUP BY region_name
ORDER BY years_of_decline DESC;

-- ============================================================================
-- 7. EXPORT-READY QUERIES
-- ============================================================================

-- 7.1 Full dataset for Ruhr cities (copy to CSV)
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.value as construction_employees,
    f.notes,
    f.extracted_at
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;

-- 7.2 Pivot table format for visualization (Ruhr cities by year)
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
WHERE f.indicator_id = 20
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY t.year
ORDER BY t.year;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
