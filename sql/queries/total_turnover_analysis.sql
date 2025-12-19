-- ============================================================================
-- TOTAL TURNOVER DATA ANALYSIS QUERIES
-- Table: 44231-01-02-4 (Indicator ID: 21)
-- Database: regional_economics
-- Created: December 18, 2025
-- ============================================================================
-- Note: This table covers ALL businesses (not just construction)
-- It includes more businesses and employees than the construction-only table
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
WHERE f.indicator_id = 21;

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
WHERE f.indicator_id = 21
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
    f.value as total_employees,
    f.notes
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 21
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
WHERE f.indicator_id = 21
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
    WHERE f.indicator_id = 21
      AND t.year = 1995
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
),
last_year AS (
    SELECT g.region_name, g.region_code, f.value as employees_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 21
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

-- ============================================================================
-- 4. COMPARISON: CONSTRUCTION vs TOTAL TURNOVER
-- ============================================================================

-- 4.1 Construction employees vs Total employees (latest year)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN f.indicator_id = 20 THEN f.value END) as construction_employees,
    MAX(CASE WHEN f.indicator_id = 21 THEN f.value END) as total_employees,
    ROUND((MAX(CASE WHEN f.indicator_id = 20 THEN f.value END) / 
           NULLIF(MAX(CASE WHEN f.indicator_id = 21 THEN f.value END), 0) * 100)::numeric, 1) 
           as construction_share_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (20, 21)
  AND t.year = 2024
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name
ORDER BY construction_share_pct DESC;

-- 4.2 Construction share evolution over time (Ruhr cities)
SELECT 
    t.year,
    g.region_name as city,
    MAX(CASE WHEN f.indicator_id = 20 THEN f.value END) as construction,
    MAX(CASE WHEN f.indicator_id = 21 THEN f.value END) as total,
    ROUND((MAX(CASE WHEN f.indicator_id = 20 THEN f.value END) / 
           NULLIF(MAX(CASE WHEN f.indicator_id = 21 THEN f.value END), 0) * 100)::numeric, 1) 
           as constr_share_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (20, 21)
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY t.year, g.region_name
ORDER BY city, year;

-- 4.3 Decline comparison: Construction vs Total (1995-2024)
WITH constr AS (
    SELECT 
        g.region_name,
        MAX(CASE WHEN t.year = 1995 THEN f.value END) as emp_1995,
        MAX(CASE WHEN t.year = 2024 THEN f.value END) as emp_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 20
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
    GROUP BY g.region_name
),
total AS (
    SELECT 
        g.region_name,
        MAX(CASE WHEN t.year = 1995 THEN f.value END) as emp_1995,
        MAX(CASE WHEN t.year = 2024 THEN f.value END) as emp_2024
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 21
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
    GROUP BY g.region_name
)
SELECT 
    c.region_name as city,
    ROUND(((c.emp_2024 - c.emp_1995) / c.emp_1995 * 100)::numeric, 1) as construction_change_pct,
    ROUND(((t.emp_2024 - t.emp_1995) / t.emp_1995 * 100)::numeric, 1) as total_change_pct,
    ROUND((((c.emp_2024 - c.emp_1995) / c.emp_1995) - 
           ((t.emp_2024 - t.emp_1995) / t.emp_1995)) * 100::numeric, 1) as difference_points
FROM constr c
JOIN total t ON c.region_name = t.region_name
ORDER BY construction_change_pct;

-- ============================================================================
-- 5. YEAR-OVER-YEAR CHANGES
-- ============================================================================

-- 5.1 Year-over-year change for a specific city (Dortmund)
WITH yearly_data AS (
    SELECT 
        t.year,
        f.value as employees,
        LAG(f.value) OVER (ORDER BY t.year) as prev_year_employees
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 21
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

-- 5.2 Peak year for each Ruhr city
WITH ranked AS (
    SELECT 
        g.region_name,
        t.year,
        f.value as employees,
        RANK() OVER (PARTITION BY g.geo_id ORDER BY f.value DESC) as rank
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 21
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT region_name as city, year as peak_year, employees as peak_employees
FROM ranked
WHERE rank = 1
ORDER BY peak_employees DESC;

-- ============================================================================
-- 6. COMPARATIVE ANALYSIS BY DECADE
-- ============================================================================

-- 6.1 Ruhr cities employment by decade
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
WHERE f.indicator_id = 21
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name, decade
ORDER BY g.region_name, decade;

-- ============================================================================
-- 7. SPECIFIC EVENTS ANALYSIS
-- ============================================================================

-- 7.1 Financial Crisis impact (2007-2010)
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
WHERE f.indicator_id = 21
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2007 AND 2010
GROUP BY g.region_name
ORDER BY change_pct;

-- 7.2 COVID-19 impact (2019-2021)
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
WHERE f.indicator_id = 21
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2019 AND 2021
GROUP BY g.region_name
ORDER BY change_pct;

-- ============================================================================
-- 8. EXPORT-READY QUERIES
-- ============================================================================

-- 8.1 Full dataset for Ruhr cities (copy to CSV)
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.value as total_employees,
    f.notes,
    f.extracted_at
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 21
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;

-- 8.2 Pivot table format for visualization (Ruhr cities by year)
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
WHERE f.indicator_id = 21
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY t.year
ORDER BY t.year;

-- 8.3 Side-by-side comparison: Construction vs Total
SELECT 
    t.year,
    MAX(CASE WHEN f.indicator_id = 20 AND g.region_code = '05913' THEN f.value END) as dortmund_construction,
    MAX(CASE WHEN f.indicator_id = 21 AND g.region_code = '05913' THEN f.value END) as dortmund_total,
    MAX(CASE WHEN f.indicator_id = 20 AND g.region_code = '05113' THEN f.value END) as essen_construction,
    MAX(CASE WHEN f.indicator_id = 21 AND g.region_code = '05113' THEN f.value END) as essen_total,
    MAX(CASE WHEN f.indicator_id = 20 AND g.region_code = '05112' THEN f.value END) as duisburg_construction,
    MAX(CASE WHEN f.indicator_id = 21 AND g.region_code = '05112' THEN f.value END) as duisburg_total
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id IN (20, 21)
  AND g.region_code IN ('05112', '05113', '05913')
GROUP BY t.year
ORDER BY t.year;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
