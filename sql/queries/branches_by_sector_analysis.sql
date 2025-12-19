-- ============================================================================
-- BRANCHES BY ECONOMIC SECTOR ANALYSIS
-- Table: 52111-02-01-4 (Indicator ID: 23)
-- Database: regional_economics
-- Created: December 18, 2025
-- ============================================================================
-- Note: Data available 2006-2023 (18 years)
-- WZ 2008 Classification: 18 economic sectors (B-S)
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
    COUNT(DISTINCT f.notes) as sectors
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23;

-- 1.2 Data summary by year
SELECT 
    t.year,
    COUNT(DISTINCT g.geo_id) as regions,
    SUM(f.value) as total_branches_nrw,
    ROUND(AVG(f.value)::numeric, 0) as avg_per_region_sector
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
GROUP BY t.year
ORDER BY t.year;

-- ============================================================================
-- 2. RUHR REGION CITIES - TOTAL BRANCHES
-- ============================================================================

-- 2.1 Total branches over time (all sectors)
SELECT 
    g.region_name as city,
    t.year,
    f.value as branches
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND f.notes = 'Total - all sectors (B-N, P-S)'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;

-- 2.2 Latest year comparison (2023)
SELECT 
    g.region_name as city,
    f.value as total_branches_2023
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND f.notes = 'Total - all sectors (B-N, P-S)'
  AND t.year = 2023
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY f.value DESC;

-- ============================================================================
-- 3. SECTOR DISTRIBUTION ANALYSIS
-- ============================================================================

-- 3.1 Top sectors in Ruhr cities (2023)
SELECT 
    g.region_name as city,
    f.notes as sector,
    f.value as branches,
    ROUND((f.value / total.total_branches * 100)::numeric, 1) as pct_of_total
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN LATERAL (
    SELECT value as total_branches
    FROM fact_demographics f2
    WHERE f2.geo_id = f.geo_id 
      AND f2.time_id = f.time_id
      AND f2.indicator_id = 23
      AND f2.notes = 'Total - all sectors (B-N, P-S)'
) total ON true
WHERE f.indicator_id = 23
  AND t.year = 2023
  AND f.notes != 'Total - all sectors (B-N, P-S)'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, f.value DESC;

-- 3.2 Sector comparison across cities (2023)
SELECT 
    f.notes as sector,
    MAX(CASE WHEN g.region_code = '05112' THEN f.value END) as duisburg,
    MAX(CASE WHEN g.region_code = '05911' THEN f.value END) as bochum,
    MAX(CASE WHEN g.region_code = '05113' THEN f.value END) as essen,
    MAX(CASE WHEN g.region_code = '05913' THEN f.value END) as dortmund,
    MAX(CASE WHEN g.region_code = '05513' THEN f.value END) as gelsenkirchen
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND t.year = 2023
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY f.notes
ORDER BY CASE 
    WHEN f.notes = 'Total - all sectors (B-N, P-S)' THEN 1
    WHEN f.notes LIKE 'B -%' THEN 2
    WHEN f.notes LIKE 'C -%' THEN 3
    WHEN f.notes LIKE 'D -%' THEN 4
    WHEN f.notes LIKE 'E -%' THEN 5
    WHEN f.notes LIKE 'F -%' THEN 6
    WHEN f.notes LIKE 'G -%' THEN 7
    WHEN f.notes LIKE 'H -%' THEN 8
    WHEN f.notes LIKE 'I -%' THEN 9
    WHEN f.notes LIKE 'J -%' THEN 10
    WHEN f.notes LIKE 'K -%' THEN 11
    WHEN f.notes LIKE 'L -%' THEN 12
    WHEN f.notes LIKE 'M -%' THEN 13
    WHEN f.notes LIKE 'N -%' THEN 14
    WHEN f.notes LIKE 'P -%' THEN 15
    WHEN f.notes LIKE 'Q -%' THEN 16
    WHEN f.notes LIKE 'R -%' THEN 17
    WHEN f.notes LIKE 'S -%' THEN 18
END;

-- ============================================================================
-- 4. TREND ANALYSIS (2006-2023)
-- ============================================================================

-- 4.1 Change in total branches (first vs last year)
WITH first_year AS (
    SELECT 
        g.region_name,
        f.value as branches_2006
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 23
      AND f.notes = 'Total - all sectors (B-N, P-S)'
      AND t.year = 2006
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
),
last_year AS (
    SELECT 
        g.region_name,
        f.value as branches_2023
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 23
      AND f.notes = 'Total - all sectors (B-N, P-S)'
      AND t.year = 2023
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    fy.region_name as city,
    fy.branches_2006,
    ly.branches_2023,
    (ly.branches_2023 - fy.branches_2006) as absolute_change,
    ROUND(((ly.branches_2023 - fy.branches_2006) / 
           NULLIF(fy.branches_2006, 0) * 100)::numeric, 1) as percent_change
FROM first_year fy
JOIN last_year ly ON fy.region_name = ly.region_name
ORDER BY percent_change;

-- 4.2 Sector-level growth (2006-2023)
WITH first_year AS (
    SELECT 
        g.region_name,
        f.notes,
        f.value as branches_2006
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 23
      AND t.year = 2006
      AND f.notes != 'Total - all sectors (B-N, P-S)'
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
    WHERE f.indicator_id = 23
      AND t.year = 2023
      AND f.notes != 'Total - all sectors (B-N, P-S)'
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
)
SELECT 
    fy.region_name as city,
    LEFT(fy.notes, 1) as sector_code,
    fy.branches_2006,
    ly.branches_2023,
    (ly.branches_2023 - fy.branches_2006) as change,
    ROUND(((ly.branches_2023 - fy.branches_2006) / 
           NULLIF(fy.branches_2006, 0) * 100)::numeric, 1) as pct_change
FROM first_year fy
JOIN last_year ly ON fy.region_name = ly.region_name AND fy.notes = ly.notes
WHERE fy.branches_2006 > 0  -- Exclude zero values
ORDER BY fy.region_name, pct_change DESC;

-- ============================================================================
-- 5. FINANCIAL CRISIS & COVID IMPACT
-- ============================================================================

-- 5.1 Financial Crisis impact (2008-2010)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 2008 THEN f.value END) as y2008,
    MAX(CASE WHEN t.year = 2009 THEN f.value END) as y2009,
    MAX(CASE WHEN t.year = 2010 THEN f.value END) as y2010,
    ROUND(((MAX(CASE WHEN t.year = 2010 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2008 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2008 THEN f.value END), 0) * 100)::numeric, 1) 
           as crisis_impact_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND f.notes = 'Total - all sectors (B-N, P-S)'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
  AND t.year BETWEEN 2008 AND 2010
GROUP BY g.region_name
ORDER BY crisis_impact_pct;

-- 5.2 COVID-19 impact (2019-2021)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN t.year = 2019 THEN f.value END) as y2019,
    MAX(CASE WHEN t.year = 2020 THEN f.value END) as y2020,
    MAX(CASE WHEN t.year = 2021 THEN f.value END) as y2021,
    MAX(CASE WHEN t.year = 2023 THEN f.value END) as y2023,
    ROUND(((MAX(CASE WHEN t.year = 2021 THEN f.value END) - 
            MAX(CASE WHEN t.year = 2019 THEN f.value END)) / 
           NULLIF(MAX(CASE WHEN t.year = 2019 THEN f.value END), 0) * 100)::numeric, 1) 
           as covid_impact_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND f.notes = 'Total - all sectors (B-N, P-S)'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name
ORDER BY covid_impact_pct;

-- ============================================================================
-- 6. MANUFACTURING vs SERVICES ANALYSIS
-- ============================================================================

-- 6.1 Manufacturing vs Services split (2023)
SELECT 
    g.region_name as city,
    MAX(CASE WHEN f.notes = 'C - Manufacturing' THEN f.value END) as manufacturing,
    MAX(CASE WHEN f.notes = 'G - Trade, vehicle maintenance' THEN f.value END) as trade,
    MAX(CASE WHEN f.notes = 'J - Information and communication' THEN f.value END) as info_comm,
    MAX(CASE WHEN f.notes = 'M - Professional, scientific services' THEN f.value END) as professional,
    MAX(CASE WHEN f.notes = 'Q - Health and social work' THEN f.value END) as health
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND t.year = 2023
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY g.region_name
ORDER BY manufacturing DESC;

-- ============================================================================
-- 7. EXPORT-READY QUERIES
-- ============================================================================

-- 7.1 Full dataset for Ruhr cities (copy to CSV)
SELECT 
    g.region_name as city,
    g.region_code,
    t.year,
    f.notes as sector,
    f.value as branches
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year, f.notes;

-- 7.2 Pivot table: Total branches by city and year
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
WHERE f.indicator_id = 23
  AND f.notes = 'Total - all sectors (B-N, P-S)'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
GROUP BY t.year
ORDER BY t.year;

-- ============================================================================
-- END OF QUERIES
-- ============================================================================
