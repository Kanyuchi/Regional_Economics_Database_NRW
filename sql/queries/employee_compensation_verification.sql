-- =============================================================================
-- Employee Compensation (Arbeitnehmerentgelt) Verification Script
-- Table: 82711-06i (State Database NRW / Landesdatenbank)
--
-- Description: Employee remuneration by economic sector (7) of the WZ 2008
--              and per employee - independent cities and districts - year
--
-- Indicators: 41-51
-- Period: 2000-2022 (extracted), 2000-2023 (available)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SECTION 1: BASIC VERIFICATION
-- -----------------------------------------------------------------------------

-- Test 1.1: Check if all Employee Compensation indicators exist
SELECT
    indicator_id,
    indicator_code,
    indicator_name,
    indicator_name_en,
    unit_of_measure
FROM dim_indicator
WHERE indicator_id BETWEEN 41 AND 55
ORDER BY indicator_id;

-- Test 1.2: Count total compensation records
SELECT
    COUNT(*) as total_compensation_records,
    COUNT(DISTINCT indicator_id) as indicator_count,
    COUNT(DISTINCT geo_id) as region_count
FROM fact_demographics
WHERE indicator_id BETWEEN 41 AND 55;

-- Test 1.3: Records per indicator
SELECT
    di.indicator_id,
    di.indicator_code,
    di.indicator_name,
    COUNT(fd.fact_id) as record_count,
    MIN(dt.year) as first_year,
    MAX(dt.year) as last_year
FROM dim_indicator di
LEFT JOIN fact_demographics fd ON di.indicator_id = fd.indicator_id
LEFT JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE di.indicator_id BETWEEN 41 AND 55
GROUP BY di.indicator_id, di.indicator_code, di.indicator_name
ORDER BY di.indicator_id;

-- -----------------------------------------------------------------------------
-- SECTION 2: TEMPORAL COVERAGE
-- -----------------------------------------------------------------------------

-- Test 2.1: Year range and coverage
SELECT
    MIN(dt.year) as first_year,
    MAX(dt.year) as last_year,
    COUNT(DISTINCT dt.year) as year_count,
    MAX(dt.year) - MIN(dt.year) + 1 as expected_years
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 41 AND 55;

-- Test 2.2: Records per year
SELECT
    dt.year,
    COUNT(*) as records,
    COUNT(DISTINCT fd.indicator_id) as indicators_present,
    COUNT(DISTINCT fd.geo_id) as regions_present
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 41 AND 55
GROUP BY dt.year
ORDER BY dt.year;

-- -----------------------------------------------------------------------------
-- SECTION 3: GEOGRAPHIC COVERAGE
-- -----------------------------------------------------------------------------

-- Test 3.1: Count unique regions
SELECT
    COUNT(DISTINCT geo_id) as total_regions
FROM fact_demographics
WHERE indicator_id BETWEEN 41 AND 55;

-- Test 3.2: Ruhr cities compensation coverage
SELECT
    dg.region_name,
    dg.region_code,
    COUNT(*) as total_records,
    COUNT(DISTINCT fd.indicator_id) as indicators,
    MIN(dt.year) as from_year,
    MAX(dt.year) as to_year
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 41 AND 55
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name, dg.region_code
ORDER BY dg.region_name;

-- -----------------------------------------------------------------------------
-- SECTION 4: TOTAL EMPLOYEE COMPENSATION (Indicator 41)
-- -----------------------------------------------------------------------------

-- Test 4.1: Total compensation time series for NRW major cities (2018-2022)
SELECT
    dg.region_name,
    dt.year,
    fd.value as compensation_mio_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 41
    AND dt.year >= 2018
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513', '05111', '05315')
ORDER BY dg.region_name, dt.year;

-- Test 4.2: Top 10 regions by total compensation (latest year)
SELECT
    dg.region_name,
    dg.region_code,
    fd.value as compensation_mio_eur,
    dt.year
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 41
    AND dt.year = (SELECT MAX(year) FROM dim_time dt2
                   JOIN fact_demographics fd2 ON dt2.time_id = fd2.time_id
                   WHERE fd2.indicator_id = 41)
ORDER BY fd.value DESC
LIMIT 10;

-- Test 4.3: Compensation growth comparison (Ruhr cities 2000 vs latest)
WITH compensation_comparison AS (
    SELECT
        dg.region_name,
        dg.region_code,
        MAX(CASE WHEN dt.year = 2000 THEN fd.value END) as comp_2000,
        MAX(CASE WHEN dt.year = 2022 THEN fd.value END) as comp_2022
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id = 41
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name, dg.region_code
)
SELECT
    region_name,
    ROUND(comp_2000::numeric, 0) as compensation_2000_mio,
    ROUND(comp_2022::numeric, 0) as compensation_2022_mio,
    ROUND(((comp_2022 - comp_2000) / comp_2000 * 100)::numeric, 1) as growth_pct
FROM compensation_comparison
WHERE comp_2000 IS NOT NULL AND comp_2022 IS NOT NULL
ORDER BY growth_pct DESC;

-- -----------------------------------------------------------------------------
-- SECTION 5: SECTORAL ANALYSIS
-- -----------------------------------------------------------------------------

-- Test 5.1: Compensation by sector for Essen (latest year)
SELECT
    di.indicator_id,
    di.indicator_name,
    di.indicator_name_en,
    fd.value as compensation_mio_eur
FROM fact_demographics fd
JOIN dim_indicator di ON fd.indicator_id = di.indicator_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 41 AND 51
    AND dg.region_code = '05113'  -- Essen
    AND dt.year = 2022
ORDER BY di.indicator_id;

-- Test 5.2: Services vs Manufacturing compensation share by region (2022)
WITH sector_shares AS (
    SELECT
        dg.region_name,
        MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) as comp_total,
        MAX(CASE WHEN fd.indicator_id = 43 THEN fd.value END) as comp_manufacturing,
        MAX(CASE WHEN fd.indicator_id = 47 THEN fd.value END) as comp_services
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id IN (41, 43, 47)
        AND dt.year = 2022
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name
)
SELECT
    region_name,
    ROUND(comp_total::numeric, 0) as total_compensation_mio,
    ROUND((comp_manufacturing / comp_total * 100)::numeric, 1) as manufacturing_share_pct,
    ROUND((comp_services / comp_total * 100)::numeric, 1) as services_share_pct
FROM sector_shares
WHERE comp_total > 0
ORDER BY services_share_pct DESC;

-- -----------------------------------------------------------------------------
-- SECTION 6: GDP VS COMPENSATION ANALYSIS (Labor Share)
-- -----------------------------------------------------------------------------

-- Test 6.1: Labor share of GDP by region (2022)
-- Note: Requires both GDP (indicator 29) and Compensation (indicator 41)
WITH labor_share AS (
    SELECT
        dg.region_name,
        dg.region_code,
        MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END) as gdp,
        MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) as compensation
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id IN (29, 41)
        AND dt.year = 2022
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name, dg.region_code
)
SELECT
    region_name,
    ROUND(gdp::numeric, 0) as gdp_mio_eur,
    ROUND(compensation::numeric, 0) as compensation_mio_eur,
    ROUND((compensation / gdp * 100)::numeric, 1) as labor_share_pct
FROM labor_share
WHERE gdp > 0 AND compensation > 0
ORDER BY labor_share_pct DESC;

-- Test 6.2: Labor share trend over time for Essen
SELECT
    dt.year,
    MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END) as gdp,
    MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) as compensation,
    ROUND((MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) /
           NULLIF(MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END), 0) * 100)::numeric, 1) as labor_share_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (29, 41)
    AND dg.region_code = '05113'  -- Essen
    AND dt.year >= 2000
GROUP BY dt.year
HAVING MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END) IS NOT NULL
   AND MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) IS NOT NULL
ORDER BY dt.year;

-- -----------------------------------------------------------------------------
-- SECTION 7: CRISIS IMPACT ANALYSIS
-- -----------------------------------------------------------------------------

-- Test 7.1: Financial Crisis Impact (2008-2010) on Ruhr cities compensation
SELECT
    dg.region_name,
    MAX(CASE WHEN dt.year = 2008 THEN fd.value END) as comp_2008,
    MAX(CASE WHEN dt.year = 2009 THEN fd.value END) as comp_2009,
    MAX(CASE WHEN dt.year = 2010 THEN fd.value END) as comp_2010,
    ROUND(((MAX(CASE WHEN dt.year = 2009 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2008 THEN fd.value END)) /
            NULLIF(MAX(CASE WHEN dt.year = 2008 THEN fd.value END), 0) * 100)::numeric, 2) as change_2008_2009_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 41
    AND dt.year BETWEEN 2008 AND 2010
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name
ORDER BY change_2008_2009_pct;

-- Test 7.2: COVID-19 Impact (2019-2021) on Ruhr cities compensation
SELECT
    dg.region_name,
    MAX(CASE WHEN dt.year = 2019 THEN fd.value END) as comp_2019,
    MAX(CASE WHEN dt.year = 2020 THEN fd.value END) as comp_2020,
    MAX(CASE WHEN dt.year = 2021 THEN fd.value END) as comp_2021,
    ROUND(((MAX(CASE WHEN dt.year = 2020 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2019 THEN fd.value END)) /
            NULLIF(MAX(CASE WHEN dt.year = 2019 THEN fd.value END), 0) * 100)::numeric, 2) as change_2019_2020_pct,
    ROUND(((MAX(CASE WHEN dt.year = 2021 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2020 THEN fd.value END)) /
            NULLIF(MAX(CASE WHEN dt.year = 2020 THEN fd.value END), 0) * 100)::numeric, 2) as recovery_2020_2021_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 41
    AND dt.year BETWEEN 2019 AND 2021
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name
ORDER BY change_2019_2020_pct;

-- -----------------------------------------------------------------------------
-- SECTION 8: STRUCTURAL CHANGE ANALYSIS
-- -----------------------------------------------------------------------------

-- Test 8.1: Manufacturing compensation trend in Ruhr area (2000-2022)
SELECT
    dg.region_name,
    dt.year,
    fd.value as manufacturing_compensation_mio
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 43  -- Manufacturing (B-E)
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    AND dt.year IN (2000, 2005, 2010, 2015, 2020, 2022)
ORDER BY dg.region_name, dt.year;

-- Test 8.2: Services sector compensation growth
WITH services_growth AS (
    SELECT
        dg.region_name,
        MAX(CASE WHEN dt.year = 2000 THEN fd.value END) as services_2000,
        MAX(CASE WHEN dt.year = 2022 THEN fd.value END) as services_2022
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id = 47  -- Services (G-U)
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name
)
SELECT
    region_name,
    ROUND(services_2000::numeric, 0) as services_2000_mio,
    ROUND(services_2022::numeric, 0) as services_2022_mio,
    ROUND(((services_2022 - services_2000) / NULLIF(services_2000, 0) * 100)::numeric, 1) as growth_pct
FROM services_growth
WHERE services_2000 > 0
ORDER BY growth_pct DESC;

-- -----------------------------------------------------------------------------
-- SECTION 9: STATISTICS SUMMARY
-- -----------------------------------------------------------------------------

-- Test 9.1: Value statistics per indicator
SELECT
    di.indicator_id,
    di.indicator_code,
    COUNT(fd.fact_id) as records,
    ROUND(MIN(fd.value)::numeric, 2) as min_value,
    ROUND(MAX(fd.value)::numeric, 2) as max_value,
    ROUND(AVG(fd.value)::numeric, 2) as avg_value,
    ROUND(STDDEV(fd.value)::numeric, 2) as stddev_value
FROM fact_demographics fd
JOIN dim_indicator di ON fd.indicator_id = di.indicator_id
WHERE fd.indicator_id BETWEEN 41 AND 55
GROUP BY di.indicator_id, di.indicator_code
ORDER BY di.indicator_id;

-- Test 9.2: Data quality check - NULL values
SELECT
    indicator_id,
    COUNT(*) as total_records,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values,
    ROUND(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as null_pct
FROM fact_demographics
WHERE indicator_id BETWEEN 41 AND 55
GROUP BY indicator_id
ORDER BY indicator_id;

-- =============================================================================
-- SECTION 10: COMBINED ANALYSIS (GDP + Compensation + Municipal Finances)
-- =============================================================================

-- Test 10.1: Comprehensive economic profile for Ruhr cities (2022)
SELECT
    dg.region_name,
    -- GDP
    MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END) as gdp_mio_eur,
    -- Total Compensation
    MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) as compensation_mio_eur,
    -- Labor share
    ROUND((MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END) /
           NULLIF(MAX(CASE WHEN fd.indicator_id = 29 THEN fd.value END), 0) * 100)::numeric, 1) as labor_share_pct,
    -- Services share of compensation
    ROUND((MAX(CASE WHEN fd.indicator_id = 47 THEN fd.value END) /
           NULLIF(MAX(CASE WHEN fd.indicator_id = 41 THEN fd.value END), 0) * 100)::numeric, 1) as services_comp_share_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (29, 41, 47)
    AND dt.year = 2022
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name
ORDER BY gdp_mio_eur DESC;

-- =============================================================================
-- END OF EMPLOYEE COMPENSATION VERIFICATION SCRIPT
-- =============================================================================
