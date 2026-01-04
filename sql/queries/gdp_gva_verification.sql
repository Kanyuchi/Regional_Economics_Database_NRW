-- =============================================================================
-- GDP and Gross Value Added (GVA) Verification Script
-- Table: 82711-01i (State Database NRW / Landesdatenbank)
-- Indicators: 29-40
-- Period: 1991-2023 (33 years)
-- =============================================================================

-- -----------------------------------------------------------------------------
-- SECTION 1: BASIC VERIFICATION
-- -----------------------------------------------------------------------------

-- Test 1.1: Check if all GDP indicators exist
SELECT
    indicator_id,
    indicator_code,
    indicator_name,
    indicator_name_en,
    unit_of_measure
FROM dim_indicator
WHERE indicator_id BETWEEN 29 AND 40
ORDER BY indicator_id;

-- Test 1.2: Count total GDP records
SELECT
    COUNT(*) as total_gdp_records,
    COUNT(DISTINCT indicator_id) as indicator_count,
    COUNT(DISTINCT geo_id) as region_count
FROM fact_demographics
WHERE indicator_id BETWEEN 29 AND 40;

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
WHERE di.indicator_id BETWEEN 29 AND 40
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
WHERE fd.indicator_id BETWEEN 29 AND 40;

-- Test 2.2: Records per year
SELECT
    dt.year,
    COUNT(*) as records,
    COUNT(DISTINCT fd.indicator_id) as indicators_present,
    COUNT(DISTINCT fd.geo_id) as regions_present
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 29 AND 40
GROUP BY dt.year
ORDER BY dt.year;

-- -----------------------------------------------------------------------------
-- SECTION 3: GEOGRAPHIC COVERAGE
-- -----------------------------------------------------------------------------

-- Test 3.1: Count unique regions
SELECT
    COUNT(DISTINCT geo_id) as total_regions
FROM fact_demographics
WHERE indicator_id BETWEEN 29 AND 40;

-- Test 3.2: Ruhr cities GDP coverage
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
WHERE fd.indicator_id BETWEEN 29 AND 40
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name, dg.region_code
ORDER BY dg.region_name;

-- -----------------------------------------------------------------------------
-- SECTION 4: GDP AT MARKET PRICES (Indicator 29)
-- -----------------------------------------------------------------------------

-- Test 4.1: GDP time series for NRW major cities (2020-2023)
SELECT
    dg.region_name,
    dt.year,
    fd.value as gdp_mio_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 29
    AND dt.year >= 2020
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513', '05111', '05315')
ORDER BY dg.region_name, dt.year;

-- Test 4.2: Top 10 regions by GDP (2023)
SELECT
    dg.region_name,
    dg.region_code,
    fd.value as gdp_2023_mio_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 29
    AND dt.year = 2023
ORDER BY fd.value DESC
LIMIT 10;

-- Test 4.3: GDP growth comparison (Ruhr cities 1991 vs 2023)
WITH gdp_comparison AS (
    SELECT
        dg.region_name,
        dg.region_code,
        MAX(CASE WHEN dt.year = 1991 THEN fd.value END) as gdp_1991,
        MAX(CASE WHEN dt.year = 2023 THEN fd.value END) as gdp_2023
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id = 29
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name, dg.region_code
)
SELECT
    region_name,
    ROUND(gdp_1991::numeric, 0) as gdp_1991_mio,
    ROUND(gdp_2023::numeric, 0) as gdp_2023_mio,
    ROUND(((gdp_2023 - gdp_1991) / gdp_1991 * 100)::numeric, 1) as growth_pct
FROM gdp_comparison
ORDER BY growth_pct DESC;

-- -----------------------------------------------------------------------------
-- SECTION 5: SECTORAL ANALYSIS (GVA by Economic Sector)
-- -----------------------------------------------------------------------------

-- Test 5.1: GVA by sector for Essen (2023)
SELECT
    di.indicator_id,
    di.indicator_name,
    di.indicator_name_en,
    fd.value as gva_mio_eur
FROM fact_demographics fd
JOIN dim_indicator di ON fd.indicator_id = di.indicator_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id BETWEEN 31 AND 40
    AND dg.region_code = '05113'  -- Essen
    AND dt.year = 2023
ORDER BY di.indicator_id;

-- Test 5.2: Services sector share (G-U) vs Manufacturing (B-E) by region (2023)
WITH sector_shares AS (
    SELECT
        dg.region_name,
        MAX(CASE WHEN fd.indicator_id = 31 THEN fd.value END) as gva_total,
        MAX(CASE WHEN fd.indicator_id = 33 THEN fd.value END) as gva_manufacturing,
        MAX(CASE WHEN fd.indicator_id = 37 THEN fd.value END) as gva_services
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id IN (31, 33, 37)
        AND dt.year = 2023
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name
)
SELECT
    region_name,
    ROUND(gva_total::numeric, 0) as gva_total_mio,
    ROUND((gva_manufacturing / gva_total * 100)::numeric, 1) as manufacturing_share_pct,
    ROUND((gva_services / gva_total * 100)::numeric, 1) as services_share_pct
FROM sector_shares
ORDER BY services_share_pct DESC;

-- -----------------------------------------------------------------------------
-- SECTION 6: GDP PER EMPLOYED PERSON (Indicator 30) - Productivity
-- -----------------------------------------------------------------------------

-- Test 6.1: Productivity ranking (2023)
SELECT
    dg.region_name,
    fd.value as gdp_per_employed_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 30
    AND dt.year = 2023
ORDER BY fd.value DESC
LIMIT 15;

-- Test 6.2: Productivity trends for Ruhr cities (2010-2023)
SELECT
    dg.region_name,
    dt.year,
    fd.value as gdp_per_employed_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 30
    AND dt.year >= 2010
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
ORDER BY dg.region_name, dt.year;

-- -----------------------------------------------------------------------------
-- SECTION 7: CRISIS IMPACT ANALYSIS
-- -----------------------------------------------------------------------------

-- Test 7.1: Financial Crisis Impact (2008-2010) on Ruhr cities GDP
SELECT
    dg.region_name,
    MAX(CASE WHEN dt.year = 2008 THEN fd.value END) as gdp_2008,
    MAX(CASE WHEN dt.year = 2009 THEN fd.value END) as gdp_2009,
    MAX(CASE WHEN dt.year = 2010 THEN fd.value END) as gdp_2010,
    ROUND(((MAX(CASE WHEN dt.year = 2009 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2008 THEN fd.value END)) /
            MAX(CASE WHEN dt.year = 2008 THEN fd.value END) * 100)::numeric, 2) as change_2008_2009_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 29
    AND dt.year BETWEEN 2008 AND 2010
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name
ORDER BY change_2008_2009_pct;

-- Test 7.2: COVID-19 Impact (2019-2021) on Ruhr cities GDP
SELECT
    dg.region_name,
    MAX(CASE WHEN dt.year = 2019 THEN fd.value END) as gdp_2019,
    MAX(CASE WHEN dt.year = 2020 THEN fd.value END) as gdp_2020,
    MAX(CASE WHEN dt.year = 2021 THEN fd.value END) as gdp_2021,
    ROUND(((MAX(CASE WHEN dt.year = 2020 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2019 THEN fd.value END)) /
            MAX(CASE WHEN dt.year = 2019 THEN fd.value END) * 100)::numeric, 2) as change_2019_2020_pct,
    ROUND(((MAX(CASE WHEN dt.year = 2021 THEN fd.value END) -
            MAX(CASE WHEN dt.year = 2020 THEN fd.value END)) /
            MAX(CASE WHEN dt.year = 2020 THEN fd.value END) * 100)::numeric, 2) as recovery_2020_2021_pct
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 29
    AND dt.year BETWEEN 2019 AND 2021
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name
ORDER BY change_2019_2020_pct;

-- -----------------------------------------------------------------------------
-- SECTION 8: STRUCTURAL CHANGE ANALYSIS
-- -----------------------------------------------------------------------------

-- Test 8.1: Manufacturing sector decline in Ruhr area (1996-2023)
-- Note: Detailed sector data available from 1996 onwards
WITH manufacturing_trend AS (
    SELECT
        dg.region_name,
        MAX(CASE WHEN dt.year = 1996 THEN fd.value END) as manufacturing_1996,
        MAX(CASE WHEN dt.year = 2023 THEN fd.value END) as manufacturing_2023
    FROM fact_demographics fd
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    JOIN dim_time dt ON fd.time_id = dt.time_id
    WHERE fd.indicator_id = 33  -- Manufacturing total (B-E)
        AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    GROUP BY dg.region_name
)
SELECT
    region_name,
    ROUND(manufacturing_1996::numeric, 0) as manufacturing_1996_mio,
    ROUND(manufacturing_2023::numeric, 0) as manufacturing_2023_mio,
    ROUND(((manufacturing_2023 - manufacturing_1996) / manufacturing_1996 * 100)::numeric, 1) as change_pct
FROM manufacturing_trend
WHERE manufacturing_1996 IS NOT NULL
ORDER BY change_pct;

-- Test 8.2: Construction sector (F) trends in Ruhr area
SELECT
    dg.region_name,
    dt.year,
    fd.value as construction_gva_mio
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 36  -- Construction (F)
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
    AND dt.year IN (1996, 2000, 2005, 2010, 2015, 2020, 2023)
ORDER BY dg.region_name, dt.year;

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
WHERE fd.indicator_id BETWEEN 29 AND 40
GROUP BY di.indicator_id, di.indicator_code
ORDER BY di.indicator_id;

-- Test 9.2: Data quality check - NULL values
SELECT
    indicator_id,
    COUNT(*) as total_records,
    SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) as null_values,
    ROUND(SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END)::numeric / COUNT(*)::numeric * 100, 2) as null_pct
FROM fact_demographics
WHERE indicator_id BETWEEN 29 AND 40
GROUP BY indicator_id
ORDER BY indicator_id;

-- =============================================================================
-- END OF GDP/GVA VERIFICATION SCRIPT
-- =============================================================================
