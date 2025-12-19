-- Corporate Insolvencies Verification Script
-- Table: 52411-02-01-4
-- Indicator: 27
-- Period: 2007-2024

-- Test 1: Check if indicator 27 exists
SELECT * FROM dim_indicator WHERE indicator_id = 27;

-- Test 2: Count total records
SELECT COUNT(*) as total_records
FROM fact_demographics 
WHERE indicator_id = 27;

-- Test 3: Check year range
SELECT 
    MIN(dt.year) as first_year,
    MAX(dt.year) as last_year,
    COUNT(DISTINCT dt.year) as year_count
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 27;

-- Test 4: Count unique regions
SELECT COUNT(DISTINCT geo_id) as region_count
FROM fact_demographics
WHERE indicator_id = 27;

-- Test 5: Check data for Ruhr cities
SELECT 
    dg.region_name,
    dg.region_code,
    COUNT(*) as records,
    MIN(dt.year) as from_year,
    MAX(dt.year) as to_year
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 27
    AND dg.region_code IN ('05113', '05913', '05911', '05112', '05513')
GROUP BY dg.region_name, dg.region_code
ORDER BY dg.region_name;

-- Test 6: Sample data for Dortmund
SELECT 
    dt.year,
    dg.region_name,
    fd.value as insolvencies,
    fd.notes
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 27
    AND dg.region_code = '05913'
ORDER BY dt.year DESC
LIMIT 10;

-- Test 7: Value statistics
SELECT 
    MIN(value) as min_value,
    MAX(value) as max_value,
    ROUND(AVG(value)::numeric, 2) as avg_value,
    ROUND(STDDEV(value)::numeric, 2) as stddev_value
FROM fact_demographics
WHERE indicator_id = 27;

-- Test 8: Records per year
SELECT 
    dt.year,
    COUNT(*) as records,
    ROUND(AVG(fd.value)::numeric, 2) as avg_insolvencies
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 27
GROUP BY dt.year
ORDER BY dt.year DESC;

