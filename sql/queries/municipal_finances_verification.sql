-- Municipal Finances (GFK) Data Verification
-- Indicator ID: 28
-- Table: 71517-01i
-- Period: 2009-2024

-- ============================================================================
-- VERIFICATION QUERIES FOR DBEAVER
-- ============================================================================

-- 1. Check if indicator exists
SELECT 
    indicator_id,
    indicator_code,
    indicator_name,
    indicator_category,
    source_system,
    source_table_id,
    unit_of_measure
FROM dim_indicator
WHERE indicator_id = 28;

-- 2. Total record count
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT fd.geo_id) as unique_regions,
    COUNT(DISTINCT dt.year) as unique_years,
    MIN(dt.year) as min_year,
    MAX(dt.year) as max_year
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28;

-- 3. Year-by-year breakdown
SELECT 
    dt.year,
    COUNT(*) as records,
    COUNT(DISTINCT fd.geo_id) as regions,
    COUNT(DISTINCT fd.notes) as payment_types,
    ROUND(AVG(fd.value), 2) as avg_value,
    ROUND(SUM(fd.value), 2) as total_value
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28
GROUP BY dt.year
ORDER BY dt.year;

-- 4. Payment types overview
SELECT 
    fd.notes as payment_type,
    COUNT(*) as record_count,
    COUNT(DISTINCT dt.year) as years_covered,
    COUNT(DISTINCT fd.geo_id) as regions,
    ROUND(AVG(fd.value), 2) as avg_value,
    ROUND(SUM(fd.value), 2) as total_value
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28
GROUP BY fd.notes
ORDER BY record_count DESC
LIMIT 20;

-- 5. Sample data for specific regions (Ruhr cities)
SELECT 
    dg.region_name,
    dg.region_code,
    dt.year,
    fd.notes as payment_type,
    fd.value
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28
    AND dg.region_code IN ('05113', '05112', '05111', '05116', '05119')  -- Essen, Duisburg, Düsseldorf, Bochum, Oberhausen
    AND dt.year IN (2009, 2015, 2020, 2024)
ORDER BY dg.region_code, dt.year, fd.notes
LIMIT 50;

-- 6. Regional coverage check
SELECT 
    COUNT(DISTINCT fd.geo_id) as total_regions_with_data,
    COUNT(DISTINCT dg.region_code) as unique_region_codes
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 28;

-- 7. Data completeness check (records per year per region)
SELECT 
    dt.year,
    COUNT(DISTINCT fd.geo_id) as regions_with_data,
    COUNT(DISTINCT fd.notes) as payment_types_per_year,
    COUNT(*) as total_records,
    ROUND(COUNT(*)::numeric / COUNT(DISTINCT fd.geo_id), 2) as avg_records_per_region
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28
GROUP BY dt.year
ORDER BY dt.year;

-- 8. Check for any NULL or missing values
SELECT 
    COUNT(*) as total_records,
    COUNT(CASE WHEN fd.value IS NULL THEN 1 END) as null_values,
    COUNT(CASE WHEN fd.notes IS NULL OR fd.notes = '' THEN 1 END) as null_notes,
    COUNT(CASE WHEN fd.geo_id IS NULL THEN 1 END) as null_geo_id,
    COUNT(CASE WHEN fd.time_id IS NULL THEN 1 END) as null_time_id
FROM fact_demographics fd
WHERE fd.indicator_id = 28;

-- 9. Top 10 regions by total value (all years combined)
SELECT 
    dg.region_name,
    dg.region_code,
    COUNT(DISTINCT dt.year) as years_covered,
    COUNT(*) as total_records,
    ROUND(SUM(fd.value), 2) as total_value_all_years
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id = 28
GROUP BY dg.region_name, dg.region_code
ORDER BY total_value_all_years DESC
LIMIT 10;

-- 10. Time series sample for one region (Dortmund - 05913)
SELECT 
    dt.year,
    fd.notes as payment_type,
    fd.value
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 28
    AND dg.region_code = '05913'
    AND fd.notes LIKE '%Einzahlungen insgesamt%'  -- Total receipts
ORDER BY dt.year;

