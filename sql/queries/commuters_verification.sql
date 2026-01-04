-- ============================================================================
-- Commuter Statistics Data Verification
-- Indicators: 101 (Incoming), 102 (Outgoing)
-- Coverage: 2002-2024 (summary data)
-- ============================================================================

-- 1. OVERVIEW: Total records loaded
-- ============================================================================
SELECT
    '1. Total Records' as check_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT geo_id) as unique_districts,
    COUNT(DISTINCT time_id) as unique_years,
    COUNT(DISTINCT indicator_id) as unique_indicators
FROM fact_demographics
WHERE indicator_id IN (101, 102);

-- 2. RECORDS BY INDICATOR
-- ============================================================================
SELECT
    '2. Records by Indicator' as check_name,
    i.indicator_id,
    i.indicator_name,
    COUNT(*) as record_count,
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT fd.geo_id) as unique_districts
FROM fact_demographics fd
JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id IN (101, 102)
GROUP BY i.indicator_id, i.indicator_name
ORDER BY i.indicator_id;

-- 3. YEAR COVERAGE
-- ============================================================================
SELECT
    '3. Year Coverage' as check_name,
    t.year,
    SUM(CASE WHEN fd.indicator_id = 101 THEN 1 ELSE 0 END) as incoming_records,
    SUM(CASE WHEN fd.indicator_id = 102 THEN 1 ELSE 0 END) as outgoing_records,
    COUNT(DISTINCT fd.geo_id) as unique_districts
FROM fact_demographics fd
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id IN (101, 102)
GROUP BY t.year
ORDER BY t.year;

-- 4. DEMOGRAPHIC BREAKDOWN
-- ============================================================================
SELECT
    '4. Demographic Breakdown' as check_name,
    CASE
        WHEN gender IS NOT NULL THEN 'Gender: ' || gender
        WHEN nationality IS NOT NULL THEN 'Nationality: ' || nationality
        WHEN notes LIKE '%trainee%' THEN 'Employment: Trainee'
        ELSE 'Total'
    END as demographic_category,
    COUNT(*) as record_count
FROM fact_demographics
WHERE indicator_id IN (101, 102)
GROUP BY
    CASE
        WHEN gender IS NOT NULL THEN 'Gender: ' || gender
        WHEN nationality IS NOT NULL THEN 'Nationality: ' || nationality
        WHEN notes LIKE '%trainee%' THEN 'Employment: Trainee'
        ELSE 'Total'
    END
ORDER BY record_count DESC;

-- 5. SAMPLE DATA: Düsseldorf 2024
-- ============================================================================
SELECT
    '5. Sample: Düsseldorf 2024' as check_name,
    g.region_name,
    t.year,
    i.indicator_name,
    COALESCE(fd.gender, 'Total') as breakdown,
    fd.value as commuters,
    SUBSTRING(fd.notes FROM 1 FOR 50) as notes_preview
FROM fact_demographics fd
JOIN dim_geography g ON fd.geo_id = g.geo_id
JOIN dim_time t ON fd.time_id = t.time_id
JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
WHERE fd.indicator_id IN (101, 102)
  AND g.region_code = '05111'  -- Düsseldorf
  AND t.year = 2024
  AND fd.nationality IS NULL  -- Exclude nationality breakdowns for clarity
  AND fd.notes NOT LIKE '%trainee%'  -- Exclude trainee breakdown
ORDER BY i.indicator_id, fd.gender NULLS FIRST;

-- 6. TOP 10 DISTRICTS BY NET COMMUTER BALANCE (2024)
-- ============================================================================
WITH commuter_balance AS (
    SELECT
        g.geo_id,
        g.region_name,
        g.region_code,
        t.year,
        SUM(CASE WHEN fd.indicator_id = 101 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END) as incoming,
        SUM(CASE WHEN fd.indicator_id = 102 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END) as outgoing
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE fd.indicator_id IN (101, 102)
      AND t.year = 2024
    GROUP BY g.geo_id, g.region_name, g.region_code, t.year
)
SELECT
    '6. Top 10 Net Importers (2024)' as check_name,
    region_name,
    region_code,
    incoming::INTEGER as incoming_commuters,
    outgoing::INTEGER as outgoing_commuters,
    (incoming - outgoing)::INTEGER as net_balance,
    ROUND((incoming - outgoing) * 100.0 / NULLIF(incoming, 0), 1) as net_balance_pct
FROM commuter_balance
WHERE incoming > 0 AND outgoing > 0
ORDER BY (incoming - outgoing) DESC
LIMIT 10;

-- 7. GENDER DISTRIBUTION TRENDS
-- ============================================================================
SELECT
    '7. Gender Distribution Over Time' as check_name,
    t.year,
    i.indicator_name,
    SUM(CASE WHEN fd.gender = 'male' THEN fd.value ELSE 0 END)::INTEGER as male_commuters,
    SUM(CASE WHEN fd.gender = 'female' THEN fd.value ELSE 0 END)::INTEGER as female_commuters,
    ROUND(SUM(CASE WHEN fd.gender = 'male' THEN fd.value ELSE 0 END) * 100.0 /
          NULLIF(SUM(CASE WHEN fd.gender IN ('male', 'female') THEN fd.value ELSE 0 END), 0), 1) as pct_male
FROM fact_demographics fd
JOIN dim_time t ON fd.time_id = t.time_id
JOIN dim_indicator i ON fd.indicator_id = i.indicator_id
WHERE fd.indicator_id IN (101, 102)
  AND fd.gender IS NOT NULL
  AND t.year IN (2002, 2010, 2015, 2020, 2024)  -- Sample years
GROUP BY t.year, i.indicator_id, i.indicator_name
ORDER BY i.indicator_id, t.year;

-- 8. DATA QUALITY CHECKS
-- ============================================================================
-- Check for NULL values (should be none for value column)
SELECT
    '8a. NULL Value Check' as check_name,
    COUNT(*) as null_value_count
FROM fact_demographics
WHERE indicator_id IN (101, 102)
  AND value IS NULL;

-- Check for negative values (should be none)
SELECT
    '8b. Negative Value Check' as check_name,
    COUNT(*) as negative_value_count
FROM fact_demographics
WHERE indicator_id IN (101, 102)
  AND value < 0;

-- Check for duplicate records (should be none)
SELECT
    '8c. Duplicate Check' as check_name,
    COUNT(*) as duplicate_groups
FROM (
    SELECT
        geo_id, time_id, indicator_id, gender, nationality, age_group, migration_background,
        COUNT(*) as dup_count
    FROM fact_demographics
    WHERE indicator_id IN (101, 102)
    GROUP BY geo_id, time_id, indicator_id, gender, nationality, age_group, migration_background
    HAVING COUNT(*) > 1
) duplicates;

-- 9. TOTAL COMMUTER VOLUME BY YEAR
-- ============================================================================
SELECT
    '9. Total Commuter Volume by Year' as check_name,
    t.year,
    SUM(CASE WHEN fd.indicator_id = 101 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END)::BIGINT as total_incoming,
    SUM(CASE WHEN fd.indicator_id = 102 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END)::BIGINT as total_outgoing,
    (SUM(CASE WHEN fd.indicator_id = 101 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END) +
     SUM(CASE WHEN fd.indicator_id = 102 AND fd.gender IS NULL AND fd.nationality IS NULL AND fd.notes NOT LIKE '%trainee%' THEN fd.value ELSE 0 END))::BIGINT as total_commuter_volume
FROM fact_demographics fd
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id IN (101, 102)
GROUP BY t.year
ORDER BY t.year;

-- 10. SUMMARY STATISTICS
-- ============================================================================
SELECT
    '10. Summary Statistics' as check_name,
    'Incoming (101)' as indicator,
    COUNT(DISTINCT geo_id) as districts,
    COUNT(DISTINCT time_id) as years,
    COUNT(*) as total_records,
    ROUND(AVG(value))::INTEGER as avg_commuters,
    MIN(value)::INTEGER as min_commuters,
    MAX(value)::INTEGER as max_commuters
FROM fact_demographics
WHERE indicator_id = 101
  AND gender IS NULL AND nationality IS NULL AND notes NOT LIKE '%trainee%'
UNION ALL
SELECT
    '10. Summary Statistics' as check_name,
    'Outgoing (102)' as indicator,
    COUNT(DISTINCT geo_id) as districts,
    COUNT(DISTINCT time_id) as years,
    COUNT(*) as total_records,
    ROUND(AVG(value))::INTEGER as avg_commuters,
    MIN(value)::INTEGER as min_commuters,
    MAX(value)::INTEGER as max_commuters
FROM fact_demographics
WHERE indicator_id = 102
  AND gender IS NULL AND nationality IS NULL AND notes NOT LIKE '%trainee%';

-- ============================================================================
-- END OF VERIFICATION QUERIES
-- ============================================================================
