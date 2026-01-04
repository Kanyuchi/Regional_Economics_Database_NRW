-- BA Employment and Wage Data Verification Queries
-- Indicators: 89 (Total Employees), 90 (Median Wage), 91 (Wage Distribution)
-- Period: 2020-2024

-- ============================================================================
-- 1. DATA COVERAGE SUMMARY
-- ============================================================================

-- Overall data coverage
SELECT
    'BA Employment/Wage Data' AS dataset,
    COUNT(*) AS total_records,
    COUNT(DISTINCT fd.geo_id) AS regions,
    COUNT(DISTINCT fd.time_id) AS years,
    MIN(dt.year) AS min_year,
    MAX(dt.year) AS max_year,
    COUNT(DISTINCT fd.indicator_id) AS indicators
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (89, 90, 91);

-- Records by indicator
SELECT
    ind.indicator_id,
    ind.indicator_name_en,
    COUNT(*) AS record_count,
    COUNT(DISTINCT fd.geo_id) AS regions,
    MIN(dt.year) AS min_year,
    MAX(dt.year) AS max_year
FROM fact_demographics fd
JOIN dim_indicator ind ON fd.indicator_id = ind.indicator_id
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (89, 90, 91)
GROUP BY ind.indicator_id, ind.indicator_name_en
ORDER BY ind.indicator_id;

-- Records by year
SELECT
    dt.year,
    COUNT(*) AS total_records,
    COUNT(CASE WHEN fd.indicator_id = 89 THEN 1 END) AS employees_records,
    COUNT(CASE WHEN fd.indicator_id = 90 THEN 1 END) AS median_wage_records,
    COUNT(CASE WHEN fd.indicator_id = 91 THEN 1 END) AS wage_dist_records
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (89, 90, 91)
GROUP BY dt.year
ORDER BY dt.year;

-- ============================================================================
-- 2. TOTAL FULL-TIME EMPLOYEES (Indicator 89)
-- ============================================================================

-- Total employees in NRW by year (all demographics combined)
SELECT
    dt.year,
    dg.region_name,
    ROUND(fd.value::numeric, 0) AS total_employees
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 89
    AND dg.region_code = '05'  -- NRW
    AND fd.gender = 'total'
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
ORDER BY dt.year;

-- Employee growth trends: NRW vs Germany
WITH employee_trends AS (
    SELECT
        dt.year,
        dg.region_code,
        dg.region_name,
        fd.value AS employees
    FROM fact_demographics fd
    JOIN dim_time dt ON fd.time_id = dt.time_id
    JOIN dim_geography dg ON fd.geo_id = dg.geo_id
    WHERE fd.indicator_id = 89
        AND dg.region_code IN ('05', 'D')
        AND fd.gender = 'total'
        AND fd.nationality IS NULL
        AND fd.age_group IS NULL
        AND fd.notes IS NULL
)
SELECT
    year,
    MAX(CASE WHEN region_code = 'D' THEN ROUND(employees::numeric, 0) END) AS germany_total,
    MAX(CASE WHEN region_code = '05' THEN ROUND(employees::numeric, 0) END) AS nrw_total,
    ROUND(
        (MAX(CASE WHEN region_code = '05' THEN employees END) /
         MAX(CASE WHEN region_code = 'D' THEN employees END) * 100)::numeric,
        2
    ) AS nrw_share_pct
FROM employee_trends
GROUP BY year
ORDER BY year;

-- Gender breakdown: Male vs Female employment
SELECT
    dt.year,
    dg.region_name,
    MAX(CASE WHEN fd.gender = 'male' THEN ROUND(fd.value::numeric, 0) END) AS male_employees,
    MAX(CASE WHEN fd.gender = 'female' THEN ROUND(fd.value::numeric, 0) END) AS female_employees,
    MAX(CASE WHEN fd.gender = 'total' THEN ROUND(fd.value::numeric, 0) END) AS total_employees,
    ROUND(
        (MAX(CASE WHEN fd.gender = 'male' THEN fd.value END) /
         MAX(CASE WHEN fd.gender = 'total' THEN fd.value END) * 100)::numeric,
        1
    ) AS male_share_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 89
    AND dg.region_code = '05'
    AND fd.gender IN ('male', 'female', 'total')
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
GROUP BY dt.year, dg.region_name
ORDER BY dt.year;

-- Age group distribution in NRW (2024)
SELECT
    CASE
        WHEN fd.age_group = 'under_25' THEN 'Under 25'
        WHEN fd.age_group = '25_to_55' THEN '25 to 55'
        WHEN fd.age_group = '55_and_over' THEN '55 and over'
    END AS age_group,
    ROUND(fd.value::numeric, 0) AS employees,
    ROUND((fd.value / SUM(fd.value) OVER () * 100)::numeric, 1) AS share_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 89
    AND dg.region_code = '05'
    AND dt.year = 2024
    AND fd.age_group IS NOT NULL
    AND fd.gender = 'total'
ORDER BY fd.value DESC;

-- ============================================================================
-- 3. MEDIAN WAGES (Indicator 90)
-- ============================================================================

-- Median wage trends: NRW vs Germany
SELECT
    dt.year,
    MAX(CASE WHEN dg.region_code = 'D' THEN ROUND(fd.value::numeric, 2) END) AS germany_median_eur,
    MAX(CASE WHEN dg.region_code = '05' THEN ROUND(fd.value::numeric, 2) END) AS nrw_median_eur,
    ROUND(
        ((MAX(CASE WHEN dg.region_code = '05' THEN fd.value END) -
          MAX(CASE WHEN dg.region_code = 'D' THEN fd.value END)) /
         MAX(CASE WHEN dg.region_code = 'D' THEN fd.value END) * 100)::numeric,
        2
    ) AS nrw_diff_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code IN ('05', 'D')
    AND fd.gender = 'total'
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
GROUP BY dt.year
ORDER BY dt.year;

-- Gender wage gap in NRW
SELECT
    dt.year,
    MAX(CASE WHEN fd.gender = 'male' THEN ROUND(fd.value::numeric, 2) END) AS male_median_eur,
    MAX(CASE WHEN fd.gender = 'female' THEN ROUND(fd.value::numeric, 2) END) AS female_median_eur,
    ROUND(
        ((MAX(CASE WHEN fd.gender = 'male' THEN fd.value END) -
          MAX(CASE WHEN fd.gender = 'female' THEN fd.value END)))::numeric,
        2
    ) AS wage_gap_eur,
    ROUND(
        ((MAX(CASE WHEN fd.gender = 'male' THEN fd.value END) -
          MAX(CASE WHEN fd.gender = 'female' THEN fd.value END)) /
         MAX(CASE WHEN fd.gender = 'female' THEN fd.value END) * 100)::numeric,
        1
    ) AS wage_gap_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code = '05'
    AND fd.gender IN ('male', 'female')
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
GROUP BY dt.year
ORDER BY dt.year;

-- Wage by education level (NRW, 2024)
SELECT
    CASE
        WHEN fd.notes LIKE '%no_vocational_degree%' THEN 'No Vocational Degree'
        WHEN fd.notes LIKE '%recognized_vocational_degree%' THEN 'Recognized Vocational Degree'
        WHEN fd.notes LIKE '%academic_degree%' THEN 'Academic Degree'
    END AS education_level,
    ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code = '05'
    AND dt.year = 2024
    AND fd.notes LIKE '%degree%'
ORDER BY fd.value;

-- Wage by skill level (NRW, 2024)
SELECT
    CASE
        WHEN fd.notes LIKE '%skill_assistant%' THEN 'Assistant (Helfer)'
        WHEN fd.notes LIKE '%skill_specialist%' THEN 'Specialist (Fachkraft)'
        WHEN fd.notes LIKE '%skill_expert%' THEN 'Expert (Spezialist)'
        WHEN fd.notes LIKE '%skill_highly_qualified%' THEN 'Highly Qualified (Experte)'
    END AS skill_level,
    ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code = '05'
    AND dt.year = 2024
    AND fd.notes LIKE '%skill_%'
ORDER BY fd.value;

-- ============================================================================
-- 4. WAGE DISTRIBUTION (Indicator 91)
-- ============================================================================

-- Wage bracket distribution in NRW (2024)
SELECT
    CASE
        WHEN fd.notes LIKE '%under_2000%' THEN 'Under 2,000 EUR'
        WHEN fd.notes LIKE '%2000_to_3000%' THEN '2,000 - 3,000 EUR'
        WHEN fd.notes LIKE '%3000_to_4000%' THEN '3,000 - 4,000 EUR'
        WHEN fd.notes LIKE '%4000_to_5000%' THEN '4,000 - 5,000 EUR'
        WHEN fd.notes LIKE '%5000_to_6000%' THEN '5,000 - 6,000 EUR'
        WHEN fd.notes LIKE '%over_6000%' THEN 'Over 6,000 EUR'
    END AS wage_bracket,
    ROUND(fd.value::numeric, 0) AS employee_count,
    ROUND((fd.value / SUM(fd.value) OVER () * 100)::numeric, 1) AS share_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 91
    AND dg.region_code = '05'
    AND dt.year = 2024
    AND fd.gender = 'total'
    AND fd.notes LIKE '%wage_bracket:%'
    AND fd.notes NOT LIKE '%demographic_category%'
ORDER BY
    CASE
        WHEN fd.notes LIKE '%under_2000%' THEN 1
        WHEN fd.notes LIKE '%2000_to_3000%' THEN 2
        WHEN fd.notes LIKE '%3000_to_4000%' THEN 3
        WHEN fd.notes LIKE '%4000_to_5000%' THEN 4
        WHEN fd.notes LIKE '%5000_to_6000%' THEN 5
        WHEN fd.notes LIKE '%over_6000%' THEN 6
    END;

-- Gender wage distribution comparison (NRW, 2024)
SELECT
    CASE
        WHEN fd.notes LIKE '%under_2000%' THEN 'Under 2,000'
        WHEN fd.notes LIKE '%2000_to_3000%' THEN '2,000-3,000'
        WHEN fd.notes LIKE '%3000_to_4000%' THEN '3,000-4,000'
        WHEN fd.notes LIKE '%4000_to_5000%' THEN '4,000-5,000'
        WHEN fd.notes LIKE '%5000_to_6000%' THEN '5,000-6,000'
        WHEN fd.notes LIKE '%over_6000%' THEN 'Over 6,000'
    END AS wage_bracket_eur,
    MAX(CASE WHEN fd.gender = 'male' THEN ROUND(fd.value::numeric, 0) END) AS male_count,
    MAX(CASE WHEN fd.gender = 'female' THEN ROUND(fd.value::numeric, 0) END) AS female_count,
    ROUND(
        (MAX(CASE WHEN fd.gender = 'male' THEN fd.value END) /
         SUM(MAX(CASE WHEN fd.gender = 'male' THEN fd.value END)) OVER () * 100)::numeric,
        1
    ) AS male_share_pct,
    ROUND(
        (MAX(CASE WHEN fd.gender = 'female' THEN fd.value END) /
         SUM(MAX(CASE WHEN fd.gender = 'female' THEN fd.value END)) OVER () * 100)::numeric,
        1
    ) AS female_share_pct
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 91
    AND dg.region_code = '05'
    AND dt.year = 2024
    AND fd.gender IN ('male', 'female')
    AND fd.notes LIKE '%wage_bracket:%'
    AND fd.notes NOT LIKE '%demographic_category%'
GROUP BY wage_bracket_eur
ORDER BY
    CASE
        WHEN fd.notes LIKE '%under_2000%' THEN 1
        WHEN fd.notes LIKE '%2000_to_3000%' THEN 2
        WHEN fd.notes LIKE '%3000_to_4000%' THEN 3
        WHEN fd.notes LIKE '%4000_to_5000%' THEN 4
        WHEN fd.notes LIKE '%5000_to_6000%' THEN 5
        WHEN fd.notes LIKE '%over_6000%' THEN 6
    END;

-- ============================================================================
-- 5. TOP NRW DISTRICTS ANALYSIS (2024)
-- ============================================================================

-- Top 10 NRW districts by median wage (2024)
SELECT
    dg.region_name,
    ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dt.year = 2024
    AND dg.region_code LIKE '05___'  -- District level (5 digits)
    AND fd.gender = 'total'
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
ORDER BY fd.value DESC
LIMIT 10;

-- Bottom 10 NRW districts by median wage (2024)
SELECT
    dg.region_name,
    ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dt.year = 2024
    AND dg.region_code LIKE '05___'  -- District level
    AND fd.gender = 'total'
    AND fd.nationality IS NULL
    AND fd.age_group IS NULL
    AND fd.notes IS NULL
ORDER BY fd.value ASC
LIMIT 10;

-- ============================================================================
-- 6. DATA QUALITY CHECKS
-- ============================================================================

-- Check for NULL values in critical fields
SELECT
    'Indicator 89 - NULL values' AS check_description,
    COUNT(*) AS null_count
FROM fact_demographics
WHERE indicator_id = 89 AND value IS NULL
UNION ALL
SELECT
    'Indicator 90 - NULL values',
    COUNT(*)
FROM fact_demographics
WHERE indicator_id = 90 AND value IS NULL
UNION ALL
SELECT
    'Indicator 91 - NULL values',
    COUNT(*)
FROM fact_demographics
WHERE indicator_id = 91 AND value IS NULL;

-- Check for negative values (should not exist)
SELECT
    'Negative values' AS check_description,
    COUNT(*) AS negative_count
FROM fact_demographics
WHERE indicator_id IN (89, 90, 91)
    AND value < 0;

-- Verify year coverage for all regions
SELECT
    dg.region_code,
    dg.region_name,
    COUNT(DISTINCT dt.year) AS years_covered,
    STRING_AGG(DISTINCT dt.year::text, ', ' ORDER BY dt.year::text) AS years
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id IN (89, 90, 91)
    AND (dg.region_code = 'D' OR dg.region_code LIKE '05%')
GROUP BY dg.region_code, dg.region_name
HAVING COUNT(DISTINCT dt.year) < 5
ORDER BY dg.region_code;
