-- ============================================================================
-- Hospitals and Available Beds Verification Queries
-- Table: 23111-01i
-- Indicators: 84 (Hospitals by Operator Type), 85 (Hospital Beds by Operator Type)
-- Period: 2002-2024
-- ============================================================================

-- Query 1: Total record count by indicator
-- Expected: ~5,800 records per indicator (53 regions × 23 years × 4 categories)
SELECT
    i.indicator_id,
    i.indicator_name_en,
    COUNT(*) as total_records,
    MIN(t.year) as earliest_year,
    MAX(t.year) as latest_year
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE i.indicator_id IN (84, 85)
GROUP BY i.indicator_id, i.indicator_name_en
ORDER BY i.indicator_id;

-- Query 2: NRW total hospitals over time (23-year trend)
-- Shows hospital consolidation trajectory
SELECT
    t.year,
    f.value as total_hospitals,
    LAG(f.value) OVER (ORDER BY t.year) as previous_year,
    f.value - LAG(f.value) OVER (ORDER BY t.year) as year_over_year_change,
    ROUND(((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
           NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100), 2) as pct_change
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 84  -- Hospitals
    AND g.region_code = '05'  -- NRW total
    AND f.notes = 'operator:total|Total'
ORDER BY t.year;

-- Query 3: NRW total hospital beds over time (23-year trend)
-- Shows capacity changes trajectory
SELECT
    t.year,
    f.value as total_beds,
    LAG(f.value) OVER (ORDER BY t.year) as previous_year,
    f.value - LAG(f.value) OVER (ORDER BY t.year) as year_over_year_change,
    ROUND(((f.value - LAG(f.value) OVER (ORDER BY t.year)) /
           NULLIF(LAG(f.value) OVER (ORDER BY t.year), 0) * 100), 2) as pct_change
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 85  -- Beds
    AND g.region_code = '05'  -- NRW total
    AND f.notes = 'operator:total|Total'
ORDER BY t.year;

-- Query 4: Hospital operator type shift (privatization analysis)
-- Compares 2002 vs 2024 operator distribution
WITH operator_comparison AS (
    SELECT
        t.year,
        SPLIT_PART(f.notes, '|', 2) as operator_type,
        f.value as hospitals
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 84  -- Hospitals
        AND g.region_code = '05'  -- NRW total
        AND t.year IN (2002, 2024)
        AND f.notes NOT LIKE '%total%'
)
SELECT
    operator_type,
    MAX(CASE WHEN year = 2002 THEN hospitals END) as hospitals_2002,
    MAX(CASE WHEN year = 2024 THEN hospitals END) as hospitals_2024,
    MAX(CASE WHEN year = 2024 THEN hospitals END) -
        MAX(CASE WHEN year = 2002 THEN hospitals END) as absolute_change,
    ROUND(((MAX(CASE WHEN year = 2024 THEN hospitals END) -
            MAX(CASE WHEN year = 2002 THEN hospitals END))::numeric /
           NULLIF(MAX(CASE WHEN year = 2002 THEN hospitals END), 0) * 100), 2) as pct_change,
    ROUND((MAX(CASE WHEN year = 2002 THEN hospitals END)::numeric /
           (SELECT SUM(hospitals) FROM operator_comparison WHERE year = 2002) * 100), 2) as share_2002,
    ROUND((MAX(CASE WHEN year = 2024 THEN hospitals END)::numeric /
           (SELECT SUM(hospitals) FROM operator_comparison WHERE year = 2024) * 100), 2) as share_2024
FROM operator_comparison
GROUP BY operator_type
ORDER BY hospitals_2024 DESC;

-- Query 5: Bed operator type shift (capacity privatization analysis)
-- Compares 2002 vs 2024 bed distribution by operator
WITH bed_comparison AS (
    SELECT
        t.year,
        SPLIT_PART(f.notes, '|', 2) as operator_type,
        f.value as beds
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 85  -- Beds
        AND g.region_code = '05'  -- NRW total
        AND t.year IN (2002, 2024)
        AND f.notes NOT LIKE '%total%'
)
SELECT
    operator_type,
    MAX(CASE WHEN year = 2002 THEN beds END) as beds_2002,
    MAX(CASE WHEN year = 2024 THEN beds END) as beds_2024,
    MAX(CASE WHEN year = 2024 THEN beds END) -
        MAX(CASE WHEN year = 2002 THEN beds END) as absolute_change,
    ROUND(((MAX(CASE WHEN year = 2024 THEN beds END) -
            MAX(CASE WHEN year = 2002 THEN beds END))::numeric /
           NULLIF(MAX(CASE WHEN year = 2002 THEN beds END), 0) * 100), 2) as pct_change,
    ROUND((MAX(CASE WHEN year = 2002 THEN beds END)::numeric /
           (SELECT SUM(beds) FROM bed_comparison WHERE year = 2002) * 100), 2) as share_2002,
    ROUND((MAX(CASE WHEN year = 2024 THEN beds END)::numeric /
           (SELECT SUM(beds) FROM bed_comparison WHERE year = 2024) * 100), 2) as share_2024
FROM bed_comparison
GROUP BY operator_type
ORDER BY beds_2024 DESC;

-- Query 6: Beds per hospital ratio over time (efficiency metric)
-- Shows whether consolidation is maintaining capacity
SELECT
    t.year,
    hospitals.value as total_hospitals,
    beds.value as total_beds,
    ROUND(beds.value / NULLIF(hospitals.value, 0), 1) as beds_per_hospital,
    ROUND((beds.value / NULLIF(hospitals.value, 0)) /
          NULLIF(LAG(beds.value / NULLIF(hospitals.value, 0)) OVER (ORDER BY t.year), 0) * 100 - 100, 2)
        as pct_change_ratio
FROM fact_demographics hospitals
JOIN fact_demographics beds ON hospitals.geo_id = beds.geo_id
    AND hospitals.time_id = beds.time_id
JOIN dim_time t ON hospitals.time_id = t.time_id
JOIN dim_geography g ON hospitals.geo_id = g.geo_id
WHERE hospitals.indicator_id = 84  -- Hospitals
    AND beds.indicator_id = 85  -- Beds
    AND g.region_code = '05'  -- NRW total
    AND hospitals.notes = 'operator:total|Total'
    AND beds.notes = 'operator:total|Total'
ORDER BY t.year;

-- Query 7: Regional hospital distribution (2024)
-- Top 10 regions by hospital count
SELECT
    g.region_name,
    f.value as total_hospitals,
    ROUND((f.value / (SELECT SUM(value) FROM fact_demographics
                      WHERE indicator_id = 84
                      AND time_id = (SELECT time_id FROM dim_time WHERE year = 2024)
                      AND notes = 'operator:total|Total') * 100), 2) as pct_of_nrw_total
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 84  -- Hospitals
    AND t.year = 2024
    AND f.notes = 'operator:total|Total'
    AND g.region_code != '05'  -- Exclude NRW total
ORDER BY f.value DESC
LIMIT 10;

-- Query 8: Regional bed capacity distribution (2024)
-- Top 10 regions by bed count
SELECT
    g.region_name,
    f.value as total_beds,
    ROUND((f.value / (SELECT SUM(value) FROM fact_demographics
                      WHERE indicator_id = 85
                      AND time_id = (SELECT time_id FROM dim_time WHERE year = 2024)
                      AND notes = 'operator:total|Total') * 100), 2) as pct_of_nrw_total
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 85  -- Beds
    AND t.year = 2024
    AND f.notes = 'operator:total|Total'
    AND g.region_code != '05'  -- Exclude NRW total
ORDER BY f.value DESC
LIMIT 10;

-- Query 9: Regions with most hospital closures (2002-2024)
WITH hospital_change AS (
    SELECT
        g.region_name,
        MAX(CASE WHEN t.year = 2002 THEN f.value END) as hospitals_2002,
        MAX(CASE WHEN t.year = 2024 THEN f.value END) as hospitals_2024,
        MAX(CASE WHEN t.year = 2024 THEN f.value END) -
            MAX(CASE WHEN t.year = 2002 THEN f.value END) as hospital_change,
        ROUND(((MAX(CASE WHEN t.year = 2024 THEN f.value END) -
                MAX(CASE WHEN t.year = 2002 THEN f.value END))::numeric /
               NULLIF(MAX(CASE WHEN t.year = 2002 THEN f.value END), 0) * 100), 2) as pct_change
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 84  -- Hospitals
        AND t.year IN (2002, 2024)
        AND f.notes = 'operator:total|Total'
        AND g.region_code != '05'  -- Exclude NRW total
    GROUP BY g.region_name
)
SELECT
    geography_name,
    hospitals_2002,
    hospitals_2024,
    hospital_change,
    pct_change
FROM hospital_change
WHERE hospital_change IS NOT NULL
ORDER BY hospital_change ASC  -- Most closures first
LIMIT 10;

-- Query 10: Operator type distribution by region (2024)
-- Shows regional variation in hospital ownership
SELECT
    g.region_name,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Public' THEN f.value END) as public_hospitals,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Private' THEN f.value END) as private_hospitals,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Non-profit' THEN f.value END) as nonprofit_hospitals,
    MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Total' THEN f.value END) as total_hospitals,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Public' THEN f.value END)::numeric /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Total' THEN f.value END), 0) * 100, 1)
        as public_pct,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Private' THEN f.value END)::numeric /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Total' THEN f.value END), 0) * 100, 1)
        as private_pct,
    ROUND(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Non-profit' THEN f.value END)::numeric /
          NULLIF(MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Total' THEN f.value END), 0) * 100, 1)
        as nonprofit_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 84  -- Hospitals
    AND t.year = 2024
    AND g.region_code != '05'  -- Exclude NRW total
GROUP BY g.region_name
HAVING MAX(CASE WHEN SPLIT_PART(f.notes, '|', 2) = 'Total' THEN f.value END) > 5  -- Regions with 6+ hospitals
ORDER BY total_hospitals DESC;

-- Query 11: Summary statistics
SELECT
    'Hospitals' as metric,
    COUNT(DISTINCT f.geo_id) as regions_covered,
    COUNT(DISTINCT f.time_id) as years_covered,
    COUNT(*) as total_records,
    MIN(f.value) as min_value,
    MAX(f.value) as max_value,
    ROUND(AVG(f.value), 2) as avg_value
FROM fact_demographics f
WHERE f.indicator_id = 84

UNION ALL

SELECT
    'Beds' as metric,
    COUNT(DISTINCT f.geo_id) as regions_covered,
    COUNT(DISTINCT f.time_id) as years_covered,
    COUNT(*) as total_records,
    MIN(f.value) as min_value,
    MAX(f.value) as max_value,
    ROUND(AVG(f.value), 2) as avg_value
FROM fact_demographics f
WHERE f.indicator_id = 85;
