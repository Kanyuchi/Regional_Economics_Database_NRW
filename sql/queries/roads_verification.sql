-- ============================================================================
-- Verification SQL Script for Interregional Roads Data
-- Table: 46271-01i (State Database NRW)
-- Indicators: 62-66 (Total, Motorway, Federal, State, District Roads)
-- ============================================================================

-- 1. Basic count and structure verification
SELECT
    'Total Records' as metric,
    COUNT(*) as value
FROM fact_demographics
WHERE indicator_id BETWEEN 62 AND 66;

-- 2. Records by indicator
SELECT
    i.indicator_id,
    i.indicator_code,
    i.indicator_name_en,
    COUNT(f.fact_id) as record_count
FROM dim_indicator i
LEFT JOIN fact_demographics f ON i.indicator_id = f.indicator_id
WHERE i.indicator_id BETWEEN 62 AND 66
GROUP BY i.indicator_id, i.indicator_code, i.indicator_name_en
ORDER BY i.indicator_id;

-- 3. Year range verification
SELECT
    MIN(t.year) as min_year,
    MAX(t.year) as max_year,
    COUNT(DISTINCT t.year) as years_count
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66;

-- 4. Geographic coverage
SELECT
    COUNT(DISTINCT g.region_code) as regions_count,
    COUNT(DISTINCT CASE WHEN LENGTH(g.region_code) = 5 THEN g.region_code END) as district_count,
    COUNT(DISTINCT CASE WHEN LENGTH(g.region_code) = 2 THEN g.region_code END) as state_count
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id BETWEEN 62 AND 66;

-- 5. NRW-wide road summary (aggregated from districts) - Current year
SELECT
    t.year,
    ROUND(SUM(CASE WHEN f.indicator_id = 62 THEN f.value END)::numeric, 1) as total_km,
    ROUND(SUM(CASE WHEN f.indicator_id = 63 THEN f.value END)::numeric, 1) as motorway_km,
    ROUND(SUM(CASE WHEN f.indicator_id = 64 THEN f.value END)::numeric, 1) as federal_km,
    ROUND(SUM(CASE WHEN f.indicator_id = 65 THEN f.value END)::numeric, 1) as state_km,
    ROUND(SUM(CASE WHEN f.indicator_id = 66 THEN f.value END)::numeric, 1) as district_km
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND LENGTH(g.region_code) = 5
GROUP BY t.year
ORDER BY t.year DESC
LIMIT 5;

-- 6. Road network composition by year (percentage shares)
SELECT
    t.year,
    ROUND(SUM(CASE WHEN f.indicator_id = 63 THEN f.value END) /
          SUM(CASE WHEN f.indicator_id = 62 THEN f.value END) * 100, 2) as motorway_pct,
    ROUND(SUM(CASE WHEN f.indicator_id = 64 THEN f.value END) /
          SUM(CASE WHEN f.indicator_id = 62 THEN f.value END) * 100, 2) as federal_pct,
    ROUND(SUM(CASE WHEN f.indicator_id = 65 THEN f.value END) /
          SUM(CASE WHEN f.indicator_id = 62 THEN f.value END) * 100, 2) as state_pct,
    ROUND(SUM(CASE WHEN f.indicator_id = 66 THEN f.value END) /
          SUM(CASE WHEN f.indicator_id = 62 THEN f.value END) * 100, 2) as district_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND LENGTH(g.region_code) = 5
GROUP BY t.year
ORDER BY t.year DESC
LIMIT 5;

-- 7. Top 10 districts by total road length (2024)
SELECT
    g.region_code,
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) as total_km,
    MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) as motorway_km,
    MAX(CASE WHEN f.indicator_id = 64 THEN f.value END) as federal_km,
    MAX(CASE WHEN f.indicator_id = 65 THEN f.value END) as state_km,
    MAX(CASE WHEN f.indicator_id = 66 THEN f.value END) as district_km
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND t.year = 2024
  AND LENGTH(g.region_code) = 5
GROUP BY g.region_code, g.region_name
ORDER BY MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) DESC
LIMIT 10;

-- 8. Districts with highest motorway density (km of motorway)
SELECT
    g.region_code,
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) as motorway_km,
    MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) as total_km,
    ROUND(MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 62 THEN f.value END), 0) * 100, 2) as motorway_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND t.year = 2024
  AND LENGTH(g.region_code) = 5
GROUP BY g.region_code, g.region_name
HAVING MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) > 0
ORDER BY MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) DESC
LIMIT 10;

-- 9. Change in road network length over time (1996 vs 2024)
WITH road_change AS (
    SELECT
        t.year,
        SUM(CASE WHEN f.indicator_id = 62 THEN f.value END) as total_km,
        SUM(CASE WHEN f.indicator_id = 63 THEN f.value END) as motorway_km,
        SUM(CASE WHEN f.indicator_id = 64 THEN f.value END) as federal_km,
        SUM(CASE WHEN f.indicator_id = 65 THEN f.value END) as state_km,
        SUM(CASE WHEN f.indicator_id = 66 THEN f.value END) as district_km
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id BETWEEN 62 AND 66
      AND LENGTH(g.region_code) = 5
      AND t.year IN (1996, 2024)
    GROUP BY t.year
)
SELECT
    r1.year as year_start,
    r2.year as year_end,
    ROUND((r2.total_km - r1.total_km)::numeric, 1) as total_change_km,
    ROUND((r2.motorway_km - r1.motorway_km)::numeric, 1) as motorway_change_km,
    ROUND((r2.total_km - r1.total_km) / r1.total_km * 100, 2) as total_change_pct,
    ROUND((r2.motorway_km - r1.motorway_km) / r1.motorway_km * 100, 2) as motorway_change_pct
FROM road_change r1, road_change r2
WHERE r1.year = 1996 AND r2.year = 2024;

-- 10. Ruhr area cities road infrastructure (2024)
-- Ruhr cities: Dortmund (05913), Essen (05113), Duisburg (05112), Bochum (05911),
-- Gelsenkirchen (05513), Oberhausen (05119), Mülheim (05117), Hagen (05914)
SELECT
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) as total_km,
    MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) as motorway_km,
    MAX(CASE WHEN f.indicator_id = 64 THEN f.value END) as federal_km,
    ROUND(MAX(CASE WHEN f.indicator_id = 63 THEN f.value END) /
          NULLIF(MAX(CASE WHEN f.indicator_id = 62 THEN f.value END), 0) * 100, 2) as motorway_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND t.year = 2024
  AND g.region_code IN ('05913', '05113', '05112', '05911', '05513', '05119', '05117', '05914')
GROUP BY g.region_name
ORDER BY MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) DESC;

-- 11. Data quality check - ensure all road types sum to total
SELECT
    t.year,
    g.region_name,
    MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) as reported_total,
    SUM(CASE WHEN f.indicator_id IN (63, 64, 65, 66) THEN f.value END) as calculated_total,
    ROUND(ABS(MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) -
          SUM(CASE WHEN f.indicator_id IN (63, 64, 65, 66) THEN f.value END))::numeric, 2) as difference
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id BETWEEN 62 AND 66
  AND t.year = 2024
GROUP BY t.year, g.region_name
HAVING ABS(MAX(CASE WHEN f.indicator_id = 62 THEN f.value END) -
       SUM(CASE WHEN f.indicator_id IN (63, 64, 65, 66) THEN f.value END)) > 1
ORDER BY difference DESC
LIMIT 5;
