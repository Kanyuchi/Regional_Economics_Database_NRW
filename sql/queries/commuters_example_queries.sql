-- ============================================================================
-- Commuter Statistics - Example Queries
-- Demonstrates how to query and analyze commuter data
-- ============================================================================

-- EXAMPLE 1: Top 10 Job Centers in NRW (2023)
-- Districts with highest incoming commuters (job importers)
-- ============================================================================
SELECT
    g.region_name,
    g.region_code,
    ROUND(fd.value) as incoming_commuters
FROM fact_demographics fd
JOIN dim_geography g ON fd.geo_id = g.geo_id
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id = 101  -- Incoming commuters
  AND t.year = 2023
  AND fd.gender IS NULL  -- Total (not gender breakdown)
  AND fd.nationality IS NULL  -- Total (not nationality breakdown)
ORDER BY fd.value DESC
LIMIT 10;

-- EXAMPLE 2: Commuter Balance for Major Cities (2023)
-- Shows net gain/loss of commuters
-- ============================================================================
WITH commuters_2023 AS (
    SELECT
        g.geo_id,
        g.region_name,
        SUM(CASE WHEN fd.indicator_id = 101 THEN fd.value ELSE 0 END) as incoming,
        SUM(CASE WHEN fd.indicator_id = 102 THEN fd.value ELSE 0 END) as outgoing
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE fd.indicator_id IN (101, 102)
      AND t.year = 2023
      AND fd.gender IS NULL
      AND fd.nationality IS NULL
    GROUP BY g.geo_id, g.region_name
)
SELECT
    region_name,
    ROUND(incoming) as incoming,
    ROUND(outgoing) as outgoing,
    ROUND(incoming - outgoing) as net_balance,
    CASE
        WHEN (incoming - outgoing) > 50000 THEN 'Major Job Center'
        WHEN (incoming - outgoing) > 10000 THEN 'Job Center'
        WHEN (incoming - outgoing) > -10000 THEN 'Balanced'
        WHEN (incoming - outgoing) > -50000 THEN 'Bedroom Community'
        ELSE 'Major Bedroom Community'
    END as classification
FROM commuters_2023
WHERE incoming > 0 AND outgoing > 0
ORDER BY (incoming - outgoing) DESC
LIMIT 20;

-- EXAMPLE 3: Gender Distribution Trends (2002-2023)
-- Track changes in male/female commuter ratios over time
-- ============================================================================
SELECT
    t.year,
    ROUND(SUM(CASE WHEN fd.gender = 'male' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END)) as male_incoming,
    ROUND(SUM(CASE WHEN fd.gender = 'female' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END)) as female_incoming,
    ROUND(100.0 * SUM(CASE WHEN fd.gender = 'female' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN fd.gender IN ('male', 'female') AND fd.indicator_id = 101 THEN fd.value ELSE 0 END), 0), 1) as pct_female
FROM fact_demographics fd
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id = 101
  AND fd.gender IN ('male', 'female')
GROUP BY t.year
ORDER BY t.year;

-- EXAMPLE 4: Commuter Growth Analysis
-- Compare 2002 baseline with 2023
-- ============================================================================
WITH baseline AS (
    SELECT
        g.geo_id,
        g.region_name,
        SUM(CASE WHEN fd.indicator_id = 101 THEN fd.value ELSE 0 END) as incoming_2002,
        SUM(CASE WHEN fd.indicator_id = 102 THEN fd.value ELSE 0 END) as outgoing_2002
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE fd.indicator_id IN (101, 102)
      AND t.year = 2002
      AND fd.gender IS NULL
      AND fd.nationality IS NULL
    GROUP BY g.geo_id, g.region_name
),
current AS (
    SELECT
        g.geo_id,
        g.region_name,
        SUM(CASE WHEN fd.indicator_id = 101 THEN fd.value ELSE 0 END) as incoming_2023,
        SUM(CASE WHEN fd.indicator_id = 102 THEN fd.value ELSE 0 END) as outgoing_2023
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE fd.indicator_id IN (101, 102)
      AND t.year = 2023
      AND fd.gender IS NULL
      AND fd.nationality IS NULL
    GROUP BY g.geo_id, g.region_name
)
SELECT
    b.region_name,
    ROUND(b.incoming_2002) as incoming_2002,
    ROUND(c.incoming_2023) as incoming_2023,
    ROUND((c.incoming_2023 - b.incoming_2002) * 100.0 / NULLIF(b.incoming_2002, 0), 1) as incoming_growth_pct,
    ROUND(b.outgoing_2002) as outgoing_2002,
    ROUND(c.outgoing_2023) as outgoing_2023,
    ROUND((c.outgoing_2023 - b.outgoing_2002) * 100.0 / NULLIF(b.outgoing_2002, 0), 1) as outgoing_growth_pct
FROM baseline b
JOIN current c ON b.geo_id = c.geo_id
WHERE b.incoming_2002 > 10000  -- Focus on larger districts
ORDER BY (c.incoming_2023 - b.incoming_2002) DESC
LIMIT 15;

-- EXAMPLE 5: Nationality Breakdown for Düsseldorf (Time Series)
-- Track foreign vs. German commuters over time
-- ============================================================================
SELECT
    t.year,
    ROUND(SUM(CASE WHEN fd.nationality = 'german' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END)) as german_incoming,
    ROUND(SUM(CASE WHEN fd.nationality = 'foreigner' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END)) as foreign_incoming,
    ROUND(100.0 * SUM(CASE WHEN fd.nationality = 'foreigner' AND fd.indicator_id = 101 THEN fd.value ELSE 0 END) /
          NULLIF(SUM(CASE WHEN fd.nationality IN ('german', 'foreigner') AND fd.indicator_id = 101 THEN fd.value ELSE 0 END), 0), 1) as pct_foreign
FROM fact_demographics fd
JOIN dim_geography g ON fd.geo_id = g.geo_id
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id = 101
  AND g.region_code = '05111'  -- Düsseldorf
  AND fd.nationality IN ('german', 'foreigner')
GROUP BY t.year
ORDER BY t.year;

-- EXAMPLE 6: Regional Comparison - Ruhr Cities vs. Rhine Cities (2023)
-- Compare commuter patterns in different urban areas
-- ============================================================================
WITH region_groups AS (
    SELECT geo_id, region_name,
        CASE
            WHEN region_name IN ('Duisburg', 'Essen', 'Dortmund', 'Bochum', 'Gelsenkirchen') THEN 'Ruhr Cities'
            WHEN region_name IN ('Düsseldorf', 'Köln', 'Bonn', 'Leverkusen') THEN 'Rhine Cities'
            ELSE 'Other'
        END as city_group
    FROM dim_geography
)
SELECT
    rg.city_group,
    COUNT(DISTINCT fd.geo_id) as num_cities,
    ROUND(AVG(CASE WHEN fd.indicator_id = 101 AND fd.gender IS NULL AND fd.nationality IS NULL THEN fd.value END)) as avg_incoming,
    ROUND(AVG(CASE WHEN fd.indicator_id = 102 AND fd.gender IS NULL AND fd.nationality IS NULL THEN fd.value END)) as avg_outgoing,
    ROUND(SUM(CASE WHEN fd.indicator_id = 101 AND fd.gender IS NULL AND fd.nationality IS NULL THEN fd.value ELSE 0 END)) as total_incoming,
    ROUND(SUM(CASE WHEN fd.indicator_id = 102 AND fd.gender IS NULL AND fd.nationality IS NULL THEN fd.value ELSE 0 END)) as total_outgoing
FROM fact_demographics fd
JOIN region_groups rg ON fd.geo_id = rg.geo_id
JOIN dim_time t ON fd.time_id = t.time_id
WHERE fd.indicator_id IN (101, 102)
  AND t.year = 2023
  AND rg.city_group IN ('Ruhr Cities', 'Rhine Cities')
GROUP BY rg.city_group;

-- EXAMPLE 7: Year-over-Year Change (2022 vs 2023)
-- Identify districts with significant commuter changes
-- ============================================================================
WITH y2022 AS (
    SELECT geo_id,
        SUM(CASE WHEN indicator_id = 101 AND gender IS NULL AND nationality IS NULL THEN value ELSE 0 END) as incoming_2022
    FROM fact_demographics fd
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE t.year = 2022
    GROUP BY geo_id
),
y2023 AS (
    SELECT geo_id,
        SUM(CASE WHEN indicator_id = 101 AND gender IS NULL AND nationality IS NULL THEN value ELSE 0 END) as incoming_2023
    FROM fact_demographics fd
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE t.year = 2023
    GROUP BY geo_id
)
SELECT
    g.region_name,
    ROUND(y2022.incoming_2022) as incoming_2022,
    ROUND(y2023.incoming_2023) as incoming_2023,
    ROUND(y2023.incoming_2023 - y2022.incoming_2022) as absolute_change,
    ROUND((y2023.incoming_2023 - y2022.incoming_2022) * 100.0 / NULLIF(y2022.incoming_2022, 0), 1) as pct_change
FROM y2022
JOIN y2023 ON y2022.geo_id = y2023.geo_id
JOIN dim_geography g ON y2022.geo_id = g.geo_id
WHERE y2022.incoming_2022 > 0
ORDER BY ABS(y2023.incoming_2023 - y2022.incoming_2022) DESC
LIMIT 15;

-- ============================================================================
-- END OF EXAMPLE QUERIES
-- ============================================================================
