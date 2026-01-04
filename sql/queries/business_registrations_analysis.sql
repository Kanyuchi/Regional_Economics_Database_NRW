/*
===================================================================================
BUSINESS REGISTRATIONS - THESIS ANALYSIS QUERIES
===================================================================================
Queries designed for thesis research on Ruhr region industrial transformation
Focus: Business dynamics, entrepreneurship, post-industrial restructuring
===================================================================================
*/

-- ===================================================================================
-- THESIS QUERY 1: Long-term Business Dynamics (1998-2024)
-- ===================================================================================
-- Research Question: How has business formation evolved in the Ruhr region?
-- Shows 27-year trend of net business creation

SELECT 
    t.year,
    g.region_name as city,
    reg.value as registrations,
    dereg.value as deregistrations,
    (reg.value - dereg.value) as net_change,
    ROUND(((reg.value - dereg.value) / reg.value) * 100, 1) as net_change_pct
FROM dim_time t
CROSS JOIN dim_geography g
LEFT JOIN fact_demographics reg ON 
    reg.geo_id = g.geo_id AND 
    reg.time_id = t.time_id AND 
    reg.indicator_id = 24 AND 
    reg.notes = 'Total registrations'
LEFT JOIN fact_demographics dereg ON 
    dereg.geo_id = g.geo_id AND 
    dereg.time_id = t.time_id AND 
    dereg.indicator_id = 25 AND 
    dereg.notes = 'Total deregistrations'
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND t.year BETWEEN 1998 AND 2024
ORDER BY t.year, g.region_name;


-- ===================================================================================
-- THESIS QUERY 2: Post-Industrial Entrepreneurship (2010-2024)
-- ===================================================================================
-- Research Question: Has entrepreneurship increased in the post-coal era?
-- Analyzes business foundations vs takeovers

SELECT 
    t.year,
    g.region_name as city,
    SUM(CASE WHEN f.notes = 'New establishments' THEN f.value ELSE 0 END) as new_establishments,
    SUM(CASE WHEN f.notes = 'Business foundations' THEN f.value ELSE 0 END) as business_foundations,
    SUM(CASE WHEN f.notes = 'Business takeovers' THEN f.value ELSE 0 END) as takeovers,
    SUM(CASE WHEN f.notes IN ('New establishments', 'Business foundations') THEN f.value ELSE 0 END) as total_entrepreneurial,
    ROUND(SUM(CASE WHEN f.notes IN ('New establishments', 'Business foundations') THEN f.value ELSE 0 END) / 
          SUM(CASE WHEN f.notes = 'Total registrations' THEN f.value ELSE NULL END) * 100, 1) as entrepreneurial_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id = 24
  AND t.year >= 2010
GROUP BY t.year, g.region_name
ORDER BY t.year DESC, g.region_name;


-- ===================================================================================
-- THESIS QUERY 3: COVID-19 Resilience Analysis (2019-2024)
-- ===================================================================================
-- Research Question: How did Ruhr cities respond to COVID shock?
-- Compares business formation before, during, and after pandemic

WITH yearly_data AS (
    SELECT 
        t.year,
        g.region_name as city,
        g.region_code,
        f.value as registrations
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
      AND f.indicator_id = 24
      AND f.notes = 'Total registrations'
      AND t.year BETWEEN 2019 AND 2024
),
baseline AS (
    SELECT region_code, AVG(registrations) as pre_covid_avg
    FROM yearly_data
    WHERE year IN (2019)
    GROUP BY region_code
)
SELECT 
    y.year,
    y.city,
    y.registrations,
    b.pre_covid_avg as baseline_2019,
    (y.registrations - b.pre_covid_avg) as deviation_from_baseline,
    ROUND(((y.registrations - b.pre_covid_avg) / b.pre_covid_avg) * 100, 1) as pct_change_from_baseline,
    CASE 
        WHEN y.year = 2019 THEN 'Pre-COVID'
        WHEN y.year IN (2020, 2021) THEN 'COVID Period'
        ELSE 'Post-COVID Recovery'
    END as period
FROM yearly_data y
JOIN baseline b ON y.region_code = b.region_code
ORDER BY y.year DESC, y.city;


-- ===================================================================================
-- THESIS QUERY 4: Comparative Economic Vitality
-- ===================================================================================
-- Research Question: Which Ruhr cities show strongest entrepreneurial activity?
-- Ranks cities by multiple business dynamism metrics

WITH metrics_2024 AS (
    SELECT 
        g.region_name as city,
        g.region_code,
        SUM(CASE WHEN f.notes = 'Total registrations' THEN f.value ELSE 0 END) as total_reg,
        SUM(CASE WHEN f.notes = 'New establishments' THEN f.value ELSE 0 END) as new_est,
        SUM(CASE WHEN f.notes = 'Business foundations' THEN f.value ELSE 0 END) as foundations
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
      AND f.indicator_id = 24
      AND t.year = 2024
    GROUP BY g.region_name, g.region_code
),
dereg_2024 AS (
    SELECT 
        g.region_code,
        SUM(CASE WHEN f.notes = 'Total deregistrations' THEN f.value ELSE 0 END) as total_dereg
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
      AND f.indicator_id = 25
      AND t.year = 2024
    GROUP BY g.region_code
)
SELECT 
    m.city,
    m.total_reg as registrations,
    d.total_dereg as deregistrations,
    (m.total_reg - d.total_dereg) as net_businesses,
    ROUND((m.total_reg - d.total_dereg) / m.total_reg * 100, 1) as survival_rate_pct,
    m.new_est as new_establishments,
    m.foundations as business_foundations,
    (m.new_est + m.foundations) as entrepreneurial_activity,
    ROUND((m.new_est + m.foundations) / m.total_reg * 100, 1) as entrepreneurial_share_pct,
    RANK() OVER (ORDER BY m.total_reg DESC) as rank_by_registrations,
    RANK() OVER (ORDER BY (m.total_reg - d.total_dereg) DESC) as rank_by_net_change
FROM metrics_2024 m
JOIN dereg_2024 d ON m.region_code = d.region_code
ORDER BY net_businesses DESC;


-- ===================================================================================
-- THESIS QUERY 5: Business Churn Rate Evolution
-- ===================================================================================
-- Research Question: Is business turnover increasing or stabilizing?
-- Calculates churn rate (deregistrations/total) over time

SELECT 
    t.year,
    g.region_name as city,
    reg.value as registrations,
    dereg.value as deregistrations,
    ROUND((dereg.value / (reg.value + dereg.value)) * 100, 1) as churn_rate_pct,
    ROUND((reg.value / dereg.value), 2) as reg_to_dereg_ratio
FROM dim_time t
CROSS JOIN dim_geography g
LEFT JOIN fact_demographics reg ON 
    reg.geo_id = g.geo_id AND 
    reg.time_id = t.time_id AND 
    reg.indicator_id = 24 AND 
    reg.notes = 'Total registrations'
LEFT JOIN fact_demographics dereg ON 
    dereg.geo_id = g.geo_id AND 
    dereg.time_id = t.time_id AND 
    dereg.indicator_id = 25 AND 
    dereg.notes = 'Total deregistrations'
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND t.year BETWEEN 2000 AND 2024
  AND reg.value IS NOT NULL
ORDER BY t.year DESC, g.region_name;


-- ===================================================================================
-- THESIS QUERY 6: Relocation Patterns (In vs Out)
-- ===================================================================================
-- Research Question: Are Ruhr cities attracting or losing businesses?
-- Analyzes business relocations in/out of regions

SELECT 
    t.year,
    g.region_name as city,
    SUM(CASE WHEN f.notes = 'Relocations into region' THEN f.value ELSE 0 END) as relocations_in,
    SUM(CASE WHEN f.notes = 'Relocations out of region' THEN f.value ELSE 0 END) as relocations_out,
    (SUM(CASE WHEN f.notes = 'Relocations into region' THEN f.value ELSE 0 END) - 
     SUM(CASE WHEN f.notes = 'Relocations out of region' THEN f.value ELSE 0 END)) as net_relocation,
    CASE 
        WHEN (SUM(CASE WHEN f.notes = 'Relocations into region' THEN f.value ELSE 0 END) - 
              SUM(CASE WHEN f.notes = 'Relocations out of region' THEN f.value ELSE 0 END)) > 0 
        THEN 'Net Attractor'
        ELSE 'Net Loser'
    END as relocation_status
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
  AND t.year BETWEEN 2015 AND 2024
GROUP BY t.year, g.region_name
ORDER BY t.year DESC, net_relocation DESC;


-- ===================================================================================
-- THESIS QUERY 7: Business Succession Patterns
-- ===================================================================================
-- Research Question: How prevalent is business succession in the Ruhr?
-- Analyzes takeovers vs handovers

SELECT 
    t.year,
    g.region_name as city,
    SUM(CASE WHEN f.notes = 'Business takeovers' THEN f.value ELSE 0 END) as takeovers,
    SUM(CASE WHEN f.notes = 'Business handovers' THEN f.value ELSE 0 END) as handovers,
    (SUM(CASE WHEN f.notes = 'Business takeovers' THEN f.value ELSE 0 END) + 
     SUM(CASE WHEN f.notes = 'Business handovers' THEN f.value ELSE 0 END)) as total_succession,
    SUM(CASE WHEN f.notes = 'Total registrations' THEN f.value ELSE 0 END) as total_registrations,
    ROUND((SUM(CASE WHEN f.notes = 'Business takeovers' THEN f.value ELSE 0 END) + 
           SUM(CASE WHEN f.notes = 'Business handovers' THEN f.value ELSE 0 END)) / 
          SUM(CASE WHEN f.notes = 'Total registrations' THEN f.value ELSE NULL END) * 100, 1) as succession_share_pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
  AND t.year >= 2015
GROUP BY t.year, g.region_name
ORDER BY t.year DESC, total_succession DESC;


-- ===================================================================================
-- THESIS QUERY 8: Summary Statistics for Thesis
-- ===================================================================================
-- Comprehensive summary for thesis introduction/results section

SELECT 
    g.region_name as city,
    COUNT(DISTINCT t.year) as years_covered,
    MIN(t.year) as first_year,
    MAX(t.year) as last_year,
    ROUND(AVG(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' THEN f.value END), 0) as avg_registrations_per_year,
    ROUND(AVG(CASE WHEN f.indicator_id = 25 AND f.notes = 'Total deregistrations' THEN f.value END), 0) as avg_deregistrations_per_year,
    MAX(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' AND t.year = 1998 THEN f.value END) as reg_1998,
    MAX(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' AND t.year = 2024 THEN f.value END) as reg_2024,
    ROUND(((MAX(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' AND t.year = 2024 THEN f.value END) - 
            MAX(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' AND t.year = 1998 THEN f.value END)) / 
           MAX(CASE WHEN f.indicator_id = 24 AND f.notes = 'Total registrations' AND t.year = 1998 THEN f.value END) * 100), 1) as pct_change_1998_2024
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
GROUP BY g.region_name
ORDER BY g.region_name;

/*
===================================================================================
USAGE NOTES FOR THESIS:
===================================================================================
- Query 1: Use for visualizing long-term trends (line charts)
- Query 2: Use for analyzing entrepreneurial activity vs consolidation
- Query 3: Use for COVID-19 impact analysis (before/during/after)
- Query 4: Use for comparative rankings and city profiles
- Query 5: Use for business dynamism/churn analysis
- Query 6: Use for regional attractiveness analysis
- Query 7: Use for business succession/Mittelstand analysis
- Query 8: Use for summary statistics in thesis text

Export results as CSV for visualization in Python/R or Excel
===================================================================================
*/

