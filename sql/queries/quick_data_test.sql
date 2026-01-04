/*
===================================================================================
QUICK DATA VERIFICATION - RUN THIS FIRST IN DBEAVER
===================================================================================
Simple queries to quickly verify your database connection and data availability
===================================================================================
*/

-- Test 1: Check if database connection works
SELECT current_database(), current_user, version();

-- Test 2: Count all tables
SELECT 
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY tablename;

-- Test 3: Check dimension tables
SELECT 'dim_indicator' as table_name, COUNT(*) as record_count FROM dim_indicator
UNION ALL
SELECT 'dim_geography', COUNT(*) FROM dim_geography
UNION ALL
SELECT 'dim_time', COUNT(*) FROM dim_time
UNION ALL
SELECT 'fact_demographics', COUNT(*) FROM fact_demographics;

-- Test 4: List all indicators
SELECT 
    indicator_id,
    indicator_code,
    indicator_name,
    source_system,
    source_table_id
FROM dim_indicator
ORDER BY indicator_id;

-- Test 5: Check business registrations specifically
SELECT 
    i.indicator_id,
    i.indicator_name,
    COUNT(*) as records
FROM fact_demographics f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE f.indicator_id IN (24, 25)
GROUP BY i.indicator_id, i.indicator_name;

-- Test 6: Simple Dortmund query (should return data)
SELECT 
    t.year,
    i.indicator_name,
    f.notes,
    f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE g.region_code = '05913'  -- Dortmund
  AND f.indicator_id = 24      -- Registrations
  AND t.year = 2024
ORDER BY f.notes
LIMIT 10;

-- Test 7: Ruhr cities check (should return 5 cities)
SELECT 
    g.region_code,
    g.region_name,
    COUNT(*) as records
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE g.region_code IN ('05913', '05113', '05112', '05911', '05513')
  AND f.indicator_id IN (24, 25)
GROUP BY g.region_code, g.region_name
ORDER BY g.region_name;

/*
===================================================================================
EXPECTED RESULTS:
===================================================================================
Test 5: Should show 2 indicators (24 and 25) with ~6,600 records each
Test 6: Should show 5 rows (one per category) for Dortmund 2024
Test 7: Should show 5 Ruhr cities: Bochum, Dortmund, Duisburg, Essen, Gelsenkirchen

If all these tests pass, your data is loaded correctly! 
Then run the detailed queries in business_registrations_verification.sql
===================================================================================
*/

