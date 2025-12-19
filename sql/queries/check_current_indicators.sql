-- Quick check of all indicators currently in the database
-- Run this in DBeaver to see what we have loaded

SELECT 
    indicator_id,
    indicator_code,
    indicator_name,
    source_table_id,
    indicator_category,
    unit_of_measure
FROM dim_indicator
ORDER BY indicator_id;

-- Count records by indicator
SELECT 
    di.indicator_id,
    di.indicator_code,
    di.source_table_id,
    COUNT(*) as record_count,
    MIN(dt.year) as first_year,
    MAX(dt.year) as last_year
FROM dim_indicator di
LEFT JOIN fact_demographics fd ON di.indicator_id = fd.indicator_id
LEFT JOIN dim_time dt ON fd.time_id = dt.time_id
GROUP BY di.indicator_id, di.indicator_code, di.source_table_id
ORDER BY di.indicator_id;

