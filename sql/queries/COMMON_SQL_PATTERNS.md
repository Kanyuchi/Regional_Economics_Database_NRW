# Common SQL Query Patterns - Regional Economics Database

## Overview

This guide provides correct SQL syntax for common query patterns used in the Regional Economics Database. All queries are tested and ready to run.

---

## 1. Compare All Fact Tables

### ✅ Correct Syntax

```sql
-- Full comparison (dimensional tables only)
SELECT
    'fact_demographics' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions
FROM fact_demographics

UNION ALL

SELECT
    'fact_ict_indicators' as fact_table,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as unique_indicators,
    COUNT(DISTINCT geo_id) as unique_regions
FROM fact_ict_indicators

ORDER BY total_records DESC;
```

### ❌ Incorrect Syntax (Will Fail)

```sql
-- ERROR: Cannot have multiple FROM clauses
SELECT COUNT(*), COUNT(DISTINCT indicator_id)
FROM fact_demographics;
FROM fact_ict_indicators;  -- ❌ Syntax error
```

**Error**: `ERROR: syntax error at or near "FROM"`

---

## 2. Standard Dimensional Join

### ✅ Correct Syntax

```sql
-- Join fact table with all dimensions
SELECT
    g.region_name,
    t.year,
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE t.year = 2020
ORDER BY f.value DESC;
```

**Result**: Returns 25 rows with all contextual information

### Key Points:
- Always use table aliases (f, g, t, i) for readability
- Join ON foreign key relationships
- Can add WHERE, ORDER BY, LIMIT as needed

---

## 3. Compare Data Across Multiple Years

### ✅ Correct Syntax (When multiple years are loaded)

```sql
SELECT
    t.year,
    COUNT(*) as record_count,
    AVG(f.value) as avg_value,
    MIN(f.value) as min_value,
    MAX(f.value) as max_value
FROM fact_ict_indicators f
JOIN dim_time t ON f.time_id = t.time_id
GROUP BY t.year
ORDER BY t.year;
```

---

## 4. Filter by Indicator Category

### ✅ Correct Syntax

```sql
SELECT
    i.indicator_category,
    COUNT(*) as fact_records,
    COUNT(DISTINCT i.indicator_id) as unique_indicators
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY i.indicator_category;
```

---

## 5. Find Top/Bottom Values

### ✅ Correct Syntax - Top 10

```sql
SELECT
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
ORDER BY f.value DESC
LIMIT 10;
```

### ✅ Correct Syntax - Bottom 10

```sql
SELECT
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
ORDER BY f.value ASC
LIMIT 10;
```

---

## 6. Export Data to CSV

### ✅ Correct Syntax

```bash
psql -h localhost -p 5432 -U fadzie -d regional_db -c "\COPY (
    SELECT
        g.region_name,
        t.year,
        i.indicator_name,
        f.value,
        f.unit
    FROM fact_ict_indicators f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_indicator i ON f.indicator_id = i.indicator_id
    ORDER BY t.year, i.indicator_id
) TO '/path/to/output.csv' WITH CSV HEADER;"
```

**Important**: Use absolute paths or `$PWD/filename.csv`

---

## 7. Check Data Loading Status

### ✅ Correct Syntax

```sql
-- Quick status check
SELECT
    'fact_ict_indicators' as table_name,
    COUNT(*) as total_records,
    COUNT(DISTINCT indicator_id) as indicators,
    COUNT(DISTINCT geo_id) as regions,
    COUNT(DISTINCT time_id) as time_periods,
    MIN(loaded_at)::date as first_loaded,
    MAX(loaded_at)::date as last_loaded
FROM fact_ict_indicators;
```

**Expected Result** (after successful load):
```
table_name          | total_records | indicators | regions | time_periods | first_loaded | last_loaded
--------------------|---------------|------------|---------|--------------|--------------|-------------
fact_ict_indicators | 25            | 25         | 1       | 1            | 2026-01-10   | 2026-01-10
```

---

## 8. Search Indicators by Keyword

### ✅ Correct Syntax

```sql
-- Find indicators containing "internet"
SELECT
    i.indicator_id,
    i.indicator_name,
    f.value,
    f.unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.indicator_name ILIKE '%internet%'
ORDER BY f.value DESC;
```

**Note**: `ILIKE` is case-insensitive; use `LIKE` for case-sensitive

---

## 9. Aggregate by Category

### ✅ Correct Syntax

```sql
-- Group indicators by custom categories
SELECT
    CASE
        WHEN i.indicator_name ILIKE '%internet%' THEN 'Internet Access'
        WHEN i.indicator_name ILIKE '%website%' THEN 'Web Presence'
        WHEN i.indicator_name ILIKE '%verkauf%' THEN 'E-Commerce'
        ELSE 'Other'
    END as category,
    COUNT(*) as indicator_count,
    ROUND(AVG(f.value)::numeric, 2) as avg_adoption_rate
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
GROUP BY category
ORDER BY avg_adoption_rate DESC;
```

---

## 10. Join Multiple Fact Tables

### ✅ Correct Syntax

```sql
-- Compare demographics and ICT data side-by-side
SELECT
    g.region_name,
    t.year,
    COUNT(DISTINCT CASE WHEN f1.indicator_id IS NOT NULL THEN f1.indicator_id END) as demo_indicators,
    COUNT(DISTINCT CASE WHEN f2.indicator_id IS NOT NULL THEN f2.indicator_id END) as ict_indicators
FROM dim_geography g
CROSS JOIN dim_time t
LEFT JOIN fact_demographics f1 ON g.geo_id = f1.geo_id AND t.time_id = f1.time_id
LEFT JOIN fact_ict_indicators f2 ON g.geo_id = f2.geo_id AND t.time_id = f2.time_id
WHERE t.year = 2020
GROUP BY g.region_name, t.year
ORDER BY g.region_name;
```

---

## Common Errors and Fixes

### Error 1: Multiple FROM Clauses

**❌ Wrong**:
```sql
SELECT COUNT(*) FROM table1;
FROM table2;  -- ERROR
```

**✅ Correct**:
```sql
SELECT COUNT(*) FROM table1
UNION ALL
SELECT COUNT(*) FROM table2;
```

### Error 2: Missing Table Alias in JOIN

**❌ Wrong**:
```sql
SELECT indicator_name, value
FROM fact_ict_indicators
JOIN dim_indicator ON indicator_id = indicator_id;  -- Ambiguous!
```

**✅ Correct**:
```sql
SELECT i.indicator_name, f.value
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id;
```

### Error 3: Wrong Date Comparison

**❌ Wrong**:
```sql
WHERE year = '2020'  -- year is INTEGER, not VARCHAR
```

**✅ Correct**:
```sql
WHERE year = 2020  -- No quotes for numbers
```

### Error 4: UNION without ALL

**⚠️ Slower**:
```sql
SELECT * FROM table1
UNION  -- Removes duplicates (slow!)
SELECT * FROM table2;
```

**✅ Faster** (if duplicates don't matter):
```sql
SELECT * FROM table1
UNION ALL  -- Keeps duplicates (faster!)
SELECT * FROM table2;
```

---

## Quick Reference Scripts

All these scripts are available in `/sql/queries/`:

| File | Purpose | Run Time |
|------|---------|----------|
| `compare_fact_tables.sql` | Compare all fact tables | 5 sec |
| `verify_ict_quick.sql` | Quick ICT verification | 5 sec |
| `verify_ict_indicators.sql` | Comprehensive ICT analysis | 30 sec |

### Running Scripts

```bash
# From project root
cd "/Volumes/NO NAME/Regional Economics Database for NRW"

# Run a specific script
psql -h localhost -p 5432 -U fadzie -d regional_db -f sql/queries/SCRIPT_NAME.sql

# Run inline query
psql -h localhost -p 5432 -U fadzie -d regional_db -c "YOUR SQL HERE"
```

---

## Performance Tips

1. **Use EXPLAIN ANALYZE** to check query performance:
   ```sql
   EXPLAIN ANALYZE
   SELECT * FROM fact_ict_indicators;
   ```

2. **Use indexes** - fact tables have indexes on:
   - geo_id
   - time_id
   - indicator_id
   - Composite indexes

3. **Use LIMIT** for testing:
   ```sql
   SELECT * FROM fact_ict_indicators LIMIT 10;
   ```

4. **Use COUNT(*) instead of COUNT(column)** when possible:
   ```sql
   -- Faster
   SELECT COUNT(*) FROM fact_ict_indicators;

   -- Slower (checks for NULLs)
   SELECT COUNT(value) FROM fact_ict_indicators;
   ```

---

## Need Help?

- **Syntax Errors**: Check quotes, commas, and keywords
- **Performance Issues**: Use EXPLAIN ANALYZE
- **Data Issues**: Run verification scripts first
- **Missing Data**: Check dim_indicator and fact tables separately

---

**Last Updated**: 2026-01-10
**Tested With**: PostgreSQL 13+
**Database**: regional_db
