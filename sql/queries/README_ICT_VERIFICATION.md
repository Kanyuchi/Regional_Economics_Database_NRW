# ICT Indicators Data Verification Guide

## Overview

This directory contains SQL verification scripts for the **ICT Indicators dataset** (table 52911-01i) from the State Database NRW (Landesdatenbank). These scripts help you verify successful data loading and explore the ICT adoption patterns in Nordrhein-Westfalen.

## Quick Start

### 1. Quick Verification (Recommended First Step)

Run this for a fast overview:

```bash
psql -h localhost -p 5432 -U fadzie -d regional_db -f sql/queries/verify_ict_quick.sql
```

**What it checks:**
- ✅ Metadata loaded (25 indicators)
- ✅ Fact records loaded (25 records)
- ✅ Years covered (2020)
- ✅ Data quality (no duplicates, value ranges)
- ✅ Sample data preview

**Expected Output:**
```
✅ ICT Indicators Metadata: 25
✅ ICT Fact Records: 25
✅ Years Covered: 1
✅ Regions Covered: 1
```

### 2. Comprehensive Verification

Run this for detailed analysis:

```bash
psql -h localhost -p 5432 -U fadzie -d regional_db -f sql/queries/verify_ict_indicators.sql
```

## What Data Was Loaded?

### Source Information
- **Table ID**: 52911-01i
- **Source**: Landesdatenbank NRW (State Database)
- **API**: Genesis REST API
- **Data Type**: State-level aggregate data (not district-level)
- **Region**: Nordrhein-Westfalen (region_code: 05)
- **Period**: 2020
- **Unit**: Percentage (Prozent)

### Database Tables

#### 1. `dim_indicator` (Metadata)
- **Indicator IDs**: 104-128 (25 indicators)
- **Category**: ICT
- **Source System**: state_db
- **Fields**: indicator_code, indicator_name, unit_of_measure, source_table_id

#### 2. `fact_ict_indicators` (Actual Data)
- **Records**: 25 fact records
- **Foreign Keys**: geo_id, time_id, indicator_id
- **Value Range**: 5.80% - 97.50%
- **Average**: 46.08%

## Key Insights from the Data

### High Adoption Indicators (Top 5)

| Rank | Indicator | Adoption Rate |
|------|-----------|---------------|
| 1 | UN mit Internetzugang (Internet Access) | 97.50% |
| 2 | UN mit ortsfester Internetverbindung (Fixed Internet) | 89.00% |
| 3 | UN mit Rechnungen in Papierform (Paper Invoices) | 85.40% |
| 4 | UN mit ausreichender Internetverbindung (Adequate Internet) | 77.70% |
| 5 | UN mit Verkäufen über Website/App an Privatkunden (B2C Sales) | 74.70% |

**Insight**: Basic internet infrastructure has near-universal adoption (97.5%), but there's still room for improvement in connection speed and quality.

### Low Adoption Indicators (Bottom 5)

| Rank | Indicator | Adoption Rate |
|------|-----------|---------------|
| 1 | UN mit Nutzung von Industrie-/Servicerobotern (Robotics) | 5.80% |
| 2 | UN mit Einstellung von IT-Fachkräften (IT Hiring) | 10.20% |
| 3 | UN mit Verkäufen über Website/App (E-commerce) | 11.50% |
| 4 | UN mit Verkäufen über Website/App/EDI | 13.40% |
| 5 | UN mit Big-Data Analyse (Big Data Analytics) | 15.30% |

**Insight**: Advanced technologies like robotics (5.8%) and big data analytics (15.3%) show low adoption, indicating significant digital transformation potential.

## SQL Script Sections

### Section 1: Metadata Verification
Checks that all 25 ICT indicators were properly loaded into `dim_indicator` with correct categories and source information.

**Key Queries:**
- Count of ICT indicators
- View all metadata
- Check for missing required fields

### Section 2: Fact Data Verification
Verifies the actual ICT data in `fact_ict_indicators` table.

**Key Queries:**
- Total record count (should be 25)
- Summary statistics (min, max, avg values)
- NULL value checks
- Sample records with joined dimensions

### Section 3: Data Quality Checks
Ensures data integrity and identifies any issues.

**Key Queries:**
- Duplicate record detection
- Value range validation (0-100% for percentages)
- Orphaned reference checks
- Missing fact data for indicators

### Section 4: Comprehensive Data View
Full dataset with all contextual information from dimension tables.

**Key Queries:**
- Full ICT dataset with geography, time, and indicator context
- Year-by-year grouping for trend analysis

### Section 5: Top/Bottom Analysis
Identifies highest and lowest ICT adoption indicators.

**Key Queries:**
- Top 10 indicators by value
- Bottom 10 indicators by value

### Section 6: Specific Indicator Analysis
Focused queries for different ICT categories.

**Key Queries:**
- Internet access indicators
- E-commerce indicators
- Website and digital presence indicators

### Section 7: Export-Ready Views
Creates a materialized view for business reports.

**View Created:**
```sql
CREATE OR REPLACE VIEW view_ict_indicators_summary AS
SELECT
    t.year,
    g.region_name,
    i.indicator_name,
    f.value || ' ' || f.unit as formatted_value,
    ...
```

### Section 8: Comparison with Other Data
Compares ICT data volume with other indicator categories in the database.

### Section 9: Quick Status Check
Master status query for rapid verification (run this first!).

## Running Individual Queries

You can run specific queries from the comprehensive script:

```bash
# Run a single query
psql -h localhost -p 5432 -U fadzie -d regional_db -c "
SELECT indicator_name, value
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY value DESC
LIMIT 5;
"
```

## Common Use Cases

### 1. Export ICT Data to CSV

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
    WHERE i.source_table_id = '52911-01i'
) TO '/path/to/ict_indicators_export.csv' WITH CSV HEADER;"
```

### 2. Check if Specific Indicator Exists

```bash
psql -h localhost -p 5432 -U fadzie -d regional_db -c "
SELECT indicator_name, value, unit
FROM fact_ict_indicators f
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.indicator_name ILIKE '%internet%'
    AND i.source_table_id = '52911-01i';
"
```

### 3. Get Summary for Business Report

```bash
psql -h localhost -p 5432 -U fadzie -d regional_db -c "
SELECT * FROM view_ict_indicators_summary
ORDER BY raw_value DESC;
"
```

## Troubleshooting

### Issue: "No rows returned"

**Possible Causes:**
1. Data not loaded yet - run the ETL pipeline first
2. Wrong indicator_id range - ICT indicators use IDs 104-128
3. Wrong source_table_id - should be '52911-01i'

**Solution:**
```sql
-- Check if data exists
SELECT COUNT(*) FROM fact_ict_indicators WHERE indicator_id >= 104;

-- Check indicator range
SELECT MIN(indicator_id), MAX(indicator_id)
FROM dim_indicator
WHERE source_table_id = '52911-01i';
```

### Issue: "Duplicate key violations"

**Cause:** Attempting to reload data without removing existing records.

**Solution:**
```sql
-- Delete existing ICT data
DELETE FROM fact_ict_indicators WHERE indicator_id >= 104 AND indicator_id <= 128;
DELETE FROM dim_indicator WHERE source_table_id = '52911-01i';
```

### Issue: "Foreign key constraint violation"

**Cause:** Indicator metadata not loaded before fact data.

**Solution:**
Always load metadata first:
1. Load dim_indicator (metadata)
2. Then load fact_ict_indicators (actual data)

## Data Interpretation Notes

### Understanding the Indicators

**"UN" = Unternehmen** (Enterprises/Companies)

Common abbreviations in indicator names:
- **UN**: Unternehmen (Enterprises)
- **Vk.**: Verkauf (Sales)
- **Beschäft.**: Beschäftigte (Employees)
- **elektr.**: elektronisch (Electronic)
- **Internetverb.**: Internetverbindung (Internet Connection)

### Value Interpretation

- All values are **percentages** representing the share of enterprises with that characteristic
- **High values (>70%)**: Widely adopted technologies (e.g., basic internet access)
- **Medium values (30-70%)**: Moderate adoption (e.g., websites, mobile internet)
- **Low values (<30%)**: Emerging or specialized technologies (e.g., robotics, big data)

### Missing Values

During transformation, 47 out of 72 extracted indicators were filtered because they contained missing value markers:
- `-` : Data not available
- `x` : Data confidential/suppressed
- `/` : No data exists
- `...` : Value unknown

This is normal and indicates that not all indicators had valid data for the year 2020.

## Next Steps

### 1. Load Additional Years

```bash
python src/pipelines/ict_indicators_pipeline.py --start-year 2021 --end-year 2025 --indicator-id-base 104
```

### 2. Create Visualizations

Export data and use tools like:
- **Tableau**: Connect to PostgreSQL directly
- **Power BI**: Use PostgreSQL connector
- **Python**: pandas + matplotlib/seaborn
- **R**: RPostgreSQL + ggplot2

### 3. Trend Analysis (Once Multiple Years Loaded)

```sql
SELECT
    t.year,
    i.indicator_name,
    f.value,
    LAG(f.value) OVER (PARTITION BY i.indicator_id ORDER BY t.year) as prev_year_value,
    f.value - LAG(f.value) OVER (PARTITION BY i.indicator_id ORDER BY t.year) as year_over_year_change
FROM fact_ict_indicators f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE i.source_table_id = '52911-01i'
ORDER BY i.indicator_id, t.year;
```

## Support

For issues or questions:
1. Check the main README: `/README_ICT_PIPELINE.md`
2. Review logs: `/data/logs/`
3. Run verification scripts to diagnose issues

## File Summary

| File | Purpose | Run Time |
|------|---------|----------|
| `verify_ict_quick.sql` | Fast overview check | ~5 seconds |
| `verify_ict_indicators.sql` | Comprehensive analysis | ~30 seconds |
| `README_ICT_VERIFICATION.md` | This documentation | - |

---

**Last Updated**: 2026-01-10
**Data Version**: 2020 (single year)
**Next Update**: After loading years 2021-2025
