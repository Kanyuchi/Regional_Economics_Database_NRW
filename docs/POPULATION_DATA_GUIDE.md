# Population Data Guide
## Regional Economics Database for NRW

---

## Quick Summary

Your database contains **TWO different population datasets** with different geographic and temporal coverage:

| Dataset | Geographic Level | Time Range | Records | Use Case |
|---------|------------------|------------|---------|----------|
| **Regional DB** | **City/District** (57 regions) | **2011-2024** | 798 | Query specific cities like Duisburg, Dortmund, Essen |
| **State DB** | **NRW State Total** | **1975-2024** | 2,500 | 50 years of NRW-wide historical trends |

---

## 1. City-Level Data (2011-2024)

### Source
- **Table**: 12411-03-03-4 (Regional Database Germany)
- **Indicator**: 1 (pop_total)
- **System**: `regional_db`

### Coverage
- **57 regions** in NRW (all districts and independent cities)
- **14 years** of data (2011-2024)
- **Reference date**: December 31 of each year

### Why Only 2011-2024?

The GENESIS Regional Database restructured their demographic data system in 2011. Before 2011:
- Data used different classification systems
- Table structures were incompatible
- Historical extraction requires different methods

**This is NOT a bug** - it's the maximum historical depth available for city-level data in the current GENESIS system.

### Example Query: City Population

```sql
SELECT
    t.year,
    g.region_name,
    f.value as population
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 1  -- Total population
  AND g.region_name IN ('Duisburg', 'Dortmund', 'Essen')
  AND f.age_group = 'total'
ORDER BY g.region_name, t.year;
```

### Sample Results (2024)
- **Duisburg**: 502,270
- **Dortmund**: 603,462
- **Essen**: 574,682

---

## 2. NRW State-Level Data (1975-2024)

### Source
- **Table**: 12411-9k06 (State Database NRW / Landesdatenbank)
- **Indicators**: 67-71
  - 67: Total Population by Age Group
  - 68: Male Population by Age Group
  - 69: Female Population by Age Group
  - 70: German Population by Age Group
  - 71: Foreign Population by Age Group
- **System**: `state_db`

### Coverage
- **1 region**: North Rhine-Westphalia (NRW) state
- **50 years** of data (1975-2024)
- **10 age groups**: Total, under 6, 6-18, 18-25, 25-30, 30-40, 40-50, 50-60, 60-65, 65+
- **Reference date**: December 31 of each year

### Historical Highlights

| Year | Event | NRW Population | Foreign % |
|------|-------|----------------|-----------|
| 1975 | Post Oil Crisis | 17,129,200 | 6.94% |
| 1990 | German Reunification | 17,349,651 | 9.29% |
| 2000 | New Millennium | 18,009,865 | 11.09% |
| 2015 | Refugee Crisis | 17,865,516 | 11.84% |
| 2024 | Current | 18,034,454 | 15.82% |

### Example Query: NRW Total Over Time

```sql
SELECT
    t.year,
    f.value as total_population
FROM fact_demographics f
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_geography g ON f.geo_id = g.geo_id
WHERE f.indicator_id = 67  -- Total population
  AND g.region_code = '05'  -- NRW state
  AND f.age_group = 'total'
ORDER BY t.year;
```

---

## 3. How to Access the Data

### Option A: SQL Queries

Use the pre-written queries in:
```
sql/queries/nrw_population_history_1975_2024.sql
```

Run with:
```bash
psql $DATABASE_URL -f sql/queries/nrw_population_history_1975_2024.sql
```

### Option B: Python Script

Use the analysis script for interactive queries:

```bash
# List available queries
python scripts/analysis/query_nrw_population_history.py --list

# Run total population query
python scripts/analysis/query_nrw_population_history.py --query total

# Export to CSV
python scripts/analysis/query_nrw_population_history.py --query foreign --output csv

# Export to JSON
python scripts/analysis/query_nrw_population_history.py --query milestones --output json
```

Available queries:
- `total` - NRW Total Population (1975-2024)
- `gender` - Gender Distribution (Decade Snapshots)
- `foreign` - Foreign Population Growth (1975-2024)
- `age_groups` - Age Group Distribution (2024)
- `milestones` - Historical Milestones
- `aging` - Aging Population Analysis

### Option C: Python API

```python
from utils.database import get_database

db = get_database('regional_economics')

# Query city-level data (2011-2024)
city_query = """
    SELECT t.year, g.region_name, f.value
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 1
      AND g.region_name = 'Duisburg'
      AND f.age_group = 'total'
    ORDER BY t.year;
"""
city_results = db.execute_query(city_query)

# Query NRW state-level data (1975-2024)
nrw_query = """
    SELECT t.year, f.value
    FROM fact_demographics f
    JOIN dim_time t ON f.time_id = t.time_id
    JOIN dim_geography g ON f.geo_id = g.geo_id
    WHERE f.indicator_id = 67
      AND g.region_code = '05'
      AND f.age_group = 'total'
    ORDER BY t.year;
"""
nrw_results = db.execute_query(nrw_query)

db.close()
```

---

## 4. Important Notes

### Age Group Filtering

**CRITICAL**: Always filter by `age_group = 'total'` when querying for total population:

```sql
WHERE f.age_group = 'total'  -- REQUIRED!
```

Without this filter, queries will return multiple rows per year (one for each age group), leading to incorrect aggregations.

### Data Quality

- ✅ **City-level data (2011-2024)**: Fixed on 2026-01-09 to remove double-counting issue
- ✅ **State-level data (1975-2024)**: Complete and accurate
- ✅ All population values now show correct totals (~500K for Duisburg, not ~1M)

### Limitations

1. **No city-level data before 2011**
   - Regional DB limitation, not a database issue
   - Historical city data would require different extraction methods

2. **State-level data is NRW-wide only**
   - Cannot break down to individual cities before 2011
   - Use city-level data (2011-2024) for city-specific queries

---

## 5. Common Use Cases

### Use Case 1: Query Specific City Population (2011-2024)

**When**: You need population for Duisburg, Dortmund, Essen, etc.
**Dataset**: Regional DB (Indicator 1)
**Time Range**: 2011-2024

```bash
# Via your MCP server or direct SQL
```

### Use Case 2: Long-term NRW Trends (1975-2024)

**When**: You need 50 years of NRW-wide historical data
**Dataset**: State DB (Indicators 67-71)
**Time Range**: 1975-2024

```bash
python scripts/analysis/query_nrw_population_history.py --query foreign
```

### Use Case 3: Immigration Analysis

**When**: Studying foreign population growth over decades
**Dataset**: State DB (Indicators 67, 71)
**Insight**: Foreign population increased from 6.94% (1975) to 15.82% (2024)

```bash
python scripts/analysis/query_nrw_population_history.py --query foreign --output csv
```

### Use Case 4: Aging Population Study

**When**: Analyzing demographic shifts (youth vs elderly)
**Dataset**: State DB (Indicator 67 with age groups)
**Time Range**: 1975-2024

```bash
python scripts/analysis/query_nrw_population_history.py --query aging
```

---

## 6. Troubleshooting

### Problem: City query returns no data before 2011

**Answer**: This is expected. Regional DB only has city-level data from 2011 onward.

### Problem: Population values are double

**Answer**: Fixed on 2026-01-09. If you still see this, ensure you're filtering by `age_group = 'total'`.

### Problem: Can't find indicators 67-71

**Answer**: These indicators are for NRW state-level data. Check:
```sql
SELECT * FROM dim_indicator WHERE indicator_id BETWEEN 67 AND 71;
```

---

## 7. Data Dictionary

### Indicator IDs

| ID | Code | Name | Source | Level | Years |
|----|------|------|--------|-------|-------|
| 1 | pop_total | Total Population | regional_db | City/District | 2011-2024 |
| 67 | pop_total_by_age | Total Population by Age Group | state_db | NRW State | 1975-2024 |
| 68 | pop_male_by_age | Male Population by Age Group | state_db | NRW State | 1975-2024 |
| 69 | pop_female_by_age | Female Population by Age Group | state_db | NRW State | 1975-2024 |
| 70 | pop_german_by_age | German Population by Age Group | state_db | NRW State | 1975-2024 |
| 71 | pop_foreign_by_age | Foreign Population by Age Group | state_db | NRW State | 1975-2024 |

### Age Groups

State DB data (indicators 67-71) includes these age breakdowns:
- `total` - All ages combined
- `under_6` - Children under 6 years
- `6_to_18` - Children and teens 6-18 years
- `18_to_25` - Young adults 18-25 years
- `25_to_30` - Young adults 25-30 years
- `30_to_40` - Adults 30-40 years
- `40_to_50` - Adults 40-50 years
- `50_to_60` - Adults 50-60 years
- `60_to_65` - Pre-retirement 60-65 years
- `65_plus` - Elderly 65+ years

---

## Summary

✅ **Your database is COMPLETE** with both datasets:
1. City-level: 2011-2024 (798 records, 57 regions)
2. State-level: 1975-2024 (2,500 records, NRW-wide)

✅ **Population data is ACCURATE** after 2026-01-09 fix

✅ **Tools available** for easy querying:
- SQL files in `sql/queries/`
- Python script at `scripts/analysis/query_nrw_population_history.py`

---

**Last Updated**: 2026-01-09
**Status**: ✅ Complete and Verified
