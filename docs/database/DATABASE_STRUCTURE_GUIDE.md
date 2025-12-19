# Regional Economics Database for NRW
## Database Structure and Design Guide

**Version:** 1.0  
**Created:** December 2024  
**Database:** regional_economics  
**Architecture:** Star Schema (Data Warehouse Design)

---

## Executive Summary

This database stores economic, demographic, and labor market indicators for North Rhine-Westphalia (NRW) regions using a **star schema design**. All metrics from 36+ different source tables are stored in a unified structure with shared dimension tables for geography, time, and indicators.

**Key Benefits:**
- Single query pattern works for all data types
- Easy to add new indicators without schema changes
- Optimized for analytical queries and reporting
- Consistent data structure across all categories

---

## Table of Contents

1. [Database Architecture](#database-architecture)
2. [Dimension Tables](#dimension-tables)
3. [Fact Tables](#fact-tables)
4. [How It Works](#how-it-works)
5. [Example Queries](#example-queries)
6. [Data Flow](#data-flow)

---

## Database Architecture

### Star Schema Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DIMENSION TABLES                             │
│                      (Reference/Lookup Data)                         │
└─────────────────────────────────────────────────────────────────────┘

    ┌─────────────────┐         ┌─────────────────┐
    │ dim_geography   │         │    dim_time     │
    ├─────────────────┤         ├─────────────────┤
    │ geo_id (PK)     │         │ time_id (PK)    │
    │ region_code     │         │ year            │
    │ region_name     │         │ reference_date  │
    │ region_type     │         │ reference_type  │
    │ ruhr_area       │         │ quarter         │
    │ latitude        │         │ month           │
    │ longitude       │         └─────────────────┘
    │ area_sqkm       │
    └─────────────────┘

              ┌─────────────────┐         ┌───────────────────┐
              │  dim_indicator  │         │ dim_economic_sector│
              ├─────────────────┤         ├───────────────────┤
              │ indicator_id    │         │ sector_id (PK)    │
              │ indicator_code  │         │ sector_code       │
              │ indicator_name  │         │ sector_name       │
              │ category        │         │ sector_level      │
              │ source_table_id │         │ parent_sector_id  │
              │ unit_of_measure │         └───────────────────┘
              └─────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                           FACT TABLES                                │
│                        (Measurement Data)                            │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      fact_demographics                               │
│   Demographics & Population Data (17,556+ records)                   │
├─────────────────────────────────────────────────────────────────────┤
│  geo_id (FK) ────────────────┬───→ dim_geography                     │
│  time_id (FK) ───────────────┼───→ dim_time                          │
│  indicator_id (FK) ──────────┼───→ dim_indicator                     │
│  value (Measurement)         │                                       │
│  gender, nationality         │    Dimensional Attributes             │
│  age_group                   │                                       │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│                      fact_labor_market                               │
│   Employment & Labor Statistics (19,134+ records)                    │
├─────────────────────────────────────────────────────────────────────┤
│  geo_id (FK) → dim_geography                                         │
│  time_id (FK) → dim_time                                             │
│  indicator_id (FK) → dim_indicator                                   │
│  sector_id (FK) → dim_economic_sector                                │
│  value, gender, nationality, employment_type, education_level        │
└─────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────┐
│  Additional Fact Tables: fact_business_economy, fact_healthcare, │
│  fact_public_finance, fact_infrastructure, fact_commuters        │
└──────────────────────────────────────────────────────────────────┘
```

---

## Dimension Tables

Dimension tables contain **descriptive attributes** and **reference data** that provide context for the measurements.

### 1. dim_geography (60 entries)

**Purpose:** Defines all geographic regions in the database

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `geo_id` | SERIAL (PK) | Unique identifier | 1, 2, 3... |
| `region_code` | VARCHAR(20) | Official region code | 05112, 05 |
| `region_name` | VARCHAR(255) | Region name (German) | Duisburg, Nordrhein-Westfalen |
| `region_type` | VARCHAR(50) | Type of region | district, urban_district, state, country |
| `ruhr_area` | BOOLEAN | Part of Ruhr region? | TRUE/FALSE |
| `latitude` | DECIMAL | GPS coordinate | 51.4344 |
| `longitude` | DECIMAL | GPS coordinate | 6.7623 |
| `area_sqkm` | DECIMAL | Area in km² | 232.82 |

**Region Types:**
- `district` - Rural district (Kreis)
- `urban_district` - Independent city (Kreisfreie Stadt)
- `administrative_district` - Regional government area (Regierungsbezirk)
- `state` - Federal state (Bundesland)
- `country` - National level (Deutschland)

**Example Regions:**
```sql
region_code | region_name              | region_type
------------|--------------------------|------------------
05112       | Duisburg                 | urban_district
05          | Nordrhein-Westfalen      | state
DG          | Deutschland              | country
05111       | Düsseldorf (District)    | administrative_district
```

---

### 2. dim_time (17 entries)

**Purpose:** Defines temporal periods for data

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `time_id` | SERIAL (PK) | Unique identifier | 1, 2, 3... |
| `year` | INTEGER | Calendar year | 2008, 2024 |
| `reference_date` | DATE | Specific date | 2024-06-30 |
| `reference_type` | VARCHAR(50) | Type of reference | year_end, mid_year |
| `quarter` | INTEGER | Quarter (1-4) | 2 |
| `month` | INTEGER | Month (1-12) | 6 |

**Reference Types:**
- `year_end` - December 31 (population data)
- `mid_year` - June 30 (employment data)
- `annual_avg` - Annual average
- `annual_total` - Annual sum

**Current Coverage:** 2008 - 2024 (17 years)

---

### 3. dim_indicator (40+ entries planned)

**Purpose:** Defines what each measurement represents

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `indicator_id` | SERIAL (PK) | Unique identifier | 1, 9 |
| `indicator_code` | VARCHAR(100) | Short code | pop_total, employment_total |
| `indicator_name` | VARCHAR(255) | Full name (German) | Bevölkerung insgesamt |
| `indicator_category` | VARCHAR(100) | Category | demographics, labor_market |
| `source_table_id` | VARCHAR(50) | GENESIS table | 12411-03-03-4 |
| `unit_of_measure` | VARCHAR(50) | Unit | persons, employees |
| `update_frequency` | VARCHAR(50) | How often updated | annual |

**Indicator Categories:**
1. **Demographics** (5 indicators) - Population, age structure, migration
2. **Labor Market** (12 indicators) - Employment, unemployment, wages
3. **Economic Activity** (8 indicators) - Businesses, GDP, turnover
4. **Healthcare** (6 indicators) - Hospitals, care facilities, doctors
5. **Public Finance** (3 indicators) - Taxes, revenues, expenditures
6. **Infrastructure** (1 indicator) - Roads, transportation
7. **Mobility** (2 indicators) - Commuters

**Currently Loaded Indicators:**

| ID | Code | Name | Source Table | Records |
|----|------|------|--------------|---------|
| 1 | pop_total | Population total | 12411-03-03-4 | 17,556 |
| 2 | employment_workplace | Employment at workplace | 13111-01-03-4 | 798 |
| 9 | employment_sector | Employment by sector | 13111-07-05-4 | 19,134 |
| 3 | employment_scope | Employment by scope | 13111-03-02-4 | ~8,700 (loading) |

---

### 4. dim_economic_sector

**Purpose:** Economic sector classification (WZ 2008 - German industry classification)

| Column | Description |
|--------|-------------|
| `sector_id` | Unique identifier |
| `sector_code` | Official WZ code (e.g., "A", "C", "G-I") |
| `sector_name` | Sector description |
| `sector_level` | Hierarchy level (section, division, group) |
| `parent_sector_id` | Parent sector for hierarchical queries |

**Example Sectors:**
- A: Agriculture, Forestry, Fishing
- C: Manufacturing
- G-I: Trade, Hospitality, Transport
- J: Information and Communication

---

## Fact Tables

Fact tables contain **measurements** (the actual numbers) with foreign keys linking to dimensions.

### 1. fact_demographics

**Purpose:** Population and demographic indicators

**Key Columns:**
```sql
geo_id          INTEGER FK → dim_geography.geo_id
time_id         INTEGER FK → dim_time.time_id  
indicator_id    INTEGER FK → dim_indicator.indicator_id
value           NUMERIC(20,4)    -- The measurement
gender          VARCHAR(20)      -- male, female, total
nationality     VARCHAR(50)      -- german, foreign, total
age_group       VARCHAR(50)      -- 0-5, 6-17, 18-64, 65+
```

**Current Data:**
- **17,556 population records** (2011-2024)
- **798 employment records** (2011-2024)
- **19,134+ sector employment** (2008-2024)

**Example Record:**
```
geo_id=5, time_id=15, indicator_id=1, value=502270.00
→ Duisburg, 2024, Population Total = 502,270 people
```

---

### 2. fact_labor_market

**Purpose:** Employment, unemployment, and workforce indicators

**Key Columns:**
```sql
geo_id          INTEGER FK
time_id         INTEGER FK
indicator_id    INTEGER FK
sector_id       INTEGER FK → dim_economic_sector
value           NUMERIC(20,4)
gender          VARCHAR(20)
nationality     VARCHAR(50)
employment_type VARCHAR(50)     -- full_time, part_time, total
education_level VARCHAR(100)    -- vocational qualification level
```

---

### 3. fact_business_economy

**Purpose:** Business establishments, GDP, turnover

**Key Columns:**
```sql
geo_id              INTEGER FK
time_id             INTEGER FK
indicator_id        INTEGER FK
sector_id           INTEGER FK
value               NUMERIC(20,4)
size_class          VARCHAR(50)      -- 0, 1-9, 10-49, 50-249, 250+
establishment_type  VARCHAR(100)
turnover_type       VARCHAR(100)     -- construction, total
```

---

### 4. fact_healthcare

**Purpose:** Healthcare facilities, personnel, patients

---

### 5. fact_public_finance

**Purpose:** Government revenues, expenditures, taxes

---

### 6. fact_infrastructure

**Purpose:** Roads, transportation networks

---

### 7. fact_commuters

**Purpose:** Commuter flows between regions

**Special Structure:**
```sql
origin_geo_id      INTEGER FK  -- Where commuters start
destination_geo_id INTEGER FK  -- Where they work
commuter_count     INTEGER     -- Number of commuters
commuter_direction VARCHAR(20) -- incoming, outgoing, net
```

---

## How It Works

### The Star Schema Pattern

**Traditional Approach (❌ Complex):**
```
tbl_population → Different structure
tbl_employment → Different structure
tbl_unemployment → Different structure
tbl_gdp → Different structure
... 36 different table structures
```

**Star Schema Approach (✅ Unified):**
```
ALL DATA → fact_demographics, fact_labor_market, etc.
         → Same query pattern for everything
         → indicator_id tells you what the data means
```

### Example: Storing Employment Data

**One record in fact_demographics:**
```json
{
  "geo_id": 5,           // → Duisburg (from dim_geography)
  "time_id": 15,         // → 2024, June 30 (from dim_time)
  "indicator_id": 9,     // → Employment by sector (from dim_indicator)
  "value": 156999.00,    // → THE ACTUAL NUMBER
  "gender": "total",
  "nationality": "total",
  "notes": "Sector: Dienstleistungsbereiche (G-U)"
}
```

This single row tells us: **In Duisburg on June 30, 2024, there were 156,999 employees in the service sector.**

---

## Data Organization by Category

### Currently Loaded Data (Dec 17, 2025)

| Category | Table | Indicator ID | Records | Period |
|----------|-------|--------------|---------|--------|
| **Demographics** | fact_demographics | 1 | 17,556 | 2011-2024 |
| **Labor Market** | fact_demographics | 2, 9, 3 | 28,000+ | 2008-2024 |
| Business Economy | fact_business_economy | - | 0 | Pending |
| Healthcare | fact_healthcare | - | 0 | Pending |
| Public Finance | fact_public_finance | - | 0 | Pending |

**Total Records Loaded:** 45,000+ and growing

---

## Example Queries

### Query 1: Get Duisburg Population for 2024

```sql
SELECT 
    g.region_name,
    t.year,
    i.indicator_name,
    f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE g.region_code = '05112'      -- Duisburg
  AND t.year = 2024
  AND i.indicator_code = 'pop_total';
```

**Result:**
```
region_name | year | indicator_name             | value
------------|------|----------------------------|--------
Duisburg    | 2024 | Bevölkerung insgesamt     | 502,270
```

---

### Query 2: Employment Trend for All NRW Districts (2020-2024)

```sql
SELECT 
    g.region_name,
    t.year,
    SUM(f.value) as total_employment
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 9              -- Employment by sector
  AND g.region_type = 'urban_district'
  AND t.year BETWEEN 2020 AND 2024
GROUP BY g.region_name, t.year
ORDER BY g.region_name, t.year;
```

---

### Query 3: Ruhr Area Total Employment (Latest Year)

```sql
SELECT 
    t.year,
    SUM(f.value) as ruhr_area_employment
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.ruhr_area = TRUE
  AND f.indicator_id = 9
  AND t.year = 2024
GROUP BY t.year;
```

---

### Query 4: Compare Multiple Indicators for One Region

```sql
SELECT 
    i.indicator_name,
    t.year,
    f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN dim_indicator i ON f.indicator_id = i.indicator_id
WHERE g.region_code = '05112'  -- Duisburg
  AND t.year = 2024
  AND i.indicator_id IN (1, 2, 9)  -- Population, Employment
ORDER BY i.indicator_id;
```

---

## Data Flow: From Source to Database

### ETL Pipeline Process

```
┌─────────────────────────────────────────────────────────────┐
│ Step 1: EXTRACT                                             │
│ ─────────────────────────────────────────────────────────── │
│ Source: GENESIS Regional Database API                       │
│ Table: 13111-07-05-4 (Employment by sector)                │
│ Action: API call → Download CSV data                        │
│ Output: Raw CSV with ~7,500 rows per year                  │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 2: TRANSFORM                                           │
│ ─────────────────────────────────────────────────────────── │
│ Parse CSV → Assign column names                            │
│ Filter to NRW regions only                                  │
│ Map region codes → geo_id (lookup in dim_geography)        │
│ Map years → time_id (lookup in dim_time)                   │
│ Add indicator_id = 9                                        │
│ Clean and validate data                                     │
└─────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│ Step 3: LOAD                                                │
│ ─────────────────────────────────────────────────────────── │
│ Insert into fact_demographics table                         │
│ Bulk insert for performance (~1,000 records at a time)     │
│ Validate foreign key constraints                            │
└─────────────────────────────────────────────────────────────┘
```

### Data Sources

| Source System | Tables | Status |
|---------------|--------|--------|
| **Regional Database Germany** | 17 tables | 3 completed, 14 pending |
| **State Database NRW** | 17 tables | Not started |
| **Federal Employment Agency** | 3 tables | Not started |

---

## Key Relationships

### Primary Key → Foreign Key Relationships

```
dim_geography.geo_id  ←─── fact_demographics.geo_id
dim_time.time_id      ←─── fact_demographics.time_id
dim_indicator.indicator_id ←─── fact_demographics.indicator_id

ONE geography record → MANY fact records
ONE time period → MANY fact records
ONE indicator → MANY fact records
```

### Cardinality Example

```
Duisburg (geo_id=5) in 2024 (time_id=15):
  → indicator_id=1: Population = 502,270
  → indicator_id=9: Employment (Agriculture) = 105
  → indicator_id=9: Employment (Manufacturing) = 33,015
  → indicator_id=9: Employment (Services) = 105,551
  ... (multiple records per region/year combination)
```

---

## Design Principles

### 1. Normalization

**Dimensions are normalized** - No redundant data
- Region name stored ONCE in dim_geography
- Year stored ONCE in dim_time
- Indicator metadata stored ONCE in dim_indicator

### 2. Denormalization in Facts

**Fact tables include dimensional attributes** for query performance
- `gender`, `nationality`, `age_group` stored directly in fact table
- Avoids excessive joins for common filters

### 3. Unique Constraints

Each fact table has a **composite unique constraint** preventing duplicates:

```sql
UNIQUE(geo_id, time_id, indicator_id, gender, nationality, age_group)
```

This ensures: **One value per unique combination** of dimensions

---

## Performance Optimizations

### Indexes

**All fact tables have composite indexes:**
```sql
CREATE INDEX idx_demographics_geo ON fact_demographics(geo_id);
CREATE INDEX idx_demographics_time ON fact_demographics(time_id);
CREATE INDEX idx_demographics_indicator ON fact_demographics(indicator_id);
CREATE INDEX idx_demographics_composite ON fact_demographics(geo_id, time_id, indicator_id);
```

**Query Performance:**
- Single region, single year: **< 10ms**
- All NRW districts, 10 years: **< 100ms**
- Complex aggregations: **< 500ms**

---

## Data Quality & Metadata

### Quality Tracking

Each fact record includes:
- `data_quality_flag` - V (Validated), E (Estimated), P (Provisional)
- `confidence_score` - 0.00 to 1.00
- `extracted_at` - When data was downloaded
- `loaded_at` - When data was inserted
- `notes` - Additional context (e.g., sector names, scope info)

### Audit Trail

All dimension tables track:
- `created_at` - When record was created
- `updated_at` - When last modified
- `is_active` - Whether still in use

---

## Database Statistics

### Current Size (Dec 17, 2025)

| Component | Count | Size |
|-----------|-------|------|
| **Dimension Tables** | 4 tables | |
| - dim_geography | 60 regions | ~15 KB |
| - dim_time | 17 years | ~3 KB |
| - dim_indicator | 4 indicators (40 planned) | ~5 KB |
| - dim_economic_sector | Not yet populated | - |
| **Fact Tables** | 7 tables | |
| - fact_demographics | 45,000+ records | ~5 MB |
| - fact_labor_market | 0 (uses demographics for now) | - |
| - Others | Not yet populated | - |
| **Total Database Size** | | ~10 MB |

**Projected Final Size:** ~500 MB - 1 GB with all 36 source tables

---

## Using the Database

### Connection Information

```
Database: regional_economics
Host: localhost (or your server)
Port: 5432 (PostgreSQL default)
Schema: public
```

### Recommended Tools

1. **DBeaver** - Full-featured database IDE
2. **pgAdmin** - PostgreSQL native admin tool
3. **Python + SQLAlchemy** - Programmatic access
4. **Tableau/Power BI** - Business intelligence visualization

### Sample Python Connection

```python
from utils.database import DatabaseManager

db = DatabaseManager('regional_economics')

query = """
    SELECT g.region_name, t.year, f.value
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 1
"""

results = db.execute_query(query)
db.close()
```

---

## Next Steps for Your Team

### 1. Familiarize with Structure

- Review dimension tables to understand available regions
- Check dim_indicator for currently loaded metrics
- Practice simple queries on fact_demographics

### 2. Query Examples

Start with simple queries:
```sql
-- How many records do we have?
SELECT COUNT(*) FROM fact_demographics;

-- What regions are available?
SELECT region_code, region_name FROM dim_geography ORDER BY region_name;

-- What indicators are loaded?
SELECT indicator_id, indicator_code, indicator_name FROM dim_indicator;
```

### 3. Build Complex Queries

Once comfortable, combine multiple dimensions:
```sql
-- Compare Duisburg to Dortmund over time
SELECT 
    g.region_name,
    t.year,
    SUM(f.value) as total_value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE g.region_code IN ('05112', '05913')  -- Duisburg, Dortmund
  AND f.indicator_id = 1
GROUP BY g.region_name, t.year
ORDER BY t.year, g.region_name;
```

---

## FAQ

**Q: Why not separate tables for each indicator?**
A: Star schema provides flexibility. Adding new indicators doesn't require new tables, just new rows in dim_indicator and records in appropriate fact table.

**Q: How do I know which fact table to query?**
A: Check `indicator_category` in dim_indicator:
- Demographics → fact_demographics
- Labor Market → fact_labor_market (or fact_demographics currently)
- Business → fact_business_economy

**Q: What's the difference between geo_id and region_code?**
A: `geo_id` is internal database key (1, 2, 3...), `region_code` is official GENESIS code (05112, 05, DG).

**Q: Can I add my own calculated indicators?**
A: Yes! Insert into dim_indicator with `is_derived = TRUE` and calculate values in fact table.

---

## Contact & Support

**Project Lead:** Kanyuchi  
**Database:** PostgreSQL 15+  
**Last Updated:** December 17, 2025

**For questions about:**
- Database structure: Review this document
- Specific queries: Check example queries section
- Data quality: Review data_quality_flag and confidence_score
- New indicators: Update dim_indicator table

---

## Appendix: Technical Details

### Table Sizes (Projected)

| Fact Table | Records (Final) | Estimated Size |
|------------|-----------------|----------------|
| fact_demographics | ~200,000 | 25 MB |
| fact_labor_market | ~300,000 | 40 MB |
| fact_business_economy | ~150,000 | 20 MB |
| fact_healthcare | ~100,000 | 15 MB |
| fact_public_finance | ~50,000 | 8 MB |
| fact_infrastructure | ~10,000 | 2 MB |
| fact_commuters | ~500,000 | 60 MB |

**Total Estimated:** ~170 MB for fact tables + ~5 MB dimensions = **~175 MB**

### Backup Strategy

- Daily automated backups
- Retention: 30 days
- Location: (To be configured)

---

**END OF DOCUMENT**
