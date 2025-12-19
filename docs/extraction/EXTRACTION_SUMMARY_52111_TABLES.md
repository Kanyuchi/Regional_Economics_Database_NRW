# Extraction Summary: Business Establishment Tables (52111)
**Date:** December 18, 2025  
**Tables:** 52111-01-02-4 & 52111-02-01-4  
**Database:** regional_economics  
**Status:** ✅ COMPLETE

---

## Tables Extracted

### Table 1: 52111-01-02-4 - Branches by Employee Size Class
- **Indicator ID:** 22
- **Records:** 1,425
- **Years:** 2019-2023 (5 years)
- **Categories:** 5 size classes
  - Total (all sizes)
  - 0 to <10 employees (Micro)
  - 10 to <50 employees (Small)
  - 50 to <250 employees (Medium)
  - 250+ employees (Large)

### Table 2: 52111-02-01-4 - Branches by Economic Sector
- **Indicator ID:** 23
- **Records:** 18,424
- **Years:** 2006-2023 (18 years)
- **Categories:** 18 economic sectors (WZ 2008)
  - B: Mining and quarrying
  - C: Manufacturing
  - D: Energy supply
  - E: Water supply
  - F: Construction
  - G: Trade, vehicle maintenance
  - H: Transport and storage
  - I: Accommodation and food services
  - J: Information and communication
  - K: Financial and insurance services
  - L: Real estate
  - M: Professional, scientific services
  - N: Other business services
  - P: Education
  - Q: Health and social work
  - R: Arts, entertainment, recreation
  - S: Other services
  - Plus: Total (all sectors B-N, P-S)

---

## SQL Scripts Created

### 1. Branches by Size Analysis
**File:** `sql/queries/branches_by_size_analysis.sql`

**Key queries:**
- Basic overview and coverage
- Ruhr cities time series (2019-2023)
- Size class distribution (micro vs large enterprises)
- COVID-19 impact analysis
- Year-over-year changes
- Export-ready pivot tables

### 2. Branches by Sector Analysis
**File:** `sql/queries/branches_by_sector_analysis.sql`

**Key queries:**
- Sector distribution by city and year
- Top sectors per city
- Manufacturing vs Services analysis
- Financial Crisis impact (2008-2010)
- COVID-19 impact (2019-2021)
- Sector-level growth trends (2006-2023)
- Export-ready formats

---

## Key Findings - Ruhr Cities

### Size Class Distribution (2023)

| City | Total Branches | Micro (0-10) | % Micro | Large (250+) |
|------|----------------|--------------|---------|--------------|
| Essen | 23,356 | 19,680 | 84.3% | 178 |
| Dortmund | 22,471 | 18,761 | 83.5% | 162 |
| Duisburg | 15,199 | 12,785 | 84.1% | 88 |
| Bochum | 13,942 | 11,718 | 84.0% | 81 |
| Gelsenkirchen | 7,989 | 6,645 | 83.2% | 59 |

**Key Insight:** Micro enterprises (0-10 employees) dominate across all cities at ~84%

### Top 3 Economic Sectors (2023)

**All cities share the same top 3:**
1. **G - Trade** (largest in all cities)
2. **M - Professional services** (2nd in all cities)
3. **F - Construction** OR **Q - Health** (varies by city)

**City-specific examples:**

**Dortmund:**
- G - Trade: 4,040 branches
- M - Professional services: 3,376 branches
- F - Construction: 2,133 branches

**Essen:**
- G - Trade: 4,149 branches (largest)
- M - Professional services: 3,843 branches
- F - Construction: 2,057 branches

**Duisburg:**
- G - Trade: 2,951 branches
- F - Construction: 1,804 branches (higher construction presence)
- M - Professional services: 1,621 branches

---

## COVID-19 Impact Analysis

### Total Branches (All Sectors)

| City | 2019 | 2020 | 2021 | 2023 | COVID Impact | Overall Change |
|------|------|------|------|------|--------------|----------------|
| Essen | 24,083 | 22,985 | 22,860 | 23,356 | -4.6% | -3.0% |
| Duisburg | 15,483 | 14,829 | 14,856 | 15,199 | -4.2% | -1.8% |
| Bochum | 14,071 | 13,491 | 13,550 | 13,942 | -4.1% | -0.9% |
| Gelsenkirchen | 7,958 | 7,634 | 7,711 | 7,989 | -4.1% | +0.4% |
| Dortmund | 22,713 | 21,889 | 21,891 | 22,471 | -3.6% | -1.1% |

**Key Finding:** All cities show ~4% decline in 2020, but strong recovery by 2023

---

## Comparison: Employment vs Establishments

### Different Trajectories

**Construction Employment (1995-2024):**
- Dortmund: -60.6%
- Essen: -55.6%
- Duisburg: -46.4%

**Total Establishments (2019-2023):**
- Dortmund: -1.1%
- Essen: -3.0%
- Duisburg: -1.8%

**Insight:** Employment declined massively while number of establishments remained relatively stable. This suggests:
- Shift to smaller businesses (more micro enterprises)
- Productivity changes
- Possible gig economy / self-employment growth
- Structural transformation of business landscape

---

## Thesis Research Applications

### 1. Institutional Analysis (CME Framework)
- Size class distribution shows strong SME (small/medium enterprise) presence
- 84% micro enterprises aligns with German Mittelstand characteristics
- Business register stability despite employment decline

### 2. Sectoral Transformation
- 18-year sector-level data captures structural changes
- Can track shift from manufacturing to services
- Financial Crisis (2008-2010) impact by sector
- COVID-19 differential sector impacts

### 3. Behavioral Economics Interventions
- Establishment counts more stable than employment
- Possible policy interventions supporting business creation
- Recent recovery (2021-2023) may reflect stimulus measures

### 4. Transferability Assessment
- Different cities show different sector specializations
- Duisburg: Higher construction sector presence
- Can identify successful diversification patterns

---

## Database Statistics (Updated)

| Metric | Value |
|--------|-------|
| **Total Records** | 73,080 |
| **Indicators** | 14 |
| **Regional DB Tables** | 14/17 (82%) |
| **Overall Progress** | 37% |

---

## Sample SQL Queries to Run

### 1. Sector Distribution for Dortmund (2023)

```sql
SELECT 
    f.notes as sector,
    f.value as branches,
    ROUND((f.value / total.total_branches * 100)::numeric, 1) as pct
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
JOIN LATERAL (
    SELECT value as total_branches
    FROM fact_demographics f2
    WHERE f2.geo_id = f.geo_id 
      AND f2.time_id = f.time_id
      AND f2.indicator_id = 23
      AND f2.notes = 'Total - all sectors (B-N, P-S)'
) total ON true
WHERE f.indicator_id = 23
  AND t.year = 2023
  AND g.region_code = '05913'  -- Dortmund
  AND f.notes != 'Total - all sectors (B-N, P-S)'
ORDER BY f.value DESC;
```

### 2. Manufacturing Sector Trend (All Cities)

```sql
SELECT 
    g.region_name as city,
    t.year,
    f.value as manufacturing_branches
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 23
  AND f.notes = 'C - Manufacturing'
  AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
ORDER BY g.region_name, t.year;
```

### 3. Health Sector Growth (2006-2023)

```sql
WITH first_last AS (
    SELECT 
        g.region_name,
        MIN(CASE WHEN t.year = 2006 THEN f.value END) as y2006,
        MAX(CASE WHEN t.year = 2023 THEN f.value END) as y2023
    FROM fact_demographics f
    JOIN dim_geography g ON f.geo_id = g.geo_id
    JOIN dim_time t ON f.time_id = t.time_id
    WHERE f.indicator_id = 23
      AND f.notes = 'Q - Health and social work'
      AND g.region_code IN ('05112', '05911', '05113', '05913', '05513')
    GROUP BY g.region_name
)
SELECT 
    region_name as city,
    y2006,
    y2023,
    (y2023 - y2006) as absolute_change,
    ROUND(((y2023 - y2006) / NULLIF(y2006, 0) * 100)::numeric, 1) as pct_change
FROM first_last
ORDER BY pct_change DESC;
```

---

## Files Created

### Code Files
1. ✅ `src/extractors/regional_db/business_extractor.py` - Added sector extraction
2. ✅ `src/transformers/business_transformer.py` - Added sector transformation
3. ✅ `pipelines/regional_db/etl_52111_01_02_4_branches_size.py`
4. ✅ `pipelines/regional_db/etl_52111_02_01_4_branches_sector.py`

### SQL Scripts
1. ✅ `sql/queries/branches_by_size_analysis.sql`
2. ✅ `sql/queries/branches_by_sector_analysis.sql`

### Data Exports
1. ✅ `timeseries_indicator_22_*.csv` (Size classes)
2. ✅ `timeseries_indicator_23_*.csv` (Sectors)

### Documentation
1. ✅ `EXTRACTION_SUMMARY_52111_TABLES.md` (this file)

---

## Important Bug Fix

**Issue:** The `notes` field was not being saved to database  
**Cause:** `db_loader.py` was missing `'notes': row.get('notes')` in record dictionary  
**Fixed:** Updated `src/loaders/db_loader.py` line 99  
**Impact:** All future extractions now properly save notes  
**Remediation:** Re-ran indicators 20 and 21 to populate notes

**Documentation:** `BUGFIX_NOTES_FIELD_2025-12-18.md`

---

## Next Steps

**Remaining Regional DB tables (3 of 17):**
1. 52311-01-04-4: Business registrations/deregistrations (2000-2023)
2. 52411-02-01-4: Corporate insolvency applications (2000-2023)
3. 71111-01-01-4: Area by type of use (annual data)

**Progress:** 82% of Regional DB tables complete

---

**Status:** ✅ ALL TASKS COMPLETE  
**Total Records Added:** 19,849 (1,425 + 18,424)  
**Database Total:** 73,080 records across 14 indicators
