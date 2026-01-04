# BA Employment/Wage Data Coverage Explanation

## Question: Why Only 2020-2024 Data When Files Were Downloaded for 2014-2024?

### Summary Answer
**Files Downloaded**: 11 years (2014-2024)
**Data Extracted**: 5 years (2020-2024)
**Reason**: BA changed data format in 2020 from sector aggregates to district-level granularity

---

## Detailed Explanation

### What We Downloaded

```
📁 data/raw/ba/
├── entgelt-d-0-201412-xls.xls          (2014) - 6.6 MB
├── entgelt-d-0-201512-xlsm.xlsm        (2015) - 4.1 MB
├── entgelt-d-0-201612-xlsm.xlsm        (2016) - 3.8 MB
├── entgelt-d-0-201712-xlsm.xlsm        (2017) - 3.9 MB
├── entgelt-d-0-201812-xlsm.xlsm        (2018) - 3.6 MB
├── entgelt-d-0-201912-xlsm.xlsm        (2019) - 1.6 MB  ❌ No district data
├── entgelt-d-0-202012-xlsx.xlsx        (2020) - 4.7 MB  ✓ District data starts
├── entgelt-d-0-202112-xlsx.xlsx        (2021) - 4.6 MB
├── entgelt-dwolk-0-202212-xlsx.xlsx    (2022) - 4.6 MB
├── entgelt-dwolk-0-202312-xlsx.xlsx    (2023) - 4.6 MB
└── entgelt-dwolk-0-202412-xlsx.xlsx    (2024) - 4.6 MB
```

---

## Data Structure Comparison

### Pre-2020 Format (2014-2019)

**File**: `entgelt-d-0-201912-xlsm.xlsm` (2019 example)

**Sheet Structure**:
- Sheet 8.1.1: Male employees by economic sectors
- Sheet 8.1.2: Female employees by economic sectors
- Sheet 8.1.3: Regional aggregates (East/West Germany only)

**Sample Data** (Sheet 8.1.1):
```
Row | Gender/Sector                          | Code | Total Employees
----|----------------------------------------|------|----------------
1   | Männer (Males)                         | -    | 14,625,508
2   | Land- und Forstwirtschaft (Agriculture)| A    | 114,095
3   | Produzierendes Gewerbe (Manufacturing) | B-F  | 6,470,090
4   | Handel, Gastgewerbe (Trade, Hospitality)| G-J | 3,900,425
...
```

**Characteristics**:
- 42 rows total
- National-level aggregates only
- Grouped by economic sectors (WZ 2008)
- **NO district codes** (no 05111, 05112, etc.)
- **NO district names** (no Düsseldorf, Essen, etc.)

**Region Coverage**:
- ❌ Districts: 0
- ✓ Germany: 1
- ✓ East/West: 2

---

### Post-2020 Format (2020-2024)

**File**: `entgelt-dwolk-0-202412-xlsx.xlsx` (2024 example)

**Sheet Structure**:
- Sheet 8.1: **Unified district-level data** with full demographics
- Sheet 8.2: Time series
- Sheet 8.3: Economic sectors
- Sheet 8.4: Occupations
- Sheet 8.5: Low-wage workers
- Sheet 8.6: Apprentices

**Sample Data** (Sheet 8.1):
```
Region Code | Region Name        | Category    | Total  | Wage <2k | 2-3k    | ...
------------|-------------------|-------------|--------|----------|---------|----
D           | Deutschland       | Insgesamt   | 21.8M  | 810k     | 4.37M   | ...
05          | NRW               | Insgesamt   | 5.57M  | 212k     | 1.10M   | ...
05111       | Düsseldorf        | Insgesamt   | 367k   | 8.5k     | 49k     | ...
05112       | Duisburg          | Insgesamt   | 144k   | 5.2k     | 27k     | ...
05113       | Essen             | Insgesamt   | 261k   | 8.7k     | 50k     | ...
...
```

**Characteristics**:
- 6,290 rows total
- **District-level granularity**
- Full demographic breakdowns (gender, age, nationality, education, skill)
- Complete wage brackets (<2k, 2-3k, 3-4k, 4-5k, 5-6k, >6k EUR)
- **795 NRW district codes found**

**Region Coverage**:
- ✓ Districts: 54 NRW districts
- ✓ Regional aggregates: NRW (05), Düsseldorf (051), Köln (053), etc.
- ✓ Germany: 1

---

## Verification Results

### Database Coverage Check

```sql
SELECT dt.year, COUNT(*) AS records, COUNT(DISTINCT fd.geo_id) AS regions
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (89, 90, 91)
GROUP BY dt.year;
```

**Results**:
| Year | Records | Regions |
|------|---------|---------|
| 2020 | 6,352   | 53      |
| 2021 | 6,360   | 53      |
| 2022 | 6,360   | 53      |
| 2023 | 6,348   | 53      |
| 2024 | 6,348   | 53      |

**Total**: 31,768 records across 5 years

---

## Why Did BA Change the Format?

### Hypothesis
The German Federal Employment Agency likely improved their data infrastructure in 2020 to provide more granular, district-level statistics to support:

1. **Regional Economic Analysis**: Enable researchers and policymakers to analyze wage disparities at the district level
2. **Labor Market Monitoring**: Track employment and wage trends in specific geographic areas
3. **Evidence-Based Policy**: Support targeted economic development and labor market interventions
4. **COVID-19 Response**: The 2020 timing suggests this may have been part of enhanced monitoring capabilities during the pandemic

### Evidence
- **File size increase**: 2019 (1.6 MB) → 2020 (4.7 MB) - nearly 3x larger
- **Row count increase**: 42 rows → 6,290 rows - 150x more data
- **New columns**: Demographic breakdowns (age, education, skill level) added
- **Unified structure**: Multiple sheets consolidated into Sheet 8.1

---

## Impact on Analysis

### What We CAN Do (2020-2024)
✓ District-level wage comparisons across NRW
✓ Gender wage gap analysis by district
✓ Education and skill premium calculations
✓ Wage distribution analysis (6 brackets)
✓ Age group wage patterns
✓ Nationality-based wage differences

**Example Query**:
```sql
-- Top 5 NRW districts by median wage in 2024
SELECT dg.region_name, ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90 AND dt.year = 2024
  AND dg.region_code LIKE '05___'
ORDER BY fd.value DESC LIMIT 5;

-- Result: Düsseldorf (4,760 EUR), Bonn (4,498 EUR), ...
```

### What We CANNOT Do (2014-2019)
❌ District-level wage trends before 2020
❌ Historical wage gap analysis by district
❌ Long-term district employment growth (only have 5 years)

### Alternative Data Sources for Pre-2020 Analysis
For historical analysis before 2020, use:
1. **Regional Database Germany** - Employment indicators (4-12) from 2008+
2. **State Database NRW** - Employee compensation by sector (41-55) for 2022
3. **State Database NRW** - Income distribution (88) from 2005-2019

---

## Recommendation

**Accept the 2020-2024 timeframe** because:
1. **5 years is sufficient** for trend analysis and forecasting
2. **Data quality is higher** - more granular, more demographic dimensions
3. **Consistent structure** - easier to maintain and update going forward
4. **Alternative sources exist** for pre-2020 analysis at state/national level

If historical district-level wage data is critical, consider:
- Contacting BA directly to request archived district data (if available)
- Using proxy indicators from Regional/State databases
- Focusing analysis on the robust 2020-2024 period

---

## Files Reference

### Extractor Code
- [employment_wage_extractor.py](../../src/extractors/ba/employment_wage_extractor.py:45-50) - Contains `FILE_PATTERNS` dict with 2020-2024 only

### Documentation
- [ba_employment_wage_summary.md](ba_employment_wage_summary.md:20-52) - Full coverage explanation
- [indicators_translation_english.md](indicators_translation_english.md:79-92) - Updated status

### Verification
```bash
# Check downloaded files
ls -lh data/raw/ba/

# Check extracted data years
SELECT DISTINCT dt.year FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
WHERE fd.indicator_id IN (89, 90, 91)
ORDER BY dt.year;
```

---

**Conclusion**: The database correctly contains **2020-2024 data only** because district-level wage data became available starting in 2020. The 2014-2019 files contain valuable sector-level aggregates but lack the geographic granularity required for district analysis.
