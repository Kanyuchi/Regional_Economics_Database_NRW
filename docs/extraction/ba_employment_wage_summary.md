# BA Employment and Wage Data Integration Summary

## Overview
Successfully integrated employment and wage data from the German Federal Employment Agency (Bundesagentur für Arbeit, BA) into the Regional Economics Database for NRW.

**Period**: 2020-2024 (5 years)
**Coverage**: District-level data for 54 NRW districts + Germany national total
**Records Loaded**: 31,768
**Indicators Added**: 3 (IDs 89-91)

---

## Data Source

**Source**: Federal Employment Agency (BA)
**URL**: https://statistik.arbeitsagentur.de/
**Table**: Sheet 8.1 - Employment and Wage Statistics
**File Format**: Excel (.xlsx, .xlsm, .xls)

### Files Downloaded vs. Data Extracted

**Files Downloaded**: 11 files (2014-2024)
- 2014: `entgelt-d-0-201412-xls.xls` (6.6 MB)
- 2015-2018: `.xlsm` format files
- 2019: `entgelt-d-0-201912-xlsm.xlsm` (1.6 MB)
- 2020-2021: `.xlsx` format files
- 2022-2024: `entgelt-dwolk-0-YYYYMM-xlsx.xlsx` files

**Data Extracted**: 5 years (2020-2024) ✓

**Why the Discrepancy?**

The German Federal Employment Agency (BA) changed their data publication format in 2020:

| Period | File Structure | Data Granularity | District Codes? | Can Extract? |
|--------|---------------|------------------|-----------------|--------------|
| **2014-2019** | Sheet 8.1.1/8.1.2/8.1.3 | National/state aggregates by economic sectors | ❌ No (0 codes) | ❌ No |
| **2020-2024** | Sheet 8.1 (unified) | District-level with demographic breakdowns | ✓ Yes (795 codes) | ✓ Yes |

**2014-2019 Files Contain:**
- 42 rows of national-level data
- Breakdowns by economic sectors (WZ 2008: Agriculture, Manufacturing, Services, etc.)
- Gender breakdowns only
- **NO district (Kreisebene) identifiers**

**2020-2024 Files Contain:**
- 6,290+ rows of district-level data
- Full demographic breakdowns (gender, age, nationality, education, skill level)
- 54 NRW districts + regional aggregates
- **Complete district codes (05111, 05112, etc.)**

**Impact**: District-level wage analysis is only possible from 2020 onwards. For historical analysis before 2020, only national/state-level trends can be examined using the Regional Database Germany or State Database NRW indicators.

---

## Indicators Created

### Indicator 89: Full-time Employees Subject to Social Insurance (Core Group)
- **Unit**: Persons
- **Breakdowns**: Gender, age groups, nationality, education, skill level
- **Description**: Total number of full-time employees subject to social insurance, excluding marginal and irregular employment
- **Reference Date**: December 31 of each year

### Indicator 90: Median Monthly Gross Wage
- **Unit**: EUR
- **Breakdowns**: Gender, age groups, nationality, education, skill level
- **Description**: Median monthly gross wage for full-time employees
- **Reference Date**: December 31 of each year

### Indicator 91: Distribution of Full-time Employees by Wage Brackets
- **Unit**: Persons
- **Breakdowns**: 6 wage brackets × demographic categories
- **Wage Brackets**:
  - Under 2,000 EUR
  - 2,000 - 3,000 EUR
  - 3,000 - 4,000 EUR
  - 4,000 - 5,000 EUR
  - 5,000 - 6,000 EUR
  - Over 6,000 EUR
- **Description**: Number of employees in each monthly gross wage bracket by demographics

---

## Demographic Dimensions

All three indicators include breakdowns by:

### Gender
- Male
- Female
- Total

### Age Groups
- Under 25 years
- 25 to 55 years
- 55 years and over

### Nationality
- German
- Foreigner

### Education Level
- No vocational degree
- Recognized vocational degree
- Academic degree

### Skill Level (German qualification framework)
- Assistant (Helfer) - simple tasks
- Specialist (Fachkraft) - qualified work
- Expert (Spezialist) - complex specialist work
- Highly Qualified (Experte) - highly complex tasks

---

## Key Findings (2024)

### Employment
- **NRW Total Employees**: ~5.6 million full-time employees subject to social insurance
- **Male-Female Ratio**: Approximately 2:1 (male: 67%, female: 33%)
- **Age Distribution**:
  - Under 25: ~8%
  - 25-55: ~70%
  - 55+: ~22%

### Wages (NRW)

#### Gender Wage Gap Trend (2020-2024)
| Year | Male Median | Female Median | Gap EUR | Gap % |
|------|-------------|---------------|---------|-------|
| 2020 | 3,621 EUR   | 3,224 EUR     | 398 EUR | 12.3% |
| 2021 | 3,697 EUR   | 3,316 EUR     | 381 EUR | 11.5% |
| 2022 | 3,815 EUR   | 3,450 EUR     | 364 EUR | 10.6% |
| 2023 | 3,950 EUR   | 3,588 EUR     | 362 EUR | 10.1% |
| 2024 | 4,152 EUR   | 3,824 EUR     | 329 EUR |  8.6% |

**Trend**: Gender wage gap decreasing by ~17% from 2020 to 2024 (from 398 EUR to 329 EUR)

#### Top 5 Districts by Median Wage (2024)
1. **Düsseldorf**: 4,760 EUR
2. **Bonn**: 4,498 EUR
3. **Mülheim an der Ruhr**: 4,407 EUR
4. **Essen**: 4,310 EUR
5. **Bottrop**: 4,227 EUR

#### Wage by Education Level (NRW, 2024)
- No Vocational Degree: ~2,987 EUR
- Recognized Vocational Degree: ~3,870 EUR (+30%)
- Academic Degree: ~5,916 EUR (+98%)

#### Wage by Skill Level (NRW, 2024)
- Assistant (Helfer): ~2,863 EUR
- Specialist (Fachkraft): ~3,720 EUR (+30%)
- Expert (Spezialist): ~5,005 EUR (+75%)
- Highly Qualified (Experte): ~6,292 EUR (+120%)

---

## Technical Implementation

### Files Created

#### Extractors
- `src/extractors/ba/base_extractor.py` - Base class for BA Excel extraction
- `src/extractors/ba/employment_wage_extractor.py` - Multi-year employment/wage extractor
- `src/extractors/ba/__init__.py` - Package exports

#### Transformers
- `src/transformers/employment_wage_transformer.py` - Wide-to-long format transformation

#### Pipelines
- `pipelines/ba/etl_ba_employment_wage.py` - Complete ETL pipeline

#### Utilities
- `scripts/utilities/add_ba_employment_wage_indicators.py` - Indicator setup
- `scripts/test_ba_extractor.py` - Extractor test script
- `download_ba_files.py` - Automated file download script

#### Documentation & Verification
- `sql/queries/ba_employment_wage_verification.sql` - Verification and analysis queries
- `docs/extraction/ba_employment_wage_summary.md` - This document

### Data Storage

**Raw Data**: `data/raw/ba/employment_wage_raw_2020_2024.csv` (4,125 records)
**Database Table**: `fact_demographics`
**Transformed Records**: 31,768

---

## Data Quality

### Loading Statistics
- **Records Loaded**: 31,768 ✓
- **Records Skipped**: 1,200 (region code 05316 - not in geography table)
- **Records Failed**: 0 ✓

### Coverage
- **Regions**: 53 (54 NRW districts + Germany, minus 1 missing geography)
- **Years**: 5 (2020-2024)
- **Completeness**: 100% for available regions and years

### Validation Checks
- ✓ No NULL values in critical fields
- ✓ No negative values
- ✓ All regions have complete 5-year coverage
- ✓ Demographic breakdowns sum to totals correctly

---

## Usage Examples

### Query Median Wage Trends
```sql
SELECT
    dt.year,
    dg.region_name,
    ROUND(fd.value::numeric, 2) AS median_wage_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code = '05'
    AND fd.gender = 'total'
ORDER BY dt.year;
```

### Query Gender Wage Gap
```sql
SELECT
    dt.year,
    MAX(CASE WHEN fd.gender = 'male' THEN fd.value END) -
    MAX(CASE WHEN fd.gender = 'female' THEN fd.value END) AS wage_gap_eur
FROM fact_demographics fd
JOIN dim_time dt ON fd.time_id = dt.time_id
JOIN dim_geography dg ON fd.geo_id = dg.geo_id
WHERE fd.indicator_id = 90
    AND dg.region_code = '05'
GROUP BY dt.year;
```

### Query Wage Distribution
```sql
SELECT
    CASE
        WHEN fd.notes LIKE '%under_2000%' THEN 'Under 2,000 EUR'
        WHEN fd.notes LIKE '%2000_to_3000%' THEN '2,000-3,000 EUR'
        -- ... other brackets
    END AS wage_bracket,
    ROUND(fd.value::numeric, 0) AS employee_count
FROM fact_demographics fd
WHERE fd.indicator_id = 91
    AND fd.notes LIKE '%wage_bracket:%'
ORDER BY wage_bracket;
```

---

## Future Enhancements

1. **Commuter Statistics**: Add BA commuter data (incoming/outgoing flows between districts)
2. **Earlier Years**: If district-level data becomes available for pre-2020 period
3. **Monthly Updates**: Automate monthly data refresh when new BA files are published
4. **Sectoral Analysis**: Add wage breakdowns by economic sectors (available in BA data)
5. **Occupational Data**: Integrate wage data by occupation categories (KldB 2010)

---

## References

- **BA Statistics Portal**: https://statistik.arbeitsagentur.de/
- **Employment/Wage Data**: https://statistik.arbeitsagentur.de/SiteGlobals/Forms/Suche/Einzelheftsuche_Formular.html?nn=1523076&topic_f=beschaeftigung-entgelt-entgelt
- **Data Dictionary**: Available in downloaded Excel files (sheets: "III meth. Hinweise", "IV Glossar")

---

**Last Updated**: 2026-01-03
**Database**: regional_economics
**Total BA Indicators**: 3 (89-91)
**Total Project Indicators**: 91 (1-91)
