# BA Additional Sheets Analysis - Extraction Potential

## Overview
Beyond Sheet 8.1 (demographics and wage brackets) which is already extracted, the BA employment/wage files contain **4 additional sheets with district-level data** that could significantly expand the database.

**Current Status**: Sheet 8.1 extracted (31,768 records, indicators 89-91)
**Potential Expansion**: ~47,000 additional records across 4 sheets

---

## Sheet-by-Sheet Analysis

### ✓ Sheet 8.2: Time Series Data
**Description**: Median wage trends over multiple years by demographic category
**District Coverage**: 795 NRW district codes found (54 districts × ~15 demographic categories)
**Data Structure**:
```
Columns: 12
- Region Code (5-digit)
- Region Name
- Demographic Category (Insgesamt, Männer, Frauen, age groups, etc.)
- Year 2020 median wage (EUR)
- Year 2021 median wage (EUR)
- Year 2022 median wage (EUR)
- Year 2023 median wage (EUR)
- Year 2024 median wage (EUR)
- [Empty column]
- West/East classification
- State (NRW)
- Employment Agency district
```

**Sample Data** (Düsseldorf 2020-2024):
```
Region: 05111 Düsseldorf, Stadt
Category: Insgesamt (Total)
2020: 4,095.83 EUR
2021: 4,198.22 EUR
2022: 4,369.27 EUR
2023: 4,527.79 EUR
2024: 4,759.61 EUR
```

**Extraction Value**: ★★★★★ (VERY HIGH)
- **Why**: Provides historical wage trends in a single view (currently split across multiple files)
- **Benefits**: Easier time-series analysis, validates Sheet 8.1 data
- **Potential Indicators**: Could enhance indicator 90 (Median Wage) with time-series dimension
- **Records**: ~795 records per year × 1 file = 795 records (consolidated view of existing data)

**Note**: This sheet appears to **consolidate the median wage data from Sheet 8.1 across all 5 years** into a single view. It doesn't add new data, but provides a more convenient format for time-series analysis.

---

### ✓ Sheet 8.3: Employment by Economic Sectors (WZ 2008)
**Description**: Employment and wage distribution by German economic sector classification (WZ 2008)
**District Coverage**: 1,166 NRW district codes (54 districts × ~21-22 sectors)
**Data Structure**:
```
Columns: 15
- Region Code (5-digit)
- Region Name
- Sector Name (WZ 2008: Agriculture, Manufacturing, Services, etc.)
- Total Employees
- Wage bracket: under 2,000 EUR
- Wage bracket: 2,000-3,000 EUR
- Wage bracket: 3,000-4,000 EUR
- Wage bracket: 4,000-5,000 EUR
- Wage bracket: 5,000-6,000 EUR
- Wage bracket: over 6,000 EUR
- Median wage (EUR)
- [Empty column]
- West/East classification
- State (NRW)
- Employment Agency district
```

**Sample Data** (Düsseldorf, Total):
```
Region: 05111 Düsseldorf, Stadt
Sector: Insgesamt (All sectors)
Total Employees: 313,393
Wage <2k: 9,251
Wage 2-3k: 41,063
Wage 3-4k: 60,839
Wage 4-5k: 58,334
Wage 5-6k: 43,131
Wage >6k: 100,775
Median: 4,759.61 EUR
```

**Economic Sectors (WZ 2008)**:
```
A    - Land- und Forstwirtschaft, Fischerei (Agriculture, forestry, fishing)
B-F  - Produzierendes Gewerbe (Manufacturing, mining, construction)
  B  - Bergbau und Gewinnung von Steinen und Erden (Mining)
  C  - Verarbeitendes Gewerbe (Manufacturing)
  D  - Energieversorgung (Energy supply)
  E  - Wasserversorgung (Water supply)
  F  - Baugewerbe (Construction)
G-J  - Handel, Verkehr, Gastgewerbe, Information und Kommunikation (Trade, transport, hospitality, IT)
  G  - Handel; Instandhaltung und Reparatur von Kraftfahrzeugen (Retail, automotive)
  H  - Verkehr und Lagerei (Transport and storage)
  I  - Gastgewerbe (Hospitality)
  J  - Information und Kommunikation (IT and communications)
K-N  - Finanz- und Versicherungsdienstleister (Finance, insurance, business services)
O-U  - Öffentliche und sonstige Dienstleister (Public and other services)
```

**Extraction Value**: ★★★★★ (VERY HIGH)
- **Why**: Provides **sectoral employment and wage analysis** - critical for economic research
- **Benefits**: Identify which sectors drive regional economies, sectoral wage disparities
- **Potential Indicators**:
  - **92**: Total Employees by Economic Sector (WZ 2008)
  - **93**: Median Wage by Economic Sector (WZ 2008)
  - **94**: Wage Distribution by Economic Sector (6 brackets × ~22 sectors)
- **Records**: ~1,166 records × 8 data points (employee count + median + 6 brackets) = ~9,300 records
- **Use Cases**:
  - Compare manufacturing vs. service sector wages by district
  - Identify high-wage sectors in each region
  - Track sectoral employment concentration

---

### ✓ Sheet 8.4: Employment by Occupations (KldB 2010)
**Description**: Employment and wage distribution by German occupation classification (KldB 2010)
**District Coverage**: 3,286 NRW district codes (54 districts × ~61 occupation categories)
**Data Structure**:
```
Columns: 15 (identical to Sheet 8.3)
- Region Code
- Region Name
- Occupation Name (KldB 2010)
- Total Employees
- 6 wage brackets
- Median wage
- West/East, State, Employment Agency
```

**Sample Data** (Düsseldorf, Total):
```
Region: 05111 Düsseldorf, Stadt
Occupation: Insgesamt (All occupations)
Total Employees: 313,393
[Same wage distribution as Sheet 8.3]
```

**Occupation Groups (KldB 2010 - Sample)**:
```
01-09: Rohstoffgewinnung, Produktion und Fertigung (Raw materials, production, manufacturing)
11-29: Bau, Architektur, Vermessung und Gebäudetechnik (Construction, architecture)
21-29: Naturwissenschaft, Geografie und Informatik (Science, geography, IT)
31-34: Verkehr, Logistik, Schutz und Sicherheit (Transport, logistics, security)
41-43: Kaufmännische Dienstleistungen, Handel, Vertrieb, Hotel und Tourismus (Business services, trade, hospitality)
51-54: Unternehmensorganisation, Buchhaltung, Recht und Verwaltung (Business organization, accounting, law)
61-63: Gesundheit, Soziales, Lehre und Erziehung (Health, social services, education)
71-73: Geisteswissenschaften, Kultur, Gestaltung (Humanities, culture, design)
81-83: Militär (Military)
```

**Extraction Value**: ★★★★☆ (HIGH)
- **Why**: **Most granular occupational data** - 61 occupation categories vs. 22 sectors
- **Benefits**: Analyze skill demands, occupation-specific wage gaps, career path analysis
- **Potential Indicators**:
  - **95**: Total Employees by Occupation (KldB 2010)
  - **96**: Median Wage by Occupation (KldB 2010)
  - **97**: Wage Distribution by Occupation (6 brackets × ~61 occupations)
- **Records**: ~3,286 records × 8 data points = ~26,300 records
- **Use Cases**:
  - Identify high-demand occupations in each district
  - Compare wages for same occupation across regions
  - Track skill evolution (IT vs. manufacturing vs. services)
- **Tradeoff**: Very large dataset - may want to extract only major occupation groups (10 categories instead of 61)

---

### ✓ Sheet 8.5: Low-Wage Workers Analysis
**Description**: Detailed analysis of workers earning below low-wage thresholds
**District Coverage**: 53 NRW districts (district-level only, no demographic breakdowns)
**Data Structure**:
```
Columns: 13
- Region Code (5-digit)
- Region Name
- Total Full-time Employees (baseline)
- Below National Threshold (2,676 EUR): Count
- Below National Threshold: Percentage
- Below West Germany Threshold (2,745 EUR): Count
- Below West Germany Threshold: Percentage
- Below East Germany Threshold (2,359 EUR): Count (often masked with 'X')
- Below East Germany Threshold: Percentage (often masked)
- [Empty column]
- West/East classification
- State (NRW)
- Employment Agency district
```

**Sample Data** (Düsseldorf):
```
Region: 05111 Düsseldorf, Stadt
Total Employees: 313,393
Below 2,676 EUR (national): 33,779 employees (10.78%)
Below 2,745 EUR (west): 37,035 employees (11.82%)
Below 2,359 EUR (east): X (masked - too few cases in West Germany)
```

**Low-Wage Thresholds (2024)**:
- **National**: 2,676 EUR (unified threshold)
- **West Germany**: 2,745 EUR (higher cost of living)
- **East Germany**: 2,359 EUR (lower regional wages)

**Extraction Value**: ★★★★☆ (HIGH)
- **Why**: **Critical for income inequality and poverty research**
- **Benefits**: Identify districts with high low-wage employment, regional wage disparities
- **Potential Indicators**:
  - **98**: Low-Wage Workers (below national threshold)
  - **99**: Low-Wage Workers (below regional threshold)
  - **100**: Low-Wage Worker Percentage
- **Records**: ~53 districts × 3 thresholds × 2 metrics (count + percentage) = ~320 records
- **Use Cases**:
  - Map low-wage employment hotspots in NRW
  - Compare low-wage rates across districts
  - Assess working poverty risk by region

---

### ✗ Sheet 8.6: Apprentices (Auszubildende)
**Description**: Employment and wage data for apprentices (vocational trainees)
**District Coverage**: ✗ **NO DISTRICT DATA** - Only Germany, West/East, and state (Länder) level
**Geographic Levels**: D (Germany), W (West), O (East), 01-16 (States)
**Data Structure**: Same as other sheets but aggregated to state level only

**Extraction Value**: ✗ (NOT RECOMMENDED)
- **Why**: No district-level granularity - doesn't fit database schema
- **Note**: Could extract NRW state-level data (region code '05') but loses district detail

---

## Recommended Extraction Priority

### Priority 1: High-Value, Moderate Effort
**Sheet 8.3: Economic Sectors (WZ 2008)**
- **Records**: ~9,300
- **Indicators**: 92-94 (3 new indicators)
- **Effort**: Moderate (similar structure to Sheet 8.1)
- **Value**: Essential for sectoral economic analysis

**Sheet 8.5: Low-Wage Workers**
- **Records**: ~320
- **Indicators**: 98-100 (3 new indicators)
- **Effort**: Low (simpler structure, fewer records)
- **Value**: Critical for inequality research

### Priority 2: High-Value, Higher Effort
**Sheet 8.4: Occupations (KldB 2010)**
- **Records**: ~26,300
- **Indicators**: 95-97 (3 new indicators)
- **Effort**: High (very granular, 61 occupation categories)
- **Value**: Most detailed labor market data
- **Consideration**: May want to extract only major occupation groups (10 instead of 61) to reduce complexity

### Priority 3: Optional, Low Priority
**Sheet 8.2: Time Series**
- **Records**: ~795
- **Indicators**: None (enhances existing indicator 90)
- **Effort**: Low
- **Value**: Convenient consolidated view, but doesn't add new information
- **Note**: Data already captured in Sheet 8.1 across 5 annual files

### Not Recommended
**Sheet 8.6: Apprentices**
- No district-level data available

---

## Total Potential Expansion

| Sheet | Records | Indicators | Priority | Effort |
|-------|---------|------------|----------|--------|
| **8.1 (✓ Extracted)** | 31,768 | 89-91 (3) | - | - |
| **8.2: Time Series** | 795 | 0 | P3 | Low |
| **8.3: Economic Sectors** | 9,300 | 92-94 (3) | **P1** | Moderate |
| **8.4: Occupations** | 26,300 | 95-97 (3) | P2 | High |
| **8.5: Low-Wage Workers** | 320 | 98-100 (3) | **P1** | Low |
| **8.6: Apprentices** | 0 | 0 | - | - |
| **TOTAL AVAILABLE** | **68,483** | **100** | - | - |

**Current Database**: 91 indicators, ~133,000 records
**After Full BA Extraction**: 100 indicators (+9), ~170,000 records (+37,000)

---

## Implementation Recommendations

### Option A: Extract All High-Value Sheets (Recommended)
Extract Sheets 8.3, 8.4, and 8.5 to maximize data coverage:
- **New Indicators**: 92-100 (9 indicators)
- **New Records**: ~35,920 records
- **Effort**: 1-2 days (create 3 new extractors similar to Sheet 8.1)
- **Benefits**: Complete BA data integration, enables sectoral and occupational analysis

### Option B: Extract Priority 1 Only (Quick Win)
Extract Sheets 8.3 and 8.5 only:
- **New Indicators**: 92-94, 98-100 (6 indicators)
- **New Records**: ~9,620 records
- **Effort**: 0.5-1 day
- **Benefits**: Covers most important use cases (sectoral analysis + inequality)

### Option C: Extract Low-Wage Workers Only (Minimal)
Extract Sheet 8.5 only:
- **New Indicators**: 98-100 (3 indicators)
- **New Records**: ~320 records
- **Effort**: 2-3 hours
- **Benefits**: Quick win for inequality research, minimal complexity

---

## Technical Considerations

### Extractor Architecture
All sheets share similar structure to Sheet 8.1:
- Skip 9-10 header rows
- Region code in column 0, region name in column 1
- Category/classification in column 2
- Data columns start at column 3
- Use same filtering logic for NRW regions (codes starting with '05')

### Database Schema
Current `fact_demographics` table can accommodate new data:
- Add `economic_sector` field (VARCHAR) for WZ 2008 codes
- Add `occupation_code` field (VARCHAR) for KldB 2010 codes
- Use `notes` field for threshold types (national/west/east for low-wage data)

### Indicator Definitions
New indicators would follow existing pattern:
```sql
-- Example: Indicator 92
INSERT INTO dim_indicator (
    indicator_id, indicator_code, indicator_name_en,
    indicator_category, indicator_subcategory,
    unit_of_measure, source_system, source_table_id
) VALUES (
    92, 'ba_employment_by_sector', 'Full-time Employees by Economic Sector (WZ 2008)',
    'Labor Market', 'Employment by Sector',
    'Persons', 'ba', 'Sheet 8.3'
);
```

---

## Next Steps

1. **Decision**: Choose extraction option (A, B, or C)
2. **Create Extractors**: Copy and modify `employment_wage_extractor.py` for new sheets
3. **Add Indicators**: Run indicator setup scripts for indicators 92-100
4. **Update Transformers**: Enhance `employment_wage_transformer.py` to handle sector/occupation fields
5. **Run ETL**: Extract, transform, and load new data
6. **Verify**: Create verification queries for new indicators
7. **Document**: Update [ba_employment_wage_summary.md](ba_employment_wage_summary.md) with new coverage

---

## Files for Reference

### Existing Implementation
- [src/extractors/ba/employment_wage_extractor.py](../../src/extractors/ba/employment_wage_extractor.py) - Sheet 8.1 extractor (template)
- [src/transformers/employment_wage_transformer.py](../../src/transformers/employment_wage_transformer.py) - Transformer (needs enhancement)
- [pipelines/ba/etl_ba_employment_wage.py](../../pipelines/ba/etl_ba_employment_wage.py) - ETL pipeline (template)

### Documentation
- [ba_employment_wage_summary.md](ba_employment_wage_summary.md) - Current BA data summary
- [ba_data_coverage_explanation.md](ba_data_coverage_explanation.md) - Why only 2020-2024
- [indicators_translation_english.md](indicators_translation_english.md) - Master indicator list

### Verification
- [sql/queries/ba_employment_wage_verification.sql](../../sql/queries/ba_employment_wage_verification.sql) - Template for verification queries

---

**Last Updated**: 2026-01-03
**Analyst**: Claude Sonnet 4.5
**Status**: Analysis Complete - Ready for Extraction Decision
