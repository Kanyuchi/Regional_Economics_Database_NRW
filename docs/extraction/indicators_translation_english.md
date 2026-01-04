# Tables for the Regional Economics AI Model - English Translation

## Data Collection Requirements

**Geographic Scope:**
- Data must be retrieved at the district level (Kreisebene) for:
  - All districts
  - Düsseldorf administrative district (total)
  - North Rhine-Westphalia (total)
  - Germany (total; not in state database)
  
**Aggregation Requirements:**
- The program must then aggregate data for:
  - Ruhr area
  - Lower Rhine region

**Temporal Scope:**
- For all available years
- Most data is annual

---

## Data Sources and Indicators

### 1. Regional Database Germany ✓ COMPLETED (17/17 tables, 99,242 records)
**URL:** https://www.regionalstatistik.de/

**Tables with the following numbers:**

| Status | Table ID | Description | Indicators |
|--------|----------|-------------|------------|
| ✓ | 12411-03-03-4 | Population by gender, nationality, and age groups as of December 31 | 1-3 |
| ✓ | 13111-01-03-4 | Employees subject to social insurance at workplace by gender and nationality - Reference date June 30 | 4 |
| ✓ | 13111-07-05-4 | Employees subject to social insurance at workplace by gender, nationality, and economic sectors - Reference date June 30 | 5 |
| ✓ | 13111-03-02-4 | With employment scope (full-time/part-time) | 6 |
| ✓ | 13111-11-04-4 | With type of vocational education qualification | 7 |
| ✓ | 13111-02-02-4 | Employees subject to social insurance at place of residence by gender and nationality | 8 |
| ✓ | 13111-04-02-4 | With employment scope | 9 |
| ✓ | 13111-12-03-4 | With type of vocational education qualification | 10 |
| ✓ | 13211-02-05-4 | Unemployed by selected groups and unemployment rates – Annual average | 11 |
| ✓ | 13312-01-05-4 | Employed persons by economic sectors – Annual average | 12 |
| ✓ | 44231-01-03-4 | Establishments, active persons, construction industry turnover – Reference date June 30 | 13 |
| ✓ | 44231-01-02-4 | Establishments, active persons, total turnover – Reference date June 30 | 14 |
| ✓ | 52111-01-02-4 | Establishments by employee size classes | 15 |
| ✓ | 52111-02-01-4 | Establishments by selected economic sections (WZ 2008) | 16 |
| ✓ | 52311-01-04-4 | Business registrations, deregistrations – Annual total | 17-18 |
| ✓ | 52411-02-01-4 | Corporate insolvency applications filed – Annual total | 19 |
| ✓ | 82000-04-01-4 | Employee compensation | 27 |

---

### 2. State Database North Rhine-Westphalia ✓ COMPLETED (17/17 tables, 175,560 records)
**URL:** https://www.landesdatenbank.nrw.de/

**Tables with the following numbers:**

| Status | Table ID | Description | Notes |
|--------|----------|-------------|-------|
| ✓ | 71517-01i | Municipal finances: Receipts by selected types of receipts | Indicator 28 |
| ✓ | 82711-01i | Gross domestic product and gross value added by economic sectors (7) of WZ 2008 | 2022 data - Indicators 29-40 |
| ✓ | 82711-06i | Employee compensation by economic sectors (7) of WZ 2008 and per employee | 2022 data - Indicators 41-55 |
| ✓ | 73111-010i | Income tax: Taxpayers, total income, income tax by income size classes of municipalities | 2021 data - Indicators 56-58 |
| ✓ | 73111-030i | Income and wage tax from 2020: Taxpayers, total income amount, income and wage tax by size classes according to total income amount of municipalities | Alternative to above - Indicators 59-61 |
| ✓ | 46271-01i | Supra-local roads in total by road classes | Indicators 62-66 |
| ✓ | 12411-9k06 | Municipal profile: Population by gender, nationality, and age groups | Indicators 67-71 |
| ✓ | 22421-02i | Persons in need of care and benefit recipients by care level and types of benefits | Indicators 72-75 |
| ✓ | 22411-02i | Persons in need of care in outpatient services by care level | Indicator 76 |
| ✓ | 22411-01i | Outpatient services by facility type and personnel in outpatient services | Indicators 77-78 |
| ✓ | 22412-01i | Residential care facilities: Available places and personnel in nursing homes | Indicators 79-81 |
| ✓ | 22412-02i | Residential care facilities: Persons in need of care in nursing homes by care level and type of care service | Indicator 82 |
| ✓ | 23111-12i | Full-time doctors by gender | Indicator 83 |
| ✓ | 23111-01i | Number of hospitals and beds provided by type of provider | Indicators 84-85 |
| ✓ | 12211-9134i | Population by migration background, employment status, and gender | Indicator 86 |
| ✓ | 12211-9124i | Population by employment status, nationality, and gender | 2019 data - Indicator 87 |
| ✓ | 12211-9114i | Population by personal net income (9) and gender | 2019 data - Indicator 88 |

---

### 3. Federal Employment Agency (BA) ✓ COMPLETED (2/2 data sources, 223,531 total records)

#### Employment and Wage Data ✓ COMPLETED
**Source:** BA Employment/Wages, Table 8 (Sheets 8.1, 8.3, 8.4, 8.5)
**URL:** https://statistik.arbeitsagentur.de/SiteGlobals/Forms/Suche/Einzelheftsuche_Formular.html?nn=1523076&topic_f=beschaeftigung-entgelt-entgelt
**Indicators:** 89-100 (12 indicators total)
**Period:** 2020-2024 (5 years, 210,067 records)

**Status:** ✓ EXTRACTED AND LOADED

**Sheet 8.1: Demographics & Wage Brackets** (31,768 records)
- **89**: Full-time Employees by Demographics
- **90**: Median Monthly Gross Wage by Demographics
- **91**: Wage Distribution by Brackets and Demographics

**Sheet 8.3: Economic Sectors (WZ 2008)** (82,484 records)
- **92**: Full-time Employees by Economic Sector (22 sectors)
- **93**: Median Monthly Gross Wage by Economic Sector
- **94**: Wage Distribution by Economic Sector and Wage Bracket

**Sheet 8.4: Occupations (KldB 2010)** (94,540 records)
- **95**: Full-time Employees by Occupation (62 occupation categories)
- **96**: Median Monthly Gross Wage by Occupation
- **97**: Wage Distribution by Occupation and Wage Bracket

**Sheet 8.5: Low-Wage Workers** (1,275 records)
- **98**: Low-Wage Workers (Count) - 3 thresholds: national, west, east
- **99**: Low-Wage Workers (Percentage) - 3 thresholds
- **100**: Full-time Employees Baseline for Low-Wage Analysis

**Coverage:** 51 NRW districts + Germany, 5 years (2020-2024)
**Note:** District-level data only available from 2020 onwards. Files from 2014-2019 contain national/state aggregates without district identifiers.

#### Commuter Statistics ✓ COMPLETED
**Source:** BA Commuter Statistics (Pendlerstatistik)
**URL:** https://statistik.arbeitsagentur.de/DE/Navigation/Statistiken/Interaktive-Statistiken/Pendler/Pendler-Nav.html
**Indicators:** 101-103 (3 indicators total)
**Period:** 2002-2024 (22 years, 13,464 records)

**Status:** ✓ EXTRACTED AND LOADED

**Incoming Commuters (Einpendler)** (6,732 records)
- **101**: Incoming Commuters - People who commute INTO an NRW district to work
  - Breakdowns by: Gender (male/female), Nationality (german/foreigner), Trainees

**Outgoing Commuters (Auspendler)** (6,732 records)
- **102**: Outgoing Commuters - People who commute OUT OF an NRW district to work
  - Breakdowns by: Gender (male/female), Nationality (german/foreigner), Trainees

**Net Commuter Balance** (Calculated)
- **103**: Net Commuter Balance (Pendlersaldo) - Calculated as (Incoming - Outgoing)
  - Positive value = Job importer (more people commute in than out)
  - Negative value = Job exporter (more residents work elsewhere)

**Coverage:** 52 NRW districts, 22 years (2002-2024, missing 2009 summary data)
**Data Types:**
- Summary data (2002-2024): District-level totals only
- Detailed data (2009, 2024): Full origin-destination flow matrices available but not loaded
**Note:** Incoming and outgoing commuters must be queried separately from BA portal

---

## Data Categories Summary

1. **Demographics:** Population structure, age distribution, migration background
2. **Labor Market:** Employment, unemployment, wages, vocational qualifications
3. **Economic Activity:** Business establishments, registrations, insolvencies, turnover
4. **Sectoral Data:** Employment and value added by economic sectors
5. **Public Finance:** Municipal revenues and income tax
6. **Infrastructure:** Roads
7. **Healthcare:** Hospitals, doctors, care facilities and capacity
8. **Mobility:** Commuter flows (in/out)

