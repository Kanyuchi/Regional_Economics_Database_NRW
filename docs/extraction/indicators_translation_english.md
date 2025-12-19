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

### 1. Regional Database Germany
**URL:** https://www.regionalstatistik.de/

**Tables with the following numbers:**

| Table ID | Description |
|----------|-------------|
| 12411-03-03-4 | Population by gender, nationality, and age groups as of December 31 |
| 13111-01-03-4 | Employees subject to social insurance at workplace by gender and nationality - Reference date June 30 |
| 13111-07-05-4 | Employees subject to social insurance at workplace by gender, nationality, and economic sectors - Reference date June 30 |
| 13111-03-02-4 | With employment scope (full-time/part-time) |
| 13111-11-04-4 | With type of vocational education qualification |
| 13111-02-02-4 | Employees subject to social insurance at place of residence by gender and nationality |
| 13111-04-02-4 | With employment scope |
| 13111-12-03-4 | With type of vocational education qualification |
| 13211-02-05-4 | Unemployed by selected groups and unemployment rates – Annual average |
| 13312-01-05-4 | Employed persons by economic sectors – Annual average |
| 44231-01-03-4 | Establishments, active persons, construction industry turnover – Reference date June 30 |
| 44231-01-02-4 | Establishments, active persons, total turnover – Reference date June 30 |
| 52111-01-02-4 | Establishments by employee size classes |
| 52111-02-01-4 | Establishments by selected economic sections (WZ 2008) |
| 52311-01-04-4 | Business registrations, deregistrations – Annual total |
| 52411-02-01-4 | Corporate insolvency applications filed – Annual total |
| 82000-04-01-4 | [Description not provided in source] |

---

### 2. State Database North Rhine-Westphalia
**URL:** https://www.landesdatenbank.nrw.de/

**Tables with the following numbers:**

| Table ID | Description | Notes |
|----------|-------------|-------|
| 71517-01i | Municipal finances: Receipts by selected types of receipts | |
| 82711-01i | Gross domestic product and gross value added by economic sectors (7) of WZ 2008 | 2022 data |
| 82711-06i | Employee compensation by economic sectors (7) of WZ 2008 and per employee | 2022 data |
| 73111-07iz | Income tax: Taxpayers, total income, income tax by income size classes of municipalities | 2021 data |
| 73111-030i | Income and wage tax from 2020: Taxpayers, total income amount, income and wage tax by size classes according to total income amount of municipalities | Alternative to above |
| 46271-01i | Supra-local roads in total by road classes | |
| 12411-9k06 | Municipal profile: Population by gender, nationality, and age groups | |
| 22421-02i | Persons in need of care and benefit recipients by care level and types of benefits | |
| 22411-02i | Persons in need of care in outpatient services by care level | |
| 22411-01i | Outpatient services by facility type and personnel in outpatient services | |
| 22412-01i | Residential care facilities: Available places and personnel in nursing homes | |
| 22412-02i | Residential care facilities: Persons in need of care in nursing homes by care level and type of care service | |
| 23111-12i | Full-time doctors by gender | |
| 23111-01i | Number of hospitals and beds provided by type of provider | |
| 12211-132i | Population by migration background, employment status, and gender | |
| 12211-124i | Population by employment status, nationality, and gender | 2019 data |
| 12211-114i | Population by personal net income (9) and gender | 2019 data |

---

### 3. Federal Employment Agency (BA)

#### Employment and Wage Data
**Source:** BA Employment/Wages, Table 8  
**URL:** https://statistik.arbeitsagentur.de/SiteGlobals/Forms/Suche/Einzelheftsuche_Formular.html?nn=1523076&topic_f=beschaeftigung-entgelt-entgelt

#### Commuter Statistics
**Source:** BA Commuter Statistics  
**URL:** https://statistik.arbeitsagentur.de/DE/Navigation/Statistiken/Interaktive-Statistiken/Pendler/Pendler-Nav.html

**Requirements:**
- Both incoming and outgoing commuters (must be queried separately)

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

