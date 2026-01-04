# Regional Economics Database for NRW
## AI-Powered Economic Analysis Infrastructure

[![Project Status](https://img.shields.io/badge/status-complete-brightgreen)]()
[![Progress](https://img.shields.io/badge/progress-100%25-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)]()
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)]()
[![Records](https://img.shields.io/badge/records-498,333-orange)]()
[![Indicators](https://img.shields.io/badge/indicators-89/103-blue)]()

---

## Overview

A comprehensive data engineering project building production-grade infrastructure for regional economic analysis in North Rhine-Westphalia (NRW), Germany's most populous state. This system integrates multi-source economic data across 54 NRW districts spanning 50 years of history (1975-2024), enabling advanced economic research and analysis.

**Project Impact:**
- Consolidating fragmented public databases from three major German statistical agencies into a unified analytical platform
- Engineering automated ETL pipelines for continuous data ingestion and quality assurance
- Enabling longitudinal analysis of regional economic transformation across diverse geographic and economic contexts
- Building scalable PostgreSQL infrastructure with time-series optimization for research and policy analysis

**Technical Scope:**
This project demonstrates end-to-end data engineering capabilities including API integration, web scraping, CSV processing, data transformation, star schema database design, and automated quality assurance workflows. The system employs sophisticated data validation techniques using five major Ruhr cities (Dortmund, Essen, Duisburg, Bochum, Gelsenkirchen) as reference points to ensure data accuracy and completeness.

### Key Achievements

- ✅ **498,333 records** across 89 economic indicators with data (103 defined)
- ✅ **50 years** of historical data (1975-2024)
- ✅ **54 NRW districts** with comprehensive coverage
- ✅ **3 major data sources**: Regional DB (100%), State DB (100%), BA (100%)
- ✅ **36/36 ETL pipelines** complete with data quality validation
- ✅ **Star schema** optimized for time-series analysis
- ✅ **100% table coverage** - all planned data sources extracted and loaded

### Current Progress

| Data Source | Tables | Indicators | Records | Progress |
|-------------|--------|------------|---------|----------|
| Regional Database Germany | 17/17 | 18/27 with data | 99,242 | **100%** ✅ |
| State Database NRW | 17/17 | 57/61 with data | 175,560 | **100%** ✅ |
| Federal Employment Agency (BA) | 2/2 | 14/15 with data | 223,531 | **100%** ✅ |
| **Total** | **36/36** | **89/103** | **498,333** | **100%** ✅ |

**Note:** Regional DB "missing" 9 indicators are dimensional data (gender, age, nationality) stored as columns. BA "missing" indicator 103 is a calculated field. State DB "missing" 4 indicators (52-55) are sectors not available at district level.

---

## Data Coverage Summary

### Geographic Coverage
- **54 NRW Districts** (Kreise)
- **5 Administrative Districts** (Regierungsbezirke)
- **1 State** (North Rhine-Westphalia)
- **1 National** (Germany for comparison)

### Temporal Coverage
- **Regional DB Germany**: 1995-2024 (30 years)
- **State DB NRW**: 2000-2024 (varies by indicator)
- **BA Employment/Wages**: 2020-2024 (5 years)
- **BA Commuters**: 2002-2024 (23 years)

### Data Categories
1. **Demographics** (Indicators 1-8, 67-71, 86-88): Population structure, age distribution, migration background, income distribution
2. **Labor Market** (Indicators 9-12, 89-103): Employment, unemployment, wages, vocational qualifications, commuter flows
3. **Economic Activity** (Indicators 13-19): Business establishments, registrations, insolvencies, turnover, construction
4. **Sectoral Data** (Indicators 20-55, 92-97): Employment, GDP, and value added by economic sectors
5. **Public Finance** (Indicators 28, 56-61): Municipal revenues and income tax
6. **Infrastructure** (Indicators 62-66): Roads by classification
7. **Healthcare** (Indicators 72-85): Hospitals, doctors, care facilities and capacity

---

## Quick Start

### 1. Check Current Status

```bash
python scripts/diagnostics/check_extracted_data.py
```

Output shows all indicators, their status, and record counts.

### 2. Run an ETL Pipeline

```bash
# Example: Extract Regional DB data
python pipelines/regional_db/etl_13312_01_05_4_employed_sector.py

# Example: Extract State DB data
python pipelines/state_db/etl_state_db_gdp.py

# Example: Extract BA data
python pipelines/ba/etl_ba_commuters.py
```

### 3. Verify Data Quality

```bash
# Verify specific indicator
python scripts/verification/verify_extraction_timeseries.py --indicator <ID>

# With CSV export for analysis
python scripts/verification/verify_extraction_timeseries.py --indicator <ID> --export-csv
```

### 4. Query the Database

```sql
-- Example: Time series for Düsseldorf employment
SELECT t.year, f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 1
  AND g.region_code = '05111'  -- Düsseldorf
ORDER BY t.year;

-- Example: Commuter balance for major cities (2023)
WITH commuters AS (
    SELECT
        g.region_name,
        SUM(CASE WHEN fd.indicator_id = 101 THEN fd.value ELSE 0 END) as incoming,
        SUM(CASE WHEN fd.indicator_id = 102 THEN fd.value ELSE 0 END) as outgoing
    FROM fact_demographics fd
    JOIN dim_geography g ON fd.geo_id = g.geo_id
    JOIN dim_time t ON fd.time_id = t.time_id
    WHERE fd.indicator_id IN (101, 102)
      AND t.year = 2023
      AND fd.gender IS NULL AND fd.nationality IS NULL
    GROUP BY g.region_name
)
SELECT
    region_name,
    incoming::INT,
    outgoing::INT,
    (incoming - outgoing)::INT as net_balance
FROM commuters
ORDER BY (incoming - outgoing) DESC
LIMIT 10;
```

---

## Project Structure

```
Regional Economics Database for NRW/
│
├── 📁 src/                              # Source code (ETL modules)
│   ├── extractors/                      # API extraction logic
│   │   ├── regional_db/                 # Regional Database extractors ✅
│   │   │   ├── base_extractor.py        # Base API client
│   │   │   ├── demographics_extractor.py
│   │   │   ├── employment_extractor.py
│   │   │   └── business_extractor.py
│   │   ├── state_db/                    # State DB extractors ✅
│   │   │   ├── base_extractor.py
│   │   │   ├── gdp_extractor.py
│   │   │   ├── municipal_finance_extractor.py
│   │   │   ├── healthcare_extractors.py
│   │   │   └── ...17 extractors total
│   │   └── ba/                          # Federal Agency extractors ✅
│   │       ├── base_extractor.py
│   │       ├── employment_wage_extractor.py
│   │       ├── economic_sector_extractor.py
│   │       ├── occupation_extractor.py
│   │       ├── low_wage_extractor.py
│   │       └── commuter_extractor.py
│   │
│   ├── transformers/                    # Data transformation logic
│   │   ├── demographics_transformer.py
│   │   ├── employment_transformer.py
│   │   ├── business_transformer.py
│   │   ├── gdp_transformer.py
│   │   ├── healthcare_transformers.py
│   │   ├── ba_additional_transformer.py
│   │   ├── commuter_transformer.py
│   │   └── ...15+ transformers total
│   │
│   ├── loaders/                         # Database loading (integrated in transformers)
│   │
│   └── utils/                           # Utilities
│       ├── database.py                  # Database connections
│       ├── logging.py                   # Logging configuration
│       └── config.py                    # Configuration utilities
│
├── 📁 pipelines/                        # ETL pipeline scripts
│   ├── regional_db/                     # 17 operational pipelines ✅
│   │   ├── etl_12411_03_03_4_population.py
│   │   ├── etl_13111_*_employment*.py   # 8 employment pipelines
│   │   ├── etl_13211_02_05_4_unemployment.py
│   │   ├── etl_13312_01_05_4_employed_sector.py
│   │   ├── etl_44231_*_construction*.py # 2 construction pipelines
│   │   ├── etl_52111_*_branches*.py     # 2 business pipelines
│   │   ├── etl_52311_01_04_4_business_registrations.py
│   │   ├── etl_52411_02_01_4_corporate_insolvencies.py
│   │   └── etl_82000_04_01_4_employee_compensation.py
│   ├── state_db/                        # 17 operational pipelines ✅
│   │   ├── etl_state_db_gdp.py
│   │   ├── etl_state_db_employee_compensation.py
│   │   ├── etl_state_db_income_tax.py
│   │   ├── etl_state_db_roads.py
│   │   ├── etl_state_db_healthcare.py
│   │   └── ...17 pipelines total
│   ├── ba/                              # 6 operational pipelines ✅
│   │   ├── etl_ba_employment_wage.py
│   │   ├── etl_ba_economic_sector.py
│   │   ├── etl_ba_occupation.py
│   │   ├── etl_ba_low_wage.py
│   │   └── etl_ba_commuters.py
│   └── TEMPLATE_etl_pipeline.py         # Pipeline template
│
├── 📁 scripts/                          # Utility scripts
│   ├── diagnostics/                     # Status checking scripts
│   │   ├── check_extracted_data.py
│   │   ├── check_all_indicators.py
│   │   └── check_completed_tables.py
│   │
│   ├── verification/                    # Data verification scripts
│   │   └── verify_extraction_timeseries.py  # Main verification tool ⭐
│   │
│   ├── fixes/                           # Data repair scripts
│   │
│   └── utilities/                       # General utilities
│       ├── populate_geography.py
│       ├── populate_indicators.py
│       ├── add_*_indicators.py          # Indicator setup scripts
│       └── show_progress.py
│
├── 📁 sql/                              # SQL files
│   ├── schema/                          # Database schema
│   │   └── 01_create_schema.sql
│   └── queries/                         # Analysis SQL scripts
│       ├── business_registrations_analysis.sql
│       ├── construction_industry_analysis.sql
│       ├── gdp_gva_verification.sql
│       ├── commuters_verification.sql
│       ├── commuters_example_queries.sql
│       └── ...20+ query files
│
├── 📁 docs/                             # Documentation
│   ├── extraction/                      # Extraction guides
│   │   ├── indicators_translation_english.md  # All indicators ⭐
│   │   ├── ba_data_coverage_explanation.md
│   │   ├── ba_employment_wage_summary.md
│   │   └── commuter_data_analysis.md
│   │
│   ├── database/                        # Database documentation
│   │   ├── database_structure_explained.md
│   │   └── data_dictionary.md
│   │
│   └── project/                         # Project planning & summaries
│       └── PROJECT_SUMMARY.md
│
├── 📁 data/                             # Data files
│   ├── reference/                       # Reference data
│   │   ├── table_registry.json          # Table tracking
│   │   ├── job_cache.json               # API job cache (Regional DB)
│   │   └── state_db_job_cache.json      # API job cache (State DB)
│   ├── raw/                             # Raw extracted data
│   │   ├── regional_db/
│   │   ├── state_db/
│   │   └── ba/
│   └── analysis/                        # Analysis outputs
│       └── timeseries/                  # CSV exports for analysis
│
├── 📁 config/                           # Configuration files
│   ├── database.yaml
│   ├── logging.yaml
│   └── sources.yaml
│
├── 📄 README.md                         # This file
├── 📄 requirements.txt                  # Python dependencies
└── 📄 .env                              # Environment variables
```

---

## Data Sources

### 1. Regional Database Germany (Regionalstatistik) ✅ COMPLETE
**URL:** https://www.regionalstatistik.de/
**Status:** 🟢 17/17 tables completed (100%) | **86,728 records** | **Indicators 1-27**

**Coverage:**
- **Period:** 1995-2024 (up to 30 years depending on indicator)
- **Geography:** 54 NRW districts + NRW state + Germany

**Key Indicators:**
- Population by demographics (age, gender, nationality)
- Employment at workplace and residence
- Employment by economic sector (WZ 2008)
- Employment by vocational qualification
- Unemployment rates
- Business establishments by size and sector
- Business registrations and deregistrations
- Corporate insolvencies
- Employee compensation by sector

### 2. State Database NRW (Landesdatenbank) ✅ COMPLETE
**URL:** https://www.landesdatenbank.nrw.de/
**Status:** 🟢 17/17 tables completed (100%) | **173,361 records** | **Indicators 28-88**

**Coverage:**
- **Period:** 2000-2024 (varies by indicator, some back to 2000)
- **Geography:** 54 NRW districts

**Key Indicators:**
- GDP and Gross Value Added by economic sector (7 sectors)
- Employee compensation by economic sector
- Municipal finances (receipts)
- Income tax by income brackets
- Road infrastructure by classification
- Population profiles (gender, nationality, age, migration background)
- Healthcare: hospitals, beds, physicians
- Long-term care: facilities, recipients, personnel
- Income distribution

### 3. Federal Employment Agency (BA) ✅ COMPLETE
**URL:** https://statistik.arbeitsagentur.de/
**Status:** 🟢 2/2 data sources completed (100%) | **223,531 records** | **Indicators 89-103**

**Coverage:**
- **Employment/Wage Period:** 2020-2024 (5 years, district-level only from 2020)
- **Commuter Period:** 2002-2024 (23 years)
- **Geography:** 51-52 NRW districts (varies by year)

**Key Indicators:**
- **Employment & Wages** (89-100): Full-time employees, median wages, wage distribution
  - By demographics (gender, age, nationality, education, skill level)
  - By economic sector (22 WZ 2008 sectors)
  - By occupation (62 KldB 2010 occupation categories)
  - Low-wage workers (3 thresholds: national, west, east)
- **Commuter Statistics** (101-103): Incoming/outgoing commuters with demographic breakdowns

---

## Database Schema

### Star Schema Design

**Dimension Tables:**
- `dim_geography`: Geographic entities (58 total, including 54 NRW districts)
- `dim_time`: Time periods (50 years: 1975-2024)
- `dim_indicator`: Economic indicators (103 total)

**Fact Tables:**
- `fact_demographics`: Population and demographic data
- `fact_labor_market`: Employment and unemployment data
- `fact_business_economy`: Business establishments and economic activity
- `fact_public_finance`: Municipal finance and tax data
- `fact_healthcare`: Healthcare facilities and capacity
- `fact_infrastructure`: Road infrastructure data

### Key Features:
- **Optimized for time-series analysis** with composite indexes
- **Dimensional modeling** for flexible filtering and aggregation
- **Metadata fields** (gender, nationality, age_group, migration_background, notes)
- **Data quality tracking** (loaded_at timestamps, source tracking)

---

## Verification Workflow

### Why Verification Matters

Every extraction is verified to ensure:
- ✅ Data accuracy and completeness
- ✅ **Ruhr region cities coverage** (key reference points)
- ✅ Time series analysis capability
- ✅ Query performance

### Ruhr Region Focus Cities

| City | Code | Why Important |
|------|------|---------------|
| **Dortmund** | 05913 | Largest Ruhr city, tech transformation |
| **Essen** | 05113 | Krupp HQ, cultural capital |
| **Duisburg** | 05112 | Steel industry, Rhine port |
| **Bochum** | 05911 | Coal mining, automotive |
| **Gelsenkirchen** | 05513 | Coal mining, energy sector |

### Run Verification

```bash
# Basic verification
python scripts/verification/verify_extraction_timeseries.py --indicator <ID>

# With CSV export
python scripts/verification/verify_extraction_timeseries.py --indicator <ID> --export-csv
```

---

## Key Findings: Ruhr Region Analysis

### Construction Industry Transformation (1995-2024)
30-year employment decline across all Ruhr cities:

| City | 1995 | 2024 | Change |
|------|------|------|--------|
| Dortmund | 8,591 | 3,385 | **-60.6%** |
| Essen | 7,311 | 3,246 | **-55.6%** |
| Duisburg | 3,233 | 1,733 | **-46.4%** |
| Bochum | 3,174 | 2,476 | -22.0% |
| Gelsenkirchen | 1,885 | 1,606 | -14.8% |

### Commuter Patterns (2023)
Job centers vs bedroom communities:

| District | Incoming | Outgoing | Net Balance | Type |
|----------|----------|----------|-------------|------|
| **Bonn** | 308,120 | N/A | +150k+ | Major Job Center |
| **Düsseldorf** | 286,090 | 99,640 | +186,450 | Major Job Center |
| **Essen** | 142,190 | 92,750 | +49,440 | Job Center |
| **Dortmund** | 122,120 | N/A | Positive | Job Center |

### Service Sector Emergence
Top 3 sectors across all Ruhr cities (2023):
1. **Trade, vehicle maintenance** (largest)
2. **Professional, scientific services**
3. **Construction or Health**

---

## Installation

### Prerequisites
- Python 3.10 or higher
- PostgreSQL 15 or higher
- Git

### Setup

1. **Clone the repository**
```bash
git clone https://github.com/Kanyuchi/Regional_Economics_Database_NRW.git
cd "Regional Economics Database for NRW"
```

2. **Create virtual environment**
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your database credentials
```

5. **Set up database**
```bash
# Create PostgreSQL database
createdb regional_db

# Run schema scripts
psql -d regional_db -f sql/schema/01_create_schema.sql

# Populate dimension tables
python scripts/utilities/populate_geography.py
python scripts/utilities/populate_indicators.py
```

6. **Verify installation**
```bash
python scripts/diagnostics/check_extracted_data.py
```

---

## Documentation

| Document | Location | Description |
|----------|----------|-------------|
| Indicators Guide | `docs/extraction/indicators_translation_english.md` | All 103 indicators explained |
| Database Structure | `docs/database/database_structure_explained.md` | Schema documentation |
| BA Data Coverage | `docs/extraction/ba_data_coverage_explanation.md` | BA data source details |
| Commuter Analysis | `docs/extraction/commuter_data_analysis.md` | Commuter data documentation |

---

## Syncing a Forked Repository

If you've forked this repository and want to keep it up-to-date with the original repository:

### 1. Add Upstream Remote
```bash
git remote add upstream https://github.com/Kanyuchi/Regional_Economics_Database_NRW.git
```

### 2. Sync with Upstream
```bash
git fetch upstream
git checkout main
git merge upstream/main
git push origin main
```

---

## Roadmap

### ✅ Phase 1: Planning & Setup (COMPLETE)
- Database schema implemented
- Development environment ready
- Documentation complete

### ✅ Phase 2: Data Source Analysis (COMPLETE)
- API endpoints documented
- Authentication configured
- Data formats analyzed

### ✅ Phase 3: Regional Database ETL (COMPLETE)
- All 17 tables extracted and loaded
- 27 indicators operational
- 86,728 records with 30-year history

### ✅ Phase 4: State Database NRW (COMPLETE)
- All 17 tables extracted and loaded
- 61 indicators operational
- 173,361 records covering health, finance, GDP, infrastructure

### ✅ Phase 5: Federal Employment Agency (COMPLETE)
- Employment/wage data (12 indicators, 2020-2024)
- Commuter statistics (3 indicators, 2002-2024)
- 223,531 records with demographic and sectoral breakdowns

### 🎯 Phase 6: Analysis & Visualization (ONGOING)
- Advanced data analysis
- Interactive visualizations
- Research insights and reports

---

## Contact

**Project Lead:** Kanyuchi
**Organization:** Duisburg Business & Innovation (DBI)
**Focus:** Regional economic data infrastructure for NRW

---

## License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

**MIT License Summary:**
- ✅ Free to use for commercial and non-commercial purposes
- ✅ Permission to modify and distribute
- ✅ Includes warranty disclaimer
- ✅ Requires attribution and license notice

For full license terms, see the LICENSE file in the root directory.

---

## Version History

| Version | Date | Changes |
|---------|------|---------|
| 1.0 | Dec 2024 | Initial project setup |
| 2.0 | Dec 17, 2024 | 10 indicators completed, verification workflow |
| 3.0 | Dec 18, 2024 | 14 indicators, 30-year historical data |
| 4.0 | Dec 19, 2024 | Regional Database Germany COMPLETE: 17/17 tables (100%), 27 indicators, 86,728 records |
| **5.0** | **Jan 4, 2026** | **ALL DATA SOURCES COMPLETE: 36/36 tables (100%), 103 indicators, 483,622 records** |

---

## Acknowledgments

Data sources:
- **Regionalstatistik.de**: Regional Database Germany
- **Landesdatenbank.nrw.de**: State Database North Rhine-Westphalia
- **Statistik.arbeitsagentur.de**: Federal Employment Agency (BA)

Built with ❤️ for economic research and regional development in NRW.
