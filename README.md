# Regional Economics Database for NRW
## AI-Powered Economic Analysis Infrastructure

[![Project Status](https://img.shields.io/badge/status-active-green)]()
[![Progress](https://img.shields.io/badge/progress-100%25-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)]()
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)]()
[![Records](https://img.shields.io/badge/records-86,728-orange)]()

---

## Overview

A comprehensive data engineering project building production-grade infrastructure for regional economic analysis in North Rhine-Westphalia (NRW), Germany's most populous state. This system integrates multi-source economic data across 58 districts spanning 30 years of history, enabling advanced economic research and analysis.

**Project Impact:**
- Consolidating fragmented public databases from three major German statistical agencies into a unified analytical platform
- Engineering automated ETL pipelines for continuous data ingestion and quality assurance
- Enabling longitudinal analysis of regional economic transformation across diverse geographic and economic contexts
- Building scalable PostgreSQL infrastructure with time-series optimization for research and policy analysis

**Technical Scope:**
This project demonstrates end-to-end data engineering capabilities including API integration, data transformation, database design, and automated quality assurance workflows. The system employs sophisticated data validation techniques using five major Ruhr cities (Dortmund, Essen, Duisburg, Bochum, Gelsenkirchen) as reference points to ensure data accuracy and completeness across all 58 NRW districts.

### Key Features
- ✅ **86,728 records** across 27 economic indicators
- ✅ **30 years** of historical data (1995-2024)
- ✅ **58 NRW districts** with complete coverage
- ✅ **Automated verification** with time series analysis
- ✅ **9 SQL analysis scripts** ready for data exploration

### Current Progress

| Data Source | Tables | Completed | Progress |
|-------------|--------|-----------|----------|
| Regional Database Germany | 17 | 17 | **100%** ✅ |
| State Database NRW | 17 | 0 | 0% |
| Federal Employment Agency | 3 | 0 | 0% |
| **Total** | **37** | **17** | **46%** |

---

## Quick Start

### 1. Check Current Status

```bash
python scripts/diagnostics/check_extracted_data.py
```

Output shows all indicators, their status, and record counts.

### 2. Extract New Data

```bash
# Run an ETL pipeline
python pipelines/regional_db/etl_13312_01_05_4_employed_sector.py
```

### 3. Verify Extraction (MANDATORY)

```bash
# Verify data quality and Ruhr cities coverage
python scripts/verification/verify_extraction_timeseries.py --indicator <ID>

# With CSV export for analysis
python scripts/verification/verify_extraction_timeseries.py --indicator <ID> --export-csv
```

### 4. Query the Database

```sql
-- Time series for Dortmund employment
SELECT t.year, f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 19
  AND g.region_code = '05913'  -- Dortmund
ORDER BY t.year;
```

---

## Project Structure

```
Regional Economics Database for NRW/
│
├── 📁 src/                              # Source code (ETL modules)
│   ├── extractors/                      # API extraction logic
│   │   ├── regional_db/                 # Regional Database extractors
│   │   │   ├── base_extractor.py        # Base API client
│   │   │   ├── demographics_extractor.py
│   │   │   ├── employment_extractor.py
│   │   │   └── business_extractor.py
│   │   ├── state_db/                    # State DB extractors (pending)
│   │   └── ba/                          # Federal Agency extractors (pending)
│   │
│   ├── transformers/                    # Data transformation logic
│   │   ├── demographics_transformer.py
│   │   ├── employment_transformer.py
│   │   └── business_transformer.py
│   │
│   ├── loaders/                         # Database loading
│   │   └── db_loader.py
│   │
│   ├── utils/                           # Utilities
│   │   ├── database.py                  # Database connections
│   │   ├── logging.py                   # Logging configuration
│   │   └── config.py                    # Configuration utilities
│   │
│   └── validation/                      # Data validation modules
│
├── 📁 pipelines/                        # ETL pipeline scripts
│   ├── regional_db/                     # 17 operational pipelines
│   │   ├── etl_12411_03_03_4_population.py
│   │   ├── etl_13111_*_employment*.py   # 8 employment pipelines
│   │   ├── etl_13211_02_05_4_unemployment.py
│   │   ├── etl_13312_01_05_4_employed_sector.py
│   │   ├── etl_44231_*_construction*.py # 2 construction pipelines
│   │   ├── etl_52111_*_branches*.py     # 2 business pipelines
│   │   ├── etl_52311_01_04_4_business_registrations.py
│   │   ├── etl_52411_02_01_4_corporate_insolvencies.py
│   │   └── etl_82000_04_01_4_employee_compensation.py
│   ├── state_db/                        # State DB pipelines (pending)
│   ├── ba/                              # Federal Agency pipelines (pending)
│   └── TEMPLATE_etl_pipeline.py         # Pipeline template
│
├── 📁 scripts/                          # Utility scripts
│   ├── diagnostics/                     # Status checking scripts
│   │   ├── check_extracted_data.py
│   │   ├── check_all_indicators.py
│   │   ├── check_completed_tables.py
│   │   ├── identify_next_table.py
│   │   └── recommend_next_table.py
│   │
│   ├── verification/                    # Data verification scripts
│   │   ├── verify_extraction_timeseries.py  # Main verification tool ⭐
│   │   ├── verify_all_notes_status.py
│   │   └── verify_*.py
│   │
│   ├── fixes/                           # Data repair scripts
│   │   ├── fix_indicator_mapping.py
│   │   └── debug_encoding.py
│   │
│   └── utilities/                       # General utilities
│       ├── populate_geography.py
│       ├── populate_indicators.py
│       └── show_progress.py
│
├── 📁 sql/                              # SQL files
│   ├── schema/                          # Database schema
│   │   └── 01_create_schema.sql
│   └── queries/                         # Analysis SQL scripts
│       ├── branches_by_sector_analysis.sql
│       ├── branches_by_size_analysis.sql
│       ├── business_registrations_analysis.sql
│       ├── business_registrations_verification.sql
│       ├── construction_industry_analysis.sql
│       ├── corporate_insolvencies_verification.sql
│       ├── quick_data_test.sql
│       └── total_turnover_analysis.sql
│
├── 📁 docs/                             # Documentation
│   ├── project/                         # Project planning & summaries
│   │   ├── PROJECT_SUMMARY.md           # Main project summary
│   │   ├── project_plan_regional_economics_db.md
│   │   ├── implementation_checklist.md
│   │   └── FINAL_SESSION_SUMMARY.md
│   │
│   ├── status/                          # Status reports
│   │   ├── TABLE_STATUS_SUMMARY.md      # Table extraction status ⭐
│   │   ├── SUPERVISOR_REPORT_2025-12-18.md
│   │   └── SESSION_SUMMARY_2025-12-18.md
│   │
│   ├── database/                        # Database documentation
│   │   ├── DATABASE_STRUCTURE_GUIDE.md
│   │   ├── data_dictionary.md
│   │   └── NORMALIZATION_SUMMARY.md
│   │
│   ├── extraction/                      # Extraction guides
│   │   ├── indicators_translation_english.md  # All planned indicators ⭐
│   │   ├── exampletable_extraction.md
│   │   └── EXTRACTION_SUMMARY_*.md
│   │
│   ├── workflow/                        # Process documentation
│   │   ├── VERIFICATION_WORKFLOW.md
│   │   ├── QUICK_VERIFICATION_GUIDE.md
│   │   └── UPDATED_WORKFLOW_*.md
│   │
│   ├── bugfixes/                        # Bug fix documentation
│   │   └── BUGFIX_NOTES_FIELD_2025-12-18.md
│   │
│   └── reference/                       # Reference materials
│       └── GENESIS-Webservices_Einfuehrung.pdf
│
├── 📁 data/                             # Data files
│   ├── reference/                       # Reference data
│   │   ├── table_registry.json          # Table tracking ⭐
│   │   └── job_cache.json               # API job cache
│   ├── analysis/                        # Analysis outputs
│   │   └── timeseries/                  # CSV exports for analysis
│   ├── processed/                       # Transformed data
│   └── raw/                             # Raw API responses
│
├── 📁 analysis/                         # Analysis scripts & outputs
│   ├── outputs/                         # Generated charts & CSVs
│   └── *.py                             # Analysis scripts
│
├── 📁 config/                           # Configuration files
│   ├── database.yaml
│   ├── logging.yaml
│   └── sources.yaml
│
├── 📁 tests/                            # Test files
│   ├── unit/                            # Unit tests
│   ├── integration/                     # Integration tests
│   ├── validation/                      # Validation tests
│   └── sandbox/                         # Development test scripts
│
├── 📁 notebooks/                        # Jupyter notebooks
│   ├── analysis/                        # Analysis notebooks
│   └── exploration/                     # Data exploration
│
├── 📁 logs/                             # Application logs
│   ├── app_*.log                        # Application logs by date
│   └── error_*.log                      # Error logs by date
│
├── 📁 backups/                          # Database backups
│   └── *.json, *.sql                    # Backup files
│
├── 📁 archive/                          # Archived/outdated files
│   └── ...                              # Old files for reference
│
├── 📄 README.md                         # This file
├── 📄 requirements.txt                  # Python dependencies
└── 📄 .env                              # Environment variables
```

---

## Data Sources

### 1. Regional Database Germany (Regionalstatistik)
**URL:** https://www.regionalstatistik.de/  
**Status:** 🟢 17/17 tables completed (100%) ✅

| Table ID | Description | Years | Records | Status |
|----------|-------------|-------|---------|--------|
| 12411-03-03-4 | Population by age, gender, nationality | 2011-2024 | 17,556 | ✅ |
| 13111-01-03-4 | Employees at workplace | 2011-2024 | 798 | ✅ |
| 13111-02-02-4 | Employees at residence | 2008-2024 | 1,083 | ✅ |
| 13111-03-02-4 | Employees by scope | 2008-2024 | 3,420 | ✅ |
| 13111-04-02-4 | Employees at residence by scope | 2008-2024 | 3,249 | ✅ |
| 13111-07-05-4 | Employees by sector | 2008-2024 | 13,554 | ✅ |
| 13111-11-04-4 | Employees by qualification | 2008-2024 | 4,161 | ✅ |
| 13111-12-03-4 | Employees at residence by qualification | 2008-2024 | 3,705 | ✅ |
| 13211-02-05-4 | Unemployment rates | 2001-2024 | 1,368 | ✅ |
| 13312-01-05-4 | Employed by sector (annual) | 2000-2023 | 1,368 | ✅ |
| 44231-01-03-4 | **Construction industry** | **1995-2024** | 1,684 | ✅ |
| 44231-01-02-4 | **Total turnover** | **1995-2024** | 1,684 | ✅ |
| 52111-01-02-4 | **Establishments by size** | **2019-2023** | 1,425 | ✅ |
| 52111-02-01-4 | **Establishments by sector** | **2006-2023** | 18,424 | ✅ |
| 52311-01-04-4 | **Business registrations & deregistrations** | **1998-2024** | 3,024 | ✅ |
| 52411-02-01-4 | **Corporate insolvencies** | **2007-2024** | 2,052 | ✅ |
| 82000-04-01-4 | **Employee compensation by sector** | **2000-2022** | 10,488 | ✅ |

### 2. State Database NRW (Landesdatenbank)
**URL:** https://www.landesdatenbank.nrw.de/  
**Status:** ⏳ Pending (17 tables)

### 3. Federal Employment Agency (Bundesagentur für Arbeit)
**URL:** https://statistik.arbeitsagentur.de/  
**Status:** ⏳ Pending (3 tables)

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

### Business Structure (2023)
- **84% micro enterprises** (0-10 employees) across all cities
- Confirms German Mittelstand institutional structure

### Service Sector Emergence
Top 3 sectors across all Ruhr cities (2023):
1. **Trade, vehicle maintenance** (largest)
2. **Professional, scientific services**
3. **Construction or Health**

### COVID-19 Resilience
- Initial 4% decline (2019-2020)
- Near-complete recovery by 2023

---

## Verification Workflow

### Why Verification Matters

Every extraction MUST be verified to ensure:
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

## Installation

### Prerequisites
- Python 3.10 or higher
- PostgreSQL 15 or higher (with PostGIS)
- Git

### Setup

1. **Clone the repository**
```bash
git clone <repository-url>
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
# Edit .env with your database credentials and API keys
```

5. **Set up database**
```bash
# Create PostgreSQL database
createdb regional_economics

# Run schema scripts
psql -d regional_economics -f sql/schema/01_create_schema.sql
```

6. **Verify installation**
```bash
python scripts/diagnostics/check_extracted_data.py
```

---

## Documentation

| Document | Location | Description |
|----------|----------|-------------|
| Project Summary | `docs/project/PROJECT_SUMMARY.md` | Current status and achievements |
| Table Status | `docs/status/TABLE_STATUS_SUMMARY.md` | Extraction progress tracking |
| Supervisor Report | `docs/status/SUPERVISOR_REPORT_2025-12-18.md` | Formal progress report |
| Indicators | `docs/extraction/indicators_translation_english.md` | All planned indicators |
| Verification | `docs/workflow/VERIFICATION_WORKFLOW.md` | Verification process guide |
| Database Guide | `docs/database/DATABASE_STRUCTURE_GUIDE.md` | Schema documentation |

---

## Roadmap

### Phase 1: Planning & Setup ✅ COMPLETE
- Database schema implemented
- Development environment ready
- Documentation complete

### Phase 2: Data Source Analysis ✅ COMPLETE
- API endpoints documented
- Authentication configured
- Data formats analyzed

### Phase 3: Regional Database ETL ✅ 100% COMPLETE
- [x] Demographics (population)
- [x] Employment (8 tables)
- [x] Unemployment
- [x] Employed by sector
- [x] **Construction industry (30 years)**
- [x] **Establishments by size & sector**
- [x] **Business registrations & deregistrations**
- [x] **Corporate insolvencies**
- [x] **Employee compensation by sector**

### Phase 4: State Database NRW ⏳ PENDING
- 17 tables to extract

### Phase 5: Federal Employment Agency ⏳ PENDING
- 3 tables to extract

### Phase 6: Analysis & Visualization ⏳ PENDING
- Master analysis notebook
- Advanced visualizations
- Final documentation

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
| 2.0 | Dec 17, 2025 | 10 indicators completed, verification workflow |
| 3.0 | Dec 18, 2025 | 14 indicators (82%), 30-year historical data, directory reorganization |
| 3.1 | Dec 19, 2025 | File organization: clean directory structure |
| **4.0** | **Dec 19, 2025** | **Regional Database Germany COMPLETE: 17/17 tables (100%), 27 indicators, 86,728 records** |
