# Regional Economics Database Project Plan
## NRW Economic Indicators System

**Project Owner:** DBI (Duisburg Business & Innovation)  
**Project Lead:** Kanyuchi  
**Start Date:** December 2024  
**Last Updated:** December 18, 2025  
**Project Type:** Data Infrastructure & ETL Pipeline  
**Project Status:** 🟢 **ACTIVE - Phase 3 In Progress**

---

## Executive Summary

This project builds a comprehensive regional economics database for North Rhine-Westphalia (NRW), aggregating data from three primary sources: the Regional Database Germany, State Database NRW, and Federal Employment Agency. The database supports AI-driven regional economic analysis at the district level, with special focus on the **Ruhr region cities** (Duisburg, Bochum, Essen, Dortmund, Gelsenkirchen) for thesis research on industrial transformation.

---

## Current Progress Summary

### Database Status (as of December 18, 2025)

| Metric | Value |
|--------|-------|
| **Total Records** | 49,863 |
| **Indicators Completed** | 10 of 19 |
| **Regional DB Tables** | 10 of 17 (59%) |
| **State DB Tables** | 0 of 17 (0%) |
| **BA Tables** | 0 of 3 (0%) |
| **Overall Progress** | 27% |

### Completed Indicators

| ID | Indicator | Table ID | Records | Years |
|----|-----------|----------|---------|-------|
| 1 | Population total | 12411-03-03-4 | 17,556 | 2011-2024 |
| 9 | Employment total | 13111-01-03-4 | 798 | 2011-2024 |
| 12 | Employment scope (workplace) | 13111-03-02-4 | 3,420 | 2008-2024 |
| 13 | Employment qualification (workplace) | 13111-11-04-4 | 4,161 | 2008-2024 |
| 14 | Employment residence | 13111-02-02-4 | 1,083 | 2008-2024 |
| 15 | Employment scope (residence) | 13111-04-02-4 | 3,249 | 2008-2024 |
| 16 | Employment qualification (residence) | 13111-12-03-4 | 3,705 | 2008-2024 |
| 17 | Employment by sector (workplace) | 13111-07-05-4 | 13,554 | 2008-2024 |
| 18 | Unemployment rate | 13211-02-05-4 | 1,368 | 2001-2024 |
| 19 | Employed by sector (annual) | 13312-01-05-4 | 1,368 | 2000-2023 |

---

## Project Objectives

### Primary Objectives
1. **Data Integration:** Consolidate 40+ economic indicators from 3 distinct data sources
2. **Geographic Coverage:** Provide district-level data for all NRW districts, with aggregations
3. **Temporal Coverage:** Historical data for all available years (annual frequency)
4. **Automation:** Build sustainable ETL pipelines for regular updates
5. **Documentation:** Create comprehensive technical and user documentation
6. **🆕 Thesis Research Support:** Enable time series analysis for Ruhr region transformation studies

### Success Metrics
- ✅ 27% of indicators successfully extracted and loaded
- ✅ Data quality validation passing rate = 100% (verified indicators)
- ⏳ Automated pipeline running successfully on scheduled intervals
- ✅ Database query response time < 2 seconds for standard queries
- ✅ Complete documentation and handover materials

---

## Learned Operational Procedures

### ETL Pipeline Workflow (Verified Best Practices)

```
1. EXTRACT → Raw data from source API (year-by-year for large tables)
2. TRANSFORM → Clean and structure data
3. LOAD → Insert into database
4. ✅ VERIFY → Run verification script (MANDATORY)
5. DOCUMENT → Update table registry
```

### Critical Lessons Learned

#### 1. Year-by-Year Extraction
**Issue:** Large tables fail when requesting all years at once
**Solution:** Extract each year separately, then combine
```python
for year in years:
    raw_data = extractor.get_table_data(table_id, startyear=year, endyear=year)
    # Process and combine
```

#### 2. Data Format Variations
**Issue:** Tables have different structures (WIDE vs LONG format)
**Solution:** Create table-specific parsers that handle:
- WIDE format: Sectors in columns (e.g., 13312-01-05-4)
- LONG format: Sectors in rows (e.g., 13111-07-05-4)
- Variable header rows (6-10 rows typically)

#### 3. Region Code Handling
**Issue:** Region codes parsed as numbers lose leading zeros
**Solution:** Force string dtype when reading CSVs
```python
df = pd.read_csv(data, dtype={0: str, 1: str, 2: str})
```

#### 4. Indicator ID Management
**Issue:** Shared indicator IDs caused data mixing
**Solution:** Create unique indicator IDs per source table
- Always check if indicator exists before creating
- Use `source_table_id` as unique identifier

#### 5. Duplicate Prevention
**Issue:** Re-running extractions creates duplicates
**Solution:** Implement duplicate cleanup after extraction
```sql
DELETE FROM fact_demographics
WHERE fact_id IN (
    SELECT fact_id FROM (
        SELECT fact_id, ROW_NUMBER() OVER (
            PARTITION BY geo_id, time_id, indicator_id, gender, nationality, age_group
            ORDER BY fact_id
        ) as rn
        FROM fact_demographics
        WHERE indicator_id = :id
    ) t WHERE rn > 1
)
```

---

## Verification Workflow (MANDATORY)

### After Every Extraction

```bash
python verify_extraction_timeseries.py --indicator <ID>
```

### Verification Checks
1. **Data Completeness:** Total records, year coverage, NULL values
2. **Ruhr Cities Coverage:** All 5 key cities have data
3. **Time Series Analysis:** Trends, changes, patterns
4. **Quality Assessment:** EXCELLENT/GOOD/FAIR/POOR rating

### Ruhr Region Focus Cities

| City | Code | Significance |
|------|------|--------------|
| **Duisburg** | 05112 | Steel industry, Rhine port |
| **Bochum** | 05911 | Coal mining, automotive |
| **Essen** | 05113 | Krupp headquarters, cultural capital |
| **Dortmund** | 05913 | Largest Ruhr city, tech hub |
| **Gelsenkirchen** | 05513 | Coal mining, energy sector |

### Verification Output Example
```
====================================================================================================
 DATA EXTRACTION VERIFICATION REPORT
====================================================================================================

 RUHR REGION CITIES COVERAGE
----------------------------------------------------------------------------------------------------
City                 Region Code  Records    Year Range      Status    
----------------------------------------------------------------------------------------------------
Duisburg             05112        24         2000-2023       OK        
Bochum               05911        24         2000-2023       OK        
Essen                05113        24         2000-2023       OK        
Dortmund             05913        24         2000-2023       OK        
Gelsenkirchen        05513        24         2000-2023       OK        

[OK] All Ruhr region cities have data

 VERIFICATION SUMMARY
----------------------------------------------------------------------------------------------------
[OK] VERIFICATION PASSED - Data is accurate and complete
====================================================================================================
```

---

## Technical Architecture

### Technology Stack (Implemented)

**Database:**
- Primary: PostgreSQL 15+ (with PostGIS for geographic operations) ✅
- Schema: Star schema with dimension and fact tables ✅

**ETL Framework:**
- Python 3.10+ ✅
- Pandas for data manipulation ✅
- SQLAlchemy for database operations ✅
- Custom extractors per data source ✅

**Data Extraction:**
- GENESIS API (REST) for Regional Database ✅
- Year-by-year extraction for large tables ✅
- Retry logic and error handling ✅

**Validation & Quality:**
- Custom verification script with time series analysis ✅
- Ruhr cities coverage verification ✅
- CSV export for further analysis ✅

**Version Control:**
- Git (local repository)

### Implemented Database Schema

```sql
-- Core Dimensions (IMPLEMENTED)
dim_geography    -- 60 NRW regions (districts + state + aggregates)
dim_time         -- Years 2000-2024 (24 entries)
dim_indicator    -- 19 indicators defined

-- Fact Tables (IMPLEMENTED)
fact_demographics    -- 49,863 records (all labor market + population)
fact_labor_market    -- Reserved for future use
fact_business_economy -- Reserved for construction, business data

-- Support Tables (IMPLEMENTED)
data_extraction_log
data_quality_checks
```

---

## Project Phases (Updated Status)

### Phase 1: Planning & Setup ✅ COMPLETE
**Duration:** 2 weeks (December 2024)

**Completed:**
- ✅ Technical architecture finalized
- ✅ Development environment set up
- ✅ Project repository created
- ✅ PostgreSQL database instance running
- ✅ Database schema implemented
- ✅ Project documentation templates created
- ✅ Version control configured

### Phase 2: Data Source Analysis ✅ COMPLETE
**Duration:** 2 weeks

**Completed:**
- ✅ Indicators mapped to data sources
- ✅ Data formats analyzed (WIDE/LONG)
- ✅ API endpoints documented (GENESIS REST API)
- ✅ Authentication configured
- ✅ Data dictionary created
- ✅ Sample extractions validated

### Phase 3: ETL Development - Regional Database 🟡 IN PROGRESS
**Duration:** 3 weeks (Current Phase)

**Completed:**
- ✅ Population extractor (12411-03-03-4)
- ✅ Employment extractors (13111 series - 7 tables)
- ✅ Unemployment extractor (13211-02-05-4)
- ✅ Employed by sector extractor (13312-01-05-4)
- ✅ Transformation logic implemented
- ✅ Geographic mapping for NRW districts
- ✅ Data quality validation rules
- ✅ Error handling and logging
- ✅ Verification workflow established

**Remaining:**
- ⏳ Construction industry (44231-01-03-4) - **NEXT**
- ⏳ Construction turnover (44231-01-02-4)
- ⏳ Establishments by size (52111-01-02-4)
- ⏳ Establishments by sector (52111-02-01-4)
- ⏳ Business registrations (52311-01-04-4)
- ⏳ Insolvencies (52411-02-01-4)
- ⏳ Table 82000-04-01-4

### Phase 4: ETL Development - State Database NRW ⏳ PENDING
**Duration:** 3 weeks

### Phase 5: ETL Development - Federal Employment Agency ⏳ PENDING
**Duration:** 2 weeks

### Phase 6: Aggregation & Derived Metrics ⏳ PENDING
**Duration:** 2 weeks

### Phase 7: Automation & Orchestration ⏳ PENDING
**Duration:** 2 weeks

### Phase 8: Quality Assurance & Testing ⏳ PENDING
**Duration:** 2 weeks

### Phase 9: Documentation & Knowledge Transfer 🟡 ONGOING
**Duration:** 2 weeks

**Completed:**
- ✅ Technical documentation
- ✅ Verification workflow documentation
- ✅ Session summaries
- ✅ Quick reference guides

### Phase 10: Deployment & Handover ⏳ PENDING
**Duration:** 1 week

---

## File Structure (Current)

```
Regional Economics Database for NRW/
│
├── 📁 src/
│   ├── extractors/
│   │   └── regional_db/
│   │       ├── base_extractor.py          ✅
│   │       ├── employment_extractor.py    ✅ (10 methods)
│   │       └── population_extractor.py    ✅
│   │
│   ├── transformers/
│   │   ├── employment_transformer.py      ✅ (6 methods)
│   │   └── demographics_transformer.py    ✅
│   │
│   ├── loaders/
│   │   └── db_loader.py                   ✅
│   │
│   └── utils/
│       ├── database.py                    ✅
│       ├── logging.py                     ✅
│       └── config.py                      ✅
│
├── 📁 pipelines/
│   └── regional_db/
│       ├── etl_12411_03_03_4_population.py           ✅
│       ├── etl_13111_01_03_4_employment.py           ✅
│       ├── etl_13111_02_02_4_employment_residence.py ✅
│       ├── etl_13111_03_02_4_employment_scope.py     ✅
│       ├── etl_13111_04_02_4_employment_residence_scope.py ✅
│       ├── etl_13111_07_05_4_employment_sector.py    ✅
│       ├── etl_13111_11_04_4_employment_qualification.py ✅
│       ├── etl_13111_12_03_4_employment_residence_qualification.py ✅
│       ├── etl_13211_02_05_4_unemployment.py         ✅
│       └── etl_13312_01_05_4_employed_sector.py      ✅
│
├── 📁 data/
│   ├── reference/
│   │   └── table_registry.json            ✅ (tracking all tables)
│   └── analysis/
│       └── timeseries/                    ✅ (CSV exports)
│
├── 📁 config/
│   ├── database.yaml                      ✅
│   └── config.yaml                        ✅
│
├── 📄 verify_extraction_timeseries.py     ✅ (verification tool)
├── 📄 check_extracted_data.py             ✅ (status overview)
├── 📄 fix_indicator_mapping.py            ✅ (data fix tool)
│
├── 📄 VERIFICATION_WORKFLOW.md            ✅
├── 📄 QUICK_VERIFICATION_GUIDE.md         ✅
├── 📄 SESSION_SUMMARY_2025-12-18.md       ✅
└── 📄 requirements.txt                    ✅
```

---

## ETL Pipeline Standards

### Pipeline Structure Template

```python
"""
ETL Pipeline: Table <TABLE_ID>
==============================
<Description>

Source: Regional Database Germany
Available Period: <YEAR_START>-<YEAR_END>
"""

PIPELINE_INFO = {
    "table_id": "<TABLE_ID>",
    "table_name": "<TABLE_NAME>",
    "source": "regional_db",
    "category": "<CATEGORY>",
    "available_period": "<YEAR_START>-<YEAR_END>",
    "indicator_id": <ID>,
}

def run_pipeline(years: list = None) -> bool:
    """
    1. EXTRACT - Get data from API (year-by-year)
    2. TRANSFORM - Clean and structure
    3. LOAD - Insert into database
    4. VERIFY - Run verification (MANDATORY)
    """
    # Implementation...
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| ETL Script | `etl_<table_id>_<short_name>.py` | `etl_13312_01_05_4_employed_sector.py` |
| Extractor Method | `extract_<indicator_name>()` | `extract_employed_by_sector()` |
| Transformer Method | `transform_<indicator_name>()` | `transform_employed_by_sector()` |
| Indicator Code | `<category>_<description>` | `employed_by_sector_annual` |

---

## Data Quality Standards

### Minimum Requirements for "PASSED" Status
- ✅ At least 1 record in database
- ✅ Completeness rate >= 85%
- ✅ All 5 Ruhr cities have data
- ✅ At least 1 year of data

### Target Standards (Excellence)
- 🎯 Completeness rate >= 95%
- 🎯 At least 10 years of continuous data
- 🎯 No missing years in available period
- 🎯 Values within expected ranges

### Quality Ratings

| Rating | Completeness | Status |
|--------|--------------|--------|
| EXCELLENT | >= 95% | ✅ Ideal |
| GOOD | >= 85% | ✅ Acceptable |
| FAIR | >= 70% | ⚠️ Review |
| POOR | < 70% | ❌ Investigate |

---

## Risk Management (Updated)

### Resolved Risks

| Risk | Resolution |
|------|------------|
| Data format inconsistencies | Table-specific parsers implemented |
| Indicator ID conflicts | Unique IDs per source table |
| API timeout issues | Year-by-year extraction with retry logic |
| Missing region codes | String dtype enforcement |

### Active Risks

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Source API changes | Medium | High | Monitor for changes; flexible parsing |
| State DB access issues | Medium | Medium | Document requirements early |
| Timeline delays | Low | Medium | Agile approach; prioritize critical tables |

---

## Resource Requirements

### Confirmed Infrastructure
- ✅ PostgreSQL database server (local)
- ✅ Python 3.11+ development environment
- ✅ GENESIS API access (Regional Database)
- ✅ ~500 MB storage for current data

### Pending Requirements
- ⏳ State Database NRW API access
- ⏳ Federal Employment Agency API access
- ⏳ Production server for automation

---

## Success Criteria (Updated)

### Technical Success
- [x] Database schema implemented and optimized
- [x] 10 indicators successfully extracted (27% complete)
- [x] Data quality checks passing 100%
- [x] Verification workflow operational
- [x] Query performance meeting targets
- [ ] All 40+ indicators extracted
- [ ] ETL pipelines running automatically
- [ ] Geographic aggregations validated

### Documentation Success
- [x] Complete technical documentation
- [x] Verification workflow documentation
- [x] Data dictionary with indicators
- [x] Operations guides
- [ ] User guide for database queries
- [ ] Full handover materials

### Operational Success
- [ ] Automated updates running
- [ ] Monitoring and alerting functional
- [x] Database queryable for time series analysis
- [x] Data accessible for thesis research

---

## Next Steps

### Immediate (Next Extraction)
1. Extract table 44231-01-03-4 (Construction industry, 1995-2024)
2. Verify with `verify_extraction_timeseries.py`
3. Update table_registry.json
4. Document any issues

### Short-term (This Week)
- Complete remaining 7 Regional Database tables
- Verify all existing indicators
- Export time series for thesis analysis

### Medium-term (This Month)
- Complete Regional Database (100%)
- Begin State Database NRW extraction
- Set up automation framework

---

## Appendices

### A. Indicator Categories (Updated)
1. Demographics (1 indicator completed)
2. Labor Market (9 indicators completed)
3. Business Economy (7 indicators pending)
4. Healthcare (pending)
5. Infrastructure (pending)
6. Public Finance (pending)
7. Commuters (pending)

### B. Geographic Entities (Implemented)
- Districts in NRW: 53 entities (in dim_geography)
- Ruhr cities: 5 focus cities for thesis research
- State total: NRW aggregate
- Country: Germany total

### C. API Reference
**Regional Database (GENESIS):**
- Base URL: `https://www.regionalstatistik.de/genesisws/rest/2020/`
- Authentication: Username/Password
- Format: `datencsv` (CSV format)
- Area: `free` (all regions)

---

## Document Version Control

| Version | Date | Changes | Author |
|---------|------|---------|--------|
| 1.0 | Dec 2024 | Initial plan creation | Kanyuchi |
| 2.0 | Dec 18, 2025 | Major update: Current status, verification workflow, lessons learned | Kanyuchi |
