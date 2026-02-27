# Regional Economics Database for NRW
## AI-Powered Economic Analysis Platform

[![Live Dashboard](https://img.shields.io/badge/dashboard-live-brightgreen)](https://d127sfxjaas1uw.cloudfront.net)
[![Project Status](https://img.shields.io/badge/status-production-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.11+-blue.svg)]()
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)]()
[![React](https://img.shields.io/badge/react-19-61DAFB.svg)]()
[![AWS](https://img.shields.io/badge/hosted-AWS-FF9900.svg)]()
[![Records](https://img.shields.io/badge/records-484,997-orange)]()
[![Indicators](https://img.shields.io/badge/indicators-103-blue)]()

---

## 🌐 Live Dashboard

**[https://d127sfxjaas1uw.cloudfront.net](https://d127sfxjaas1uw.cloudfront.net)**

An interactive economic analytics platform for North Rhine-Westphalia (NRW), Germany — comparing Duisburg, Düsseldorf, Essen, Oberhausen, and Mülheim an der Ruhr across 50 years of economic, demographic, and labour market data.

| What you can explore | Tab |
|---|---|
| Population, GDP, unemployment KPIs at a glance | Overview |
| Age, nationality, migration, income distribution | Demographics |
| Employment, wages, commuters, vocational qualifications | Labor Market |
| Business registrations, GDP by sector, insolvencies | Business & GDP |
| Broadband and digitisation indicators | ICT |
| Municipal revenues and income tax | Public Finance |
| 50-year historical trends with D3.js charts | Trends |
| Natural language data queries via AI chatbot | Chat |

---

## Overview

A production-grade data engineering project consolidating fragmented public economic data from three major German statistical agencies into a unified analytical platform hosted on AWS.

**Project Impact:**
- Integrates 484,997 records across 103 economic indicators spanning 1975–2024
- Covers 54 NRW districts from three independent data sources, previously inaccessible in a single system
- Enables longitudinal analysis of regional economic transformation across the Ruhr region and wider NRW
- Deployed as a publicly accessible web application with automated CI/CD and 24/7 monitoring

---

## AWS Production Architecture

```
Browser
  └── HTTPS → CloudFront CDN (d127sfxjaas1uw.cloudfront.net)
                ├── /* ──────────────→ S3 Bucket (React SPA, immutable cache)
                └── /api/* ──────────→ Elastic Beanstalk (Node.js 20 API)
                                           └── RDS PostgreSQL 15
                                               (484,997 rows, eu-central-1)
```

| Component | Service | Details |
|---|---|---|
| **Frontend** | Amazon CloudFront + S3 | React 19 + Vite 7, CDN-delivered, immutable asset caching |
| **Backend API** | AWS Elastic Beanstalk | Node.js 20, Amazon Linux 2023, t3.small |
| **Database** | Amazon RDS PostgreSQL 15 | db.t3.micro, 20GB gp3, SSL enforced |
| **CI/CD** | GitHub Actions | Auto-deploy on push to `main` |
| **Monitoring** | CloudWatch + SNS | 5 alarms across RDS and CloudFront |

### API Endpoints

All endpoints are accessible through the CloudFront domain:

| Endpoint | Description |
|---|---|
| `GET /api/health` | Service health check |
| `GET /api/cities` | The 5 comparison cities |
| `GET /api/indicators` | All 103 economic indicators |
| `GET /api/years` | Available year range |
| `GET /api/demographics/:year` | Demographics data for a year |
| `GET /api/labor-market/:year` | Labour market data for a year |
| `GET /api/timeseries/:indicatorCode` | Full time series for any indicator |
| `POST /api/chat` | AI chatbot (natural language data queries) |

---

## Data Coverage

### Summary

| Data Source | Tables | Indicators | Records | Coverage |
|---|---|---|---|---|
| Regional Database Germany | 17/17 | 27 | 99,242 | **100%** ✅ |
| State Database NRW | 17/17 | 61 | 175,560 | **100%** ✅ |
| Federal Employment Agency (BA) | 2/2 | 15 | 223,531 | **100%** ✅ |
| **Total** | **36/36** | **103** | **484,997** | **100%** ✅ |

### Geographic Coverage
- **54 NRW Districts** (Kreise)
- **5 Administrative Districts** (Regierungsbezirke)
- **1 State** (North Rhine-Westphalia)
- **1 National** (Germany for comparison)

### Temporal Coverage
- **Regional DB Germany**: 1995–2024 (30 years)
- **State DB NRW**: 2000–2024 (varies by indicator)
- **BA Employment/Wages**: 2020–2024 (5 years)
- **BA Commuters**: 2002–2024 (23 years)

### Data Categories
1. **Demographics** (Indicators 1–8, 67–71, 86–88): Population structure, age distribution, migration background, income distribution
2. **Labour Market** (Indicators 9–12, 89–103): Employment, unemployment, wages, vocational qualifications, commuter flows
3. **Economic Activity** (Indicators 13–19): Business establishments, registrations, insolvencies, turnover, construction
4. **Sectoral Data** (Indicators 20–55, 92–97): Employment, GDP, and value added by economic sector
5. **Public Finance** (Indicators 28, 56–61): Municipal revenues and income tax
6. **Infrastructure** (Indicators 62–66): Roads by classification
7. **Healthcare** (Indicators 72–85): Hospitals, doctors, care facilities and capacity

---

## CI/CD Pipeline

Deployments are fully automated via GitHub Actions:

```
git push main
  ├── frontend/** changed → build Vite → sync to S3 → invalidate CloudFront
  └── backend/**  changed → install EB CLI → eb deploy → /api/health smoke test
```

Workflow files: `.github/workflows/deploy-frontend.yml`, `.github/workflows/deploy-backend.yml`

---

## Project Structure

```
Regional Economics Database for NRW/
│
├── 📁 .github/workflows/            # CI/CD (GitHub Actions)
│   ├── deploy-frontend.yml          # React → S3 + CloudFront
│   └── deploy-backend.yml           # Node.js → Elastic Beanstalk
│
├── 📁 duisburg-web-application/     # Production web application
│   ├── backend/                     # Node.js / Express API
│   │   ├── server.js                # All API routes
│   │   ├── db.js                    # PostgreSQL connection (RDS)
│   │   ├── Procfile                 # EB process manager
│   │   └── .ebextensions/           # EB platform config
│   └── frontend/                    # React 19 + Vite 7 + D3.js
│       └── src/
│           ├── App.jsx              # Main dashboard component
│           ├── components/          # Chart + UI components
│           └── services/api.js      # Axios API client
│
├── 📁 scripts/aws/                  # AWS deployment & test scripts
│   ├── 01_create_rds_instance.sh    # Day 1: RDS provisioning
│   ├── 02_migrate_to_rds.sh         # Day 1: Database migration
│   ├── 03_verify_migration.py       # Day 1: 57-test verification
│   ├── 04_deploy_to_beanstalk.sh    # Day 2: EB deployment
│   ├── 05_test_beanstalk_api.sh     # Day 2: 24-test API suite
│   ├── 06_deploy_frontend_cdn.sh    # Day 3: S3 + CloudFront
│   ├── 07_test_end_to_end.sh        # Day 3: 19-test E2E suite
│   ├── 08_setup_monitoring.sh       # Day 4: CloudWatch + SNS
│   └── 09_test_day4.sh              # Day 4: CI/CD verification
│
├── 📁 src/                          # Python ETL source code
│   ├── extractors/                  # API extraction (3 sources)
│   ├── transformers/                # Data transformation (15+)
│   └── loaders/                     # Database loading
│
├── 📁 pipelines/                    # 36 ETL pipeline scripts
│   ├── regional_db/                 # 17 pipelines ✅
│   ├── state_db/                    # 17 pipelines ✅
│   └── ba/                          # 6 pipelines ✅  (2 sources)
│
├── 📁 sql/                          # Database schema + queries
│   ├── schema/01_create_schema.sql  # Star schema DDL
│   └── queries/                     # 20+ analysis queries
│
├── 📁 docs/                         # Full documentation
│   ├── database/                    # Schema guides, data dictionary
│   ├── extraction/                  # Source documentation
│   └── project/                     # Project summaries
│
└── 📁 config/                       # YAML configuration
    ├── database.yaml
    └── sources.yaml
```

---

## Local Development

### Prerequisites
- Python 3.10+, Node.js 18+, PostgreSQL 15+

### Backend (local)
```bash
cd duisburg-web-application/backend
cp .env.example .env          # fill in DB credentials
npm install
npm run dev                   # starts on http://localhost:5000
```

### Frontend (local)
```bash
cd duisburg-web-application/frontend
npm install
VITE_API_BASE="http://localhost:5000" npm run dev   # starts on http://localhost:5173
```

### Python ETL pipelines
```bash
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env          # fill in DB + API credentials
python scripts/diagnostics/check_extracted_data.py
```

---

## Database Schema

### Star Schema Design

**Dimension Tables:**
- `dim_geography` — 58 geographic entities (54 NRW districts + aggregates)
- `dim_time` — 50 years: 1975–2024
- `dim_indicator` — 103 economic indicators

**Fact Tables:**
- `fact_demographics` — Population and demographic data
- `fact_labor_market` — Employment and unemployment
- `fact_business_economy` — Business activity and economic output
- `fact_public_finance` — Municipal finance and tax data
- `fact_healthcare` — Healthcare facilities and capacity
- `fact_infrastructure` — Road infrastructure

### Example Queries

```sql
-- Time series for Duisburg total population
SELECT t.year, f.value
FROM fact_demographics f
JOIN dim_geography g ON f.geo_id = g.geo_id
JOIN dim_time t ON f.time_id = t.time_id
WHERE f.indicator_id = 1
  AND g.region_code = '05112'  -- Duisburg
ORDER BY t.year;

-- Commuter balance for major NRW cities (2023)
WITH commuters AS (
  SELECT g.region_name,
    SUM(CASE WHEN fd.indicator_id = 101 THEN fd.value ELSE 0 END) as incoming,
    SUM(CASE WHEN fd.indicator_id = 102 THEN fd.value ELSE 0 END) as outgoing
  FROM fact_demographics fd
  JOIN dim_geography g ON fd.geo_id = g.geo_id
  JOIN dim_time t ON fd.time_id = t.time_id
  WHERE fd.indicator_id IN (101, 102) AND t.year = 2023
    AND fd.gender IS NULL AND fd.nationality IS NULL
  GROUP BY g.region_name
)
SELECT region_name, incoming::INT, outgoing::INT,
       (incoming - outgoing)::INT AS net_balance
FROM commuters ORDER BY net_balance DESC LIMIT 10;
```

---

## Data Sources

### 1. Regional Database Germany (Regionalstatistik) ✅
**URL:** https://www.regionalstatistik.de/
**Status:** 17/17 tables | 27 indicators | 99,242 records | 1995–2024

### 2. State Database NRW (Landesdatenbank) ✅
**URL:** https://www.landesdatenbank.nrw.de/
**Status:** 17/17 tables | 61 indicators | 175,560 records | 2000–2024

### 3. Federal Employment Agency (BA) ✅
**URL:** https://statistik.arbeitsagentur.de/
**Status:** 2/2 sources | 15 indicators | 223,531 records | 2002–2024

---

## Key Findings: Ruhr Region

### Construction Employment (1995–2024)
30-year structural decline across all Ruhr cities:

| City | 1995 | 2024 | Change |
|---|---|---|---|
| Dortmund | 8,591 | 3,385 | **−60.6%** |
| Essen | 7,311 | 3,246 | **−55.6%** |
| Duisburg | 3,233 | 1,733 | **−46.4%** |

### Commuter Patterns (2023)
| District | Incoming | Outgoing | Net |
|---|---|---|---|
| Düsseldorf | 286,090 | 99,640 | **+186,450** |
| Essen | 142,190 | 92,750 | **+49,440** |

---

## Deployment History

| Day | Milestone | Tests |
|---|---|---|
| **Day 1** | RDS PostgreSQL provisioned and migrated (484,997 rows) | 57/57 ✅ |
| **Day 2** | Elastic Beanstalk API live | 24/24 ✅ |
| **Day 3** | S3 + CloudFront CDN deployed | 19/19 ✅ |
| **Day 4** | GitHub Actions CI/CD + CloudWatch monitoring | 20/20 ✅ |

---

## Documentation

| Document | Location | Description |
|---|---|---|
| Web Application | `duisburg-web-application/README.md` | Dashboard setup and API reference |
| Indicators Guide | `docs/extraction/indicators_translation_english.md` | All 103 indicators |
| Database Structure | `docs/database/database_structure_explained.md` | Schema documentation |
| Data Dictionary | `docs/database/data_dictionary.md` | Field-level definitions |
| BA Data Coverage | `docs/extraction/ba_data_coverage_explanation.md` | Employment agency data |

---

## Roadmap

### ✅ Phase 1–5: Data Infrastructure (COMPLETE)
All 36 ETL pipelines operational, 484,997 records loaded across 103 indicators.

### ✅ Phase 6: Web Application & AWS Deployment (COMPLETE)
- React 19 + Node.js dashboard live at https://d127sfxjaas1uw.cloudfront.net
- AWS architecture: CloudFront → S3 + Elastic Beanstalk → RDS
- Automated CI/CD via GitHub Actions
- CloudWatch monitoring with email alerts

### 🎯 Phase 7: Enhancements (PLANNED)
- Custom domain name
- OpenAI chatbot key (for live AI queries)
- AWS Budget alert

---

## Version History

| Version | Date | Changes |
|---|---|---|
| 1.0 | Dec 2024 | Initial project setup |
| 2.0 | Dec 17, 2024 | 10 indicators, verification workflow |
| 3.0 | Dec 18, 2024 | 14 indicators, 30-year historical data |
| 4.0 | Dec 19, 2024 | Regional DB complete: 17/17 tables, 86,728 records |
| 5.0 | Jan 4, 2026 | All data sources complete: 36/36 tables, 103 indicators |
| **6.0** | **Feb 25, 2026** | **AWS production deployment: CloudFront + EB + RDS + CI/CD** |

---

## Contact

**Project:** Regional Economics Database for NRW
**Repository:** https://github.com/Kanyuchi/Regional_Economics_Database_NRW
**Organisation:** Duisburg Business & Innovation (DBI)

---

## License

MIT License — free to use, modify, and distribute with attribution.

---

## Acknowledgments

- **Regionalstatistik.de** — Regional Database Germany
- **Landesdatenbank.nrw.de** — State Database North Rhine-Westphalia
- **Statistik.arbeitsagentur.de** — Federal Employment Agency (BA)

Built for economic research and regional development in NRW.
