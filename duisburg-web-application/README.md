# NRW Regional Economics Dashboard

A production web application for visualising 50 years of economic, demographic, and labour market data across five Ruhr cities — hosted on AWS.

## 🌐 Live Application

**https://d127sfxjaas1uw.cloudfront.net**

---

## Dashboard Tabs

| Tab | What it shows |
|---|---|
| **Overview** | KPI cards — Population, GDP, Unemployment, Median Wage |
| **Demographics** | Population structure, age, nationality, migration background |
| **Labor Market** | Employment, unemployment, wages, commuter flows |
| **Business & GDP** | Business registrations, GDP by sector, insolvencies |
| **ICT** | Broadband and digitisation indicators |
| **Finance** | Municipal revenues and income tax |
| **Trends** | 50-year D3.js time series with multi-city overlay |
| **Chat** | AI assistant for natural language data queries |

**Cities covered:** Duisburg · Düsseldorf · Essen · Oberhausen · Mülheim an der Ruhr

---

## Production Architecture

```
Browser → CloudFront (HTTPS)
            ├── /*       → S3 bucket (React SPA)
            └── /api/*   → Elastic Beanstalk (Node.js API)
                              └── RDS PostgreSQL 15 (484,997 rows)
```

| Layer | Service | Details |
|---|---|---|
| CDN | Amazon CloudFront | Distribution `ESPS80U2L42VS`, PriceClass_100 |
| Frontend | Amazon S3 | `regional-nrw-frontend-329631044553`, private + OAC |
| Backend | AWS Elastic Beanstalk | `regional-nrw-env`, Node.js 20, t3.small, eu-central-1 |
| Database | Amazon RDS PostgreSQL 15 | `regional-economics-db`, db.t3.micro, 20GB gp3 |

---

## Technology Stack

### Frontend
- **React 19** — UI framework
- **Vite 7** — build tool (content-hashed assets, immutable CDN cache)
- **D3.js 7** — all chart rendering
- **Axios** — API client

### Backend
- **Node.js 20** / **Express 4** — API server
- **pg (node-postgres)** — PostgreSQL driver
- **OpenAI SDK** — AI chatbot (requires `OPENAI_API_KEY`)

---

## API Reference

Base URL (production): `https://d127sfxjaas1uw.cloudfront.net`

| Method | Endpoint | Description |
|---|---|---|
| GET | `/api/health` | Service health check |
| GET | `/api/cities` | The 5 comparison cities |
| GET | `/api/indicators` | All 103 economic indicators |
| GET | `/api/years` | Available year range |
| GET | `/api/demographics/:year` | Demographics data for a year |
| GET | `/api/labor-market/:year` | Labour market data for a year |
| GET | `/api/business-economy/:year` | Business economy data for a year |
| GET | `/api/public-finance/:year` | Public finance data for a year |
| GET | `/api/timeseries/:indicatorCode` | Full time series for any indicator |
| GET | `/api/indicators/group/:tab` | Indicators grouped by dashboard tab |
| POST | `/api/chat` | AI chatbot — natural language data query |

### Example API calls
```bash
# Health check
curl https://d127sfxjaas1uw.cloudfront.net/api/health

# Get all indicators
curl https://d127sfxjaas1uw.cloudfront.net/api/indicators

# Population time series
curl https://d127sfxjaas1uw.cloudfront.net/api/timeseries/total_population

# Labour market for 2023
curl https://d127sfxjaas1uw.cloudfront.net/api/labor-market/2023
```

---

## Local Development

### Prerequisites
- Node.js 18+
- Access to a PostgreSQL database (local or the production RDS endpoint)

### Backend

```bash
cd duisburg-web-application/backend
npm install

# Copy and fill in environment variables
cp .env.example .env
```

Minimum `.env` for local development:
```
DB_HOST=localhost
DB_PORT=5432
DB_NAME=regional_db
DB_USER=your_user
DB_PASSWORD=your_password
PORT=5000
CORS_ORIGINS=http://localhost:5173
OPENAI_API_KEY=          # optional — enables AI chat
OPENAI_MODEL=gpt-4o-mini
```

```bash
npm run dev    # starts on http://localhost:5000 with nodemon
```

### Frontend

```bash
cd duisburg-web-application/frontend
npm install
VITE_API_BASE="http://localhost:5000" npm run dev
# opens http://localhost:5173
```

> **Note:** Leave `VITE_API_BASE` empty for production builds — CloudFront proxies `/api/*` to the backend so the app uses `window.location.origin` at runtime.

---

## CI/CD — Automated Deployments

Any push to `main` triggers automatic deployment via GitHub Actions:

- **Frontend changes** (`frontend/**`) → Vite build → S3 sync → CloudFront invalidation
- **Backend changes** (`backend/**`) → EB CLI deploy → `/api/health` smoke test

Workflow files:
- `.github/workflows/deploy-frontend.yml`
- `.github/workflows/deploy-backend.yml`

Required GitHub Secrets: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `CF_DISTRIBUTION_ID`

---

## Monitoring

Five CloudWatch alarms are active:

| Alarm | Metric | Threshold |
|---|---|---|
| `nrw-rds-cpu-high` | RDS CPUUtilization | > 80% for 15 min |
| `nrw-rds-storage-low` | RDS FreeStorageSpace | < 5 GB |
| `nrw-rds-connections-high` | RDS DatabaseConnections | > 80 |
| `nrw-cf-5xx-rate` | CloudFront 5xxErrorRate | > 1% |
| `nrw-cf-error-rate` | CloudFront TotalErrorRate | > 5% |

Alerts → SNS → email (shaunkudzi@gmail.com)

---

## AWS Deployment Scripts

All deployment scripts are in `scripts/aws/`:

```bash
# Re-deploy frontend manually
bash scripts/aws/06_deploy_frontend_cdn.sh

# Re-deploy backend manually
bash scripts/aws/04_deploy_to_beanstalk.sh

# Run end-to-end tests
bash scripts/aws/07_test_end_to_end.sh

# Re-run monitoring setup / update alert email
ALERT_EMAIL="you@example.com" bash scripts/aws/08_setup_monitoring.sh

# Verify Day 4 CI/CD setup
bash scripts/aws/09_test_day4.sh
```

---

## Troubleshooting

### Dashboard not loading
- Check `https://d127sfxjaas1uw.cloudfront.net/api/health` — should return `{"status":"ok"}`
- If health returns 5xx, check EB environment health in AWS Console

### Local dev — no data
- Confirm PostgreSQL is running and `.env` DB credentials are correct
- Run `npm run test:db` from the `backend/` directory to test the connection

### CORS error in local dev
- Ensure `CORS_ORIGINS=http://localhost:5173` is set in `backend/.env`
- Restart the backend after changing `.env`

---

## License

MIT — see root `LICENSE` file.
