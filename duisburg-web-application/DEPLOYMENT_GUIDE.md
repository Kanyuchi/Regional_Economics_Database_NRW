# Deployment Guide — NRW Regional Economics Dashboard

This guide documents the **current production deployment** on AWS and how to re-run or update it.

## Live Application

| Service | URL |
|---|---|
| **Dashboard (frontend)** | https://d127sfxjaas1uw.cloudfront.net |
| **API health check** | https://d127sfxjaas1uw.cloudfront.net/api/health |
| **EB backend (direct)** | http://regional-nrw-env.eba-s26pyuip.eu-central-1.elasticbeanstalk.com |

---

## Architecture

```
Browser → HTTPS → CloudFront (d127sfxjaas1uw.cloudfront.net)
                    ├── /* (default)  → S3 regional-nrw-frontend-329631044553
                    │                   React SPA, Vite-hashed assets, immutable cache
                    └── /api/*        → Elastic Beanstalk regional-nrw-env
                                        Node.js 20, t3.small, eu-central-1
                                          └── RDS PostgreSQL 15
                                              regional-economics-db
                                              db.t3.micro, 20GB gp3
```

### Key AWS Resources

| Resource | Identifier | Region |
|---|---|---|
| CloudFront distribution | `ESPS80U2L42VS` | Global |
| S3 bucket | `regional-nrw-frontend-329631044553` | eu-central-1 |
| EB application | `regional-nrw-api` | eu-central-1 |
| EB environment | `regional-nrw-env` | eu-central-1 |
| RDS instance | `regional-economics-db` | eu-central-1 |
| IAM deploy user | `regional-nrw-deploy` | Global |
| SNS topic (RDS alerts) | `regional-nrw-alerts` | eu-central-1 |
| SNS topic (CF alerts) | `regional-nrw-alerts-cf` | us-east-1 |

---

## CI/CD — Automated Deployment (Normal Flow)

For day-to-day changes, just push to `main`:

```bash
git add .
git commit -m "your change"
git push origin main
```

GitHub Actions handles the rest:
- `frontend/**` changes → `.github/workflows/deploy-frontend.yml` runs
- `backend/**` changes → `.github/workflows/deploy-backend.yml` runs

View run status: https://github.com/Kanyuchi/Regional_Economics_Database_NRW/actions

---

## Manual Re-deployment

### Frontend (React → S3 → CloudFront)

```bash
bash scripts/aws/06_deploy_frontend_cdn.sh
```

What it does:
1. `npm run build` in `frontend/` with `VITE_API_BASE=""`
2. Syncs `dist/` to S3 (assets: immutable cache, index.html: no-cache)
3. Creates CloudFront invalidation on `/*`

Then run end-to-end tests:
```bash
bash scripts/aws/07_test_end_to_end.sh
```

### Backend (Node.js → Elastic Beanstalk)

```bash
bash scripts/aws/04_deploy_to_beanstalk.sh
```

Or deploy directly from the backend directory:
```bash
cd duisburg-web-application/backend
eb deploy regional-nrw-env --timeout 20
```

Then run API tests:
```bash
bash scripts/aws/05_test_beanstalk_api.sh
```

---

## Environment Variables on EB

The backend reads these from Elastic Beanstalk environment variables (set via `eb setenv`):

| Variable | Description |
|---|---|
| `DB_HOST` | RDS endpoint |
| `DB_PORT` | `5432` |
| `DB_NAME` | `regional_db` |
| `DB_USER` | Database user |
| `DB_PASSWORD` | Database password |
| `DB_SSL` | `true` |
| `PORT` | `8080` |
| `HOST` | `0.0.0.0` |
| `CORS_ORIGINS` | `https://d127sfxjaas1uw.cloudfront.net` |
| `OPENAI_API_KEY` | OpenAI key (for `/api/chat`) |
| `OPENAI_MODEL` | `gpt-4o-mini` |

To update a variable:
```bash
cd duisburg-web-application/backend
eb setenv OPENAI_API_KEY="sk-..." -e regional-nrw-env
```

---

## Monitoring

### View CloudWatch Alarms
- RDS alarms: https://eu-central-1.console.aws.amazon.com/cloudwatch/home?region=eu-central-1#alarmsV2:
- CloudFront alarms: https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:

### Update Alert Email
```bash
ALERT_EMAIL="new@email.com" bash scripts/aws/08_setup_monitoring.sh
```

### Verify Full Day 4 Setup
```bash
bash scripts/aws/09_test_day4.sh
```

---

## Security Notes

- The S3 bucket is **fully private** — only CloudFront can read from it via OAC (Origin Access Control)
- Direct S3 URLs return 403 by design
- RDS is not publicly accessible — only reachable from the EB security group
- CORS is locked to the CloudFront domain (not `*`)
- All HTTPS; HTTP redirects to HTTPS at the CloudFront layer

---

## Local Development Setup

See `duisburg-web-application/README.md` for full local setup instructions.

Quick start:
```bash
# Backend
cd duisburg-web-application/backend && npm install && npm run dev

# Frontend (new terminal)
cd duisburg-web-application/frontend && npm install
VITE_API_BASE="http://localhost:5000" npm run dev
```

---

## Costs (approximate, eu-central-1)

| Service | Type | Est. Monthly |
|---|---|---|
| RDS db.t3.micro | On-demand | ~$15 |
| EB t3.small | On-demand | ~$15 |
| S3 + CloudFront | Pay-per-use | < $1 |
| **Total** | | **~$30/month** |

---

## Deployment History

| Date | Action | Script |
|---|---|---|
| Feb 25, 2026 | RDS provisioned, 484,997 rows migrated | `01`, `02`, `03` |
| Feb 25, 2026 | Elastic Beanstalk deployed (24/24 API tests) | `04`, `05` |
| Feb 25, 2026 | CloudFront + S3 deployed (19/19 E2E tests) | `06`, `07` |
| Feb 25, 2026 | CI/CD + CloudWatch monitoring (20/20 tests) | `08`, `09` |
