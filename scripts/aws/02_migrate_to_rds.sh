#!/usr/bin/env bash
# =============================================================================
# Day 1 – Step 2: Migrate local PostgreSQL → AWS RDS
# =============================================================================
# Usage:
#   export RDS_HOST="your-instance.xxxxx.eu-central-1.rds.amazonaws.com"
#   export RDS_MASTER_PASSWORD="YourSecurePassword123!"
#   bash scripts/aws/02_migrate_to_rds.sh
#
# Source DB env vars (from your existing local setup):
#   PG_POSTGRES_HOST     – Local DB host      (default: localhost)
#   PG_POSTGRES_PORT     – Local DB port      (default: 5432)
#   PG_POSTGRES_USER     – Local DB user      (default: postgres)
#   PG_POSTGRES_PASS     – Local DB password
#
# Target RDS env vars:
#   RDS_HOST             – RDS endpoint (required)
#   RDS_MASTER_USER      – RDS master user    (default: regional_admin)
#   RDS_MASTER_PASSWORD  – RDS master password (required)
#
# What this script does:
#   1. Verifies connectivity to both source and target DBs
#   2. Pre-creates PostGIS + uuid-ossp extensions on RDS
#      (must happen BEFORE restore because dim_geography uses GEOMETRY type)
#   3. pg_dump from local in custom (-Fc) format
#   4. pg_restore to RDS with --no-owner --no-acl
#   5. Prints record count summary
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()   { echo -e "${GREEN}  ✓${RESET} $*"; }
info() { echo -e "${CYAN}  →${RESET} $*"; }
warn() { echo -e "${YELLOW}  ⚠${RESET} $*"; }
fail() { echo -e "${RED}  ✗ ERROR:${RESET} $*" >&2; exit 1; }
banner() { echo -e "\n${BOLD}${CYAN}[ $1 ]${RESET} $2"; }

# ── Configuration ──────────────────────────────────────────────────────────────
# Source: local PostgreSQL (uses PG_POSTGRES_* vars from existing pipeline setup)
SRC_HOST="${PG_POSTGRES_HOST:-localhost}"
SRC_PORT="${PG_POSTGRES_PORT:-5432}"
SRC_DB="regional_db"
SRC_USER="${PG_POSTGRES_USER:-postgres}"
SRC_PASS="${PG_POSTGRES_PASS:-}"

# Target: AWS RDS
RDS_HOST="${RDS_HOST:?Error: RDS_HOST must be set (run 01_create_rds_instance.sh first)}"
RDS_PORT="5432"
RDS_DB="regional_db"
RDS_USER="${RDS_MASTER_USER:-regional_admin}"
RDS_PASS="${RDS_MASTER_PASSWORD:?Error: RDS_MASTER_PASSWORD must be set}"

DUMP_FILE="$(dirname "$0")/regional_db_backup_$(date +%Y%m%d_%H%M%S).dump"
DUMP_LOG="$(dirname "$0")/migration_$(date +%Y%m%d_%H%M%S).log"

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 1 – Migrating regional_db → AWS RDS${RESET}"
echo -e "${BOLD}  Source : ${SRC_HOST}:${SRC_PORT}/${SRC_DB}${RESET}"
echo -e "${BOLD}  Target : ${RDS_HOST}:${RDS_PORT}/${RDS_DB}${RESET}"
echo -e "${BOLD}  Dump   : ${DUMP_FILE}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Prerequisite checks ────────────────────────────────────────────────────────
command -v pg_dump    >/dev/null 2>&1 || fail "pg_dump not found. Install: brew install postgresql@15"
command -v pg_restore >/dev/null 2>&1 || fail "pg_restore not found. Install: brew install postgresql@15"
command -v psql       >/dev/null 2>&1 || fail "psql not found. Install: brew install postgresql@15"

# ── 1. Verify source connectivity ─────────────────────────────────────────────
banner "1/5" "Verifying source DB connectivity (local)"

PGPASSWORD="$SRC_PASS" psql \
  -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" \
  -c "SELECT 1" -q --no-align --tuples-only > /dev/null 2>&1 \
  || fail "Cannot connect to source DB at ${SRC_HOST}:${SRC_PORT}/${SRC_DB}"

SRC_TOTAL=$(PGPASSWORD="$SRC_PASS" psql \
  -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" \
  -c "SELECT SUM(n_live_tup) FROM pg_stat_user_tables" \
  -q --no-align --tuples-only 2>/dev/null || echo "unknown")

ok "Source connected. Approximate total rows: ${SRC_TOTAL}"

# ── 2. Verify target (RDS) connectivity ───────────────────────────────────────
banner "2/5" "Verifying target RDS connectivity"

PGPASSWORD="$RDS_PASS" psql \
  "host=$RDS_HOST port=$RDS_PORT dbname=$RDS_DB user=$RDS_USER sslmode=require" \
  -c "SELECT version()" -q --no-align --tuples-only > /dev/null 2>&1 \
  || fail "Cannot connect to RDS at ${RDS_HOST}. Check security group and endpoint."

RDS_VERSION=$(PGPASSWORD="$RDS_PASS" psql \
  "host=$RDS_HOST port=$RDS_PORT dbname=$RDS_DB user=$RDS_USER sslmode=require" \
  -c "SELECT version()" -q --no-align --tuples-only 2>/dev/null | head -1)

ok "RDS connected: $RDS_VERSION"

# ── 3. Pre-create extensions on RDS ──────────────────────────────────────────
# CRITICAL: dim_geography uses GEOMETRY(MULTIPOLYGON, 4326) – PostGIS MUST
# exist before pg_restore attempts to create that table. Without this step,
# the entire restore fails with "type geometry does not exist".
banner "3/5" "Creating extensions on RDS (PostGIS + uuid-ossp)"

PGPASSWORD="$RDS_PASS" psql \
  "host=$RDS_HOST port=$RDS_PORT dbname=$RDS_DB user=$RDS_USER sslmode=require" \
  -v ON_ERROR_STOP=1 \
  -c "CREATE EXTENSION IF NOT EXISTS postgis;" \
  -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" \
  -c "CREATE EXTENSION IF NOT EXISTS postgis_topology;" \
  > /dev/null 2>&1 && ok "Extensions created: postgis, uuid-ossp, postgis_topology" || \
  warn "Some extensions may not be available (postgis_topology is optional)"

# Verify PostGIS is working
POSTGIS_VER=$(PGPASSWORD="$RDS_PASS" psql \
  "host=$RDS_HOST port=$RDS_PORT dbname=$RDS_DB user=$RDS_USER sslmode=require" \
  -c "SELECT PostGIS_version()" -q --no-align --tuples-only 2>/dev/null || echo "unavailable")
ok "PostGIS version: $POSTGIS_VER"

# ── 4. pg_dump from source ─────────────────────────────────────────────────────
banner "4/5" "Running pg_dump from local PostgreSQL"
info "Format: custom (-Fc) | Flags: --no-owner --no-acl --schema=public"
info "Output: $DUMP_FILE"

PGPASSWORD="$SRC_PASS" pg_dump \
  -h "$SRC_HOST" \
  -p "$SRC_PORT" \
  -U "$SRC_USER" \
  -d "$SRC_DB" \
  --format=custom \
  --no-owner \
  --no-acl \
  --schema=public \
  --file="$DUMP_FILE" \
  --verbose 2>&1 | tee "$DUMP_LOG" | grep -E "(dumping|reading|saving|pg_dump)" | head -30

DUMP_SIZE=$(du -sh "$DUMP_FILE" 2>/dev/null | cut -f1 || echo "unknown")
ok "Dump complete: ${DUMP_FILE} (${DUMP_SIZE})"

# ── 5. pg_restore to RDS ──────────────────────────────────────────────────────
banner "5/5" "Running pg_restore to RDS"
info "Flags: --no-owner --no-acl -j 4 (parallel with 4 workers)"

RESTORE_LOG="${DUMP_LOG/.log/_restore.log}"

# pg_restore exits with code 1 even on partial success (some warnings are expected
# for things like role 'postgres' not existing). We capture the exit code separately.
PGPASSWORD="$RDS_PASS" pg_restore \
  -h "$RDS_HOST" \
  -p "$RDS_PORT" \
  -U "$RDS_USER" \
  -d "$RDS_DB" \
  --no-owner \
  --no-acl \
  -j 4 \
  --verbose \
  "$DUMP_FILE" 2>&1 | tee "$RESTORE_LOG" | grep -E "(creating|processing|setting|error|Error)" | head -50
RESTORE_EXIT=${PIPESTATUS[0]}

# Exit code 1 from pg_restore usually means non-fatal warnings (role not found etc.)
if [[ "$RESTORE_EXIT" -gt 1 ]]; then
  fail "pg_restore failed with exit code $RESTORE_EXIT. Check: $RESTORE_LOG"
fi

# Count errors in restore log (role-not-found warnings are acceptable)
FATAL_ERRORS=$(grep -i "ERROR:" "$RESTORE_LOG" | grep -v "role.*does not exist" | wc -l | tr -d ' ')
if [[ "$FATAL_ERRORS" -gt 0 ]]; then
  warn "Restore completed with $FATAL_ERRORS potential errors. Review: $RESTORE_LOG"
else
  ok "Restore completed cleanly (role-not-found warnings are expected and safe)"
fi

# ── Post-migration record count summary ───────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}── Post-Migration Record Counts on RDS ─────────────────${RESET}"

TABLES=(
  "dim_geography"
  "dim_time"
  "dim_indicator"
  "dim_economic_sector"
  "fact_demographics"
  "fact_labor_market"
  "fact_business_economy"
  "fact_healthcare"
  "fact_public_finance"
  "fact_infrastructure"
  "fact_commuters"
)

TOTAL_RDS=0
for TABLE in "${TABLES[@]}"; do
  COUNT=$(PGPASSWORD="$RDS_PASS" psql \
    "host=$RDS_HOST port=$RDS_PORT dbname=$RDS_DB user=$RDS_USER sslmode=require" \
    -c "SELECT COUNT(*) FROM ${TABLE}" \
    -q --no-align --tuples-only 2>/dev/null || echo "ERROR")

  SRC_COUNT=$(PGPASSWORD="$SRC_PASS" psql \
    -h "$SRC_HOST" -p "$SRC_PORT" -U "$SRC_USER" -d "$SRC_DB" \
    -c "SELECT COUNT(*) FROM ${TABLE}" \
    -q --no-align --tuples-only 2>/dev/null || echo "ERROR")

  MATCH="✓"
  [[ "$COUNT" != "$SRC_COUNT" ]] && MATCH="✗ MISMATCH"

  printf "  %-35s  RDS: %-8s  Local: %-8s  %s\n" "$TABLE" "$COUNT" "$SRC_COUNT" "$MATCH"
  [[ "$COUNT" =~ ^[0-9]+$ ]] && TOTAL_RDS=$((TOTAL_RDS + COUNT))
done

echo ""
echo -e "${BOLD}  Total records on RDS: ${TOTAL_RDS}${RESET}"
echo -e "\n${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  ✓ Migration complete${RESET}"
echo -e "${BOLD}  Dump file   : ${DUMP_FILE}${RESET}"
echo -e "${BOLD}  Restore log : ${RESTORE_LOG}${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${YELLOW}Next step – run verification tests:${RESET}"
echo -e "  export RDS_HOST=${RDS_HOST}"
echo -e "  export RDS_MASTER_PASSWORD='...'"
echo -e "  python3 scripts/aws/03_verify_migration.py"
echo ""
