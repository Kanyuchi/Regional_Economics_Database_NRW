#!/usr/bin/env bash
# =============================================================================
# Day 1 – Step 1: Provision AWS RDS PostgreSQL for regional_db
# =============================================================================
# Usage:
#   export RDS_MASTER_PASSWORD="YourSecurePassword123!"
#   bash scripts/aws/01_create_rds_instance.sh
#
# Required env vars:
#   RDS_MASTER_PASSWORD   – Master password for the new RDS instance
#
# Optional env vars (defaults shown):
#   AWS_REGION            – AWS region            (default: eu-central-1)
#   DB_INSTANCE_ID        – RDS instance ID        (default: regional-economics-db)
#   RDS_MASTER_USER       – Master username        (default: regional_admin)
#   INSTANCE_CLASS        – RDS instance class     (default: db.t3.micro)
#   PUBLICLY_ACCESSIBLE   – Expose public endpoint (default: true, for migration)
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
AWS_REGION="${AWS_REGION:-eu-central-1}"
DB_INSTANCE_ID="${DB_INSTANCE_ID:-regional-economics-db}"
DB_NAME="regional_db"
RDS_MASTER_USER="${RDS_MASTER_USER:-regional_admin}"
RDS_MASTER_PASSWORD="${RDS_MASTER_PASSWORD:?Error: RDS_MASTER_PASSWORD must be set}"
INSTANCE_CLASS="${INSTANCE_CLASS:-db.t3.micro}"
PG_VERSION="15"
PUBLICLY_ACCESSIBLE="${PUBLICLY_ACCESSIBLE:-true}"

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 1 – Provisioning RDS PostgreSQL (eu-central-1)${RESET}"
echo -e "${BOLD}  Instance : ${DB_INSTANCE_ID}${RESET}"
echo -e "${BOLD}  Class    : ${INSTANCE_CLASS} (~\$15/month)${RESET}"
echo -e "${BOLD}  Database : ${DB_NAME}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Prerequisite checks ────────────────────────────────────────────────────────
command -v aws  >/dev/null 2>&1 || fail "AWS CLI not found. Install: brew install awscli"
command -v jq   >/dev/null 2>&1 || fail "jq not found. Install: brew install jq"
aws sts get-caller-identity --region "$AWS_REGION" >/dev/null 2>&1 \
  || fail "AWS CLI not authenticated. Run: aws configure"
ok "AWS CLI authenticated"

# ── 1. Discover default VPC and subnets ────────────────────────────────────────
banner "1/6" "Discovering default VPC and subnets"

DEFAULT_VPC_ID=$(aws ec2 describe-vpcs \
  --filters "Name=isDefault,Values=true" \
  --region "$AWS_REGION" \
  --query 'Vpcs[0].VpcId' \
  --output text)

[[ "$DEFAULT_VPC_ID" == "None" || -z "$DEFAULT_VPC_ID" ]] && \
  fail "No default VPC found in $AWS_REGION. Create one or set VPC_ID manually."

ok "Default VPC: $DEFAULT_VPC_ID"

# Get at least 2 subnets across different AZs (RDS subnet group requirement)
SUBNET_IDS=$(aws ec2 describe-subnets \
  --filters "Name=vpc-id,Values=$DEFAULT_VPC_ID" "Name=default-for-az,Values=true" \
  --region "$AWS_REGION" \
  --query 'Subnets[*].SubnetId' \
  --output text | tr '\t' ' ')

SUBNET_COUNT=$(echo "$SUBNET_IDS" | wc -w | tr -d ' ')
[[ "$SUBNET_COUNT" -lt 2 ]] && \
  fail "Need at least 2 subnets across AZs. Found: $SUBNET_COUNT"

ok "Found $SUBNET_COUNT subnets: $SUBNET_IDS"

# ── 2. Create security group ───────────────────────────────────────────────────
banner "2/6" "Creating security group"

# Check if it already exists
EXISTING_SG=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=regional-db-sg" "Name=vpc-id,Values=$DEFAULT_VPC_ID" \
  --region "$AWS_REGION" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || echo "None")

if [[ "$EXISTING_SG" != "None" && -n "$EXISTING_SG" ]]; then
  warn "Security group 'regional-db-sg' already exists: $EXISTING_SG"
  SG_ID="$EXISTING_SG"
else
  SG_ID=$(aws ec2 create-security-group \
    --group-name "regional-db-sg" \
    --description "RDS PostgreSQL for Regional Economics DB - allow from dev + beanstalk" \
    --vpc-id "$DEFAULT_VPC_ID" \
    --region "$AWS_REGION" \
    --query 'GroupId' \
    --output text)
  ok "Security group created: $SG_ID"
fi

# Allow inbound 5432 from your current IP (for the migration phase)
YOUR_IP=$(curl -s --max-time 5 https://checkip.amazonaws.com || echo "")
if [[ -n "$YOUR_IP" ]]; then
  aws ec2 authorize-security-group-ingress \
    --group-id "$SG_ID" \
    --protocol tcp \
    --port 5432 \
    --cidr "${YOUR_IP}/32" \
    --region "$AWS_REGION" 2>/dev/null && \
    ok "Allowed inbound 5432 from your IP: ${YOUR_IP}/32" || \
    warn "Rule may already exist for ${YOUR_IP}/32 (skipping)"
else
  warn "Could not detect your IP. Add inbound 5432 rule manually for migration."
fi

# ── 3. Create subnet group ─────────────────────────────────────────────────────
banner "3/6" "Creating DB subnet group"

SUBNET_GROUP_EXISTS=$(aws rds describe-db-subnet-groups \
  --region "$AWS_REGION" \
  --query "DBSubnetGroups[?DBSubnetGroupName=='regional-db-subnet-group'].DBSubnetGroupName" \
  --output text 2>/dev/null || echo "")

if [[ -n "$SUBNET_GROUP_EXISTS" ]]; then
  warn "Subnet group 'regional-db-subnet-group' already exists, reusing."
else
  # Convert space-separated subnet IDs to the correct CLI format
  SUBNET_ARRAY=$(echo "$SUBNET_IDS" | tr ' ' '\n' | jq -R . | jq -s .)
  aws rds create-db-subnet-group \
    --db-subnet-group-name "regional-db-subnet-group" \
    --db-subnet-group-description "Subnet group for Regional Economics DB" \
    --subnet-ids $(echo "$SUBNET_IDS") \
    --region "$AWS_REGION" \
    --output text > /dev/null
  ok "Subnet group created: regional-db-subnet-group"
fi

# ── 4. Create parameter group ──────────────────────────────────────────────────
banner "4/6" "Creating DB parameter group"

PG_GROUP_EXISTS=$(aws rds describe-db-parameter-groups \
  --region "$AWS_REGION" \
  --query "DBParameterGroups[?DBParameterGroupName=='regional-db-pg15'].DBParameterGroupName" \
  --output text 2>/dev/null || echo "")

if [[ -n "$PG_GROUP_EXISTS" ]]; then
  warn "Parameter group 'regional-db-pg15' already exists, reusing."
else
  aws rds create-db-parameter-group \
    --db-parameter-group-name "regional-db-pg15" \
    --db-parameter-group-family "postgres15" \
    --description "Regional Economics DB - PostgreSQL 15 parameters" \
    --region "$AWS_REGION" \
    --output text > /dev/null
  ok "Parameter group 'regional-db-pg15' created"
fi

# ── 5. Create RDS instance ─────────────────────────────────────────────────────
banner "5/6" "Creating RDS instance (⏳ takes ~5 minutes)"

INSTANCE_EXISTS=$(aws rds describe-db-instances \
  --region "$AWS_REGION" \
  --query "DBInstances[?DBInstanceIdentifier=='${DB_INSTANCE_ID}'].DBInstanceIdentifier" \
  --output text 2>/dev/null || echo "")

if [[ -n "$INSTANCE_EXISTS" ]]; then
  warn "Instance '${DB_INSTANCE_ID}' already exists. Skipping creation."
else
  PA_FLAG="--publicly-accessible"
  [[ "$PUBLICLY_ACCESSIBLE" != "true" ]] && PA_FLAG="--no-publicly-accessible"

  aws rds create-db-instance \
    --db-instance-identifier "$DB_INSTANCE_ID" \
    --db-instance-class "$INSTANCE_CLASS" \
    --engine postgres \
    --engine-version "$PG_VERSION" \
    --master-username "$RDS_MASTER_USER" \
    --master-user-password "$RDS_MASTER_PASSWORD" \
    --db-name "$DB_NAME" \
    --allocated-storage 20 \
    --max-allocated-storage 100 \
    --storage-type gp3 \
    --vpc-security-group-ids "$SG_ID" \
    --db-subnet-group-name "regional-db-subnet-group" \
    --db-parameter-group-name "regional-db-pg15" \
    --backup-retention-period 7 \
    --preferred-backup-window "02:00-03:00" \
    --preferred-maintenance-window "sun:04:00-sun:05:00" \
    --no-multi-az \
    --deletion-protection \
    --enable-performance-insights \
    --performance-insights-retention-period 7 \
    --tags Key=Project,Value=RegionalEconomicsNRW Key=Environment,Value=Production \
    $PA_FLAG \
    --region "$AWS_REGION" \
    --output text > /dev/null

  ok "RDS provisioning started for '${DB_INSTANCE_ID}'"
  info "Waiting for instance to become available..."
  aws rds wait db-instance-available \
    --db-instance-identifier "$DB_INSTANCE_ID" \
    --region "$AWS_REGION"
fi

# ── 6. Retrieve and save endpoint ─────────────────────────────────────────────
banner "6/6" "Retrieving endpoint"

RDS_ENDPOINT=$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query 'DBInstances[0].Endpoint.Address' \
  --output text)

RDS_STATUS=$(aws rds describe-db-instances \
  --db-instance-identifier "$DB_INSTANCE_ID" \
  --region "$AWS_REGION" \
  --query 'DBInstances[0].DBInstanceStatus' \
  --output text)

# Save endpoint to file for use by the migration script
echo "$RDS_ENDPOINT" > "$(dirname "$0")/.rds_endpoint"

echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  ✓ RDS Instance READY${RESET}"
echo -e "${BOLD}  Status   : ${RDS_STATUS}${RESET}"
echo -e "${BOLD}  Endpoint : ${RDS_ENDPOINT}${RESET}"
echo -e "${BOLD}  Port     : 5432${RESET}"
echo -e "${BOLD}  Database : ${DB_NAME}${RESET}"
echo -e "${BOLD}  User     : ${RDS_MASTER_USER}${RESET}"
echo -e "${BOLD}  Region   : ${AWS_REGION}${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${YELLOW}Next step – run the migration:${RESET}"
echo -e "  export RDS_HOST=${RDS_ENDPOINT}"
echo -e "  export RDS_MASTER_PASSWORD='YourSecurePassword123!'"
echo -e "  bash scripts/aws/02_migrate_to_rds.sh"
echo ""
