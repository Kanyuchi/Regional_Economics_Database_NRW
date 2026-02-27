#!/usr/bin/env bash
# =============================================================================
# Day 2 – Step 4: Deploy Node.js backend to AWS Elastic Beanstalk
# =============================================================================
# Usage:
#   export DB_PASSWORD="YourSecurePassword123!"
#   export OPENAI_API_KEY="sk-..."
#   bash scripts/aws/04_deploy_to_beanstalk.sh
#
# Prerequisites (do these once in the AWS Console before running):
#   1. IAM → Users → Create user → name: regional-nrw-deploy
#   2. Attach policies:
#        AdministratorAccess-AWSElasticBeanstalk   ← replaces the deprecated
#        AmazonS3FullAccess                             AWSElasticBeanstalkFullAccess
#        AmazonEC2FullAccess
#   3. Create Access Key → add to ~/.aws/credentials as [regional-nrw]
#   4. pip3 install awsebcli && eb --version
#
# Required env vars:
#   DB_PASSWORD     – RDS master password (set in Day 1)
#   OPENAI_API_KEY  – OpenAI API key
#
# Optional env vars (defaults shown):
#   AWS_REGION      – AWS region                    (default: eu-central-1)
#   AWS_PROFILE     – AWS named profile             (default: regional-nrw)
#   EB_APP_NAME     – Elastic Beanstalk app name    (default: regional-nrw-api)
#   EB_ENV_NAME     – Elastic Beanstalk env name    (default: regional-nrw-env)
#   DB_HOST         – RDS endpoint (auto-read from .rds_endpoint if unset)
#   DB_USER         – RDS master user               (default: regional_admin)
#   DB_NAME         – RDS database name             (default: regional_db)
#   DB_PORT         – RDS port                      (default: 5432)
#   OPENAI_MODEL    – OpenAI model to use           (default: gpt-4o-mini)
#   CORS_ORIGINS    – Allowed CORS origins          (default: *)
# =============================================================================

set -euo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()     { echo -e "${GREEN}  ✓${RESET} $*"; }
info()   { echo -e "${CYAN}  →${RESET} $*"; }
warn()   { echo -e "${YELLOW}  ⚠${RESET} $*"; }
fail()   { echo -e "${RED}  ✗ ERROR:${RESET} $*" >&2; exit 1; }
banner() { echo -e "\n${BOLD}${CYAN}[ $1 ]${RESET} $2"; }

# ── Configuration ──────────────────────────────────────────────────────────────
AWS_REGION="${AWS_REGION:-eu-central-1}"
AWS_PROFILE="${AWS_PROFILE:-regional-nrw}"
EB_APP_NAME="${EB_APP_NAME:-regional-nrw-api}"
EB_ENV_NAME="${EB_ENV_NAME:-regional-nrw-env}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
BACKEND_DIR="${REPO_ROOT}/duisburg-web-application/backend"

# RDS connection (read endpoint saved by Day 1 script if DB_HOST not set)
RDS_ENDPOINT_FILE="${SCRIPT_DIR}/.rds_endpoint"
if [[ -z "${DB_HOST:-}" ]]; then
  [[ -f "$RDS_ENDPOINT_FILE" ]] || \
    fail "DB_HOST not set and ${RDS_ENDPOINT_FILE} not found. Run 01_create_rds_instance.sh first."
  DB_HOST="$(cat "$RDS_ENDPOINT_FILE")"
fi

DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-regional_db}"
DB_USER="${DB_USER:-regional_admin}"
DB_PASSWORD="${DB_PASSWORD:?Error: DB_PASSWORD must be set (RDS master password from Day 1)}"
DB_SSL="${DB_SSL:-true}"
OPENAI_API_KEY="${OPENAI_API_KEY:?Error: OPENAI_API_KEY must be set}"
OPENAI_MODEL="${OPENAI_MODEL:-gpt-4o-mini}"
CORS_ORIGINS="${CORS_ORIGINS:-*}"

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 2 – Deploying backend to AWS Elastic Beanstalk${RESET}"
echo -e "${BOLD}  App     : ${EB_APP_NAME}${RESET}"
echo -e "${BOLD}  Env     : ${EB_ENV_NAME}${RESET}"
echo -e "${BOLD}  Region  : ${AWS_REGION}${RESET}"
echo -e "${BOLD}  RDS     : ${DB_HOST}${RESET}"
echo -e "${BOLD}  Profile : ${AWS_PROFILE}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Prerequisite checks ────────────────────────────────────────────────────────
banner "0/6" "Checking prerequisites"

command -v aws  >/dev/null 2>&1 || fail "AWS CLI not found. Install: brew install awscli"
command -v eb   >/dev/null 2>&1 || fail "EB CLI not found. Install: pip3 install awsebcli"
command -v jq   >/dev/null 2>&1 || fail "jq not found. Install: brew install jq"

aws sts get-caller-identity \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  >/dev/null 2>&1 || fail "AWS profile '${AWS_PROFILE}' not authenticated. Check ~/.aws/credentials"

ok "AWS CLI authenticated (profile: ${AWS_PROFILE})"

EB_VERSION=$(eb --version 2>&1 | head -1)
ok "EB CLI ready: ${EB_VERSION}"

[[ -d "$BACKEND_DIR" ]] || fail "Backend directory not found: ${BACKEND_DIR}"
ok "Backend directory: ${BACKEND_DIR}"

# ── 1. eb init: Register the EB application ────────────────────────────────────
banner "1/6" "Initialising Elastic Beanstalk application"

cd "$BACKEND_DIR"

if [[ -f ".elasticbeanstalk/config.yml" ]]; then
  warn ".elasticbeanstalk/config.yml already exists – skipping init (delete it to re-init)"
else
  # Write config.yml directly — avoids eb init interactive prompts (no --no-interactive flag in v3.26)
  mkdir -p .elasticbeanstalk
  cat > .elasticbeanstalk/config.yml <<EBCFG
branch-defaults:
  default:
    environment: ${EB_ENV_NAME}
    group_suffix: null
global:
  application_name: ${EB_APP_NAME}
  branch: null
  default_ec2_keyname: null
  default_platform: Node.js 20 running on 64bit Amazon Linux 2023
  default_region: ${AWS_REGION}
  include_git_submodules: true
  instance_profile: null
  platform_name: null
  platform_version: null
  profile: ${AWS_PROFILE}
  repository: null
  sc: null
  workspace_type: Application
EBCFG
  ok "Application '${EB_APP_NAME}' registered in ${AWS_REGION} (config.yml written directly)"
fi

# ── 2. eb create: Provision the environment ────────────────────────────────────
banner "2/6" "Provisioning Elastic Beanstalk environment (⏳ ~5 minutes)"

EXISTING_ENV=$(aws elasticbeanstalk describe-environments \
  --application-name "$EB_APP_NAME" \
  --environment-names "$EB_ENV_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query 'Environments[?Status!=`Terminated`].EnvironmentName' \
  --output text 2>/dev/null || echo "")

if [[ -n "$EXISTING_ENV" ]]; then
  warn "Environment '${EB_ENV_NAME}' already exists – skipping eb create"
  warn "To redeploy code, run: eb deploy ${EB_ENV_NAME} --profile ${AWS_PROFILE}"
else
  eb create "$EB_ENV_NAME" \
    --instance-type t3.small \
    --single \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE"
  ok "Environment '${EB_ENV_NAME}' provisioned"
fi

# ── 3. eb setenv: Inject secrets and config ─────────────────────────────────────
banner "3/6" "Injecting environment variables"

eb setenv \
  "DB_HOST=${DB_HOST}" \
  "DB_PORT=${DB_PORT}" \
  "DB_NAME=${DB_NAME}" \
  "DB_USER=${DB_USER}" \
  "DB_PASSWORD=${DB_PASSWORD}" \
  "DB_SSL=${DB_SSL}" \
  "OPENAI_API_KEY=${OPENAI_API_KEY}" \
  "OPENAI_MODEL=${OPENAI_MODEL}" \
  "CORS_ORIGINS=${CORS_ORIGINS}" \
  -e "$EB_ENV_NAME" \
  --profile "$AWS_PROFILE"

ok "Environment variables set (DB_PASSWORD and OPENAI_API_KEY redacted from logs)"

# ── 4. Update RDS security group to allow EB → RDS ───────────────────────────
banner "4/6" "Updating RDS security group to allow EB → RDS traffic"

cd "$SCRIPT_DIR"

# Find the RDS security group ('regional-db-sg' created in Day 1)
RDS_SG_ID=$(aws ec2 describe-security-groups \
  --filters "Name=group-name,Values=regional-db-sg" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query 'SecurityGroups[0].GroupId' \
  --output text 2>/dev/null || echo "")

if [[ -z "$RDS_SG_ID" || "$RDS_SG_ID" == "None" ]]; then
  warn "Could not find 'regional-db-sg'. You may need to add the EB SG manually."
  warn "Find the EB environment's security group in EC2 console and allow port 5432."
else
  ok "Found RDS security group: ${RDS_SG_ID}"

  # Find the EB environment's security group (tagged by EB)
  EB_SG_ID=$(aws ec2 describe-security-groups \
    --filters \
      "Name=tag:elasticbeanstalk:environment-name,Values=${EB_ENV_NAME}" \
    --region "$AWS_REGION" \
    --profile "$AWS_PROFILE" \
    --query 'SecurityGroups[0].GroupId' \
    --output text 2>/dev/null || echo "")

  # Fallback: look up via the EB instance if tag lookup returned nothing
  if [[ -z "$EB_SG_ID" || "$EB_SG_ID" == "None" ]]; then
    info "Tag lookup returned nothing – trying via EB instance..."
    INSTANCE_ID=$(aws elasticbeanstalk describe-environment-resources \
      --environment-name "$EB_ENV_NAME" \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE" \
      --query 'EnvironmentResources.Instances[0].Id' \
      --output text 2>/dev/null || echo "")

    if [[ -n "$INSTANCE_ID" && "$INSTANCE_ID" != "None" ]]; then
      EB_SG_ID=$(aws ec2 describe-instances \
        --instance-ids "$INSTANCE_ID" \
        --region "$AWS_REGION" \
        --profile "$AWS_PROFILE" \
        --query 'Reservations[0].Instances[0].SecurityGroups[0].GroupId' \
        --output text 2>/dev/null || echo "")
    fi
  fi

  if [[ -z "$EB_SG_ID" || "$EB_SG_ID" == "None" ]]; then
    warn "Could not detect EB security group. Add inbound 5432 rule to ${RDS_SG_ID} manually."
  else
    ok "Found EB security group: ${EB_SG_ID}"

    # Add inbound rule: allow 5432 from EB SG to RDS SG (idempotent – silences duplicate error)
    aws ec2 authorize-security-group-ingress \
      --group-id "$RDS_SG_ID" \
      --protocol tcp \
      --port 5432 \
      --source-group "$EB_SG_ID" \
      --region "$AWS_REGION" \
      --profile "$AWS_PROFILE" 2>/dev/null && \
      ok "Inbound rule added: ${RDS_SG_ID} allows 5432 from ${EB_SG_ID}" || \
      warn "Rule may already exist (skipping duplicate)"
  fi
fi

# ── 5. Retrieve the live URL ───────────────────────────────────────────────────
banner "5/6" "Retrieving live URL"

EB_URL=$(aws elasticbeanstalk describe-environments \
  --application-name "$EB_APP_NAME" \
  --environment-names "$EB_ENV_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query 'Environments[0].CNAME' \
  --output text 2>/dev/null || echo "")

EB_STATUS=$(aws elasticbeanstalk describe-environments \
  --application-name "$EB_APP_NAME" \
  --environment-names "$EB_ENV_NAME" \
  --region "$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --query 'Environments[0].Health' \
  --output text 2>/dev/null || echo "Unknown")

# Save URL for the test script
echo "http://${EB_URL}" > "${SCRIPT_DIR}/.eb_url"
ok "URL saved to ${SCRIPT_DIR}/.eb_url"

# ── 6. Summary ────────────────────────────────────────────────────────────────
banner "6/6" "Deployment complete"

echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  ✓ Elastic Beanstalk Deployment Complete${RESET}"
echo -e "${BOLD}  App     : ${EB_APP_NAME}${RESET}"
echo -e "${BOLD}  Env     : ${EB_ENV_NAME}${RESET}"
echo -e "${BOLD}  Status  : ${EB_STATUS}${RESET}"
echo -e "${BOLD}  URL     : http://${EB_URL}${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${CYAN}Next steps:${RESET}"
echo -e "  1. Smoke-test the live API:"
echo -e "     ${BOLD}bash scripts/aws/05_test_beanstalk_api.sh${RESET}"
echo -e ""
echo -e "  2. Verify health endpoint directly:"
echo -e "     ${BOLD}curl http://${EB_URL}/api/health${RESET}"
echo -e ""
echo -e "  3. View logs if health is Yellow/Red:"
echo -e "     ${BOLD}eb logs ${EB_ENV_NAME} --profile ${AWS_PROFILE}${RESET}"
echo -e ""
echo -e "  4. Redeploy after code changes:"
echo -e "     ${BOLD}cd ${BACKEND_DIR} && eb deploy ${EB_ENV_NAME} --profile ${AWS_PROFILE}${RESET}"
echo ""
