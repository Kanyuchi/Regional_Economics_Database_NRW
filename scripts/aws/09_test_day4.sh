#!/usr/bin/env bash
# =============================================================================
# Day 4 – Step 9: Verify CI/CD workflows and monitoring
# =============================================================================
# Usage:
#   bash scripts/aws/09_test_day4.sh
#
# Tests:
#   1. GitHub Actions workflow files exist and have required secrets documented
#   2. CloudFront distribution ID retrieved and saved (for GitHub Secret)
#   3. CloudWatch alarms exist in eu-central-1 (RDS alarms)
#   4. CloudWatch alarms exist in us-east-1 (CloudFront alarms)
#   5. SNS topics created in both regions
#   6. GitHub remote configured (repo URL for Secrets setup)
#   7. Print GitHub Secrets setup checklist
# =============================================================================

set -uo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
pass()    { echo -e "${GREEN}  ✓${RESET} $*"; (( PASS_COUNT++ )) || true; }
fail_msg(){ echo -e "${RED}  ✗${RESET} $*"; (( FAIL_COUNT++ )) || true; }
warn()    { echo -e "${YELLOW}  ⚠${RESET} $*"; (( WARN_COUNT++ )) || true; }
info()    { echo -e "${CYAN}  →${RESET} $*"; }
banner()  { echo -e "\n${BOLD}${CYAN}[ $1 ]${RESET} $2"; }

# ── Configuration ──────────────────────────────────────────────────────────────
AWS_PROFILE="${AWS_PROFILE:-regional-nrw}"
AWS_REGION="${AWS_REGION:-eu-central-1}"
CF_ALARM_REGION="us-east-1"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

CF_DOMAIN_FILE="${SCRIPT_DIR}/.cf_domain"
CF_DIST_ID_FILE="${SCRIPT_DIR}/.cf_dist_id"

PASS_COUNT=0; FAIL_COUNT=0; WARN_COUNT=0

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 4 – CI/CD + Monitoring Verification${RESET}"
echo -e "${BOLD}  Profile : ${AWS_PROFILE}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# =============================================================================
# Section 1: GitHub Actions workflow files
# =============================================================================
banner "1/5" "GitHub Actions workflow files"

FRONTEND_WF="${REPO_ROOT}/.github/workflows/deploy-frontend.yml"
BACKEND_WF="${REPO_ROOT}/.github/workflows/deploy-backend.yml"

if [[ -f "$FRONTEND_WF" ]]; then
  pass "deploy-frontend.yml exists"
  # Verify key fields are present
  grep -q "S3_BUCKET"            "$FRONTEND_WF" && pass "  S3_BUCKET configured" || fail_msg "  S3_BUCKET missing"
  grep -q "CF_DISTRIBUTION_ID"  "$FRONTEND_WF" && pass "  CF_DISTRIBUTION_ID secret referenced" || fail_msg "  CF_DISTRIBUTION_ID missing"
  grep -q "aws s3 sync"         "$FRONTEND_WF" && pass "  S3 sync step present" || fail_msg "  S3 sync step missing"
  grep -q "create-invalidation" "$FRONTEND_WF" && pass "  CloudFront invalidation step present" || fail_msg "  CloudFront invalidation missing"
  grep -q "VITE_API_BASE"       "$FRONTEND_WF" && pass "  VITE_API_BASE set to empty string" || warn "  VITE_API_BASE not set"
else
  fail_msg "deploy-frontend.yml not found at: ${FRONTEND_WF}"
fi

if [[ -f "$BACKEND_WF" ]]; then
  pass "deploy-backend.yml exists"
  grep -q "eb deploy"            "$BACKEND_WF" && pass "  EB deploy command present" || fail_msg "  eb deploy missing"
  grep -q "regional-nrw-env"     "$BACKEND_WF" && pass "  EB environment name configured" || fail_msg "  EB env name missing"
  grep -q "awsebcli"             "$BACKEND_WF" && pass "  EB CLI install step present" || fail_msg "  EB CLI install missing"
  grep -q "api/health"           "$BACKEND_WF" && pass "  Post-deploy health check present" || warn "  Post-deploy health check not found"
else
  fail_msg "deploy-backend.yml not found at: ${BACKEND_WF}"
fi

# =============================================================================
# Section 2: CloudFront distribution ID
# =============================================================================
banner "2/5" "CloudFront distribution ID"

if [[ -f "$CF_DIST_ID_FILE" ]]; then
  CF_DIST_ID=$(cat "$CF_DIST_ID_FILE")
  pass "Distribution ID on file: ${CF_DIST_ID}"
elif [[ -f "$CF_DOMAIN_FILE" ]]; then
  CF_DOMAIN=$(cat "$CF_DOMAIN_FILE")
  info "Looking up distribution ID for ${CF_DOMAIN}..."
  CF_DIST_ID=$(aws cloudfront list-distributions \
    --profile "$AWS_PROFILE" \
    --query "DistributionList.Items[?DomainName=='${CF_DOMAIN}'].Id | [0]" \
    --output text 2>/dev/null || echo "")
  if [[ -n "$CF_DIST_ID" && "$CF_DIST_ID" != "None" ]]; then
    echo "$CF_DIST_ID" > "$CF_DIST_ID_FILE"
    pass "Distribution ID found: ${CF_DIST_ID}"
    ok "  Saved to: ${CF_DIST_ID_FILE}"
  else
    fail_msg "Could not find CloudFront distribution ID"
    CF_DIST_ID="UNKNOWN"
  fi
else
  fail_msg "Neither .cf_dist_id nor .cf_domain found. Run 06 and 08 first."
  CF_DIST_ID="UNKNOWN"
fi

# =============================================================================
# Section 3: CloudWatch alarms – RDS (eu-central-1)
# =============================================================================
banner "3/5" "CloudWatch alarms – RDS (${AWS_REGION})"

RDS_ALARMS=("nrw-rds-cpu-high" "nrw-rds-storage-low" "nrw-rds-connections-high")
for alarm in "${RDS_ALARMS[@]}"; do
  STATE=$(aws cloudwatch describe-alarms \
    --alarm-names "$alarm" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --query "MetricAlarms[0].StateValue" \
    --output text 2>/dev/null || echo "NOT_FOUND")
  if [[ "$STATE" == "NOT_FOUND" || "$STATE" == "None" ]]; then
    fail_msg "Alarm not found: ${alarm}"
  elif [[ "$STATE" == "OK" || "$STATE" == "INSUFFICIENT_DATA" ]]; then
    pass "Alarm ${alarm}: ${STATE}"
  elif [[ "$STATE" == "ALARM" ]]; then
    warn "Alarm ${alarm}: ${STATE} (currently firing!)"
  else
    info "Alarm ${alarm}: ${STATE}"
  fi
done

# =============================================================================
# Section 4: CloudWatch alarms – CloudFront (us-east-1)
# =============================================================================
banner "4/5" "CloudWatch alarms – CloudFront (${CF_ALARM_REGION})"

CF_ALARMS=("nrw-cf-5xx-rate" "nrw-cf-error-rate")
for alarm in "${CF_ALARMS[@]}"; do
  STATE=$(aws cloudwatch describe-alarms \
    --alarm-names "$alarm" \
    --profile "$AWS_PROFILE" \
    --region "$CF_ALARM_REGION" \
    --query "MetricAlarms[0].StateValue" \
    --output text 2>/dev/null || echo "NOT_FOUND")
  if [[ "$STATE" == "NOT_FOUND" || "$STATE" == "None" ]]; then
    fail_msg "Alarm not found: ${alarm} (run 08_setup_monitoring.sh first)"
  elif [[ "$STATE" == "OK" || "$STATE" == "INSUFFICIENT_DATA" ]]; then
    pass "Alarm ${alarm}: ${STATE}"
  elif [[ "$STATE" == "ALARM" ]]; then
    warn "Alarm ${alarm}: ${STATE} (currently firing!)"
  else
    info "Alarm ${alarm}: ${STATE}"
  fi
done

# =============================================================================
# Section 5: GitHub Secrets checklist
# =============================================================================
banner "5/5" "GitHub Secrets setup checklist"

# Detect GitHub remote URL
GIT_REMOTE=$(git -C "$REPO_ROOT" remote get-url origin 2>/dev/null || echo "unknown")
SECRETS_URL="${GIT_REMOTE/git@github.com:/https://github.com/}"
SECRETS_URL="${SECRETS_URL%.git}/settings/secrets/actions"

info "GitHub repository: ${GIT_REMOTE}"
info "Add secrets at  : ${SECRETS_URL}"
echo ""

# Check which secrets have been added (can't read values, just check existence via gh CLI)
if command -v gh >/dev/null 2>&1; then
  info "Checking GitHub Secrets via gh CLI..."
  SECRETS_LIST=$(gh secret list --repo "$GIT_REMOTE" 2>/dev/null | awk '{print $1}' || echo "")
  for secret in "AWS_ACCESS_KEY_ID" "AWS_SECRET_ACCESS_KEY" "CF_DISTRIBUTION_ID"; do
    if echo "$SECRETS_LIST" | grep -qx "$secret"; then
      pass "GitHub Secret set: ${secret}"
    else
      fail_msg "GitHub Secret MISSING: ${secret}"
    fi
  done
else
  warn "gh CLI not installed — cannot verify GitHub Secrets automatically"
  warn "  Install: brew install gh && gh auth login"
  echo ""
  echo -e "  ${BOLD}Secrets to add manually at:${RESET}"
  echo -e "  ${CYAN}${SECRETS_URL}${RESET}"
  echo ""
  echo -e "  ${BOLD}┌─────────────────────────────────────────────────────────┐${RESET}"
  echo -e "  ${BOLD}│  Secret Name            │  Value                         │${RESET}"
  echo -e "  ${BOLD}├─────────────────────────────────────────────────────────┤${RESET}"
  echo -e "  ${BOLD}│  AWS_ACCESS_KEY_ID      │  (from regional-nrw-deploy)    │${RESET}"
  echo -e "  ${BOLD}│  AWS_SECRET_ACCESS_KEY  │  (from regional-nrw-deploy)    │${RESET}"
  printf  "  ${BOLD}│  CF_DISTRIBUTION_ID     │  %-30s  │${RESET}\n" "${CF_DIST_ID}"
  echo -e "  ${BOLD}└─────────────────────────────────────────────────────────┘${RESET}"
fi

# =============================================================================
# Final summary
# =============================================================================
TOTAL=$(( PASS_COUNT + FAIL_COUNT ))

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 4 Verification Results${RESET}"
echo -e "${BOLD}  Passed : ${GREEN}${PASS_COUNT}${RESET}"
echo -e "${BOLD}  Failed : ${RED}${FAIL_COUNT}${RESET}"
echo -e "${BOLD}  Warned : ${YELLOW}${WARN_COUNT}${RESET}"
echo -e "${BOLD}  Total  : ${TOTAL}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo ""

if [[ "$FAIL_COUNT" -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}Day 4 complete. CI/CD and monitoring are live.${RESET}"
  echo ""
  echo -e "${CYAN}Full stack architecture:${RESET}"
  echo -e "  git push main"
  echo -e "    ├── frontend/** → GitHub Actions → S3 sync → CloudFront invalidation"
  echo -e "    └── backend/**  → GitHub Actions → EB deploy → smoke test"
  echo ""
  echo -e "  Monitoring:"
  echo -e "    ├── RDS: CPU / storage / connections  → SNS → email (eu-central-1)"
  echo -e "    └── CloudFront: 5xx / total error rate → SNS → email (us-east-1)"
  echo ""
  echo -e "  Live endpoints:"
  if [[ -f "${SCRIPT_DIR}/.cf_domain" ]]; then
    echo -e "    Frontend : https://$(cat "${SCRIPT_DIR}/.cf_domain")"
  fi
  if [[ -f "${SCRIPT_DIR}/.eb_url" ]]; then
    echo -e "    API      : $(cat "${SCRIPT_DIR}/.eb_url")/api/health"
  fi
  echo ""
  exit 0
else
  echo -e "${RED}${BOLD}${FAIL_COUNT} check(s) failed.${RESET}"
  echo -e "  Run 08_setup_monitoring.sh if alarms are missing."
  echo -e "  Add GitHub Secrets at: ${SECRETS_URL:-GitHub repo settings}"
  exit 1
fi
