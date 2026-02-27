#!/usr/bin/env bash
# =============================================================================
# Day 4 – Step 8: CloudWatch Alarms + SNS Notifications
# =============================================================================
# Usage:
#   bash scripts/aws/08_setup_monitoring.sh
#   # Optional: set alert email before running
#   ALERT_EMAIL="you@example.com" bash scripts/aws/08_setup_monitoring.sh
#
# What this creates:
#   SNS topic  : regional-nrw-alerts     (eu-central-1 – for RDS alarms)
#   SNS topic  : regional-nrw-alerts-cf  (us-east-1    – for CloudFront alarms)
#   CloudWatch Alarms:
#     [eu-central-1]
#       nrw-rds-cpu-high          – RDS CPUUtilization > 80% for 3 periods
#       nrw-rds-storage-low       – RDS FreeStorageSpace < 5 GB
#       nrw-rds-connections-high  – RDS DatabaseConnections > 80
#     [us-east-1 – CloudFront metrics always live here]
#       nrw-cf-5xx-rate           – CloudFront 5xxErrorRate > 1%
#       nrw-cf-error-rate         – CloudFront TotalErrorRate > 5%
#
# Outputs:
#   scripts/aws/.cf_dist_id  – CloudFront distribution ID (needed for GitHub Secret)
# =============================================================================

set -uo pipefail

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()     { echo -e "${GREEN}  ✓${RESET} $*"; }
info()   { echo -e "${CYAN}  →${RESET} $*"; }
warn()   { echo -e "${YELLOW}  ⚠${RESET} $*"; }
fail()   { echo -e "${RED}  ✗ ERROR:${RESET} $*" >&2; exit 1; }
banner() { echo -e "\n${BOLD}${CYAN}[ $1 ]${RESET} $2"; }

# ── Configuration ──────────────────────────────────────────────────────────────
AWS_PROFILE="${AWS_PROFILE:-regional-nrw}"
AWS_REGION="${AWS_REGION:-eu-central-1}"
ALERT_EMAIL="${ALERT_EMAIL:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CF_DOMAIN_FILE="${SCRIPT_DIR}/.cf_domain"
CF_DIST_ID_FILE="${SCRIPT_DIR}/.cf_dist_id"

RDS_INSTANCE="regional-economics-db"
SNS_TOPIC_NAME="regional-nrw-alerts"
SNS_TOPIC_NAME_CF="regional-nrw-alerts-cf"

# CloudFront metrics are only published in us-east-1 — alarms must live there
CF_ALARM_REGION="us-east-1"

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 4 – CloudWatch Monitoring Setup${RESET}"
echo -e "${BOLD}  Profile : ${AWS_PROFILE}${RESET}"
echo -e "${BOLD}  Region  : ${AWS_REGION} (RDS) + ${CF_ALARM_REGION} (CloudFront)${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Step 0: Prerequisite checks ────────────────────────────────────────────────
banner "0/4" "Checking prerequisites"

command -v aws >/dev/null 2>&1 || fail "AWS CLI not found"
command -v jq  >/dev/null 2>&1 || fail "jq not found. Install: brew install jq"

ACCOUNT_ID=$(aws sts get-caller-identity \
  --profile "$AWS_PROFILE" --query Account --output text 2>/dev/null) || \
  fail "AWS profile '${AWS_PROFILE}' not authenticated. Run: aws configure --profile regional-nrw"
ok "Authenticated as account: ${ACCOUNT_ID} (profile: ${AWS_PROFILE})"

[[ -f "$CF_DOMAIN_FILE" ]] || fail "CloudFront domain file not found: ${CF_DOMAIN_FILE}. Run 06_deploy_frontend_cdn.sh first."
CF_DOMAIN=$(cat "$CF_DOMAIN_FILE")
ok "CloudFront domain: ${CF_DOMAIN}"

# ── Step 1: Look up CloudFront distribution ID ─────────────────────────────────
banner "1/4" "Looking up CloudFront distribution ID"

CF_DIST_ID=$(aws cloudfront list-distributions \
  --profile "$AWS_PROFILE" \
  --query "DistributionList.Items[?DomainName=='${CF_DOMAIN}'].Id | [0]" \
  --output text 2>/dev/null || echo "")

if [[ -z "$CF_DIST_ID" || "$CF_DIST_ID" == "None" ]]; then
  fail "Could not find CloudFront distribution for domain '${CF_DOMAIN}'"
fi

echo "$CF_DIST_ID" > "$CF_DIST_ID_FILE"
ok "Distribution ID: ${CF_DIST_ID}"
ok "Saved to: ${CF_DIST_ID_FILE}"
info "  ★ Add this as GitHub Secret CF_DISTRIBUTION_ID"

# ── Step 2: Create SNS topics ──────────────────────────────────────────────────
banner "2/4" "Creating SNS topics for alarm notifications"

# Check SNS permissions first
SNS_PERM_CHECK=$(aws sns list-topics \
  --profile "$AWS_PROFILE" --region "$AWS_REGION" \
  --output text 2>&1) && SNS_OK=true || SNS_OK=false

if [[ "$SNS_OK" == "false" ]]; then
  warn "SNS permission denied — alarms will be created WITHOUT email notifications"
  warn "  To enable email alerts, add this inline policy to regional-nrw-deploy in IAM:"
  warn "  IAM → Users → regional-nrw-deploy → Add permissions → Attach policies directly"
  warn "  Policy: AmazonSNSFullAccess  (or inline: sns:CreateTopic, sns:Subscribe, sns:ListTopics)"
  warn "  Then re-run this script to add notifications to the alarms."
  SNS_ARN=""
  SNS_ARN_CF=""
else
  # eu-central-1 topic (RDS alarms)
  SNS_ARN=$(aws sns create-topic \
    --name "$SNS_TOPIC_NAME" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION" \
    --query 'TopicArn' --output text 2>/dev/null || echo "")
  if [[ -n "$SNS_ARN" ]]; then
    ok "SNS topic (RDS): ${SNS_ARN}"
  else
    warn "SNS topic creation failed for eu-central-1"
    SNS_ARN=""
  fi

  # us-east-1 topic (CloudFront alarms)
  SNS_ARN_CF=$(aws sns create-topic \
    --name "$SNS_TOPIC_NAME_CF" \
    --profile "$AWS_PROFILE" \
    --region "$CF_ALARM_REGION" \
    --query 'TopicArn' --output text 2>/dev/null || echo "")
  if [[ -n "$SNS_ARN_CF" ]]; then
    ok "SNS topic (CloudFront): ${SNS_ARN_CF}"
  else
    warn "SNS topic creation failed for us-east-1"
    SNS_ARN_CF=""
  fi

  # Subscribe email if provided
  if [[ -n "$ALERT_EMAIL" ]]; then
    [[ -n "$SNS_ARN" ]] && aws sns subscribe \
      --topic-arn "$SNS_ARN" --protocol email \
      --notification-endpoint "$ALERT_EMAIL" \
      --profile "$AWS_PROFILE" --region "$AWS_REGION" \
      --output text >/dev/null
    [[ -n "$SNS_ARN_CF" ]] && aws sns subscribe \
      --topic-arn "$SNS_ARN_CF" --protocol email \
      --notification-endpoint "$ALERT_EMAIL" \
      --profile "$AWS_PROFILE" --region "$CF_ALARM_REGION" \
      --output text >/dev/null
    ok "Email subscription sent to: ${ALERT_EMAIL}"
    warn "  Check your inbox and click 'Confirm subscription' in BOTH confirmation emails"
  else
    warn "ALERT_EMAIL not set — no email notifications will be sent."
    warn "  Re-run with: ALERT_EMAIL=you@example.com bash $0"
  fi
fi

# ── Helper: build alarm action args only when SNS ARN is non-empty ──────────────
# AWS CLI rejects empty --alarm-actions/--ok-actions with a client-side error.
alarm_action_args() {
  local arn="$1"
  if [[ -n "$arn" ]]; then
    echo "--alarm-actions ${arn} --ok-actions ${arn}"
  fi
}

put_alarm() {
  local alarm_name="$1"; local description="$2"; local namespace="$3"
  local metric="$4"; local dims="$5"; local period="$6"
  local eval_periods="$7"; local threshold="$8"; local operator="$9"
  local region="${10}"; local sns_arn="${11}"
  local action_args
  action_args=$(alarm_action_args "$sns_arn")

  # shellcheck disable=SC2086
  if aws cloudwatch put-metric-alarm \
    --alarm-name "$alarm_name" \
    --alarm-description "$description" \
    --namespace "$namespace" \
    --metric-name "$metric" \
    --dimensions $dims \
    --statistic "Average" \
    --period "$period" \
    --evaluation-periods "$eval_periods" \
    --threshold "$threshold" \
    --comparison-operator "$operator" \
    --treat-missing-data "notBreaching" \
    $action_args \
    --profile "$AWS_PROFILE" \
    --region "$region" 2>&1; then
    ok "Alarm created: ${alarm_name}"
  else
    warn "Alarm creation failed: ${alarm_name} — may need cloudwatch:PutMetricAlarm permission"
    warn "  IAM → Users → regional-nrw-deploy → Add permissions → CloudWatchFullAccess"
  fi
}

# ── Step 3: RDS CloudWatch Alarms (eu-central-1) ───────────────────────────────
banner "3/4" "Creating RDS CloudWatch alarms (eu-central-1)"

# Alarm 1: CPU utilisation > 80% for 15 consecutive minutes (3 × 5-min periods)
put_alarm \
  "nrw-rds-cpu-high" \
  "RDS CPU utilisation above 80% for 15 minutes" \
  "AWS/RDS" "CPUUtilization" \
  "Name=DBInstanceIdentifier,Value=${RDS_INSTANCE}" \
  300 3 80 "GreaterThanThreshold" \
  "$AWS_REGION" "$SNS_ARN"

# Alarm 2: Free storage < 5 GB (5368709120 bytes)
put_alarm \
  "nrw-rds-storage-low" \
  "RDS free storage below 5 GB — consider resizing volume" \
  "AWS/RDS" "FreeStorageSpace" \
  "Name=DBInstanceIdentifier,Value=${RDS_INSTANCE}" \
  300 1 5368709120 "LessThanThreshold" \
  "$AWS_REGION" "$SNS_ARN"

# Alarm 3: Connection count > 80 (db.t3.micro max_connections ≈ 87)
put_alarm \
  "nrw-rds-connections-high" \
  "RDS connection count near limit — db.t3.micro max ~87" \
  "AWS/RDS" "DatabaseConnections" \
  "Name=DBInstanceIdentifier,Value=${RDS_INSTANCE}" \
  60 5 80 "GreaterThanThreshold" \
  "$AWS_REGION" "$SNS_ARN"

# ── Step 4: CloudFront CloudWatch Alarms (us-east-1) ───────────────────────────
banner "4/4" "Creating CloudFront alarms (us-east-1 — where CF metrics live)"
info "CloudFront publishes all metrics to us-east-1 regardless of distribution region"

# Alarm 4: CloudFront 5xx error rate > 1%
put_alarm \
  "nrw-cf-5xx-rate" \
  "CloudFront server error rate above 1% — backend may be down" \
  "AWS/CloudFront" "5xxErrorRate" \
  "Name=DistributionId,Value=${CF_DIST_ID} Name=Region,Value=Global" \
  300 2 1 "GreaterThanThreshold" \
  "$CF_ALARM_REGION" "$SNS_ARN_CF"

# Alarm 5: CloudFront total error rate > 5%
put_alarm \
  "nrw-cf-error-rate" \
  "CloudFront total error rate (4xx+5xx) above 5%" \
  "AWS/CloudFront" "TotalErrorRate" \
  "Name=DistributionId,Value=${CF_DIST_ID} Name=Region,Value=Global" \
  300 2 5 "GreaterThanThreshold" \
  "$CF_ALARM_REGION" "$SNS_ARN_CF"

# ── Summary ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Monitoring Setup Complete${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${GREEN}  Alarms created:${RESET}"
echo "    [eu-central-1]"
echo "      nrw-rds-cpu-high         – CPU > 80% for 15 min"
echo "      nrw-rds-storage-low      – Free disk < 5 GB"
echo "      nrw-rds-connections-high – Connections > 80"
echo "    [us-east-1]"
echo "      nrw-cf-5xx-rate          – 5xx error rate > 1%"
echo "      nrw-cf-error-rate        – Total error rate > 5%"
echo ""
echo -e "${BOLD}  CloudFront Distribution ID: ${CYAN}${CF_DIST_ID}${RESET}"
echo -e "  ${YELLOW}★ Add as GitHub Secret:${RESET} CF_DISTRIBUTION_ID = ${CF_DIST_ID}"
echo ""
if [[ -n "$ALERT_EMAIL" ]]; then
  echo -e "${CYAN}  Check inbox for SNS confirmation emails from both regions.${RESET}"
fi
echo -e "  View alarms: https://eu-central-1.console.aws.amazon.com/cloudwatch/home?region=eu-central-1#alarmsV2:"
echo -e "  View alarms: https://us-east-1.console.aws.amazon.com/cloudwatch/home?region=us-east-1#alarmsV2:"
echo ""
