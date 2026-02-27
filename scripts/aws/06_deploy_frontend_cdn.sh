#!/usr/bin/env bash
# =============================================================================
# Day 3 – Step 6: Build React frontend and deploy to S3 + CloudFront CDN
# =============================================================================
# Usage:
#   bash scripts/aws/06_deploy_frontend_cdn.sh
#
# Prerequisites (one-time in AWS Console):
#   IAM → Users → regional-nrw-deploy → Add permissions:
#     CloudFrontFullAccess
#   (The S3 + EB permissions from Day 2 remain in place)
#
# What this script does:
#   1. Builds the React/Vite frontend for production
#      (VITE_API_BASE is intentionally left empty so the app uses
#       window.location.origin at runtime — CloudFront proxies /api/* to EB)
#   2. Creates a private S3 bucket for static assets
#   3. Creates a CloudFront Origin Access Control (OAC) for secure S3 access
#   4. Creates a CloudFront distribution with two origins:
#        Origin 1 (default): S3 bucket → React SPA (with SPA 404→index.html)
#        Origin 2 (/api/*):  EB backend → API proxy (no caching)
#   5. Updates the S3 bucket policy to allow only CloudFront to read it
#   6. Uploads dist/ to S3 with correct cache headers:
#        index.html    → no-cache (ensures users pick up new deploys)
#        assets/*      → immutable 1-year (content-hashed by Vite)
#   7. Waits for CloudFront distribution to finish deploying (~5-15 min)
#   8. Updates EB CORS_ORIGINS to the CloudFront HTTPS domain
#   9. Saves the live URL to scripts/aws/.cf_domain
#
# Optional env vars (defaults shown):
#   AWS_REGION    – AWS region            (default: eu-central-1)
#   AWS_PROFILE   – AWS named profile     (default: regional-nrw)
#   EB_ENV_NAME   – EB environment name   (default: regional-nrw-env)
#   EB_APP_NAME   – EB application name   (default: regional-nrw-api)
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
EB_ENV_NAME="${EB_ENV_NAME:-regional-nrw-env}"
EB_APP_NAME="${EB_APP_NAME:-regional-nrw-api}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
FRONTEND_DIR="${REPO_ROOT}/duisburg-web-application/frontend"
BACKEND_DIR="${REPO_ROOT}/duisburg-web-application/backend"

EB_URL="regional-nrw-env.eba-s26pyuip.eu-central-1.elasticbeanstalk.com"

ACCOUNT_ID=$(aws sts get-caller-identity \
  --profile "$AWS_PROFILE" --query Account --output text)
BUCKET_NAME="regional-nrw-frontend-${ACCOUNT_ID}"
CF_COMMENT="Regional Economics NRW Dashboard"
OAC_NAME="regional-nrw-s3-oac"
CF_DOMAIN_FILE="${SCRIPT_DIR}/.cf_domain"

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 3 – Frontend CDN Deployment (S3 + CloudFront)${RESET}"
echo -e "${BOLD}  Bucket   : ${BUCKET_NAME}${RESET}"
echo -e "${BOLD}  Region   : ${AWS_REGION}${RESET}"
echo -e "${BOLD}  Profile  : ${AWS_PROFILE}${RESET}"
echo -e "${BOLD}  EB Origin: ${EB_URL}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Step 0: Prerequisite checks ────────────────────────────────────────────────
banner "0/9" "Checking prerequisites"

command -v aws  >/dev/null 2>&1 || fail "AWS CLI not found"
command -v npm  >/dev/null 2>&1 || fail "npm not found. Install Node.js >=18"
command -v jq   >/dev/null 2>&1 || fail "jq not found. Install: brew install jq"

aws sts get-caller-identity \
  --profile "$AWS_PROFILE" >/dev/null 2>&1 || \
  fail "AWS profile '${AWS_PROFILE}' not authenticated"

# Verify CloudFront permissions
aws cloudfront list-distributions \
  --profile "$AWS_PROFILE" \
  --output text >/dev/null 2>&1 || \
  fail "CloudFront access denied. Add CloudFrontFullAccess policy to ${AWS_PROFILE} IAM user."

ok "AWS authenticated (profile: ${AWS_PROFILE}, account: ${ACCOUNT_ID})"
ok "CloudFront permissions confirmed"
[[ -d "$FRONTEND_DIR" ]] || fail "Frontend directory not found: ${FRONTEND_DIR}"
ok "Frontend directory: ${FRONTEND_DIR}"

# ── Step 1: Build React frontend ───────────────────────────────────────────────
banner "1/9" "Building React frontend for production"

cd "$FRONTEND_DIR"

info "Installing dependencies..."
npm install --silent 2>&1 | tail -5

info "Running production build (VITE_API_BASE='' → uses window.location.origin)..."
# Explicitly clear VITE_API_BASE so the app uses window.location.origin at runtime.
# CloudFront will proxy /api/* to EB, so the app just calls its own origin.
VITE_API_BASE="" npm run build 2>&1

[[ -f "dist/index.html" ]] || fail "Build failed: dist/index.html not found"
JS_COUNT=$(find dist/assets -name "*.js" 2>/dev/null | wc -l | tr -d ' ')
CSS_COUNT=$(find dist/assets -name "*.css" 2>/dev/null | wc -l | tr -d ' ')
ok "Build complete: dist/index.html + ${JS_COUNT} JS + ${CSS_COUNT} CSS chunks"

cd "$SCRIPT_DIR"

# ── Step 2: Create S3 bucket ───────────────────────────────────────────────────
banner "2/9" "Creating S3 bucket (private)"

CREATE_OUT=$(aws s3api create-bucket \
  --bucket "$BUCKET_NAME" \
  --region "$AWS_REGION" \
  --create-bucket-configuration LocationConstraint="$AWS_REGION" \
  --profile "$AWS_PROFILE" \
  --output text 2>&1) && \
  ok "Bucket created: s3://${BUCKET_NAME}" || {
    if echo "$CREATE_OUT" | grep -q "BucketAlreadyOwnedByYou\|BucketAlreadyExists"; then
      warn "Bucket '${BUCKET_NAME}' already exists – reusing"
    else
      fail "S3 bucket creation failed: ${CREATE_OUT}"
    fi
  }

# Enforce private access – CloudFront OAC provides the only read path
aws s3api put-public-access-block \
  --bucket "$BUCKET_NAME" \
  --public-access-block-configuration \
    BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true \
  --profile "$AWS_PROFILE"
ok "Public access blocked on bucket"

# ── Step 3: Create CloudFront Origin Access Control ────────────────────────────
banner "3/9" "Creating CloudFront Origin Access Control (OAC)"

EXISTING_OAC_ID=$(aws cloudfront list-origin-access-controls \
  --profile "$AWS_PROFILE" \
  --query "OriginAccessControlList.Items[?Name=='${OAC_NAME}'].Id" \
  --output text 2>/dev/null || echo "")

if [[ -n "$EXISTING_OAC_ID" && "$EXISTING_OAC_ID" != "None" ]]; then
  OAC_ID="$EXISTING_OAC_ID"
  warn "OAC '${OAC_NAME}' already exists: ${OAC_ID}"
else
  OAC_ID=$(aws cloudfront create-origin-access-control \
    --origin-access-control-config \
      "Name=${OAC_NAME},Description=S3 OAC for Regional NRW Frontend,OriginAccessControlOriginType=s3,SigningBehavior=always,SigningProtocol=sigv4" \
    --profile "$AWS_PROFILE" \
    --query 'OriginAccessControl.Id' \
    --output text)
  ok "OAC created: ${OAC_ID}"
fi

# ── Step 4: Create CloudFront distribution ─────────────────────────────────────
banner "4/9" "Creating CloudFront distribution (~2 min to initiate)"

# Check for existing distribution by comment
EXISTING_CF_DOMAIN=$(aws cloudfront list-distributions \
  --profile "$AWS_PROFILE" \
  --query "DistributionList.Items[?Comment=='${CF_COMMENT}'] | [0].DomainName" \
  --output text 2>/dev/null || echo "")

EXISTING_CF_ID=$(aws cloudfront list-distributions \
  --profile "$AWS_PROFILE" \
  --query "DistributionList.Items[?Comment=='${CF_COMMENT}'] | [0].Id" \
  --output text 2>/dev/null || echo "")

if [[ -n "$EXISTING_CF_DOMAIN" && "$EXISTING_CF_DOMAIN" != "None" ]]; then
  CF_DOMAIN="$EXISTING_CF_DOMAIN"
  CF_DIST_ID="$EXISTING_CF_ID"
  warn "CloudFront distribution already exists: https://${CF_DOMAIN}"
else
  # Write the full distribution config to a temp file
  CALLER_REF="regional-nrw-$(date +%s)"
  CF_CONFIG_FILE="/tmp/cf_config_$$.json"

  cat > "$CF_CONFIG_FILE" <<EOF
{
  "CallerReference": "${CALLER_REF}",
  "Comment": "${CF_COMMENT}",
  "DefaultRootObject": "index.html",
  "Origins": {
    "Quantity": 2,
    "Items": [
      {
        "Id": "S3Origin",
        "DomainName": "${BUCKET_NAME}.s3.${AWS_REGION}.amazonaws.com",
        "S3OriginConfig": { "OriginAccessIdentity": "" },
        "OriginAccessControlId": "${OAC_ID}"
      },
      {
        "Id": "EBOrigin",
        "DomainName": "${EB_URL}",
        "CustomOriginConfig": {
          "HTTPPort": 80,
          "HTTPSPort": 443,
          "OriginProtocolPolicy": "http-only",
          "OriginSslProtocols": { "Quantity": 1, "Items": ["TLSv1.2"] },
          "OriginReadTimeout": 30,
          "OriginKeepaliveTimeout": 5
        }
      }
    ]
  },
  "DefaultCacheBehavior": {
    "TargetOriginId": "S3Origin",
    "ViewerProtocolPolicy": "redirect-to-https",
    "AllowedMethods": {
      "Quantity": 2,
      "Items": ["GET", "HEAD"],
      "CachedMethods": { "Quantity": 2, "Items": ["GET", "HEAD"] }
    },
    "CachePolicyId": "658327ea-f89d-4fab-a63d-7e88639e58f6",
    "Compress": true,
    "FunctionAssociations": { "Quantity": 0 }
  },
  "CacheBehaviors": {
    "Quantity": 1,
    "Items": [
      {
        "PathPattern": "/api/*",
        "TargetOriginId": "EBOrigin",
        "ViewerProtocolPolicy": "https-only",
        "AllowedMethods": {
          "Quantity": 7,
          "Items": ["DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"],
          "CachedMethods": { "Quantity": 2, "Items": ["GET", "HEAD"] }
        },
        "CachePolicyId": "4135ea2d-6df8-44a3-9df3-4b5a84be39ad",
        "OriginRequestPolicyId": "b689b0a8-53d0-40ab-baf2-68738e2966ac",
        "Compress": true,
        "FunctionAssociations": { "Quantity": 0 }
      }
    ]
  },
  "CustomErrorResponses": {
    "Quantity": 2,
    "Items": [
      {
        "ErrorCode": 403,
        "ResponsePagePath": "/index.html",
        "ResponseCode": "200",
        "ErrorCachingMinTTL": 0
      },
      {
        "ErrorCode": 404,
        "ResponsePagePath": "/index.html",
        "ResponseCode": "200",
        "ErrorCachingMinTTL": 0
      }
    ]
  },
  "PriceClass": "PriceClass_100",
  "Enabled": true,
  "HttpVersion": "http2and3",
  "IsIPV6Enabled": true
}
EOF

  CF_CREATE_OUT=$(aws cloudfront create-distribution \
    --distribution-config "file://${CF_CONFIG_FILE}" \
    --profile "$AWS_PROFILE")
  rm -f "$CF_CONFIG_FILE"

  CF_DIST_ID=$(echo "$CF_CREATE_OUT" | jq -r '.Distribution.Id')
  CF_DOMAIN=$(echo  "$CF_CREATE_OUT" | jq -r '.Distribution.DomainName')
  ok "CloudFront distribution created: ${CF_DIST_ID}"
  info "Domain: https://${CF_DOMAIN}"
fi

echo "$CF_DOMAIN" > "$CF_DOMAIN_FILE"
ok "CloudFront domain saved to ${CF_DOMAIN_FILE}"

# ── Step 5: Update S3 bucket policy (allow only this CloudFront distribution) ──
banner "5/9" "Updating S3 bucket policy for CloudFront OAC"

BUCKET_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowCloudFrontOAC",
      "Effect": "Allow",
      "Principal": { "Service": "cloudfront.amazonaws.com" },
      "Action": "s3:GetObject",
      "Resource": "arn:aws:s3:::${BUCKET_NAME}/*",
      "Condition": {
        "StringEquals": {
          "AWS:SourceArn": "arn:aws:cloudfront::${ACCOUNT_ID}:distribution/${CF_DIST_ID}"
        }
      }
    }
  ]
}
EOF
)

aws s3api put-bucket-policy \
  --bucket "$BUCKET_NAME" \
  --policy "$BUCKET_POLICY" \
  --profile "$AWS_PROFILE"
ok "Bucket policy set – only CloudFront distribution ${CF_DIST_ID} can read objects"

# ── Step 6: Upload frontend files to S3 ───────────────────────────────────────
banner "6/9" "Uploading frontend dist/ to S3"

# Non-HTML assets: Vite content-hashes filenames → safe to cache for 1 year
info "Uploading assets (immutable cache)..."
aws s3 sync "${FRONTEND_DIR}/dist/" "s3://${BUCKET_NAME}/" \
  --delete \
  --exclude "*.html" \
  --cache-control "public, max-age=31536000, immutable" \
  --profile "$AWS_PROFILE"

# index.html: no-cache so users always get the freshest shell
info "Uploading index.html (no-cache)..."
aws s3 cp "${FRONTEND_DIR}/dist/index.html" "s3://${BUCKET_NAME}/index.html" \
  --cache-control "no-cache, no-store, must-revalidate" \
  --content-type "text/html; charset=utf-8" \
  --profile "$AWS_PROFILE"

OBJECT_COUNT=$(aws s3 ls "s3://${BUCKET_NAME}" --recursive \
  --profile "$AWS_PROFILE" | wc -l | tr -d ' ')
ok "Uploaded ${OBJECT_COUNT} files to s3://${BUCKET_NAME}"

# ── Step 7: Wait for CloudFront to finish deploying ────────────────────────────
banner "7/9" "Waiting for CloudFront distribution to deploy (up to 15 min)"

CF_STATUS=$(aws cloudfront get-distribution \
  --id "$CF_DIST_ID" \
  --profile "$AWS_PROFILE" \
  --query 'Distribution.Status' \
  --output text 2>/dev/null || echo "Unknown")

if [[ "$CF_STATUS" == "Deployed" ]]; then
  ok "Distribution already in Deployed state"
else
  info "Status: ${CF_STATUS} — waiting for Deployed state..."
  aws cloudfront wait distribution-deployed \
    --id "$CF_DIST_ID" \
    --profile "$AWS_PROFILE" 2>&1
  ok "CloudFront distribution is Deployed"
fi

# ── Step 8: Update EB CORS_ORIGINS to the CloudFront domain ───────────────────
banner "8/9" "Locking CORS_ORIGINS to CloudFront domain"

cd "$BACKEND_DIR"
eb setenv \
  "CORS_ORIGINS=https://${CF_DOMAIN}" \
  -e "$EB_ENV_NAME" \
  --profile "$AWS_PROFILE" 2>&1
ok "CORS_ORIGINS set to https://${CF_DOMAIN}"
cd "$SCRIPT_DIR"

# ── Step 9: Summary ────────────────────────────────────────────────────────────
banner "9/9" "Day 3 deployment complete"

echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}  ✓ Frontend CDN Live${RESET}"
echo -e "${BOLD}  CloudFront  : https://${CF_DOMAIN}${RESET}"
echo -e "${BOLD}  API via CDN : https://${CF_DOMAIN}/api/health${RESET}"
echo -e "${BOLD}  S3 Bucket   : s3://${BUCKET_NAME}${RESET}"
echo -e "${BOLD}  Distribution: ${CF_DIST_ID}${RESET}"
echo -e "${BOLD}  CORS locked : https://${CF_DOMAIN}${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${CYAN}Next step – run end-to-end tests:${RESET}"
echo -e "  ${BOLD}bash scripts/aws/07_test_end_to_end.sh${RESET}"
echo ""
echo -e "${CYAN}To redeploy frontend after code changes:${RESET}"
echo -e "  ${BOLD}VITE_API_BASE='' npm run build --prefix ${FRONTEND_DIR}${RESET}"
echo -e "  ${BOLD}aws s3 sync ${FRONTEND_DIR}/dist/ s3://${BUCKET_NAME}/ --delete --exclude '*.html' --cache-control 'public, max-age=31536000, immutable' --profile ${AWS_PROFILE}${RESET}"
echo -e "  ${BOLD}aws s3 cp ${FRONTEND_DIR}/dist/index.html s3://${BUCKET_NAME}/index.html --cache-control 'no-cache, no-store, must-revalidate' --profile ${AWS_PROFILE}${RESET}"
echo ""
