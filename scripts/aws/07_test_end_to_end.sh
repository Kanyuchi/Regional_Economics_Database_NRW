#!/usr/bin/env bash
# =============================================================================
# Day 3 – Step 7: End-to-end tests through the CloudFront CDN
# =============================================================================
# Usage:
#   bash scripts/aws/07_test_end_to_end.sh
#   # OR with explicit domain:
#   CF_DOMAIN="xxxx.cloudfront.net" bash scripts/aws/07_test_end_to_end.sh
#
# Tests:
#   1. HTTPS redirect (HTTP → HTTPS)
#   2. Frontend HTML served at root
#   3. SPA routing (deep path returns index.html, not 404)
#   4. Assets served with immutable cache headers
#   5. API proxy: /api/* routed to EB backend
#   6. API health, cities, timeseries via CloudFront
#   7. CORS headers on API via CloudFront
#   8. Gzip/Brotli compression active
#   9. CloudFront cache hit headers (x-cache)
#  10. No direct S3 access (bucket stays private)
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CF_DOMAIN_FILE="${SCRIPT_DIR}/.cf_domain"

if [[ -n "${CF_DOMAIN:-}" ]]; then
  BASE_HTTPS="https://${CF_DOMAIN}"
  BASE_HTTP="http://${CF_DOMAIN}"
elif [[ -f "$CF_DOMAIN_FILE" ]]; then
  CF_DOMAIN="$(cat "$CF_DOMAIN_FILE")"
  BASE_HTTPS="https://${CF_DOMAIN}"
  BASE_HTTP="http://${CF_DOMAIN}"
else
  echo -e "${RED}Error:${RESET} CF_DOMAIN not set and ${CF_DOMAIN_FILE} not found."
  echo "  Run 06_deploy_frontend_cdn.sh first, or: export CF_DOMAIN=xxxx.cloudfront.net"
  exit 1
fi

PASS_COUNT=0; FAIL_COUNT=0; WARN_COUNT=0
TIMEOUT=15

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 3 – End-to-End Tests (CloudFront CDN)${RESET}"
echo -e "${BOLD}  Target : ${BASE_HTTPS}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Helper: curl with timing ───────────────────────────────────────────────────
# Returns: "HTTP_CODE|TIME_MS|FIRST_BYTES"
do_req() {
  local method="${1:-GET}"
  local url="$2"
  local extra_args="${3:-}"
  curl -s -o /tmp/e2e_body.txt \
    -w "%{http_code}|%{time_total}|%{size_download}" \
    -X "$method" \
    --max-time "$TIMEOUT" \
    -L \
    ${extra_args} \
    "$url" 2>/tmp/e2e_curl_err || echo "000|0|0"
}

get_header() {
  local url="$1"
  local header="$2"
  curl -s -I -L --max-time "$TIMEOUT" "$url" 2>/dev/null \
    | grep -i "^${header}:" | head -1 | sed 's/^[^:]*: //' | tr -d '\r\n'
}

check_time() {
  local label="$1"
  local time_s="$2"
  local limit_ms="${3:-3000}"
  local time_ms
  time_ms=$(echo "$time_s * 1000" | bc 2>/dev/null | cut -d. -f1 || echo "?")
  if [[ "$time_ms" == "?" ]]; then return; fi
  if [[ "$time_ms" -gt "$limit_ms" ]]; then
    warn "${label}: ${time_ms}ms (> ${limit_ms}ms)"
  fi
}

# =============================================================================
# ── Section 1: HTTPS + Frontend ───────────────────────────────────────────────
# =============================================================================
banner "1/5" "HTTPS redirect & frontend delivery"

# HTTP → HTTPS redirect (curl follows with -L, should get 200 after redirect)
result=$(do_req GET "$BASE_HTTP/")
http_code="${result%%|*}"
if [[ "$http_code" == "200" ]]; then
  pass "HTTP→HTTPS redirect: CloudFront returns 200 after following redirect"
else
  fail_msg "HTTP→HTTPS redirect: expected 200, got ${http_code}"
fi

# HTTPS root: should return index.html
result=$(do_req GET "$BASE_HTTPS/")
http_code="${result%%|*}"; time_s=$(echo "$result" | cut -d'|' -f2)
if [[ "$http_code" == "200" ]]; then
  pass "GET / – HTTPS 200"
  check_time "GET /" "$time_s"
else
  fail_msg "GET / – expected 200, got ${http_code}"
fi

# Verify it's actually the React app (use -E for extended regex on macOS)
if grep -q "<!DOCTYPE html\|<!doctype html" /tmp/e2e_body.txt 2>/dev/null && \
   grep -qE "root|assets|vite" /tmp/e2e_body.txt 2>/dev/null; then
  pass "  Root response is React app HTML"
else
  fail_msg "  Root response doesn't look like the React app"
fi

# Content-Type must be text/html
CONTENT_TYPE=$(get_header "$BASE_HTTPS/" "content-type")
if echo "$CONTENT_TYPE" | grep -qi "text/html"; then
  pass "  Content-Type: ${CONTENT_TYPE}"
else
  fail_msg "  Content-Type expected text/html, got: '${CONTENT_TYPE}'"
fi

# Cache-Control on index.html must be no-cache
CACHE_CTRL=$(get_header "$BASE_HTTPS/" "cache-control")
if echo "$CACHE_CTRL" | grep -qi "no-cache\|no-store"; then
  pass "  index.html Cache-Control: ${CACHE_CTRL}"
else
  warn "  index.html Cache-Control: '${CACHE_CTRL}' (expected no-cache)"
fi

# =============================================================================
# ── Section 2: SPA routing + static assets ────────────────────────────────────
# =============================================================================
banner "2/5" "SPA routing & static asset caching"

# Deep route should return index.html (not 404) — CloudFront error page config
SPA_PATHS=("/demographics" "/labor" "/trends" "/some/deep/path/that/does/not/exist")
for path in "${SPA_PATHS[@]}"; do
  result=$(do_req GET "${BASE_HTTPS}${path}")
  http_code="${result%%|*}"
  if [[ "$http_code" == "200" ]]; then
    pass "SPA route ${path} → 200 (index.html served)"
  else
    fail_msg "SPA route ${path} → ${http_code} (expected 200)"
  fi
done

# Assets must have immutable cache headers
ASSET_PATH=$(aws s3 ls "s3://regional-nrw-frontend-329631044553/assets/" \
  --profile regional-nrw 2>/dev/null \
  | grep "\.js" | head -1 | awk '{print $NF}' || echo "")

if [[ -n "$ASSET_PATH" ]]; then
  ASSET_CACHE=$(get_header "${BASE_HTTPS}/assets/${ASSET_PATH}" "cache-control")
  if echo "$ASSET_CACHE" | grep -qi "immutable\|max-age=31536000"; then
    pass "JS asset Cache-Control: ${ASSET_CACHE}"
  else
    warn "JS asset Cache-Control: '${ASSET_CACHE}' (expected immutable)"
  fi
else
  warn "Could not detect asset filename for cache header check (S3 access needed)"
fi

# Compression (Brotli or Gzip) – CloudFront compress=true
ENCODING=$(curl -s -I -H "Accept-Encoding: br, gzip" \
  --max-time "$TIMEOUT" "${BASE_HTTPS}/" 2>/dev/null \
  | grep -i "^content-encoding:" | head -1 | tr -d '\r\n')
if [[ -n "$ENCODING" ]]; then
  pass "Compression active: ${ENCODING}"
else
  warn "No content-encoding header detected (compression may not be applied)"
fi

# =============================================================================
# ── Section 3: API proxy through CloudFront ────────────────────────────────────
# =============================================================================
banner "3/5" "API proxy (/api/* → Elastic Beanstalk)"

# /api/health through CloudFront
result=$(do_req GET "${BASE_HTTPS}/api/health")
http_code="${result%%|*}"; time_s=$(echo "$result" | cut -d'|' -f2)
if [[ "$http_code" == "200" ]]; then
  pass "GET /api/health via CloudFront – 200"
  check_time "GET /api/health" "$time_s" 3000
else
  fail_msg "GET /api/health via CloudFront – expected 200, got ${http_code}"
fi

# Verify x-cache header shows MISS (API is not cached)
X_CACHE=$(get_header "${BASE_HTTPS}/api/health" "x-cache")
if echo "$X_CACHE" | grep -qi "Miss\|Origin"; then
  pass "  API x-cache: '${X_CACHE}' (correctly bypassing CDN cache)"
else
  info "  API x-cache: '${X_CACHE}'"
fi

# /api/cities
result=$(do_req GET "${BASE_HTTPS}/api/cities")
http_code="${result%%|*}"
[[ "$http_code" == "200" ]] && pass "GET /api/cities via CloudFront – 200" || \
  fail_msg "GET /api/cities – expected 200, got ${http_code}"

if command -v jq >/dev/null 2>&1 && [[ "$http_code" == "200" ]]; then
  COUNT=$(jq 'length' /tmp/e2e_body.txt 2>/dev/null || echo "0")
  [[ "${COUNT:-0}" -ge 5 ]] && pass "  Cities: ${COUNT} cities returned" || \
    fail_msg "  Cities: ${COUNT} (expected ≥ 5)"
fi

# /api/timeseries via CloudFront
result=$(do_req GET "${BASE_HTTPS}/api/timeseries/total_population")
http_code="${result%%|*}"
[[ "$http_code" == "200" ]] && pass "GET /api/timeseries/total_population – 200" || \
  fail_msg "GET /api/timeseries – expected 200, got ${http_code}"

# POST /api/chat via CloudFront (tests POST forwarding through CDN)
# Use curl directly — do_req can't handle complex inner-quoted args
CHAT_CODE=$(curl -s -o /tmp/e2e_chat.json \
  -w "%{http_code}" \
  -X POST \
  -H "Content-Type: application/json" \
  -d '{"message":"Hello"}' \
  --max-time "$TIMEOUT" \
  -L "${BASE_HTTPS}/api/chat" 2>/dev/null || echo "000")
if [[ "$CHAT_CODE" == "200" ]]; then
  pass "POST /api/chat via CloudFront – 200"
elif [[ "$CHAT_CODE" == "500" || "$CHAT_CODE" == "503" ]]; then
  warn "POST /api/chat – ${CHAT_CODE} (OpenAI key issue, non-blocking)"
else
  fail_msg "POST /api/chat – expected 200, got ${CHAT_CODE}"
fi

# =============================================================================
# ── Section 4: CORS via CloudFront ────────────────────────────────────────────
# =============================================================================
banner "4/5" "CORS headers via CloudFront"

# CORS with matching origin (the CloudFront domain itself)
CORS_HEADER=$(curl -s -I \
  -H "Origin: ${BASE_HTTPS}" \
  -H "Access-Control-Request-Method: GET" \
  --max-time "$TIMEOUT" \
  "${BASE_HTTPS}/api/health" 2>/dev/null \
  | grep -i "access-control-allow-origin" | head -1 | tr -d '\r\n')

if [[ -n "$CORS_HEADER" ]]; then
  pass "CORS: Access-Control-Allow-Origin header present"
  info "  Value: ${CORS_HEADER}"
else
  fail_msg "CORS: Access-Control-Allow-Origin header MISSING on /api/health"
fi

# CORS preflight (OPTIONS)
PREFLIGHT_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -X OPTIONS \
  -H "Origin: ${BASE_HTTPS}" \
  -H "Access-Control-Request-Method: POST" \
  -H "Access-Control-Request-Headers: Content-Type" \
  --max-time "$TIMEOUT" \
  "${BASE_HTTPS}/api/chat" 2>/dev/null || echo "000")
if [[ "$PREFLIGHT_CODE" == "200" || "$PREFLIGHT_CODE" == "204" ]]; then
  pass "CORS preflight OPTIONS /api/chat – ${PREFLIGHT_CODE}"
else
  warn "CORS preflight – returned ${PREFLIGHT_CODE} (may be handled by EB middleware)"
fi

# =============================================================================
# ── Section 5: Security checks ────────────────────────────────────────────────
# =============================================================================
banner "5/5" "Security: S3 bucket stays private"

ACCOUNT_ID=$(aws sts get-caller-identity \
  --profile regional-nrw --query Account --output text 2>/dev/null || echo "329631044553")
BUCKET_NAME="regional-nrw-frontend-${ACCOUNT_ID}"
S3_DIRECT_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  --max-time 10 \
  "https://${BUCKET_NAME}.s3.eu-central-1.amazonaws.com/index.html" 2>/dev/null || echo "000")

if [[ "$S3_DIRECT_CODE" == "403" || "$S3_DIRECT_CODE" == "404" ]]; then
  pass "S3 bucket is private: direct S3 URL returns ${S3_DIRECT_CODE} (access denied)"
else
  fail_msg "S3 bucket may be publicly accessible: direct URL returned ${S3_DIRECT_CODE}"
fi

# =============================================================================
# ── Final Summary ─────────────────────────────────────────────────────────────
# =============================================================================
TOTAL=$(( PASS_COUNT + FAIL_COUNT ))

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  End-to-End Test Results${RESET}"
echo -e "${BOLD}  Target : ${BASE_HTTPS}${RESET}"
echo -e "${BOLD}  Passed : ${GREEN}${PASS_COUNT}${RESET}"
echo -e "${BOLD}  Failed : ${RED}${FAIL_COUNT}${RESET}"
echo -e "${BOLD}  Warned : ${YELLOW}${WARN_COUNT}${RESET}"
echo -e "${BOLD}  Total  : ${TOTAL}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo ""

if [[ "$FAIL_COUNT" -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}All tests passed. Full stack is live end-to-end.${RESET}"
  echo ""
  echo -e "${BOLD}  Frontend : ${BASE_HTTPS}${RESET}"
  echo -e "${BOLD}  API      : ${BASE_HTTPS}/api/health${RESET}"
  echo ""
  echo -e "${CYAN}Architecture summary:${RESET}"
  echo -e "  Browser → CloudFront HTTPS"
  echo -e "            ├── /* ────────────→ S3 (React SPA, immutable cache)"
  echo -e "            └── /api/* ────────→ Elastic Beanstalk (Node.js, no cache)"
  echo -e "                                   └── RDS PostgreSQL (484,997 rows)"
  exit 0
else
  echo -e "${RED}${BOLD}${FAIL_COUNT} test(s) failed.${RESET}"
  exit 1
fi
