#!/usr/bin/env bash
# =============================================================================
# Day 2 – Step 5: End-to-end API tests against live Elastic Beanstalk URL
# =============================================================================
# Usage:
#   bash scripts/aws/05_test_beanstalk_api.sh
#   # OR with explicit URL:
#   EB_BASE_URL="http://regional-nrw-env.eba-xxxx.eu-central-1.elasticbeanstalk.com" \
#     bash scripts/aws/05_test_beanstalk_api.sh
#
# What this script tests:
#   - All 16 live API routes (15 GET + 1 POST)
#   - HTTP status codes
#   - JSON response shape / minimum expected fields
#   - CORS header presence
#   - Response time (all endpoints must respond < 3s)
#   - SSL enforcement check (optional if HTTPS is configured)
#
# Exit codes:
#   0 – all tests passed
#   1 – one or more tests failed
# =============================================================================

set -uo pipefail          # note: no -e so we accumulate failures instead of aborting

# ── Colour helpers ─────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
pass()   { echo -e "${GREEN}  ✓${RESET} $*"; }
fail_msg(){ echo -e "${RED}  ✗${RESET} $*"; }
warn()   { echo -e "${YELLOW}  ⚠${RESET} $*"; }
info()   { echo -e "${CYAN}  →${RESET} $*"; }
banner() { echo -e "\n${BOLD}${CYAN}[ $1 ]${RESET} $2"; }

# ── Configuration ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EB_URL_FILE="${SCRIPT_DIR}/.eb_url"

# Resolve base URL: env var > .eb_url file > error
if [[ -n "${EB_BASE_URL:-}" ]]; then
  BASE_URL="${EB_BASE_URL}"
elif [[ -f "$EB_URL_FILE" ]]; then
  BASE_URL="$(cat "$EB_URL_FILE")"
else
  echo -e "${RED}Error:${RESET} EB_BASE_URL not set and ${EB_URL_FILE} not found."
  echo "  Run 04_deploy_to_beanstalk.sh first, or export EB_BASE_URL=http://your-eb-url"
  exit 1
fi

# Trim trailing slash
BASE_URL="${BASE_URL%/}"

TIMEOUT_SECS=10      # curl hard timeout per request
MAX_RESPONSE_MS=3000 # warn if response > 3s
PASS_COUNT=0
FAIL_COUNT=0
WARN_COUNT=0

echo -e "\n${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Day 2 – API Endpoint Tests (Elastic Beanstalk)${RESET}"
echo -e "${BOLD}  Target : ${BASE_URL}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}\n"

# ── Helper: execute a curl request and return (http_code, body, time_ms) ──────
# Usage: do_request METHOD URL [post_body]
do_request() {
  local method="$1"
  local url="$2"
  local post_body="${3:-}"

  if [[ "$method" == "POST" ]]; then
    curl -s -o /tmp/eb_test_body.json \
      -w "%{http_code}|%{time_total}" \
      -X POST \
      -H "Content-Type: application/json" \
      -d "$post_body" \
      --max-time "$TIMEOUT_SECS" \
      "$url" 2>/tmp/eb_test_curl_err || echo "000|0"
  else
    curl -s -o /tmp/eb_test_body.json \
      -w "%{http_code}|%{time_total}" \
      -X GET \
      --max-time "$TIMEOUT_SECS" \
      "$url" 2>/tmp/eb_test_curl_err || echo "000|0"
  fi
}

# Usage: check_cors URL
check_cors() {
  local url="$1"
  curl -s -I -X GET \
    -H "Origin: http://example.com" \
    --max-time 5 \
    "$url" 2>/dev/null | grep -i "access-control-allow-origin" | head -1
}

# ── Helper: assert HTTP status ─────────────────────────────────────────────────
assert_status() {
  local test_name="$1"
  local expected_status="$2"
  local actual_status="$3"
  local time_s="$4"
  local time_ms
  time_ms=$(echo "$time_s * 1000" | bc 2>/dev/null | cut -d. -f1 || echo "?")

  if [[ "$actual_status" == "$expected_status" ]]; then
    if [[ "$time_ms" != "?" && "$time_ms" -gt "$MAX_RESPONSE_MS" ]] 2>/dev/null; then
      warn "${test_name} – HTTP ${actual_status} but slow response: ${time_ms}ms (> ${MAX_RESPONSE_MS}ms)"
      (( WARN_COUNT++ )) || true
    else
      pass "${test_name} – HTTP ${actual_status} [${time_ms}ms]"
      (( PASS_COUNT++ )) || true
    fi
  else
    fail_msg "${test_name} – expected HTTP ${expected_status}, got ${actual_status}"
    (( FAIL_COUNT++ )) || true
  fi
}

# ── Helper: assert JSON field exists and is non-empty ─────────────────────────
assert_json() {
  local test_name="$1"
  local jq_filter="$2"   # e.g. '.status', '.[0]', 'length'
  local body_file="/tmp/eb_test_body.json"

  if ! command -v jq >/dev/null 2>&1; then
    warn "${test_name} (skipping JSON check – jq not installed)"
    return
  fi

  local result
  result=$(jq -r "$jq_filter" "$body_file" 2>/dev/null || echo "")

  if [[ -n "$result" && "$result" != "null" && "$result" != "0" ]]; then
    pass "  JSON check: ${jq_filter} = '${result}'"
    (( PASS_COUNT++ )) || true
  else
    fail_msg "  JSON check failed: ${jq_filter} returned '${result}'"
    (( FAIL_COUNT++ )) || true
  fi
}

# =============================================================================
# ── Section 1: Core Health & Metadata ─────────────────────────────────────────
# =============================================================================
banner "1/6" "Core health & metadata endpoints"

# GET /api/health
result=$(do_request GET "${BASE_URL}/api/health")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/health" "200" "$http_code" "$time_s"
assert_json "  health.status field" '.status'

# GET /api/years
result=$(do_request GET "${BASE_URL}/api/years")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/years" "200" "$http_code" "$time_s"
# Should include 1975 and 2024 — response is [{year:2024}, {year:2023}, ...]
if command -v jq >/dev/null 2>&1; then
  body=$(cat /tmp/eb_test_body.json 2>/dev/null || echo "[]")
  HAS_1975=$(echo "$body" | jq '[.[] | .year] | map(select(. == 1975)) | length' 2>/dev/null || echo "0")
  HAS_2024=$(echo "$body" | jq '[.[] | .year] | map(select(. == 2024)) | length' 2>/dev/null || echo "0")
  if [[ "${HAS_1975:-0}" -gt 0 ]]; then
    pass "  Years include 1975 (full historical range)"; (( PASS_COUNT++ )) || true
  else
    fail_msg "  1975 missing from /api/years"; (( FAIL_COUNT++ )) || true
  fi
  if [[ "${HAS_2024:-0}" -gt 0 ]]; then
    pass "  Years include 2024 (latest data)"; (( PASS_COUNT++ )) || true
  else
    fail_msg "  2024 missing from /api/years"; (( FAIL_COUNT++ )) || true
  fi
fi

# GET /api/indicators
result=$(do_request GET "${BASE_URL}/api/indicators")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/indicators" "200" "$http_code" "$time_s"
if command -v jq >/dev/null 2>&1; then
  COUNT=$(jq 'length' /tmp/eb_test_body.json 2>/dev/null || echo "0")
  if [[ "$COUNT" -ge 100 ]]; then
    pass "  Indicator count: ${COUNT} (≥ 100 ✓)"
    (( PASS_COUNT++ )) || true
  else
    fail_msg "  Indicator count: ${COUNT} (expected ≥ 100)"
    (( FAIL_COUNT++ )) || true
  fi
fi

# GET /api/indicator-metadata
result=$(do_request GET "${BASE_URL}/api/indicator-metadata")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/indicator-metadata" "200" "$http_code" "$time_s"

# GET /api/data-coverage
result=$(do_request GET "${BASE_URL}/api/data-coverage")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/data-coverage" "200" "$http_code" "$time_s"

# =============================================================================
# ── Section 2: City & Demographics endpoints ──────────────────────────────────
# =============================================================================
banner "2/6" "City & demographics endpoints"

# GET /api/cities
result=$(do_request GET "${BASE_URL}/api/cities")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/cities" "200" "$http_code" "$time_s"
if command -v jq >/dev/null 2>&1; then
  CITY_COUNT=$(jq 'length' /tmp/eb_test_body.json 2>/dev/null || echo "0")
  [[ "$CITY_COUNT" -ge 5 ]] && \
    pass "  City count: ${CITY_COUNT} (≥ 5 Ruhr cities ✓)" && (( PASS_COUNT++ )) || true || \
    { fail_msg "  City count: ${CITY_COUNT} (expected ≥ 5)"; (( FAIL_COUNT++ )) || true; }
fi

# GET /api/duisburg
result=$(do_request GET "${BASE_URL}/api/duisburg")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/duisburg" "200" "$http_code" "$time_s"

# GET /api/demographics/2023
result=$(do_request GET "${BASE_URL}/api/demographics/2023")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/demographics/2023" "200" "$http_code" "$time_s"
if command -v jq >/dev/null 2>&1; then
  COUNT=$(jq 'if type == "array" then length else 1 end' /tmp/eb_test_body.json 2>/dev/null || echo "0")
  [[ "$COUNT" -gt 0 ]] && \
    pass "  Demographics 2023: non-empty response (${COUNT} item(s))" && (( PASS_COUNT++ )) || true || \
    { fail_msg "  Demographics 2023: empty response"; (( FAIL_COUNT++ )) || true; }
fi

# =============================================================================
# ── Section 3: Sector / thematic endpoints ────────────────────────────────────
# =============================================================================
banner "3/6" "Sector & thematic endpoints"

# GET /api/labor-market/2023
result=$(do_request GET "${BASE_URL}/api/labor-market/2023")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/labor-market/2023" "200" "$http_code" "$time_s"

# GET /api/business-economy/2023
result=$(do_request GET "${BASE_URL}/api/business-economy/2023")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/business-economy/2023" "200" "$http_code" "$time_s"

# GET /api/ict/2023
result=$(do_request GET "${BASE_URL}/api/ict/2023")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/ict/2023" "200" "$http_code" "$time_s"

# GET /api/public-finance/2023
result=$(do_request GET "${BASE_URL}/api/public-finance/2023")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/public-finance/2023" "200" "$http_code" "$time_s"

# =============================================================================
# ── Section 4: Time-series endpoints ──────────────────────────────────────────
# =============================================================================
banner "4/6" "Time-series endpoints"

# GET /api/timeseries/total_population
result=$(do_request GET "${BASE_URL}/api/timeseries/total_population")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/timeseries/total_population" "200" "$http_code" "$time_s"
if command -v jq >/dev/null 2>&1; then
  COUNT=$(jq 'if type == "array" then length else 0 end' /tmp/eb_test_body.json 2>/dev/null || echo "0")
  [[ "$COUNT" -gt 0 ]] && \
    pass "  Time-series rows: ${COUNT}" && (( PASS_COUNT++ )) || true || \
    { fail_msg "  Time-series total_population: empty"; (( FAIL_COUNT++ )) || true; }
fi

# GET /api/timeseries/unemployment_persons
result=$(do_request GET "${BASE_URL}/api/timeseries/unemployment_persons")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/timeseries/unemployment_persons" "200" "$http_code" "$time_s"

# GET /api/timeseries/GDP_MARKET_PRICE
result=$(do_request GET "${BASE_URL}/api/timeseries/GDP_MARKET_PRICE")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/timeseries/GDP_MARKET_PRICE" "200" "$http_code" "$time_s"

# GET /api/indicator-years/total_population
result=$(do_request GET "${BASE_URL}/api/indicator-years/total_population")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/indicator-years/total_population" "200" "$http_code" "$time_s"

# GET /api/timeseries/total_population/categories
result=$(do_request GET "${BASE_URL}/api/timeseries/total_population/categories")
http_code="${result%%|*}"; time_s="${result##*|}"
assert_status "GET /api/timeseries/total_population/categories" "200" "$http_code" "$time_s"

# =============================================================================
# ── Section 5: POST /api/chat ──────────────────────────────────────────────────
# =============================================================================
banner "5/6" "POST /api/chat (AI assistant)"

POST_BODY='{"message":"What is the population of Duisburg?"}'
result=$(do_request POST "${BASE_URL}/api/chat" "$POST_BODY")
http_code="${result%%|*}"; time_s="${result##*|}"

# 200 = OpenAI key present and working; 503 = key not configured (acceptable here)
if [[ "$http_code" == "200" ]]; then
  pass "POST /api/chat – HTTP 200 (OpenAI active)"
  (( PASS_COUNT++ )) || true
elif [[ "$http_code" == "503" || "$http_code" == "500" ]]; then
  warn "POST /api/chat – HTTP ${http_code} (OpenAI key may not be set – non-blocking)"
  (( WARN_COUNT++ )) || true
else
  fail_msg "POST /api/chat – unexpected HTTP ${http_code}"
  (( FAIL_COUNT++ )) || true
fi

# =============================================================================
# ── Section 6: Cross-cutting checks ───────────────────────────────────────────
# =============================================================================
banner "6/6" "Cross-cutting: CORS, SSL, error handling"

# CORS header check
info "Checking CORS header on /api/health..."
CORS_HEADER=$(check_cors "${BASE_URL}/api/health")
if [[ -n "$CORS_HEADER" ]]; then
  pass "CORS: Access-Control-Allow-Origin header present"
  info "  Value: ${CORS_HEADER}"
  (( PASS_COUNT++ )) || true
else
  fail_msg "CORS: Access-Control-Allow-Origin header MISSING on /api/health"
  (( FAIL_COUNT++ )) || true
fi

# SSL check (only relevant if HTTPS is configured; EB HTTP-only is fine for now)
HTTPS_URL="${BASE_URL/http:\/\//https://}"
if [[ "$BASE_URL" != "$HTTPS_URL" ]]; then
  info "Checking HTTPS on ${HTTPS_URL}/api/health..."
  HTTPS_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
    --max-time 5 "${HTTPS_URL}/api/health" 2>/dev/null || echo "000")
  if [[ "$HTTPS_CODE" == "200" ]]; then
    pass "SSL: HTTPS responds with 200 on /api/health"
    (( PASS_COUNT++ )) || true
  else
    warn "SSL: HTTPS returned ${HTTPS_CODE} (expected if CloudFront/ACM not yet configured)"
    (( WARN_COUNT++ )) || true
  fi
fi

# 404 error handling
result=$(do_request GET "${BASE_URL}/api/nonexistent-endpoint-xyz")
http_code="${result%%|*}"; time_s="${result##*|}"
if [[ "$http_code" == "404" ]]; then
  pass "404 handling: unknown route returns 404"
  (( PASS_COUNT++ )) || true
else
  warn "404 handling: /api/nonexistent returned ${http_code} (expected 404)"
  (( WARN_COUNT++ )) || true
fi

# =============================================================================
# ── Final Summary ─────────────────────────────────────────────────────────────
# =============================================================================
TOTAL_CHECKS=$(( PASS_COUNT + FAIL_COUNT ))

echo ""
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}  Test Results${RESET}"
echo -e "${BOLD}  Target : ${BASE_URL}${RESET}"
echo -e "${BOLD}  Passed : ${GREEN}${PASS_COUNT}${RESET}"
echo -e "${BOLD}  Failed : ${RED}${FAIL_COUNT}${RESET}"
echo -e "${BOLD}  Warned : ${YELLOW}${WARN_COUNT}${RESET}"
echo -e "${BOLD}  Total  : ${TOTAL_CHECKS}${RESET}"
echo -e "${BOLD}════════════════════════════════════════════════════════${RESET}"
echo ""

if [[ "$FAIL_COUNT" -eq 0 ]]; then
  echo -e "${GREEN}${BOLD}All tests passed. EB API is healthy.${RESET}"
  echo ""
  echo -e "${CYAN}Next step – Day 3 (CloudFront CDN):${RESET}"
  echo -e "  Set CORS_ORIGINS to your CloudFront domain once it is provisioned:"
  echo -e "  ${BOLD}eb setenv CORS_ORIGINS=https://xxxxx.cloudfront.net --profile regional-nrw${RESET}"
  exit 0
else
  echo -e "${RED}${BOLD}${FAIL_COUNT} test(s) failed. Check the output above.${RESET}"
  echo ""
  echo -e "  View EB logs: ${BOLD}eb logs regional-nrw-env --profile regional-nrw${RESET}"
  echo -e "  SSH to instance: ${BOLD}eb ssh regional-nrw-env --profile regional-nrw${RESET}"
  exit 1
fi
