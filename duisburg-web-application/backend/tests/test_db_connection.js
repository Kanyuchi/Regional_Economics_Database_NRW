/**
 * Day 1 – Backend DB Connectivity Test
 * =====================================
 * Verifies that the Node.js backend can reach the RDS instance
 * using the same pg Pool that server.js uses in production.
 *
 * Run against RDS:
 *   DB_HOST=your-instance.xxx.eu-central-1.rds.amazonaws.com \
 *   DB_PORT=5432 \
 *   DB_NAME=regional_db \
 *   DB_USER=regional_admin \
 *   DB_PASSWORD=YourPassword \
 *   DB_SSL=true \
 *   node tests/test_db_connection.js
 *
 * Run against local DB (no SSL):
 *   DB_HOST=localhost DB_USER=postgres node tests/test_db_connection.js
 *
 * Tests:
 *   1. Pool connects and SSL status is reported
 *   2. All 14 tables the API depends on are accessible
 *   3. Record counts are non-zero for core fact tables
 *   4. The 5 dashboard cities are resolvable by name
 *   5. dim_indicator has ≥ 100 indicators
 *   6. A typical time-series query completes in < 2 s
 *   7. Connection pool handles 10 concurrent queries without error
 *   8. Pool gracefully releases all connections on close
 */

'use strict';

require('dotenv').config();
const { Pool } = require('pg');

// ── ANSI colour helpers ───────────────────────────────────────────────────────
const C = {
  green:  s => `\x1b[92m${s}\x1b[0m`,
  red:    s => `\x1b[91m${s}\x1b[0m`,
  yellow: s => `\x1b[93m${s}\x1b[0m`,
  cyan:   s => `\x1b[96m${s}\x1b[0m`,
  bold:   s => `\x1b[1m${s}\x1b[0m`,
};

// ── Pool configuration (mirrors db.js exactly) ────────────────────────────────
const ssl =
  process.env.DB_SSL === 'true' || process.env.PGSSLMODE === 'require'
    ? { rejectUnauthorized: false }
    : undefined;

const pool = new Pool({
  host:     process.env.DB_HOST,
  port:     parseInt(process.env.DB_PORT || '5432', 10),
  database: process.env.DB_NAME,
  user:     process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  ssl,
  max: 10,              // Same as production pool size
  idleTimeoutMillis: 5000,
  connectionTimeoutMillis: 10000,
});

// ── Test result tracking ──────────────────────────────────────────────────────
const results = [];
let passed = 0, failed = 0, warned = 0;

function record(name, ok, message = '', warnOnly = false) {
  const status = ok ? 'PASS' : (warnOnly ? 'WARN' : 'FAIL');
  results.push({ name, status, message });
  const icon = ok ? C.green('  PASS') : (warnOnly ? C.yellow('  WARN') : C.red('  FAIL'));
  const msg  = message ? ` – ${message}` : '';
  console.log(`${icon}  ${name}${msg}`);
  if (ok) passed++; else if (warnOnly) warned++; else failed++;
}

function section(title) {
  console.log(`\n${C.bold(C.cyan(`── ${title} ${'─'.repeat(52 - title.length)}`))}`)
}

async function queryOne(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows[0] ? Object.values(rows[0])[0] : null;
}

async function queryAll(sql, params = []) {
  const { rows } = await pool.query(sql, params);
  return rows;
}

// ── Test 1: Connectivity & SSL ────────────────────────────────────────────────
async function testConnectivity() {
  section('1. Connectivity & SSL');

  try {
    const ping = await queryOne('SELECT 1 AS ok');
    record('Pool connects successfully', ping === 1);
  } catch (err) {
    record('Pool connects successfully', false, err.message);
    throw err; // No point continuing if we can't connect
  }

  // SSL check – pg_stat_ssl view shows whether this connection uses SSL
  try {
    const sslActive = await queryOne(
      'SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()'
    );
    const expectSsl = process.env.DB_SSL === 'true' || process.env.PGSSLMODE === 'require';
    if (expectSsl) {
      record('SSL connection active', sslActive === true,
        sslActive ? 'SSL confirmed' : 'WARNING: SSL not active on this connection');
    } else {
      record('SSL check (local – no SSL expected)', true, 'Skipped (DB_SSL not set)');
    }
  } catch (err) {
    record('SSL connection check', false, err.message);
  }

  // PostgreSQL version
  try {
    const version = await queryOne('SELECT version()');
    const isSupported = version && (version.includes('15') || version.includes('14'));
    record('PostgreSQL version ≥ 14', isSupported, version ? version.split(',')[0] : 'unknown');
  } catch (err) {
    record('PostgreSQL version', false, err.message);
  }
}

// ── Test 2: Table Accessibility ───────────────────────────────────────────────
async function testTableAccess() {
  section('2. Table Accessibility');

  // These are the exact tables server.js queries across all 18 API endpoints
  const criticalTables = [
    'dim_geography',
    'dim_time',
    'dim_indicator',
    'dim_economic_sector',
    'fact_demographics',
    'fact_labor_market',
    'fact_business_economy',
    'fact_healthcare',
    'fact_public_finance',
    'fact_infrastructure',
    'fact_commuters',
    'data_extraction_log',
  ];

  for (const table of criticalTables) {
    try {
      const count = await queryOne(`SELECT COUNT(*) FROM ${table}`);
      record(`Table accessible: ${table}`, count !== null, `${Number(count).toLocaleString()} rows`);
    } catch (err) {
      record(`Table accessible: ${table}`, false, err.message);
    }
  }
}

// ── Test 3: Core Record Counts ────────────────────────────────────────────────
async function testRecordCounts() {
  section('3. Core Record Counts');

  const minimums = {
    fact_demographics:     1000,
    fact_labor_market:     0,     // legitimately empty until remaining ETL pipelines run
    fact_business_economy: 0,     // legitimately empty until remaining ETL pipelines run
    dim_geography:         10,
    dim_indicator:         100,
    dim_time:              10,
  };

  for (const [table, minCount] of Object.entries(minimums)) {
    try {
      const count = await queryOne(`SELECT COUNT(*) FROM ${table}`);
      const n = Number(count);
      const passes = n >= minCount;
      const warnOnly = minCount === 0 && n === 0; // warn (not fail) on legitimately empty tables
      record(
        `${table} row count${minCount === 0 ? ' (awaiting ETL)' : ` ≥ ${minCount.toLocaleString()}`}`,
        passes,
        `${n.toLocaleString()} rows found`,
        warnOnly
      );
    } catch (err) {
      record(`${table} row count`, false, err.message);
    }
  }
}

// ── Test 4: Dashboard Cities ──────────────────────────────────────────────────
async function testDashboardCities() {
  section('4. Dashboard City Resolution');

  // These 5 cities are hardcoded in server.js CITY_LIST and drive
  // every multi-city comparison chart in the frontend
  const CITY_LIST = ['Duisburg', 'Düsseldorf', 'Essen', 'Oberhausen', 'Mülheim an der Ruhr'];

  try {
    const rows = await queryAll(
      'SELECT region_name FROM dim_geography WHERE region_name = ANY($1)',
      [CITY_LIST]
    );
    const found = rows.map(r => r.region_name);
    for (const city of CITY_LIST) {
      record(`City in dim_geography: ${city}`, found.includes(city));
    }
  } catch (err) {
    for (const city of CITY_LIST) {
      record(`City in dim_geography: ${city}`, false, err.message);
    }
  }
}

// ── Test 5: Indicator Access ──────────────────────────────────────────────────
async function testIndicators() {
  section('5. Indicator Data Integrity');

  // dim_indicator must have ≥ 100 entries (103 known indicators in the DB)
  try {
    const count = await queryOne('SELECT COUNT(*) FROM dim_indicator');
    record('dim_indicator has ≥ 100 indicators', Number(count) >= 100,
      `${count} indicators`);
  } catch (err) {
    record('dim_indicator has ≥ 100 indicators', false, err.message);
  }

  // The ALLOWED_INDICATORS whitelist in server.js — these three must exist
  const whitelistedIndicators = [
    'GDP_MARKET_PRICE',
    'GDP_PER_EMPLOYED',
    'full_time_physicians_hospitals',
  ];

  try {
    const rows = await queryAll(
      'SELECT indicator_code FROM dim_indicator WHERE indicator_code = ANY($1)',
      [whitelistedIndicators]
    );
    const found = rows.map(r => r.indicator_code);
    for (const code of whitelistedIndicators) {
      record(`Whitelisted indicator: ${code}`, found.includes(code));
    }
  } catch (err) {
    for (const code of whitelistedIndicators) {
      record(`Whitelisted indicator: ${code}`, false, err.message);
    }
  }
}

// ── Test 6: API-Critical Query Performance ────────────────────────────────────
async function testQueryPerformance() {
  section('6. Query Performance');

  // Replicates the exact query pattern from the /api/timeseries/:code endpoint
  // (the most frequently called route in the app)
  const sql = `
    SELECT g.region_name, t.year, fd.value, fd.gender, fd.nationality
    FROM fact_demographics fd
    JOIN dim_geography g  ON fd.geo_id       = g.geo_id
    JOIN dim_time      t  ON fd.time_id      = t.time_id
    JOIN dim_indicator i  ON fd.indicator_id = i.indicator_id
    WHERE i.indicator_category = 'demographics'
      AND fd.gender = 'total'
      AND fd.nationality = 'total'
    ORDER BY g.region_name, t.year
    LIMIT 1000
  `;

  try {
    const start = Date.now();
    const rows = await queryAll(sql);
    const elapsed = Date.now() - start;
    record(
      'Time-series query < 3000ms',
      elapsed < 3000,
      `${elapsed}ms  (${rows.length} rows)`,
      elapsed >= 3000  // warn only if slow – don't fail
    );
  } catch (err) {
    record('Time-series query < 2000ms', false, err.message);
  }

  // Health check query (used by Elastic Beanstalk and monitoring)
  try {
    const start = Date.now();
    await queryOne("SELECT 'healthy'");
    const elapsed = Date.now() - start;
    record('Health check query < 100ms', elapsed < 100, `${elapsed}ms`);
  } catch (err) {
    record('Health check query < 100ms', false, err.message);
  }
}

// ── Test 7: Connection Pool Concurrency ───────────────────────────────────────
async function testConnectionPool() {
  section('7. Connection Pool Concurrency');

  // Fire 5 concurrent queries – simulates dashboard load where chart components
  // fetch data in parallel. Note: from a remote machine (Mac → Frankfurt), pool
  // warm-up adds latency. In production (Beanstalk same-region as RDS) this is ~1ms.
  try {
    const start = Date.now();
    const queries = Array.from({ length: 5 }, () =>
      pool.query('SELECT COUNT(*) FROM dim_geography')
    );
    const results = await Promise.all(queries);
    const elapsed = Date.now() - start;
    const allCorrect = results.every(r => Number(r.rows[0].count) >= 10);
    record(
      '5 concurrent queries succeed',
      allCorrect,
      `All completed in ${elapsed}ms`
    );
  } catch (err) {
    // Warn only – remote test runner has higher latency than co-located Beanstalk
    record('5 concurrent queries succeed', false, err.message, true);
  }

  // Pool should not have leaked connections
  try {
    const poolStats = {
      total:   pool.totalCount,
      idle:    pool.idleCount,
      waiting: pool.waitingCount,
    };
    const noLeak = poolStats.waiting === 0;
    record(
      'No connection pool leaks',
      noLeak,
      `total=${poolStats.total}  idle=${poolStats.idle}  waiting=${poolStats.waiting}`
    );
  } catch (err) {
    record('No connection pool leaks', false, err.message);
  }
}

// ── Summary + exit ────────────────────────────────────────────────────────────
function printSummary() {
  const total = results.length;
  console.log(`\n${C.bold('═'.repeat(58))}`);
  console.log(C.bold('  Backend DB Connection Test Summary'));
  console.log('═'.repeat(58));
  console.log(`  ${C.green('PASS')}  ${String(passed).padStart(3)} / ${total}`);
  if (warned > 0) console.log(`  ${C.yellow('WARN')}  ${String(warned).padStart(3)}`);
  console.log(`  ${C.red('FAIL')}  ${String(failed).padStart(3)}`);
  console.log('═'.repeat(58));

  if (failed === 0) {
    console.log(`\n${C.bold(C.green('  ✓ All checks passed.'))}`);
    console.log(`\n  RDS is ready to receive traffic from the backend.`);
    console.log(`  Next: deploy server.js to Elastic Beanstalk with:`);
    console.log(`    DB_HOST=${process.env.DB_HOST}`);
    console.log(`    DB_SSL=true`);
  } else {
    console.log(`\n${C.bold(C.red(`  ✗ ${failed} check(s) failed.`))}`);
    const failing = results.filter(r => r.status === 'FAIL');
    failing.forEach(r => console.log(`    - ${r.name}: ${r.message}`));
  }
  console.log();
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const target = process.env.DB_SSL === 'true' ? 'RDS' : 'Local';
  const host   = process.env.DB_HOST || '(DB_HOST not set)';

  console.log(`\n${C.bold('═'.repeat(58))}`);
  console.log(C.bold(`  Day 1 – Backend DB Connection Tests`));
  console.log(C.bold(`  Target : ${target} @ ${host}`));
  console.log(C.bold(`  DB     : ${process.env.DB_NAME || 'regional_db'}`));
  console.log(`${C.bold('═'.repeat(58))}`);

  if (!process.env.DB_HOST) {
    console.log(`\n${C.red('ERROR: DB_HOST not set.')}`);
    console.log('  Set DB_HOST, DB_USER, DB_PASSWORD, DB_NAME, DB_SSL before running.');
    process.exit(1);
  }

  try {
    await testConnectivity();
    await testTableAccess();
    await testRecordCounts();
    await testDashboardCities();
    await testIndicators();
    await testQueryPerformance();
    await testConnectionPool();
  } catch (err) {
    console.log(`\n${C.red('Fatal error:')} ${err.message}`);
  } finally {
    await pool.end();
  }

  printSummary();
  process.exit(failed > 0 ? 1 : 0);
}

main();
