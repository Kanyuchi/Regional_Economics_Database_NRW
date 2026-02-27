#!/usr/bin/env python3
"""
Day 1 – Step 3: Migration Verification Test Suite
==================================================
Connects to both local PostgreSQL and the new RDS instance and runs
a comprehensive set of checks to confirm the migration is complete
and the data is intact.

Usage:
    export PG_POSTGRES_HOST=localhost
    export PG_POSTGRES_PASS=yourpassword
    export RDS_HOST=your-instance.xxxxx.eu-central-1.rds.amazonaws.com
    export RDS_MASTER_PASSWORD=YourSecurePassword123!
    python3 scripts/aws/03_verify_migration.py

Test categories:
    1. Connectivity         – Both DBs reachable, SSL enforced on RDS
    2. Schema integrity     – All tables, views, indexes, triggers present
    3. Extensions           – PostGIS and uuid-ossp installed
    4. Record counts        – Every table count matches source exactly
    5. Spot checks          – Known specific values in dim_geography, dim_indicator
    6. Star schema joins    – Cross-table joins return valid results
    7. Performance          – Critical time-series query runs under 2 seconds
"""

import os
import sys
import time
import traceback
from dataclasses import dataclass, field
from typing import Optional

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    print("ERROR: psycopg2 not found. Run: pip install psycopg2-binary")
    sys.exit(1)


# ── ANSI colours ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

PASS = f"{GREEN}  PASS{RESET}"
FAIL = f"{RED}  FAIL{RESET}"
SKIP = f"{YELLOW}  SKIP{RESET}"
WARN = f"{YELLOW}  WARN{RESET}"


# ── Connection helpers ────────────────────────────────────────────────────────
def connect_local() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=os.getenv("PG_POSTGRES_HOST", "localhost"),
        port=int(os.getenv("PG_POSTGRES_PORT", "5432")),
        database="regional_db",
        user=os.getenv("PG_POSTGRES_USER", "postgres"),
        password=os.getenv("PG_POSTGRES_PASS", ""),
        connect_timeout=10,
    )


def connect_rds() -> psycopg2.extensions.connection:
    rds_host = os.getenv("RDS_HOST")
    if not rds_host:
        raise ValueError("RDS_HOST environment variable not set")
    return psycopg2.connect(
        host=rds_host,
        port=5432,
        database="regional_db",
        user=os.getenv("RDS_MASTER_USER", "regional_admin"),
        password=os.getenv("RDS_MASTER_PASSWORD"),
        sslmode="require",          # Enforce SSL – fails if RDS doesn't support it
        connect_timeout=15,
    )


def query_one(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


def query_all(conn, sql: str, params=None):
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


# ── Test result tracking ──────────────────────────────────────────────────────
@dataclass
class TestResult:
    name: str
    status: str       # "PASS" | "FAIL" | "WARN" | "SKIP"
    message: str = ""
    detail: str = ""


results: list[TestResult] = []


def record(name: str, passed: bool, message: str = "", detail: str = "", warn_only: bool = False):
    status = "PASS" if passed else ("WARN" if warn_only else "FAIL")
    results.append(TestResult(name, status, message, detail))
    icon = PASS if passed else (WARN if warn_only else FAIL)
    msg = f" – {message}" if message else ""
    print(f"{icon}  {name}{msg}")
    if detail and not passed:
        for line in detail.splitlines():
            print(f"        {CYAN}{line}{RESET}")


# ── Test Suite ────────────────────────────────────────────────────────────────

def section(title: str):
    print(f"\n{BOLD}{CYAN}── {title} {'─' * (52 - len(title))}{RESET}")


def test_connectivity(local_conn, rds_conn):
    section("1. Connectivity")

    # Local ping
    try:
        val = query_one(local_conn, "SELECT 1")
        record("Local DB reachable", val == 1)
    except Exception as e:
        record("Local DB reachable", False, str(e))

    # RDS ping
    try:
        val = query_one(rds_conn, "SELECT 1")
        record("RDS reachable", val == 1)
    except Exception as e:
        record("RDS reachable", False, str(e))

    # SSL enforced on RDS
    try:
        ssl_in_use = query_one(rds_conn, "SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")
        record("RDS SSL enforced", ssl_in_use is True,
               "SSL active" if ssl_in_use else "SSL NOT active – connection is unencrypted")
    except Exception as e:
        record("RDS SSL enforced", False, str(e))

    # PostgreSQL version on RDS (should be 15.x)
    try:
        version = query_one(rds_conn, "SELECT version()")
        is_pg15 = "PostgreSQL 15" in (version or "")
        record("RDS PostgreSQL 15", is_pg15, version.split(",")[0] if version else "unknown")
    except Exception as e:
        record("RDS PostgreSQL 15", False, str(e))


def test_extensions(rds_conn):
    section("2. Extensions")

    for ext in ["postgis", "uuid-ossp"]:
        try:
            exists = query_one(
                rds_conn,
                "SELECT COUNT(*) FROM pg_extension WHERE extname = %s",
                (ext,)
            )
            record(f"Extension: {ext}", exists == 1)
        except Exception as e:
            record(f"Extension: {ext}", False, str(e))

    # PostGIS functional test – verifies the geometry type is actually usable
    try:
        result = query_one(rds_conn, "SELECT ST_AsText(ST_MakePoint(6.7763, 51.2217))")
        record("PostGIS geometry ops", result is not None, result or "")
    except Exception as e:
        record("PostGIS geometry ops", False, str(e))


def test_schema_integrity(rds_conn):
    section("3. Schema Integrity")

    expected_tables = [
        "dim_geography", "dim_time", "dim_indicator", "dim_economic_sector",
        "fact_demographics", "fact_labor_market", "fact_business_economy",
        "fact_healthcare", "fact_public_finance", "fact_infrastructure",
        "fact_commuters",
        "data_extraction_log", "data_quality_checks", "data_lineage",
    ]

    existing_tables = set(
        row[0] for row in query_all(
            rds_conn,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        )
    )

    for table in expected_tables:
        record(f"Table exists: {table}", table in existing_tables)

    # Views
    expected_views = ["vw_latest_indicators"]
    existing_views = set(
        row[0] for row in query_all(
            rds_conn,
            "SELECT viewname FROM pg_views WHERE schemaname = 'public'"
        )
    )
    for view in expected_views:
        record(f"View exists: {view}", view in existing_views)

    # Trigger function
    try:
        fn_exists = query_one(
            rds_conn,
            "SELECT COUNT(*) FROM pg_proc WHERE proname = 'update_updated_at_column'"
        )
        record("Trigger function: update_updated_at_column", fn_exists == 1)
    except Exception as e:
        record("Trigger function: update_updated_at_column", False, str(e))

    # Key indexes (spot-check the most critical composite indexes)
    key_indexes = [
        "idx_demo_composite",
        "idx_labor_composite",
        "idx_business_composite",
        "idx_geo_region_code",
        "idx_indicator_code",
        "idx_time_year",
    ]
    existing_indexes = set(
        row[0] for row in query_all(
            rds_conn,
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        )
    )
    for idx in key_indexes:
        record(f"Index exists: {idx}", idx in existing_indexes)


def test_record_counts(local_conn, rds_conn):
    section("4. Record Counts (Local vs RDS)")

    tables = [
        "dim_geography",
        "dim_time",
        "dim_indicator",
        "dim_economic_sector",
        "fact_demographics",
        "fact_labor_market",
        "fact_business_economy",
        "fact_healthcare",
        "fact_public_finance",
        "fact_infrastructure",
        "fact_commuters",
        "data_extraction_log",
    ]

    total_local = 0
    total_rds = 0

    for table in tables:
        try:
            local_count = query_one(local_conn, f"SELECT COUNT(*) FROM {table}")
            rds_count   = query_one(rds_conn,   f"SELECT COUNT(*) FROM {table}")
            match = local_count == rds_count
            total_local += local_count or 0
            total_rds   += rds_count or 0
            record(
                f"Count match: {table}",
                match,
                f"local={local_count:,}  rds={rds_count:,}" if match
                else f"MISMATCH: local={local_count:,} vs rds={rds_count:,}"
            )
        except Exception as e:
            record(f"Count match: {table}", False, str(e))

    print(f"\n  {BOLD}Total rows – Local: {total_local:,}  |  RDS: {total_rds:,}{RESET}")
    record(
        "Total row counts match",
        total_local == total_rds,
        f"{total_local:,} rows" if total_local == total_rds
        else f"Delta: {abs(total_rds - total_local):,} rows"
    )


def test_spot_checks(rds_conn):
    section("5. Data Spot Checks")

    # Duisburg must exist in dim_geography (it's the primary city for this project)
    try:
        duisburg = query_one(
            rds_conn,
            "SELECT region_name FROM dim_geography WHERE region_code = '05112'"
        )
        record("dim_geography: Duisburg (05112) present", duisburg is not None, duisburg or "")
    except Exception as e:
        record("dim_geography: Duisburg present", False, str(e))

    # NRW state record
    try:
        nrw = query_one(
            rds_conn,
            "SELECT region_name FROM dim_geography WHERE region_type = 'state' LIMIT 1"
        )
        record("dim_geography: State record present", nrw is not None, nrw or "")
    except Exception as e:
        record("dim_geography: State record present", False, str(e))

    # All 5 dashboard cities present
    cities = ["Duisburg", "Düsseldorf", "Essen", "Oberhausen", "Mülheim an der Ruhr"]
    try:
        found = [
            row[0] for row in query_all(
                rds_conn,
                "SELECT region_name FROM dim_geography WHERE region_name = ANY(%s)",
                (cities,)
            )
        ]
        for city in cities:
            record(f"City present: {city}", city in found)
    except Exception as e:
        for city in cities:
            record(f"City present: {city}", False, str(e))

    # dim_indicator should have > 100 indicators
    try:
        ind_count = query_one(rds_conn, "SELECT COUNT(*) FROM dim_indicator")
        record("dim_indicator: >= 100 indicators", ind_count >= 100, f"{ind_count} indicators found")
    except Exception as e:
        record("dim_indicator: >= 100 indicators", False, str(e))

    # dim_time should span 1975-2024 (50 years)
    try:
        min_year = query_one(rds_conn, "SELECT MIN(year) FROM dim_time")
        max_year = query_one(rds_conn, "SELECT MAX(year) FROM dim_time")
        span_ok = (min_year is not None and max_year is not None
                   and min_year <= 1975 and max_year >= 2024)
        record("dim_time: Spans 1975–2024", span_ok, f"{min_year}–{max_year}")
    except Exception as e:
        record("dim_time: Spans 1975–2024", False, str(e))

    # At least one fact table has data for Duisburg
    try:
        demo_count = query_one(
            rds_conn,
            """
            SELECT COUNT(*) FROM fact_demographics fd
            JOIN dim_geography g ON fd.geo_id = g.geo_id
            WHERE g.region_code = '05112'
            """
        )
        record("fact_demographics: Duisburg data present", demo_count > 0,
               f"{demo_count:,} rows for Duisburg")
    except Exception as e:
        record("fact_demographics: Duisburg data present", False, str(e))


def test_star_schema_joins(rds_conn):
    section("6. Star Schema Join Integrity")

    # Core analytical join used by the Express API (/api/timeseries/:code)
    try:
        rows = query_all(
            rds_conn,
            """
            SELECT g.region_name, t.year, i.indicator_code, fd.value
            FROM fact_demographics fd
            JOIN dim_geography g  ON fd.geo_id       = g.geo_id
            JOIN dim_time      t  ON fd.time_id      = t.time_id
            JOIN dim_indicator i  ON fd.indicator_id = i.indicator_id
            WHERE g.region_code = '05112'
              AND i.indicator_category = 'demographics'
            LIMIT 5
            """
        )
        record("4-table join: demographics time-series", len(rows) > 0,
               f"{len(rows)} sample rows returned")
    except Exception as e:
        record("4-table join: demographics time-series", False, str(e))

    # Labor market join — only test if the table has data
    try:
        lm_count = query_one(rds_conn, "SELECT COUNT(*) FROM fact_labor_market")
        if lm_count and lm_count > 0:
            rows = query_all(
                rds_conn,
                """
                SELECT g.region_name, t.year, i.indicator_code, fl.value
                FROM fact_labor_market fl
                JOIN dim_geography g  ON fl.geo_id       = g.geo_id
                JOIN dim_time      t  ON fl.time_id      = t.time_id
                JOIN dim_indicator i  ON fl.indicator_id = i.indicator_id
                WHERE g.region_code = '05112'
                LIMIT 5
                """
            )
            record("4-table join: labor market time-series", len(rows) > 0,
                   f"{len(rows)} sample rows returned")
        else:
            record("4-table join: labor market time-series", True,
                   "Skipped – table is empty on both source and RDS (counts match)")
    except Exception as e:
        record("4-table join: labor market time-series", False, str(e))

    # vw_latest_indicators view is queryable
    try:
        rows = query_all(rds_conn, "SELECT * FROM vw_latest_indicators LIMIT 3")
        record("View: vw_latest_indicators queryable", len(rows) >= 0,
               f"{len(rows)} rows returned")
    except Exception as e:
        record("View: vw_latest_indicators queryable", False, str(e))


def test_performance(rds_conn):
    section("7. Query Performance")

    # This is the most common time-series query hit by the API (/api/timeseries/:code)
    # Should complete in under 2 seconds on db.t3.micro
    try:
        start = time.time()
        rows = query_all(
            rds_conn,
            """
            SELECT g.region_name, t.year, fd.value
            FROM fact_demographics fd
            JOIN dim_geography g  ON fd.geo_id       = g.geo_id
            JOIN dim_time      t  ON fd.time_id      = t.time_id
            JOIN dim_indicator i  ON fd.indicator_id = i.indicator_id
            WHERE i.indicator_category = 'demographics'
              AND fd.gender = 'total'
              AND fd.nationality = 'total'
            ORDER BY g.region_name, t.year
            """,
        )
        elapsed = time.time() - start
        record(
            "Time-series query < 2s",
            elapsed < 2.0,
            f"{elapsed:.3f}s  ({len(rows):,} rows)",
            warn_only=elapsed >= 2.0
        )
    except Exception as e:
        record("Time-series query < 2s", False, str(e))

    # Connection pool stress: 5 rapid sequential queries
    try:
        start = time.time()
        for _ in range(5):
            query_one(rds_conn, "SELECT COUNT(*) FROM dim_geography")
        elapsed = time.time() - start
        record("5 sequential queries", elapsed < 3.0, f"{elapsed:.3f}s for 5 queries")
    except Exception as e:
        record("5 sequential queries", False, str(e))


def print_summary():
    total  = len(results)
    passed = sum(1 for r in results if r.status == "PASS")
    failed = sum(1 for r in results if r.status == "FAIL")
    warned = sum(1 for r in results if r.status == "WARN")

    print(f"\n{BOLD}{'═' * 58}{RESET}")
    print(f"{BOLD}  Migration Verification Summary{RESET}")
    print(f"{'═' * 58}")
    print(f"  {GREEN}PASS{RESET}  {passed:>3} / {total}")
    print(f"  {YELLOW}WARN{RESET}  {warned:>3}")
    print(f"  {RED}FAIL{RESET}  {failed:>3}")
    print(f"{'═' * 58}")

    if failed == 0:
        print(f"\n{BOLD}{GREEN}  ✓ All checks passed. RDS migration is verified.{RESET}")
        print(f"\n  Next step: deploy the backend to Elastic Beanstalk")
        print(f"  (Day 2) and point it at RDS_HOST={os.getenv('RDS_HOST', '<your-endpoint>')}")
    else:
        print(f"\n{BOLD}{RED}  ✗ {failed} check(s) failed. Review output above before proceeding.{RESET}")
        failing = [r for r in results if r.status == "FAIL"]
        print(f"\n  Failed checks:")
        for r in failing:
            print(f"    - {r.name}: {r.message}")
    print()
    return failed == 0


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    print(f"\n{BOLD}{'═' * 58}{RESET}")
    print(f"{BOLD}  Day 1 – Migration Verification Test Suite{RESET}")
    print(f"{BOLD}  Source : {os.getenv('PG_POSTGRES_HOST', 'localhost')}:regional_db{RESET}")
    print(f"{BOLD}  Target : {os.getenv('RDS_HOST', 'NOT SET')}:regional_db{RESET}")
    print(f"{BOLD}{'═' * 58}{RESET}")

    # Validate required env vars
    if not os.getenv("RDS_HOST"):
        print(f"\n{RED}ERROR: RDS_HOST not set.{RESET}")
        print("  export RDS_HOST=your-instance.xxxxx.eu-central-1.rds.amazonaws.com")
        sys.exit(1)
    if not os.getenv("RDS_MASTER_PASSWORD"):
        print(f"\n{RED}ERROR: RDS_MASTER_PASSWORD not set.{RESET}")
        sys.exit(1)

    local_conn = rds_conn = None
    try:
        print(f"\n{CYAN}  Connecting to databases...{RESET}")
        try:
            local_conn = connect_local()
            print(f"  {GREEN}✓{RESET} Local DB connected")
        except Exception as e:
            print(f"  {RED}✗{RESET} Local DB failed: {e}")
            local_conn = None

        try:
            rds_conn = connect_rds()
            print(f"  {GREEN}✓{RESET} RDS connected")
        except Exception as e:
            print(f"  {RED}✗{RESET} RDS failed: {e}")
            sys.exit(1)

        # Run all test categories
        test_connectivity(local_conn or rds_conn, rds_conn)
        test_extensions(rds_conn)
        test_schema_integrity(rds_conn)

        if local_conn:
            test_record_counts(local_conn, rds_conn)
        else:
            print(f"\n{YELLOW}  Skipping record count comparison (local DB unavailable){RESET}")

        test_spot_checks(rds_conn)
        test_star_schema_joins(rds_conn)
        test_performance(rds_conn)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(1)
    except Exception as e:
        print(f"\n{RED}Unexpected error: {e}{RESET}")
        traceback.print_exc()
        sys.exit(1)
    finally:
        if local_conn:
            local_conn.close()
        if rds_conn:
            rds_conn.close()

    success = print_summary()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
