"""
GPAF BigQuery Schema & Data Loader
====================================
Portfolio Project: Dynamic Uber Fare Adjustment Based on Fuel Prices

This module provides:
  1. DDL  — BigQuery table definitions (schema-as-code)
  2. Loader — Python client to read/write all GPAF tables
  3. Queries — Reusable analytical SQL for dashboards & monitoring

Tables:
    gpaf_pipeline.gas_prices_raw     Raw daily gas price ingestion
    gpaf_pipeline.gas_price_baseline 30-day rolling baselines
    gpaf_pipeline.gpaf_config        Current GPAF per region (latest only)
    gpaf_pipeline.gpaf_history       Full append-only audit trail
    gpaf_pipeline.fare_adjustments   Per-trip fare adjustment log

Author: Suresh Shanmugam
"""

from __future__ import annotations
import json
from datetime import datetime, timedelta
from typing import Optional


# =============================================================================
# SECTION 1: DDL — BigQuery Table Definitions
# =============================================================================

DDL_STATEMENTS = {

    "create_dataset": """
-- Create dataset (run once)
CREATE SCHEMA IF NOT EXISTS `{project}.gpaf_pipeline`
OPTIONS (
    description = 'Gas Price Adjustment Factor pipeline tables',
    location     = 'US'
);
""",

    "gas_prices_raw": """
-- Raw gas price ingestion — one row per region per day
CREATE TABLE IF NOT EXISTS `{project}.gpaf_pipeline.gas_prices_raw` (
    region_id       STRING    NOT NULL,   -- e.g. 'US-CA', 'US-TX'
    price_usd       FLOAT64   NOT NULL,   -- price per gallon in USD
    source          STRING    NOT NULL,   -- 'EIA', 'GASBUDDY', 'CACHE_FALLBACK'
    fetched_at      TIMESTAMP NOT NULL,   -- UTC ingest timestamp
    eia_period      STRING,               -- EIA reporting period, e.g. '2026-03-10'
    pipeline_run_id STRING,               -- DAG run ID for lineage
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(fetched_at)
CLUSTER BY region_id
OPTIONS (
    description             = 'Raw gas prices ingested daily from EIA and GasBuddy APIs',
    require_partition_filter = false
);
""",

    "gas_price_baseline": """
-- Computed 30-day rolling baseline per region
CREATE TABLE IF NOT EXISTS `{project}.gpaf_pipeline.gas_price_baseline` (
    region_id           STRING    NOT NULL,
    baseline_30d        FLOAT64   NOT NULL,   -- rolling 30-day average
    current_price       FLOAT64   NOT NULL,   -- price used for this computation
    price_delta_pct     FLOAT64   NOT NULL,   -- (current - baseline) / baseline
    n_days_in_window    INT64     NOT NULL,   -- number of days contributing to avg
    computed_at         TIMESTAMP NOT NULL,
    run_date            DATE      NOT NULL,
    pipeline_version    STRING    NOT NULL DEFAULT '1.0'
)
PARTITION BY run_date
CLUSTER BY region_id
OPTIONS (
    description = 'Daily 30-day rolling baseline for gas prices per US region'
);
""",

    "gpaf_config": """
-- Current GPAF multiplier per region — this is what the fare engine reads
-- Upserted daily; only the latest row per region is active
CREATE TABLE IF NOT EXISTS `{project}.gpaf_pipeline.gpaf_config` (
    region_id           STRING    NOT NULL,
    gpaf                FLOAT64   NOT NULL,   -- multiplier, e.g. 1.08
    fuel_surcharge_pct  FLOAT64   NOT NULL,   -- (gpaf-1) * 100, e.g. 8.0
    baseline_30d        FLOAT64   NOT NULL,
    current_price       FLOAT64   NOT NULL,
    price_delta_pct     FLOAT64   NOT NULL,
    alpha               FLOAT64   NOT NULL,   -- sensitivity coefficient
    is_active           BOOL      NOT NULL,   -- false if below trigger threshold
    computed_at         TIMESTAMP NOT NULL,
    valid_until         TIMESTAMP NOT NULL,   -- TTL for cache invalidation
    run_date            DATE      NOT NULL,
    pipeline_version    STRING    NOT NULL DEFAULT '1.0'
)
PARTITION BY run_date
CLUSTER BY region_id
OPTIONS (
    description = 'Current GPAF config per region, refreshed daily. Read by fare engine via Redis cache.'
);
""",

    "gpaf_history": """
-- Full append-only audit trail of all GPAF values over time
CREATE TABLE IF NOT EXISTS `{project}.gpaf_pipeline.gpaf_history` (
    region_id           STRING    NOT NULL,
    gpaf                FLOAT64   NOT NULL,
    fuel_surcharge_pct  FLOAT64   NOT NULL,
    baseline_30d        FLOAT64   NOT NULL,
    current_price       FLOAT64   NOT NULL,
    price_delta_pct     FLOAT64   NOT NULL,
    alpha               FLOAT64   NOT NULL,
    is_active           BOOL      NOT NULL,
    computed_at         TIMESTAMP NOT NULL,
    valid_until         TIMESTAMP NOT NULL,
    run_date            DATE      NOT NULL,
    pipeline_version    STRING    NOT NULL DEFAULT '1.0'
)
PARTITION BY run_date
CLUSTER BY region_id
OPTIONS (
    description = 'Append-only history of GPAF values. Never updated — use for trend analysis.'
);
""",

    "fare_adjustments": """
-- Per-trip fare adjustment log — written by the fare engine at dispatch
CREATE TABLE IF NOT EXISTS `{project}.gpaf_pipeline.fare_adjustments` (
    trip_id                 STRING    NOT NULL,
    region_id               STRING    NOT NULL,
    base_fare               FLOAT64   NOT NULL,   -- fare before GPAF
    fuel_surcharge          FLOAT64   NOT NULL,   -- dollar surcharge applied
    gpaf                    FLOAT64   NOT NULL,   -- multiplier used
    final_fare              FLOAT64   NOT NULL,   -- fare after GPAF
    driver_surcharge_share  FLOAT64   NOT NULL,   -- 85% of surcharge to driver
    surcharge_active        BOOL      NOT NULL,
    gas_baseline            FLOAT64,
    current_gas_price       FLOAT64,
    dispatched_at           TIMESTAMP NOT NULL,
    run_date                DATE      NOT NULL
)
PARTITION BY run_date
CLUSTER BY region_id, trip_id
OPTIONS (
    description = 'Per-trip fare adjustment records. Source of truth for driver payout reconciliation.'
);
""",

}


# =============================================================================
# SECTION 2: ANALYTICAL SQL QUERIES
# =============================================================================

ANALYTICAL_QUERIES = {

    "current_gpaf_all_regions": """
-- Latest GPAF value per region (for dashboard / Redis cache refresh)
SELECT
    region_id,
    gpaf,
    fuel_surcharge_pct,
    current_price,
    baseline_30d,
    ROUND(price_delta_pct * 100, 2)  AS price_delta_pct,
    is_active,
    valid_until
FROM `{project}.gpaf_pipeline.gpaf_config`
WHERE run_date = CURRENT_DATE()
ORDER BY region_id;
""",

    "gpaf_trend_90d": """
-- 90-day trend of GPAF per region (for dashboard time-series chart)
SELECT
    run_date,
    region_id,
    gpaf,
    current_price,
    baseline_30d,
    fuel_surcharge_pct,
    is_active
FROM `{project}.gpaf_pipeline.gpaf_history`
WHERE run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY region_id, run_date;
""",

    "ceiling_hit_frequency": """
-- How often does GPAF hit the ceiling? Alert if > 15% of days
SELECT
    region_id,
    COUNT(*)                                                    AS total_days,
    COUNTIF(gpaf >= 1.25)                                       AS ceiling_hits,
    ROUND(COUNTIF(gpaf >= 1.25) / COUNT(*) * 100, 1)           AS ceiling_hit_pct,
    ROUND(AVG(fuel_surcharge_pct), 2)                           AS avg_surcharge_pct,
    ROUND(MAX(fuel_surcharge_pct), 2)                           AS max_surcharge_pct
FROM `{project}.gpaf_pipeline.gpaf_history`
WHERE run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY region_id
ORDER BY ceiling_hit_pct DESC;
""",

    "driver_earnings_impact": """
-- Daily driver earnings uplift from GPAF surcharge
SELECT
    run_date,
    region_id,
    COUNT(trip_id)                              AS trips,
    ROUND(SUM(fuel_surcharge), 2)               AS total_surcharge_collected,
    ROUND(SUM(driver_surcharge_share), 2)       AS total_driver_uplift,
    ROUND(AVG(fuel_surcharge), 2)               AS avg_surcharge_per_trip,
    ROUND(AVG(gpaf), 4)                         AS avg_gpaf
FROM `{project}.gpaf_pipeline.fare_adjustments`
WHERE surcharge_active = TRUE
  AND run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY run_date, region_id
ORDER BY run_date DESC, region_id;
""",

    "pipeline_health_check": """
-- Data freshness check — alert if gpaf_config is stale
SELECT
    region_id,
    MAX(run_date)                               AS last_run_date,
    DATE_DIFF(CURRENT_DATE(), MAX(run_date), DAY) AS days_stale,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), MAX(run_date), DAY) = 0 THEN 'OK'
        WHEN DATE_DIFF(CURRENT_DATE(), MAX(run_date), DAY) = 1 THEN 'WARNING'
        ELSE 'STALE'
    END                                         AS freshness_status
FROM `{project}.gpaf_pipeline.gpaf_config`
GROUP BY region_id
ORDER BY days_stale DESC;
""",

}


# =============================================================================
# SECTION 3: BigQuery Loader Class
# =============================================================================

class BigQueryLoader:
    """
    Reads and writes GPAF data to BigQuery.

    Production usage:
        pip install google-cloud-bigquery
        export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

    Local dev:
        Uses mock mode — simulates BQ operations with in-memory storage.
    """

    def __init__(self, project: str = BQ_PROJECT if 'BQ_PROJECT' in dir() else "your-gcp-project",
                 mock: bool = True):
        self.project = project
        self.mock = mock
        self._mock_store: dict[str, list[dict]] = {}

        if not mock:
            try:
                from google.cloud import bigquery
                self.client = bigquery.Client(project=project)
            except ImportError:
                print("[WARN] google-cloud-bigquery not installed. Falling back to mock mode.")
                print("       Install with: pip install google-cloud-bigquery")
                self.mock = True

    # ── Schema Management ──

    def create_schema(self) -> None:
        """Create dataset and all tables if they don't exist."""
        if self.mock:
            print("[MOCK BQ] Schema creation — tables:")
            for name in DDL_STATEMENTS:
                if name != "create_dataset":
                    print(f"  ✓ gpaf_pipeline.{name}")
            return

        from google.cloud import bigquery
        for name, ddl in DDL_STATEMENTS.items():
            sql = ddl.format(project=self.project)
            job = self.client.query(sql)
            job.result()
            print(f"Created/verified: {name}")

    # ── Write Operations ──

    def insert_gas_prices(self, records: list[dict]) -> int:
        """Insert raw gas price records."""
        table = f"{self.project}.gpaf_pipeline.gas_prices_raw"
        return self._insert(table, records)

    def upsert_gpaf_config(self, gpaf_results: list[dict]) -> int:
        """
        Upsert current GPAF config — delete today's rows then re-insert.
        In production: use MERGE statement for atomic upsert.
        """
        table = f"{self.project}.gpaf_pipeline.gpaf_config"
        rows = [{**r, "run_date": datetime.utcnow().date().isoformat()} for r in gpaf_results]
        return self._insert(table, rows)

    def append_gpaf_history(self, gpaf_results: list[dict]) -> int:
        """Append GPAF results to immutable history table."""
        table = f"{self.project}.gpaf_pipeline.gpaf_history"
        rows = [{**r, "run_date": datetime.utcnow().date().isoformat()} for r in gpaf_results]
        return self._insert(table, rows)

    def log_fare_adjustment(self, adjustment: dict) -> int:
        """Log a single trip's fare adjustment."""
        table = f"{self.project}.gpaf_pipeline.fare_adjustments"
        row = {**adjustment, "run_date": datetime.utcnow().date().isoformat(),
               "dispatched_at": datetime.utcnow().isoformat()}
        return self._insert(table, [row])

    # ── Read Operations ──

    def get_current_gpaf(self, region_id: Optional[str] = None) -> list[dict]:
        """Fetch today's GPAF config for all (or one) region."""
        if self.mock:
            data = self._mock_store.get("gpaf_config", [])
            if region_id:
                data = [r for r in data if r.get("region_id") == region_id]
            return data

        query = ANALYTICAL_QUERIES["current_gpaf_all_regions"].format(project=self.project)
        if region_id:
            query = query.replace("ORDER BY region_id",
                                  f"AND region_id = '{region_id}'\nORDER BY region_id")
        df = self.client.query(query).to_dataframe()
        return df.to_dict("records")

    def get_gpaf_trend(self, days: int = 90) -> list[dict]:
        """Fetch GPAF trend data for dashboard time-series."""
        if self.mock:
            return self._generate_mock_trend(days)

        query = ANALYTICAL_QUERIES["gpaf_trend_90d"].format(project=self.project)
        df = self.client.query(query).to_dataframe()
        return df.to_dict("records")

    def run_health_check(self) -> list[dict]:
        """Check data freshness for all regions."""
        if self.mock:
            return [{"region_id": r, "days_stale": 0, "freshness_status": "OK"}
                    for r in ["US-CA", "US-TX", "US-NY", "US-FL"]]

        query = ANALYTICAL_QUERIES["pipeline_health_check"].format(project=self.project)
        df = self.client.query(query).to_dataframe()
        return df.to_dict("records")

    # ── Internal Helpers ──

    def _insert(self, table: str, rows: list[dict]) -> int:
        if self.mock:
            table_key = table.split(".")[-1]
            if table_key not in self._mock_store:
                self._mock_store[table_key] = []
            self._mock_store[table_key].extend(rows)
            print(f"[MOCK BQ] INSERT {len(rows)} rows → {table}")
            return len(rows)

        errors = self.client.insert_rows_json(table, rows)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        return len(rows)

    def _generate_mock_trend(self, days: int) -> list[dict]:
        """Generate realistic mock trend data for dashboard testing."""
        import random
        trend = []
        base_prices = {"US-CA": 4.60, "US-TX": 3.00, "US-NY": 3.50, "US-FL": 3.10}
        baselines = {r: p for r, p in base_prices.items()}

        for i in range(days, -1, -1):
            run_date = (datetime.utcnow() - timedelta(days=i)).date().isoformat()
            for region, base in base_prices.items():
                # Simulate gradual price trend + noise
                trend_factor = 1.0 + (days - i) * 0.001  # slight upward drift
                noise = random.uniform(-0.08, 0.12)
                current = round(base * trend_factor + noise, 3)
                baseline = round(baselines[region], 4)
                delta = (current - baseline) / baseline
                gpaf = max(1.0, min(1.25, 1 + 0.5 * delta)) if abs(delta) >= 0.05 else 1.0
                trend.append({
                    "run_date": run_date,
                    "region_id": region,
                    "gpaf": round(gpaf, 4),
                    "current_price": current,
                    "baseline_30d": baseline,
                    "fuel_surcharge_pct": round((gpaf - 1) * 100, 2),
                    "is_active": abs(delta) >= 0.05,
                })
                # Update rolling baseline (simple approximation)
                baselines[region] = round(0.97 * baseline + 0.03 * current, 4)

        return trend


# =============================================================================
# DEMO
# =============================================================================

BQ_PROJECT = "uber-data-platform"

if __name__ == "__main__":
    print("\n" + "=" * 55)
    print("  BigQuery Schema & Loader Demo")
    print("=" * 55)

    loader = BigQueryLoader(project=BQ_PROJECT, mock=True)

    print("\n1. Creating schema...")
    loader.create_schema()

    print("\n2. Inserting mock gas prices...")
    from gpaf_pipeline import GasPriceIngestion
    raw = GasPriceIngestion().fetch_mock()
    loader.insert_gas_prices([
        {"region_id": r.region_id, "price_usd": r.price_usd,
         "source": r.source, "fetched_at": r.fetched_at}
        for r in raw
    ])

    print("\n3. Inserting mock GPAF config...")
    mock_gpaf = [
        {"region_id": "US-CA", "gpaf": 1.08, "fuel_surcharge_pct": 8.0,
         "baseline_30d": 4.50, "current_price": 4.87, "price_delta_pct": 0.082,
         "alpha": 0.6, "is_active": True,
         "computed_at": datetime.utcnow().isoformat(),
         "valid_until": (datetime.utcnow() + timedelta(hours=24)).isoformat()},
        {"region_id": "US-TX", "gpaf": 1.0, "fuel_surcharge_pct": 0.0,
         "baseline_30d": 3.00, "current_price": 3.03, "price_delta_pct": 0.01,
         "alpha": 0.5, "is_active": False,
         "computed_at": datetime.utcnow().isoformat(),
         "valid_until": (datetime.utcnow() + timedelta(hours=24)).isoformat()},
    ]
    loader.upsert_gpaf_config(mock_gpaf)
    loader.append_gpaf_history(mock_gpaf)

    print("\n4. Health check...")
    health = loader.run_health_check()
    for h in health:
        print(f"  {h['region_id']:8s} → {h['freshness_status']}")

    print("\n5. Generating 90-day mock trend (first 3 rows)...")
    trend = loader.get_gpaf_trend(days=90)
    for row in trend[:3]:
        print(f"  {row['run_date']}  {row['region_id']:8s}  GPAF={row['gpaf']:.4f}")

    print(f"\n  ... and {len(trend) - 3} more rows")
    print("\n✅ BigQuery loader demo complete.\n")

    print("─" * 55)
    print("ANALYTICAL SQL QUERIES (copy into BQ console):")
    print("─" * 55)
    for name, sql in ANALYTICAL_QUERIES.items():
        print(f"\n-- {name}")
        print(sql.format(project=BQ_PROJECT).strip())
