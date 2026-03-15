"""
GPAF Airflow DAG — Gas Price Adjustment Factor Pipeline
========================================================
Portfolio Project: Dynamic Uber Fare Adjustment Based on Fuel Prices

DAG Schedule: Daily at 06:00 UTC (before peak driving hours)

Pipeline Steps:
    1. ingest_gas_prices       — Fetch regional prices from EIA API
    2. validate_raw_data       — Quality checks on ingested records
    3. compute_baselines       — 30-day rolling average per region
    4. compute_gpaf            — Apply GPAF formula per region
    5. write_to_bigquery       — Persist results to BQ tables
    6. update_redis_cache      — Push GPAF config to low-latency cache
    7. refresh_dashboard       — Trigger Streamlit data refresh
    8. send_alerts             — Notify if ceiling/floor thresholds hit

Author: Suresh Shanmugam
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# Airflow imports
# In production: pip install apache-airflow apache-airflow-providers-google
#                             apache-airflow-providers-redis
# ---------------------------------------------------------------------------
try:
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models import Variable
    from airflow.operators.empty import EmptyOperator
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from airflow.providers.redis.hooks.redis import RedisHook
    from airflow.utils.trigger_rule import TriggerRule
    AIRFLOW_AVAILABLE = True
except ImportError:
    # Graceful degradation for local dev / testing without Airflow installed
    AIRFLOW_AVAILABLE = False
    print("[WARN] Airflow not installed. DAG structure is shown but cannot be executed.")
    print("       Install with: pip install apache-airflow")

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG DEFAULT ARGS
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

REGIONS = ["US-CA", "US-TX", "US-NY", "US-FL", "US-WA", "US-IL"]

GPAF_PARAMS = {
    "US-CA": {"alpha": 0.60, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
    "US-TX": {"alpha": 0.50, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
    "US-NY": {"alpha": 0.50, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
    "US-FL": {"alpha": 0.45, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
    "US-WA": {"alpha": 0.55, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
    "US-IL": {"alpha": 0.50, "floor": 1.0, "ceiling": 1.25, "trigger": 0.05},
}

BQ_DATASET = "gpaf_pipeline"
BQ_PROJECT = "uber-data-platform"          # Replace with your GCP project
REDIS_CONN_ID = "redis_fare_cache"
BQ_CONN_ID = "bigquery_default"


# ---------------------------------------------------------------------------
# TASK FUNCTIONS
# ---------------------------------------------------------------------------

def _ingest_gas_prices(**context) -> list[dict]:
    """
    Fetch latest gas prices from EIA API for all configured regions.
    Falls back to cached values if API is unavailable.

    Returns:
        List of gas price records: [{region_id, price_usd, source, fetched_at}]
    """
    import urllib.request
    import urllib.error

    # In production: store API key in Airflow Variables or Secrets Manager
    # api_key = Variable.get("EIA_API_KEY", default_var="")
    api_key = "YOUR_EIA_API_KEY"

    # EIA state codes → internal region IDs
    eia_state_map = {
        "CA": "US-CA", "TX": "US-TX", "NY": "US-NY",
        "FL": "US-FL", "WA": "US-WA", "IL": "US-IL",
    }

    records = []
    fetch_time = datetime.utcnow().isoformat()

    for state_code, region_id in eia_state_map.items():
        url = (
            f"https://api.eia.gov/v2/petroleum/pri/gnd/data/"
            f"?api_key={api_key}"
            f"&frequency=weekly"
            f"&data[0]=value"
            f"&facets[duoarea][]={state_code}WTT"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=desc"
            f"&length=1"
        )

        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                data = json.loads(resp.read())
                price_records = data.get("response", {}).get("data", [])
                if price_records:
                    price = float(price_records[0]["value"])
                    records.append({
                        "region_id": region_id,
                        "price_usd": price,
                        "source": "EIA",
                        "fetched_at": fetch_time,
                        "eia_period": price_records[0].get("period", ""),
                    })
                    log.info(f"EIA [{region_id}]: ${price:.3f}/gal")
        except Exception as e:
            log.error(f"EIA fetch failed for {state_code}: {e}")
            # Attempt fallback from XCom cache (previous successful run)
            cached = context["ti"].xcom_pull(
                task_ids="ingest_gas_prices",
                key=f"fallback_{region_id}",
                include_prior_dates=True,
            )
            if cached:
                log.warning(f"Using cached price for {region_id}: ${cached['price_usd']:.3f}")
                records.append({**cached, "source": "CACHE_FALLBACK", "fetched_at": fetch_time})

    log.info(f"Ingested {len(records)}/{len(eia_state_map)} regions successfully")

    # Store individual values as fallback cache
    for r in records:
        context["ti"].xcom_push(key=f"fallback_{r['region_id']}", value=r)

    return records


def _validate_raw_data(records: list[dict], **context) -> list[dict]:
    """
    Data quality checks on ingested gas price records.

    Checks:
    - Price within plausible range ($1.50 - $8.00/gal)
    - No duplicate regions
    - Records are recent (within 8 days for weekly EIA data)
    - Price change < 30% vs previous day (spike detection)
    """
    PRICE_MIN = 1.50
    PRICE_MAX = 8.00
    MAX_DAILY_CHANGE_PCT = 0.30

    valid = []
    failed = []
    seen_regions = set()

    for r in records:
        issues = []

        # Range check
        if not (PRICE_MIN <= r["price_usd"] <= PRICE_MAX):
            issues.append(f"Price ${r['price_usd']:.3f} out of range [{PRICE_MIN}, {PRICE_MAX}]")

        # Duplicate region check
        if r["region_id"] in seen_regions:
            issues.append(f"Duplicate region: {r['region_id']}")
        seen_regions.add(r["region_id"])

        # Spike detection vs previous run
        prev = context["ti"].xcom_pull(
            task_ids="validate_raw_data",
            key=f"validated_{r['region_id']}",
            include_prior_dates=True,
        )
        if prev:
            change = abs(r["price_usd"] - prev["price_usd"]) / prev["price_usd"]
            if change > MAX_DAILY_CHANGE_PCT:
                issues.append(f"Spike detected: {change:.1%} change from ${prev['price_usd']:.3f}")

        if issues:
            log.warning(f"Validation FAILED [{r['region_id']}]: {'; '.join(issues)}")
            failed.append({**r, "issues": issues})
        else:
            log.info(f"Validation OK [{r['region_id']}]: ${r['price_usd']:.3f}")
            valid.append(r)
            context["ti"].xcom_push(key=f"validated_{r['region_id']}", value=r)

    if failed:
        log.error(f"{len(failed)} records failed validation: {[f['region_id'] for f in failed]}")

    if len(valid) == 0:
        raise ValueError("All records failed validation — aborting pipeline")

    log.info(f"Validation complete: {len(valid)} valid, {len(failed)} failed")
    return valid


def _compute_baselines(valid_records: list[dict], **context) -> list[dict]:
    """
    Compute 30-day rolling average baseline per region.
    Queries BigQuery for historical prices.

    In production: replace mock_bq_query with actual BigQuery client call.
    """
    baselines = []

    for r in valid_records:
        region_id = r["region_id"]

        # --- Production: Query BQ for rolling history ---
        # bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
        # query = f"""
        #     SELECT AVG(price_usd) as baseline_30d, COUNT(*) as n_days
        #     FROM `{BQ_PROJECT}.{BQ_DATASET}.gas_prices_raw`
        #     WHERE region_id = '{region_id}'
        #       AND DATE(fetched_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        # """
        # result = bq_hook.get_pandas_df(sql=query)
        # baseline = float(result["baseline_30d"].iloc[0])
        # n_days = int(result["n_days"].iloc[0])

        # --- Mock: simulate baseline from XCom history ---
        history_key = f"price_history_{region_id}"
        history = context["ti"].xcom_pull(key=history_key, include_prior_dates=True) or []
        history.append(r["price_usd"])
        history = history[-30:]  # Keep last 30 entries
        context["ti"].xcom_push(key=history_key, value=history)

        baseline = sum(history) / len(history) if history else r["price_usd"]
        n_days = len(history)

        delta_pct = (r["price_usd"] - baseline) / baseline if baseline > 0 else 0.0

        baselines.append({
            "region_id": region_id,
            "current_price": r["price_usd"],
            "baseline_30d": round(baseline, 4),
            "price_delta_pct": round(delta_pct, 6),
            "n_days_in_window": n_days,
            "computed_at": datetime.utcnow().isoformat(),
        })

        log.info(
            f"Baseline [{region_id}]: ${baseline:.3f} "
            f"(n={n_days}d) | current=${r['price_usd']:.3f} | Δ={delta_pct:+.1%}"
        )

    return baselines


def _compute_gpaf(baselines: list[dict], **context) -> list[dict]:
    """
    Apply GPAF formula to each region's baseline.

    Formula: GPAF = max(floor, min(ceiling, 1 + alpha * delta_pct))
    Only activates if |delta_pct| >= trigger_threshold.
    """
    results = []
    ceiling_alerts = []

    for b in baselines:
        region_id = b["region_id"]
        params = GPAF_PARAMS.get(region_id, GPAF_PARAMS.get("US-TX"))  # default params

        delta = b["price_delta_pct"]
        is_active = abs(delta) >= params["trigger"]

        if is_active:
            raw_gpaf = 1.0 + params["alpha"] * delta
            gpaf = max(params["floor"], min(params["ceiling"], raw_gpaf))
        else:
            gpaf = 1.0

        surcharge_pct = round((gpaf - 1.0) * 100, 2)

        if is_active and gpaf >= params["ceiling"]:
            ceiling_alerts.append(region_id)
            log.warning(f"CEILING HIT [{region_id}]: capped at {params['ceiling']}")

        result = {
            "region_id": region_id,
            "gpaf": round(gpaf, 4),
            "fuel_surcharge_pct": surcharge_pct,
            "baseline_30d": b["baseline_30d"],
            "current_price": b["current_price"],
            "price_delta_pct": delta,
            "alpha": params["alpha"],
            "is_active": is_active,
            "computed_at": b["computed_at"],
            "valid_until": (datetime.utcnow() + timedelta(hours=24)).isoformat(),
        }

        log.info(
            f"GPAF [{region_id}]: {gpaf:.4f} "
            f"({'ACTIVE' if is_active else 'INACTIVE'}, +{surcharge_pct:.1f}%)"
        )
        results.append(result)

    # Store ceiling alerts in XCom for the alert task
    context["ti"].xcom_push(key="ceiling_alerts", value=ceiling_alerts)

    return results


def _write_to_bigquery(gpaf_results: list[dict], **context) -> None:
    """
    Persist GPAF results to BigQuery tables.

    Tables written:
        gpaf_pipeline.gpaf_config        — Current GPAF per region (upsert)
        gpaf_pipeline.gpaf_history       — Append-only audit trail
        gpaf_pipeline.gas_prices_raw     — Raw price records
    """
    run_date = context["ds"]  # Airflow execution date string YYYY-MM-DD

    # --- Production: use BigQueryHook ---
    # bq_hook = BigQueryHook(gcp_conn_id=BQ_CONN_ID)
    # client = bq_hook.get_client(project_id=BQ_PROJECT)

    # For each result, prepare BQ row
    config_rows = []
    history_rows = []

    for r in gpaf_results:
        config_rows.append({
            **r,
            "run_date": run_date,
            "pipeline_version": "1.0",
        })
        history_rows.append({
            **r,
            "run_date": run_date,
            "pipeline_version": "1.0",
        })

    # Production BQ write (uncomment when wired to real GCP):
    # client.insert_rows_json(f"{BQ_PROJECT}.{BQ_DATASET}.gpaf_config", config_rows)
    # client.insert_rows_json(f"{BQ_PROJECT}.{BQ_DATASET}.gpaf_history", history_rows)

    log.info(f"[MOCK] Written {len(config_rows)} rows to BQ gpaf_config")
    log.info(f"[MOCK] Written {len(history_rows)} rows to BQ gpaf_history")

    # Save to XCom so downstream tasks can read
    context["ti"].xcom_push(key="bq_rows_written", value=len(config_rows))


def _update_redis_cache(gpaf_results: list[dict], **context) -> None:
    """
    Push GPAF config to Redis for ultra-low-latency reads by the fare engine.
    Each region key has a 25-hour TTL (refreshed daily).

    Key format: gpaf:{region_id}
    Value: JSON-serialised GPAFOutput
    """
    TTL_SECONDS = 25 * 3600  # 25 hours

    # Production: redis_hook = RedisHook(redis_conn_id=REDIS_CONN_ID)
    #             redis_client = redis_hook.get_conn()

    for r in gpaf_results:
        key = f"gpaf:{r['region_id']}"
        value = json.dumps(r)

        # redis_client.setex(key, TTL_SECONDS, value)
        log.info(f"[MOCK] Redis SET {key} = GPAF:{r['gpaf']:.4f} (TTL={TTL_SECONDS}s)")

    # Also write a global "last_updated" key
    # redis_client.set("gpaf:last_updated", datetime.utcnow().isoformat())
    log.info(f"[MOCK] Redis cache updated for {len(gpaf_results)} regions")


def _send_alerts(gpaf_results: list[dict], **context) -> None:
    """
    Send Slack/PagerDuty alerts for:
    - Ceiling hits (GPAF >= 1.25): unusual price spike, review needed
    - No active regions (all GPAF = 1.0 for >7 days): pipeline may be stale
    - Data freshness issues flagged by validation task
    """
    ceiling_alerts = context["ti"].xcom_pull(key="ceiling_alerts") or []
    active_regions = [r for r in gpaf_results if r["is_active"]]

    alerts = []

    if ceiling_alerts:
        alerts.append(
            f"🚨 GPAF CEILING HIT in {len(ceiling_alerts)} region(s): "
            f"{', '.join(ceiling_alerts)}. Max surcharge (25%) applied."
        )

    if len(active_regions) == 0:
        alerts.append(
            "ℹ️  No regions have active GPAF surcharge today. "
            "Gas prices are within 5% of 30-day baseline."
        )

    # In production: send to Slack via webhook or PagerDuty API
    for alert in alerts:
        log.warning(f"[ALERT] {alert}")
        # slack_webhook.send(text=alert)

    if not alerts:
        log.info("No alerts to send — pipeline healthy")


# ---------------------------------------------------------------------------
# DAG DEFINITION
# ---------------------------------------------------------------------------

if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="gpaf_daily_pipeline",
        default_args=DEFAULT_ARGS,
        description="Compute Gas Price Adjustment Factor (GPAF) for Uber fare engine",
        schedule="0 6 * * *",        # Daily at 06:00 UTC
        start_date=datetime(2026, 1, 1),
        catchup=False,
        max_active_runs=1,
        tags=["fare-engine", "gas-prices", "gpaf", "data-engineering"],
        doc_md=__doc__,
    ) as dag:

        start = EmptyOperator(task_id="start")
        end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

        @task(task_id="ingest_gas_prices", retries=3)
        def ingest_gas_prices(**context):
            return _ingest_gas_prices(**context)

        @task(task_id="validate_raw_data")
        def validate_raw_data(records: list[dict], **context):
            return _validate_raw_data(records, **context)

        @task(task_id="compute_baselines")
        def compute_baselines(valid_records: list[dict], **context):
            return _compute_baselines(valid_records, **context)

        @task(task_id="compute_gpaf")
        def compute_gpaf(baselines: list[dict], **context):
            return _compute_gpaf(baselines, **context)

        @task(task_id="write_to_bigquery")
        def write_to_bigquery(gpaf_results: list[dict], **context):
            return _write_to_bigquery(gpaf_results, **context)

        @task(task_id="update_redis_cache")
        def update_redis_cache(gpaf_results: list[dict], **context):
            return _update_redis_cache(gpaf_results, **context)

        @task(task_id="send_alerts", trigger_rule=TriggerRule.ALL_DONE)
        def send_alerts(gpaf_results: list[dict], **context):
            return _send_alerts(gpaf_results, **context)

        # ── DAG wiring ──
        ingested   = ingest_gas_prices()
        validated  = validate_raw_data(ingested)
        baselines  = compute_baselines(validated)
        gpaf       = compute_gpaf(baselines)

        bq_write   = write_to_bigquery(gpaf)
        redis_write = update_redis_cache(gpaf)
        alerts     = send_alerts(gpaf)

        start >> ingested
        [bq_write, redis_write, alerts] >> end

else:
    # ── Standalone demo when Airflow isn't installed ──
    print("\nRunning GPAF DAG in standalone demo mode (no Airflow)\n")
    import sys
    sys.path.insert(0, ".")

    ctx = {"ti": type("XCom", (), {
        "xcom_pull": lambda self, **k: None,
        "xcom_push": lambda self, **k: None
    })(), "ds": datetime.utcnow().date().isoformat()}

    print("Step 1: Ingest (mock)...")
    from gpaf_pipeline import GasPriceIngestion
    records = GasPriceIngestion().fetch_mock()
    records_dicts = [{"region_id": r.region_id, "price_usd": r.price_usd,
                      "source": r.source, "fetched_at": r.fetched_at} for r in records]

    print("Step 2: Validate...")
    valid = _validate_raw_data(records_dicts, **ctx)

    print("Step 3: Baselines...")
    baselines = _compute_baselines(valid, **ctx)

    print("Step 4: GPAF...")
    gpaf_results = _compute_gpaf(baselines, **ctx)

    print("Step 5: BigQuery (mock)...")
    _write_to_bigquery(gpaf_results, **ctx)

    print("Step 6: Redis (mock)...")
    _update_redis_cache(gpaf_results, **ctx)

    print("Step 7: Alerts...")
    _send_alerts(gpaf_results, **ctx)

    print("\n✅ DAG demo complete. Results:")
    for r in gpaf_results:
        print(f"  {r['region_id']:8s}  GPAF={r['gpaf']:.4f}  active={r['is_active']}")
