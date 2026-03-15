# ⛽ GPAF — Gas Price Adjustment Factor

> A production-grade data engineering system that dynamically adjusts Uber ride fares based on real-time regional fuel prices — built by a driver who also happens to be a data engineer.

[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8+-017CEE?style=flat-square&logo=apache-airflow&logoColor=white)](https://airflow.apache.org)
[![BigQuery](https://img.shields.io/badge/BigQuery-GCP-4285F4?style=flat-square&logo=google-cloud&logoColor=white)](https://cloud.google.com/bigquery)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)](https://streamlit.io)
[![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)](LICENSE)

---

## The Problem

Uber drivers absorb 100% of fuel cost volatility. Gas prices swing **20–40% seasonally** and can spike over 60% during supply shocks — yet the fare algorithm doesn't account for any of it.

Uber's 2022 response was a flat **$0.50 surcharge per trip**, regardless of distance. A 2-mile ride and a 20-mile ride got identical compensation despite wildly different fuel consumption.

**GPAF fixes this with a continuous, proportional, automatic multiplier.**

---

## The Solution

```
GPAF = max(floor,  min(ceiling,  1 + α × (P_current − P_baseline) / P_baseline))

Final Fare = Base Fare × GPAF
```

| Parameter | Value | Description |
|---|---|---|
| `α` (alpha) | 0.45 – 0.60 | Sensitivity, tuned per market |
| `floor` | 1.00 | Riders never benefit from cheap gas at driver expense |
| `ceiling` | 1.25 | Max 25% surcharge — protects rider affordability |
| `trigger` | ±5% | Ignore noise; only activate on meaningful swings |
| `baseline` | 30-day rolling avg | Per-region, smooths short-term spikes |

### Example

> Gas baseline: **$3.50** → Current price: **$4.20** → Delta: **+20%**
>
> GPAF = 1 + 0.5 × 0.20 = **1.10**
>
> $12.00 base fare → **$13.20 final fare** (+$1.20 fuel surcharge)

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Apache Airflow DAG                    │
│              (Daily @ 06:00 UTC)                        │
│                                                          │
│  [EIA API] ──► ingest ──► validate ──► baselines        │
│                                           │              │
│                                      compute_gpaf        │
│                                      /    │    \         │
│                               [BigQuery] [Redis] [Alerts]│
└─────────────────────────────────────────────────────────┘
         │                    │
    [Streamlit]          [Fare Engine]
    Dashboard            (reads Redis
    (analytics)           at dispatch)
```

### Data Flow

1. **Ingest** — EIA API fetches weekly gas prices per US state (free, official)
2. **Validate** — Range checks, spike detection, duplicate guards
3. **Baseline** — 30-day rolling average computed per region from BigQuery history
4. **Compute** — GPAF formula applied with floor/ceiling guardrails
5. **Serve** — Results written to BigQuery (analytics) + Redis (fare engine, <1ms reads)
6. **Monitor** — Streamlit dashboard shows live GPAF, trends, and driver earnings impact

---

## Repository Structure

```
gpaf/
├── gpaf_pipeline.py        # Core pipeline: 5 modular classes, runs standalone
├── gpaf_airflow_dag.py     # Airflow DAG: 7 tasks, retries, XCom, alerts
├── gpaf_bigquery.py        # BQ schema DDL, loader class, 5 analytical SQL queries
├── gpaf_dashboard.py       # Streamlit dashboard: metrics, charts, simulator
├── GPAF_Feature_Spec.docx  # Product feature spec (PM-ready)
└── README.md               # This file
```

---

## Quickstart

### 1. Run the core pipeline (no dependencies needed)

```bash
python gpaf_pipeline.py
```

```
GPAF Config Output:
Region       Baseline    Current     GPAF    Surcharge   Active
--------------------------------------------------------------
US-CA      $   4.620  $   4.850   1.0800        +8.0%       YES
US-FL      $   3.109  $   3.250   1.0000        +0.0%        no
US-NY      $   3.516  $   3.650   1.0000        +0.0%        no
US-TX      $   2.994  $   3.100   1.0000        +0.0%        no
```

### 2. Run the Streamlit dashboard

```bash
pip install streamlit plotly pandas
streamlit run gpaf_dashboard.py
```

Open [http://localhost:8501](http://localhost:8501)

### 3. Test the BigQuery loader (mock mode)

```bash
pip install google-cloud-bigquery   # optional — falls back to mock without it
python gpaf_bigquery.py
```

### 4. Deploy the Airflow DAG

```bash
pip install apache-airflow apache-airflow-providers-google apache-airflow-providers-redis

# Copy DAG to your Airflow dags/ folder
cp gpaf_airflow_dag.py $AIRFLOW_HOME/dags/

# Set required Airflow Variables
airflow variables set EIA_API_KEY "your_eia_api_key_here"

# The DAG will appear in the UI as: gpaf_daily_pipeline
```

---

## Module Reference

### `gpaf_pipeline.py` — Core Classes

| Class | Responsibility |
|---|---|
| `GasPriceIngestion` | Fetches from EIA API; mock mode for local dev |
| `BaselineTransformer` | Computes 30-day rolling average per region |
| `GPAFCalculator` | Applies formula with floor/ceiling/trigger guardrails |
| `FareAdjustmentEngine` | Applies GPAF to any trip fare at dispatch time |
| `GPAFPipeline` | Orchestrates the full daily run; Airflow-ready |

### `gpaf_airflow_dag.py` — DAG Tasks

| Task | Description |
|---|---|
| `ingest_gas_prices` | EIA API fetch with XCom fallback cache |
| `validate_raw_data` | Range checks, spike detection, dedup |
| `compute_baselines` | BQ query for 30-day rolling avg |
| `compute_gpaf` | Formula + ceiling alert detection |
| `write_to_bigquery` | Upsert config + append history |
| `update_redis_cache` | Push to Redis with 25h TTL |
| `send_alerts` | Slack/PagerDuty on ceiling hits |

### `gpaf_bigquery.py` — Tables & Queries

| Table | Description |
|---|---|
| `gas_prices_raw` | Raw daily ingestion, partitioned by date |
| `gas_price_baseline` | 30-day rolling baselines per region |
| `gpaf_config` | Current GPAF per region (upserted daily) |
| `gpaf_history` | Append-only audit trail |
| `fare_adjustments` | Per-trip fare adjustment log |

Includes 5 ready-to-run analytical SQL queries: trend analysis, ceiling hit frequency, driver earnings uplift, and pipeline health check.

### `gpaf_dashboard.py` — Dashboard Sections

| Section | What it shows |
|---|---|
| Region cards | Live GPAF per market with active/ceiling badges |
| Gas price trend | Current price vs 30-day baseline overlay |
| GPAF time series | Multiplier trend across all regions |
| Fare simulator | Interactive breakdown: base → surcharge → driver take |
| GPAF heatmap | All regions × time, colour-coded by multiplier |
| Pipeline health | Data freshness status per region |

---

## Configuration

### Region tuning (`gpaf_pipeline.py` → `REGION_CONFIGS`)

```python
REGION_CONFIGS = {
    "US-CA": GPAFConfig(region_id="US-CA", alpha=0.60),  # Higher — CA is volatile
    "US-TX": GPAFConfig(region_id="US-TX", alpha=0.50),
    "US-NY": GPAFConfig(region_id="US-NY", alpha=0.50),
    ...
}
```

### Guardrails (per-region override)

```python
GPAFConfig(
    region_id="US-CA",
    alpha=0.60,               # Sensitivity coefficient
    floor=1.0,                # Minimum multiplier
    ceiling=1.25,             # Maximum multiplier
    trigger_threshold=0.05,   # Min delta to activate (5%)
    baseline_window_days=30,  # Rolling window length
)
```

---

## GCP Setup (Production)

```bash
# 1. Create GCP project and enable APIs
gcloud services enable bigquery.googleapis.com redis.googleapis.com

# 2. Create BigQuery dataset
bq mk --dataset --location=US your-project:gpaf_pipeline

# 3. Run schema creation
python -c "
from gpaf_bigquery import BigQueryLoader
loader = BigQueryLoader(project='your-project', mock=False)
loader.create_schema()
"

# 4. Set up Airflow connections
airflow connections add bigquery_default \
    --conn-type google_cloud_platform \
    --conn-extra '{"project": "your-project", "key_path": "/path/to/sa.json"}'

airflow connections add redis_fare_cache \
    --conn-type redis \
    --conn-host your-redis-host \
    --conn-port 6379
```

---

## Why This Matters (Business Case)

| Metric | Target |
|---|---|
| Driver earnings variance | −15% weekly volatility |
| Driver retention (high-gas markets) | +10% vs control |
| Rider cancellation rate on adjusted fares | Within +2% of baseline |
| Pipeline data freshness SLA | Gas prices ≤ 24h old |
| Ceiling trigger alert threshold | Alert if > 15% of trips hit 1.25× |

**Versus Uber's flat $0.50 surcharge:**
- A 20-mile trip in California during a gas spike earns ~$3.60 more with GPAF vs $0.50 flat
- Surcharge auto-adjusts without manual intervention from ops teams
- Full receipt line-item ("Fuel surcharge: $X.XX") builds rider trust

---

## Data Sources

- **[EIA Open Data API](https://www.eia.gov/opendata/)** — U.S. Energy Information Administration. Free, official, weekly state-level gas prices. No key required for basic endpoints.
- **[GasBuddy API](https://www.gasbuddy.com)** — Crowdsourced, near real-time, city-level granularity. Requires commercial key.

---

## Roadmap

- [ ] Add GasBuddy as secondary real-time source with weighted blending
- [ ] Expand to international markets (India, UK, Brazil)
- [ ] ML-based alpha tuning using historical demand elasticity data
- [ ] dbt models for analytical layer on top of BigQuery raw tables
- [ ] Great Expectations data quality suite integration
- [ ] Terraform infrastructure-as-code for full GCP deployment

---

## Author
# Suresh Shanmugam

Built by an Uber driver and data engineer who got tired of watching gas prices move while fares stood still.

*This is a portfolio/concept project. Not affiliated with Uber Technologies, Inc.*

---

## License

MIT — use freely, contribute back.
