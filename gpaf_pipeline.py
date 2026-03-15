"""
GPAF Data Pipeline — Gas Price Adjustment Factor
=================================================
Portfolio Project: Dynamic Uber Fare Adjustment Based on Fuel Prices

Architecture:
  1. Ingestion   — Fetch regional gas prices from EIA & GasBuddy APIs
  2. Transform   — Compute 30-day rolling baseline per region
  3. Compute     — Calculate GPAF multiplier per region
  4. Serve       — Write to gpaf_config table (read by fare engine at dispatch time)
  5. Audit       — Log all fare adjustments for analytics

Author: Suresh Shanmugam
Version: 1.0
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from typing import Optional
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────

@dataclass
class GPAFConfig:
    """Tunable parameters per market region."""
    region_id: str
    alpha: float = 0.5          # Sensitivity: how aggressively fares respond
    floor: float = 1.0          # Minimum GPAF (riders never benefit from cheap gas)
    ceiling: float = 1.25       # Maximum GPAF (protects rider affordability)
    trigger_threshold: float = 0.05  # Only activate if delta >= 5%
    baseline_window_days: int = 30

REGION_CONFIGS = {
    "US-CA": GPAFConfig(region_id="US-CA", alpha=0.6),  # Higher alpha for CA (extreme volatility)
    "US-TX": GPAFConfig(region_id="US-TX", alpha=0.5),
    "US-NY": GPAFConfig(region_id="US-NY", alpha=0.5),
    "US-FL": GPAFConfig(region_id="US-FL", alpha=0.45),
    "DEFAULT": GPAFConfig(region_id="DEFAULT", alpha=0.5),
}


# ─────────────────────────────────────────────
# DATA MODELS
# ─────────────────────────────────────────────

@dataclass
class GasPriceRecord:
    region_id: str
    price_usd: float
    source: str
    fetched_at: str


@dataclass
class BaselineRecord:
    region_id: str
    baseline_30d: float
    current_price: float
    price_delta_pct: float
    computed_at: str


@dataclass
class GPAFOutput:
    region_id: str
    gpaf: float
    fuel_surcharge_pct: float  # % above base fare
    baseline: float
    current_price: float
    alpha: float
    is_active: bool            # False if below trigger threshold
    computed_at: str
    valid_until: str           # UTC, refreshed daily


# ─────────────────────────────────────────────
# STEP 1: INGESTION — Fetch Gas Prices
# ─────────────────────────────────────────────

class GasPriceIngestion:
    """
    Fetches regional gas prices from the U.S. EIA API.
    EIA API v2 is free and official — no key required for basic endpoints.
    Docs: https://www.eia.gov/opendata/
    """

    EIA_BASE = "https://api.eia.gov/v2/petroleum/pri/gnd/data/"
    GASBUDDY_BASE = "https://api.gasbuddy.com/v1/price"  # Illustrative — requires key

    # EIA region codes → our internal region IDs
    EIA_REGION_MAP = {
        "CA": "US-CA",
        "TX": "US-TX",
        "NY": "US-NY",
        "FL": "US-FL",
    }

    def fetch_eia(self, api_key: str, state_code: str) -> Optional[GasPriceRecord]:
        """
        Fetch latest regular unleaded gas price for a US state from EIA.
        Returns price per gallon in USD.
        """
        params = f"?api_key={api_key}&frequency=weekly&data[0]=value&facets[duoarea][]={state_code}WTT&sort[0][column]=period&sort[0][direction]=desc&length=1"
        url = self.EIA_BASE + params

        try:
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                records = data.get("response", {}).get("data", [])
                if not records:
                    log.warning(f"No EIA data for state {state_code}")
                    return None

                price = float(records[0]["value"])
                region = self.EIA_REGION_MAP.get(state_code, "DEFAULT")

                log.info(f"EIA: {region} gas price = ${price:.3f}/gal")
                return GasPriceRecord(
                    region_id=region,
                    price_usd=price,
                    source="EIA",
                    fetched_at=datetime.utcnow().isoformat()
                )

        except (urllib.error.URLError, KeyError, ValueError) as e:
            log.error(f"EIA fetch failed for {state_code}: {e}")
            return None

    def fetch_mock(self) -> list[GasPriceRecord]:
        """
        Mock gas prices for local development / testing.
        Simulates realistic 2024-2026 price environment.
        """
        mock_prices = {
            "US-CA": 4.85,
            "US-TX": 3.10,
            "US-NY": 3.65,
            "US-FL": 3.25,
        }
        now = datetime.utcnow().isoformat()
        return [
            GasPriceRecord(region_id=region, price_usd=price, source="MOCK", fetched_at=now)
            for region, price in mock_prices.items()
        ]


# ─────────────────────────────────────────────
# STEP 2: TRANSFORM — Compute 30-Day Baseline
# ─────────────────────────────────────────────

class BaselineTransformer:
    """
    Computes the 30-day rolling average gas price per region.
    In production this would query a time-series table in Redshift/BigQuery.
    Here we simulate with a lightweight in-memory store.
    """

    def __init__(self):
        # In production: replace with DB queries
        # Schema: { region_id: [{"price": float, "date": str}, ...] }
        self._history: dict[str, list[dict]] = {}

    def add_price(self, record: GasPriceRecord):
        """Append new price to history (deduped by date)."""
        if record.region_id not in self._history:
            self._history[record.region_id] = []

        today = datetime.utcnow().date().isoformat()
        existing_dates = {e["date"] for e in self._history[record.region_id]}
        if today not in existing_dates:
            self._history[record.region_id].append({
                "price": record.price_usd,
                "date": today
            })

        # Keep only last 35 days (buffer)
        self._history[record.region_id] = sorted(
            self._history[record.region_id], key=lambda x: x["date"]
        )[-35:]

    def compute_baseline(self, region_id: str, current_price: float,
                         window_days: int = 30) -> BaselineRecord:
        """
        Calculate rolling 30-day average and price delta.
        Falls back to current price if insufficient history.
        """
        history = self._history.get(region_id, [])
        cutoff = (datetime.utcnow() - timedelta(days=window_days)).date().isoformat()
        window = [h["price"] for h in history if h["date"] >= cutoff]

        if len(window) < 7:
            log.warning(f"Insufficient history for {region_id} ({len(window)} days). Using current price as baseline.")
            baseline = current_price
        else:
            baseline = sum(window) / len(window)

        delta_pct = (current_price - baseline) / baseline if baseline > 0 else 0.0

        log.info(f"Baseline [{region_id}]: ${baseline:.3f} (n={len(window)} days) | current=${current_price:.3f} | delta={delta_pct:+.1%}")

        return BaselineRecord(
            region_id=region_id,
            baseline_30d=round(baseline, 4),
            current_price=current_price,
            price_delta_pct=round(delta_pct, 6),
            computed_at=datetime.utcnow().isoformat()
        )


# ─────────────────────────────────────────────
# STEP 3: COMPUTE — Calculate GPAF
# ─────────────────────────────────────────────

class GPAFCalculator:
    """
    Core GPAF formula:
        GPAF = max(floor, min(ceiling,  1 + alpha * delta_pct))
    Only activates if |delta| >= trigger_threshold.
    """

    def compute(self, baseline: BaselineRecord, config: GPAFConfig) -> GPAFOutput:
        delta = baseline.price_delta_pct
        is_active = abs(delta) >= config.trigger_threshold

        if is_active:
            raw_gpaf = 1.0 + config.alpha * delta
            gpaf = max(config.floor, min(config.ceiling, raw_gpaf))
        else:
            gpaf = 1.0  # Below threshold — no adjustment

        surcharge_pct = (gpaf - 1.0) * 100
        valid_until = (datetime.utcnow() + timedelta(hours=24)).isoformat()

        result = GPAFOutput(
            region_id=config.region_id,
            gpaf=round(gpaf, 4),
            fuel_surcharge_pct=round(surcharge_pct, 2),
            baseline=baseline.baseline_30d,
            current_price=baseline.current_price,
            alpha=config.alpha,
            is_active=is_active,
            computed_at=datetime.utcnow().isoformat(),
            valid_until=valid_until
        )

        log.info(
            f"GPAF [{config.region_id}]: {gpaf:.4f} "
            f"({'ACTIVE +' if is_active else 'INACTIVE'}{surcharge_pct:.1f}%) "
            f"[delta={delta:+.1%}, alpha={config.alpha}]"
        )

        # Alert if ceiling triggered
        if is_active and gpaf >= config.ceiling:
            log.warning(f"CEILING HIT [{config.region_id}]: GPAF capped at {config.ceiling}. "
                        f"Raw would have been {1 + config.alpha * delta:.4f}")

        return result


# ─────────────────────────────────────────────
# STEP 4: SERVE — Apply to Fare at Dispatch
# ─────────────────────────────────────────────

class FareAdjustmentEngine:
    """
    Called at ride dispatch time to apply GPAF to base fare.
    In production: reads gpaf_config table from Redis (low-latency cache).
    """

    def __init__(self, gpaf_store: dict[str, GPAFOutput]):
        self._store = gpaf_store  # region_id -> GPAFOutput

    def adjust_fare(self, region_id: str, base_fare: float, trip_id: str) -> dict:
        """Apply GPAF to base fare and return full fare breakdown."""
        config_key = region_id if region_id in self._store else "DEFAULT"
        gpaf_data = self._store.get(config_key)

        if gpaf_data is None or not gpaf_data.is_active:
            return {
                "trip_id": trip_id,
                "region_id": region_id,
                "base_fare": round(base_fare, 2),
                "fuel_surcharge": 0.00,
                "gpaf": 1.0,
                "final_fare": round(base_fare, 2),
                "surcharge_active": False
            }

        fuel_surcharge = round(base_fare * (gpaf_data.gpaf - 1.0), 2)
        final_fare = round(base_fare * gpaf_data.gpaf, 2)
        driver_surcharge_share = round(fuel_surcharge * 0.85, 2)  # 85% to driver

        result = {
            "trip_id": trip_id,
            "region_id": region_id,
            "base_fare": round(base_fare, 2),
            "fuel_surcharge": fuel_surcharge,
            "gpaf": gpaf_data.gpaf,
            "final_fare": final_fare,
            "driver_surcharge_share": driver_surcharge_share,
            "surcharge_active": True,
            "gas_baseline": gpaf_data.baseline,
            "current_gas_price": gpaf_data.current_price,
        }

        log.info(f"Fare [{trip_id}]: ${base_fare:.2f} → ${final_fare:.2f} (surcharge: ${fuel_surcharge:.2f})")
        return result


# ─────────────────────────────────────────────
# STEP 5: PIPELINE ORCHESTRATOR
# ─────────────────────────────────────────────

class GPAFPipeline:
    """
    Orchestrates the full daily pipeline run.
    In production: triggered by Apache Airflow DAG on a daily schedule.
    """

    def __init__(self, use_mock: bool = True):
        self.ingestion = GasPriceIngestion()
        self.transformer = BaselineTransformer()
        self.calculator = GPAFCalculator()
        self.use_mock = use_mock
        self.gpaf_store: dict[str, GPAFOutput] = {}

        # Seed history with 30 days of mock historical data
        self._seed_history()

    def _seed_history(self):
        """Populate price history to simulate 30 days of data."""
        import random
        base_prices = {"US-CA": 4.60, "US-TX": 3.00, "US-NY": 3.50, "US-FL": 3.10}
        for region, base in base_prices.items():
            for i in range(30, 0, -1):
                date = (datetime.utcnow() - timedelta(days=i)).date().isoformat()
                # Simulate mild trend + noise
                price = round(base + random.uniform(-0.15, 0.15), 3)
                if region not in self.transformer._history:
                    self.transformer._history[region] = []
                self.transformer._history[region].append({"price": price, "date": date})

    def run(self) -> dict[str, GPAFOutput]:
        """Execute the full pipeline and return GPAF config per region."""
        log.info("=" * 55)
        log.info("GPAF PIPELINE — Daily Run")
        log.info(f"Timestamp: {datetime.utcnow().isoformat()}")
        log.info("=" * 55)

        # Step 1: Ingest
        records = self.ingestion.fetch_mock() if self.use_mock else []
        log.info(f"Ingested {len(records)} gas price records")

        # Step 2: Add to history + compute baselines
        baselines = {}
        for record in records:
            self.transformer.add_price(record)
            config = REGION_CONFIGS.get(record.region_id, REGION_CONFIGS["DEFAULT"])
            baseline = self.transformer.compute_baseline(
                record.region_id, record.price_usd, config.baseline_window_days
            )
            baselines[record.region_id] = baseline

        # Step 3: Compute GPAF
        for region_id, baseline in baselines.items():
            config = REGION_CONFIGS.get(region_id, REGION_CONFIGS["DEFAULT"])
            gpaf_output = self.calculator.compute(baseline, config)
            self.gpaf_store[region_id] = gpaf_output

        log.info(f"\nGPAF Config written for {len(self.gpaf_store)} regions")
        return self.gpaf_store


# ─────────────────────────────────────────────
# DEMO / TEST HARNESS
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("\n" + "="*55)
    print("  GPAF Pipeline Demo")
    print("="*55)

    # Run the pipeline
    pipeline = GPAFPipeline(use_mock=True)
    gpaf_store = pipeline.run()

    # Print GPAF summary table
    print("\n📊 GPAF Config Output:")
    print(f"{'Region':<10} {'Baseline':>10} {'Current':>10} {'GPAF':>8} {'Surcharge':>12} {'Active':>8}")
    print("-" * 62)
    for region, g in sorted(gpaf_store.items()):
        status = "YES" if g.is_active else "no"
        print(f"{region:<10} ${g.baseline:>8.3f}  ${g.current_price:>8.3f}  {g.gpaf:>7.4f}  {g.fuel_surcharge_pct:>+10.1f}%  {status:>8}")

    # Simulate fare adjustments for sample trips
    fare_engine = FareAdjustmentEngine(gpaf_store)
    print("\n🚗 Sample Fare Adjustments:")
    print(f"{'Trip ID':<15} {'Region':<10} {'Base':>8} {'Surcharge':>12} {'Final':>8}")
    print("-" * 58)

    test_trips = [
        ("TRIP-001", "US-CA", 12.50),
        ("TRIP-002", "US-TX",  8.75),
        ("TRIP-003", "US-NY", 22.00),
        ("TRIP-004", "US-FL", 15.00),
        ("TRIP-005", "US-CA", 45.00),  # Long ride
    ]

    for trip_id, region, base_fare in test_trips:
        result = fare_engine.adjust_fare(region, base_fare, trip_id)
        surcharge_str = f"+${result['fuel_surcharge']:.2f}" if result["surcharge_active"] else "  $0.00"
        print(f"{trip_id:<15} {region:<10} ${result['base_fare']:>6.2f}  {surcharge_str:>10}  ${result['final_fare']:>6.2f}")

    print("\n✅ Pipeline complete.\n")
