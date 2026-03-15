"""
GPAF Real-Time Dashboard — Streamlit App
=========================================
Portfolio Project: Dynamic Uber Fare Adjustment Based on Fuel Prices

Run locally:
    pip install streamlit plotly pandas
    streamlit run gpaf_dashboard.py

Features:
    • Live GPAF status per region (metric cards + gauge)
    • 90-day gas price trend with baseline overlay
    • GPAF multiplier heatmap over time
    • Per-trip fare adjustment simulator (live)
    • Pipeline health status
    • Driver earnings uplift estimator
"""

import json
import random
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="GPAF Dashboard",
    page_icon="⛽",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# THEME & CUSTOM CSS
# ─────────────────────────────────────────────

st.markdown("""
<style>
    /* ── Fonts ── */
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
    }

    /* ── App background ── */
    .stApp { background-color: #0D1117; color: #E6EDF3; }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] {
        background-color: #161B22;
        border-right: 1px solid #30363D;
    }

    /* ── Metric cards ── */
    [data-testid="metric-container"] {
        background-color: #161B22;
        border: 1px solid #30363D;
        border-radius: 8px;
        padding: 16px;
    }

    /* ── Headers ── */
    h1, h2, h3 { font-family: 'IBM Plex Mono', monospace; }
    h1 { color: #58A6FF; letter-spacing: -0.5px; }
    h2 { color: #E6EDF3; font-size: 1.1rem; }
    h3 { color: #8B949E; font-size: 0.9rem; font-weight: 400; }

    /* ── Status badge ── */
    .status-active {
        background: #1A3A1A; color: #3FB950; border: 1px solid #3FB950;
        padding: 2px 10px; border-radius: 12px; font-size: 11px;
        font-family: 'IBM Plex Mono', monospace; font-weight: 600;
    }
    .status-inactive {
        background: #1A1A2E; color: #8B949E; border: 1px solid #30363D;
        padding: 2px 10px; border-radius: 12px; font-size: 11px;
        font-family: 'IBM Plex Mono', monospace;
    }
    .status-ceiling {
        background: #3A1A1A; color: #F85149; border: 1px solid #F85149;
        padding: 2px 10px; border-radius: 12px; font-size: 11px;
        font-family: 'IBM Plex Mono', monospace; font-weight: 600;
    }

    /* ── Fare breakdown box ── */
    .fare-box {
        background: #161B22; border: 1px solid #30363D; border-radius: 8px;
        padding: 20px; font-family: 'IBM Plex Mono', monospace; font-size: 13px;
    }
    .fare-line { display: flex; justify-content: space-between; padding: 4px 0;
                 border-bottom: 1px solid #21262D; }
    .fare-total { display: flex; justify-content: space-between; padding: 8px 0;
                  font-weight: 600; font-size: 16px; color: #58A6FF; }

    /* ── Pipeline health ── */
    .health-ok    { color: #3FB950; font-weight: 600; }
    .health-warn  { color: #D29922; font-weight: 600; }
    .health-stale { color: #F85149; font-weight: 600; }

    /* ── Divider ── */
    hr { border-color: #21262D; }

    /* ── Plotly chart bg ── */
    .js-plotly-plot { border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────
# DATA LAYER  (mock — wire to BigQueryLoader for production)
# ─────────────────────────────────────────────

REGIONS = ["US-CA", "US-TX", "US-NY", "US-FL", "US-WA", "US-IL"]
REGION_LABELS = {
    "US-CA": "California", "US-TX": "Texas", "US-NY": "New York",
    "US-FL": "Florida", "US-WA": "Washington", "US-IL": "Illinois",
}
ALPHA_MAP = {"US-CA": 0.6, "US-TX": 0.5, "US-NY": 0.5,
             "US-FL": 0.45, "US-WA": 0.55, "US-IL": 0.5}

BASE_PRICES = {"US-CA": 4.72, "US-TX": 3.05, "US-NY": 3.60,
               "US-FL": 3.28, "US-WA": 4.10, "US-IL": 3.40}
BASE_BASELINES = {"US-CA": 4.38, "US-TX": 3.01, "US-NY": 3.55,
                  "US-FL": 3.21, "US-WA": 3.85, "US-IL": 3.36}


@st.cache_data(ttl=300)  # Refresh every 5 minutes
def get_current_gpaf() -> pd.DataFrame:
    rows = []
    for region in REGIONS:
        current = BASE_PRICES[region] + random.uniform(-0.05, 0.05)
        baseline = BASE_BASELINES[region]
        delta = (current - baseline) / baseline
        alpha = ALPHA_MAP[region]
        is_active = abs(delta) >= 0.05
        raw_gpaf = 1 + alpha * delta if is_active else 1.0
        gpaf = max(1.0, min(1.25, raw_gpaf))
        rows.append({
            "region_id": region,
            "region_name": REGION_LABELS[region],
            "current_price": round(current, 3),
            "baseline_30d": round(baseline, 3),
            "price_delta_pct": round(delta * 100, 2),
            "gpaf": round(gpaf, 4),
            "fuel_surcharge_pct": round((gpaf - 1) * 100, 2),
            "is_active": is_active,
            "hit_ceiling": gpaf >= 1.249,
            "alpha": alpha,
        })
    return pd.DataFrame(rows)


@st.cache_data(ttl=600)
def get_trend_data(days: int = 90) -> pd.DataFrame:
    rows = []
    prices = {r: BASE_PRICES[r] for r in REGIONS}
    baselines = {r: BASE_BASELINES[r] for r in REGIONS}

    for i in range(days, -1, -1):
        d = (datetime.now() - timedelta(days=i)).date()
        for region in REGIONS:
            noise = random.gauss(0, 0.04)
            trend = (days - i) * 0.0015
            current = round(prices[region] + noise + trend, 3)
            prices[region] = round(prices[region] * 0.99 + current * 0.01, 3)
            baseline = round(baselines[region], 4)
            baselines[region] = round(baselines[region] * 0.97 + current * 0.03, 4)
            delta = (current - baseline) / baseline
            alpha = ALPHA_MAP[region]
            is_active = abs(delta) >= 0.05
            raw_gpaf = 1 + alpha * delta if is_active else 1.0
            gpaf = round(max(1.0, min(1.25, raw_gpaf)), 4)
            rows.append({
                "date": d, "region_id": region,
                "region_name": REGION_LABELS[region],
                "current_price": current, "baseline_30d": baseline,
                "gpaf": gpaf, "fuel_surcharge_pct": round((gpaf - 1) * 100, 2),
                "is_active": is_active,
            })
    return pd.DataFrame(rows)


@st.cache_data(ttl=60)
def get_pipeline_health() -> list[dict]:
    return [
        {"region_id": r, "last_run": datetime.now().strftime("%Y-%m-%d %H:%M UTC"),
         "days_stale": 0, "status": "OK", "rows_written": random.randint(4, 6)}
        for r in REGIONS
    ]


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────

with st.sidebar:
    st.markdown("## ⛽ GPAF Monitor")
    st.markdown("*Gas Price Adjustment Factor*")
    st.divider()

    selected_regions = st.multiselect(
        "Regions", options=REGIONS,
        default=REGIONS,
        format_func=lambda r: REGION_LABELS[r]
    )

    trend_days = st.slider("Trend window (days)", 14, 90, 90)

    st.divider()
    st.markdown("**Formula**")
    st.code("GPAF = 1 + α × (P_now − P_base) / P_base", language="text")
    st.caption("Floors at 1.0 · Caps at 1.25\nActivates when |Δ| ≥ 5%")

    st.divider()
    if st.button("🔄 Refresh data"):
        st.cache_data.clear()
        st.rerun()

    last_refresh = datetime.now().strftime("%H:%M:%S")
    st.caption(f"Last refresh: {last_refresh} UTC")


# ─────────────────────────────────────────────
# MAIN CONTENT
# ─────────────────────────────────────────────

current_df = get_current_gpaf()
current_df = current_df[current_df["region_id"].isin(selected_regions)]
trend_df = get_trend_data(trend_days)
trend_df = trend_df[trend_df["region_id"].isin(selected_regions)]

# ── Title bar ──
col_title, col_status = st.columns([3, 1])
with col_title:
    st.markdown("# GPAF Real-Time Dashboard")
    st.markdown(f"###### {datetime.now().strftime('%A, %B %d %Y  •  %H:%M UTC')}")
with col_status:
    active_count = current_df["is_active"].sum()
    ceiling_count = current_df["hit_ceiling"].sum()
    st.metric("Active surcharges", f"{active_count} / {len(current_df)}")
    if ceiling_count > 0:
        st.error(f"⚠️ {ceiling_count} region(s) at ceiling")

st.divider()

# ── Section 1: Region metric cards ──
st.markdown("## Current GPAF by Region")

cols = st.columns(len(current_df))
for idx, (_, row) in enumerate(current_df.iterrows()):
    with cols[idx]:
        if row["hit_ceiling"]:
            badge = '<span class="status-ceiling">⚠ CEILING</span>'
        elif row["is_active"]:
            badge = '<span class="status-active">● ACTIVE</span>'
        else:
            badge = '<span class="status-inactive">○ inactive</span>'

        st.markdown(f"**{row['region_name']}**")
        st.markdown(badge, unsafe_allow_html=True)
        st.metric(
            label="GPAF multiplier",
            value=f"{row['gpaf']:.4f}",
            delta=f"+{row['fuel_surcharge_pct']:.1f}% surcharge" if row["is_active"] else "no surcharge",
            delta_color="inverse" if row["is_active"] else "off",
        )
        st.caption(f"⛽ ${row['current_price']:.3f}/gal")
        st.caption(f"📊 baseline ${row['baseline_30d']:.3f}")
        st.caption(f"Δ {row['price_delta_pct']:+.1f}%")

st.divider()

# ── Section 2: Charts ──
chart_col1, chart_col2 = st.columns([3, 2])

with chart_col1:
    st.markdown("## Gas Price Trend vs Baseline")
    region_for_chart = st.selectbox(
        "Region", options=selected_regions,
        format_func=lambda r: REGION_LABELS[r], key="trend_region"
    )
    region_trend = trend_df[trend_df["region_id"] == region_for_chart]

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(
        x=region_trend["date"], y=region_trend["current_price"],
        name="Current price", line=dict(color="#58A6FF", width=2),
        fill=None,
    ))
    fig_trend.add_trace(go.Scatter(
        x=region_trend["date"], y=region_trend["baseline_30d"],
        name="30-day baseline", line=dict(color="#8B949E", width=1.5, dash="dash"),
    ))
    # Shade area between current and baseline
    fig_trend.add_trace(go.Scatter(
        x=list(region_trend["date"]) + list(region_trend["date"])[::-1],
        y=list(region_trend["current_price"]) + list(region_trend["baseline_30d"])[::-1],
        fill="toself", fillcolor="rgba(88,166,255,0.08)",
        line=dict(color="rgba(255,255,255,0)"),
        showlegend=False, hoverinfo="skip",
    ))
    fig_trend.update_layout(
        plot_bgcolor="#0D1117", paper_bgcolor="#0D1117",
        font=dict(color="#8B949E", family="IBM Plex Mono"),
        xaxis=dict(gridcolor="#21262D", showgrid=True),
        yaxis=dict(gridcolor="#21262D", showgrid=True, tickprefix="$"),
        legend=dict(bgcolor="#161B22", bordercolor="#30363D", borderwidth=1),
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
    )
    st.plotly_chart(fig_trend, use_container_width=True)

with chart_col2:
    st.markdown("## GPAF Multiplier Over Time")
    fig_gpaf = px.line(
        trend_df, x="date", y="gpaf", color="region_id",
        labels={"gpaf": "GPAF", "date": "", "region_id": "Region"},
        color_discrete_sequence=["#58A6FF", "#3FB950", "#D29922", "#F85149", "#BC8CFF", "#79C0FF"],
    )
    fig_gpaf.add_hline(y=1.0, line_dash="dot", line_color="#30363D", annotation_text="floor 1.0")
    fig_gpaf.add_hline(y=1.25, line_dash="dot", line_color="#F85149", annotation_text="ceiling 1.25")
    fig_gpaf.update_layout(
        plot_bgcolor="#0D1117", paper_bgcolor="#0D1117",
        font=dict(color="#8B949E", family="IBM Plex Mono"),
        xaxis=dict(gridcolor="#21262D"),
        yaxis=dict(gridcolor="#21262D", range=[0.95, 1.30]),
        legend=dict(bgcolor="#161B22", bordercolor="#30363D"),
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
    )
    st.plotly_chart(fig_gpaf, use_container_width=True)

st.divider()

# ── Section 3: Fare Simulator ──
st.markdown("## Fare Adjustment Simulator")
sim_col1, sim_col2 = st.columns([2, 1])

with sim_col1:
    sim_region = st.selectbox(
        "Region", options=selected_regions,
        format_func=lambda r: REGION_LABELS[r], key="sim_region"
    )
    base_fare = st.slider("Base fare ($)", 5.0, 60.0, 14.0, 0.50)

    row = current_df[current_df["region_id"] == sim_region].iloc[0]
    fuel_surcharge = round(base_fare * (row["gpaf"] - 1.0), 2)
    final_fare = round(base_fare * row["gpaf"], 2)
    driver_share = round(fuel_surcharge * 0.85, 2)
    uber_cut = round(base_fare * 0.25, 2)
    driver_base = round(base_fare * 0.75, 2)
    driver_total = round(driver_base + driver_share, 2)

with sim_col2:
    st.markdown(f"""
    <div class="fare-box">
        <div style="color:#8B949E;font-size:11px;margin-bottom:12px">
            FARE BREAKDOWN — {REGION_LABELS[sim_region]}
        </div>
        <div class="fare-line">
            <span>Base fare</span>
            <span>${base_fare:.2f}</span>
        </div>
        <div class="fare-line">
            <span>Fuel surcharge</span>
            <span style="color:{'#D29922' if fuel_surcharge > 0 else '#8B949E'}">
                +${fuel_surcharge:.2f}
            </span>
        </div>
        <div class="fare-line">
            <span>Uber cut (25%)</span>
            <span style="color:#F85149">−${uber_cut:.2f}</span>
        </div>
        <div class="fare-line">
            <span>Driver surcharge share (85%)</span>
            <span style="color:#3FB950">+${driver_share:.2f}</span>
        </div>
        <div style="border-top:1px solid #30363D;margin:8px 0"></div>
        <div class="fare-total">
            <span>Driver earns</span>
            <span style="color:#58A6FF">${driver_total:.2f}</span>
        </div>
        <div style="color:#8B949E;font-size:10px;margin-top:4px">
            GPAF: {row['gpaf']:.4f} &nbsp;|&nbsp; 
            Δ gas: {row['price_delta_pct']:+.1f}%
        </div>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ── Section 4: Heatmap ──
st.markdown("## GPAF Heatmap — All Regions Over Time")
pivot = trend_df.pivot_table(index="region_id", columns="date", values="gpaf")
fig_heat = px.imshow(
    pivot,
    color_continuous_scale=[[0, "#0D1117"], [0.4, "#1A3A1A"], [0.7, "#D29922"], [1.0, "#F85149"]],
    zmin=1.0, zmax=1.25,
    labels=dict(color="GPAF"),
    aspect="auto",
)
fig_heat.update_layout(
    plot_bgcolor="#0D1117", paper_bgcolor="#0D1117",
    font=dict(color="#8B949E", family="IBM Plex Mono"),
    coloraxis_colorbar=dict(title="GPAF", tickfont=dict(color="#8B949E")),
    margin=dict(l=10, r=10, t=10, b=10),
    height=220,
    yaxis=dict(tickvals=list(range(len(selected_regions))),
               ticktext=[REGION_LABELS[r] for r in selected_regions]),
)
st.plotly_chart(fig_heat, use_container_width=True)

st.divider()

# ── Section 5: Pipeline Health ──
st.markdown("## Pipeline Health")
health = get_pipeline_health()
h_cols = st.columns(len(health))
for idx, h in enumerate(health):
    with h_cols[idx]:
        color = "health-ok" if h["status"] == "OK" else "health-warn"
        st.markdown(f"**{REGION_LABELS.get(h['region_id'], h['region_id'])}**")
        st.markdown(f'<span class="{color}">{h["status"]}</span>', unsafe_allow_html=True)
        st.caption(f"Last run: {h['last_run']}")
        st.caption(f"Rows written: {h['rows_written']}")

st.divider()
st.caption("GPAF Dashboard v1.0  •  Portfolio Project  •  Data refreshes every 5 min")
