"""AdPulse dashboard (Streamlit).

Run locally:  streamlit run src/adpulse/dashboard/app.py
Opens at:     http://localhost:8501

Data path: Streamlit -> FastAPI (HTTP/JSON) -> PostgreSQL.
"""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st

from adpulse.dashboard.api_client import AdPulseClient, ApiError, format_number
from adpulse.dashboard.demo_source import DemoDataSource

DEMO_MODE = os.getenv("ADPULSE_MODE", "live").lower() == "demo"

# Validated dark categorical palette (colour-blind checked on the #1b1c30 surface).
BLUE, ORANGE, AQUA, YELLOW, MAGENTA, GREEN, VIOLET, RED = (
    "#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767")
SURFACE = "#1b1c30"
GRID = "rgba(255,255,255,0.08)"
TEXT_2 = "#c3c2b7"

st.set_page_config(page_title="AdPulse Analytics", page_icon="📈", layout="wide")

st.markdown(
    f"""
    <style>
      .block-container {{ padding-top: 1.6rem; }}
      .kpi {{ background: {SURFACE}; border: 1px solid rgba(144,133,233,.18); border-radius: 14px;
              padding: 14px 18px 12px; position: relative; overflow: hidden; }}
      .kpi:after {{ content: ""; position: absolute; left: 12%; right: 12%; bottom: 0; height: 3px;
                    border-radius: 3px; background: linear-gradient(90deg, {BLUE}, {VIOLET});
                    box-shadow: 0 0 14px {VIOLET}; }}
      .kpi .label {{ color: {TEXT_2}; font-size: .80rem; letter-spacing: .02em; }}
      .kpi .value {{ color: #f0efec; font-size: 1.75rem; font-weight: 700; line-height: 1.25; }}
      .kpi .sub {{ color: {TEXT_2}; font-size: .72rem; }}
      .brand {{ font-size: 1.35rem; font-weight: 700; }}
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_client():
    """Live mode: HTTP client for FastAPI. Demo mode: read the exported snapshot."""
    if DEMO_MODE:
        return DemoDataSource()
    return AdPulseClient(os.getenv("API_BASE_URL", "http://localhost:8000"))


def kpi(col, label: str, value: str, sub: str = "") -> None:
    col.markdown(f'<div class="kpi"><div class="label">{label}</div>'
                 f'<div class="value">{value}</div><div class="sub">{sub}</div></div>', unsafe_allow_html=True)


def style(fig, height: int = 300):
    fig.update_layout(
        template="plotly_dark", height=height, margin=dict(l=10, r=10, t=40, b=10),
        paper_bgcolor=SURFACE, plot_bgcolor=SURFACE, font=dict(color="#f0efec", size=12),
        title_font=dict(size=14), hoverlabel=dict(bgcolor="#11121f"), legend_title_text="",
    )
    fig.update_xaxes(gridcolor=GRID, zeroline=False)
    fig.update_yaxes(gridcolor=GRID, zeroline=False)
    return fig


# ------------------------------------------------------------------ sidebar
with st.sidebar:
    st.markdown('<div class="brand">📈 AdPulse</div>', unsafe_allow_html=True)
    st.caption("Real-time AdTech analytics · synthetic data")
    page = st.radio("Navigate", ["Overview", "Campaigns", "Publishers", "Fraud & Quality"],
                    label_visibility="collapsed")
    minutes = st.select_slider("Time range (minutes)", options=[15, 30, 60, 180, 720], value=60)
    refresh = False if DEMO_MODE else st.toggle("Auto-refresh every 10 s", value=True)
    st.divider()
    st.caption("Kafka → Spark → S3 Parquet + PostgreSQL → FastAPI → Streamlit")
    if DEMO_MODE:
        st.info("**Demo mode** – a synthetic snapshot exported from a real local run of the full "
                "Kafka + Spark pipeline. Run it yourself with Docker: see the README.")


@st.fragment(run_every=10 if refresh else None)
def render() -> None:
    client = get_client()
    try:
        summary = client.summary()
    except ApiError as exc:
        st.error(f"Cannot reach the API ({exc}). Is the `api` container running?")
        return

    if page == "Overview":
        render_overview(client, summary)
    elif page == "Campaigns":
        render_campaigns(client)
    elif page == "Publishers":
        render_publishers(client)
    else:
        render_quality(client, summary)
    if DEMO_MODE:
        st.caption(f"Snapshot exported: {client.metadata().get('exported_at')} · data is 100% synthetic")
    else:
        st.caption(f"Last pipeline update: {summary.get('last_update') or 'no data yet'}")


def render_overview(client: AdPulseClient, s: dict) -> None:
    st.subheader("Overview")
    c = st.columns(6)
    kpi(c[0], "Impressions", format_number(s["impressions"]), "ads shown")
    kpi(c[1], "Clicks", format_number(s["clicks"]), "after de-duplication")
    kpi(c[2], "CTR", f"{s['ctr_pct']:.2f}%", "clicks / impressions")
    kpi(c[3], "Conversions", format_number(s["conversions"]), "purchases / sign-ups")
    kpi(c[4], "Conversion rate", f"{s['conversion_rate_pct']:.2f}%", "conversions / clicks")
    kpi(c[5], "Suspicious users", format_number(s["suspicious_users"]), "click bursts flagged")
    st.write("")

    ts = pd.DataFrame(client.timeseries(minutes))
    left, right = st.columns(2)
    if ts.empty:
        left.info("Waiting for the first Spark micro-batch…")
    else:
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        f1 = px.area(ts, x="window_start", y="impressions", title="Impressions per minute",
                     color_discrete_sequence=[BLUE])
        f1.update_traces(line_width=2)
        left.plotly_chart(style(f1), use_container_width=True)
        f2 = px.line(ts, x="window_start", y="clicks", title="Clicks per minute",
                     color_discrete_sequence=[VIOLET], markers=True)
        f2.update_traces(line_width=2, marker_size=8)
        right.plotly_chart(style(f2), use_container_width=True)

    camp = pd.DataFrame(client.campaigns())
    if not camp.empty:
        camp = camp.sort_values("ctr_pct", ascending=True)
        f3 = px.bar(camp, x="ctr_pct", y="campaign_name", orientation="h", title="CTR by campaign (%)",
                    color_discrete_sequence=[BLUE], text="ctr_pct",
                    hover_data={"impressions": True, "clicks": True})
        f3.update_traces(texttemplate="%{text:.2f}%", textposition="outside", marker_line_width=0)
        f3.update_layout(yaxis_title="", xaxis_title="CTR %")
        st.plotly_chart(style(f3, 360), use_container_width=True)


def render_campaigns(client: AdPulseClient) -> None:
    st.subheader("Campaign performance")
    df = pd.DataFrame(client.campaigns())
    if df.empty:
        st.info("No data yet")
        return
    left, right = st.columns(2)
    f1 = px.bar(df.sort_values("clicks"), x="clicks", y="campaign_name", orientation="h",
                title="Clicks by campaign", color_discrete_sequence=[VIOLET])
    left.plotly_chart(style(f1, 340).update_layout(yaxis_title=""), use_container_width=True)
    f2 = px.bar(df.sort_values("cvr_pct"), x="cvr_pct", y="campaign_name", orientation="h",
                title="Conversion rate by campaign (%)", color_discrete_sequence=[AQUA])
    right.plotly_chart(style(f2, 340).update_layout(yaxis_title=""), use_container_width=True)
    st.dataframe(
        df[["campaign_id", "campaign_name", "advertiser", "impressions", "clicks", "ctr_pct", "conversions",
            "cvr_pct", "spend_usd", "revenue_usd"]],
        hide_index=True, use_container_width=True,
        column_config={"ctr_pct": st.column_config.NumberColumn("CTR %", format="%.2f"),
                       "cvr_pct": st.column_config.NumberColumn("CVR %", format="%.2f"),
                       "spend_usd": st.column_config.NumberColumn("Spend $", format="$%.2f"),
                       "revenue_usd": st.column_config.NumberColumn("Revenue $", format="$%.2f")},
    )
    choice = st.selectbox("Drill down into a campaign", df["campaign_id"] + " · " + df["campaign_name"])
    cid = choice.split(" · ")[0]
    ts = pd.DataFrame(client.timeseries(minutes, cid))
    if not ts.empty:
        ts["window_start"] = pd.to_datetime(ts["window_start"])
        f3 = px.line(ts, x="window_start", y="ctr_pct", title=f"CTR per minute · {choice}",
                     color_discrete_sequence=[BLUE])
        st.plotly_chart(style(f3), use_container_width=True)


def render_publishers(client: AdPulseClient) -> None:
    st.subheader("Publisher performance")
    df = pd.DataFrame(client.publishers())
    if df.empty:
        st.info("No data yet")
        return
    f1 = px.bar(df.sort_values("impressions"), x="impressions", y="publisher_name", orientation="h",
                title="Impressions by publisher", color_discrete_sequence=[BLUE], hover_data=["ctr_pct"])
    st.plotly_chart(style(f1, 320).update_layout(yaxis_title=""), use_container_width=True)
    st.dataframe(df, hide_index=True, use_container_width=True)
    ranks = pd.DataFrame(client.rankings())
    st.markdown("**Publisher rank inside each campaign** (Spark batch job, window function `RANK()`)")
    if ranks.empty:
        st.caption("Run the batch job to fill this table:  `docker compose run --rm spark-batch`")
    else:
        st.dataframe(ranks[["campaign_name", "rank_in_campaign", "publisher_name", "clicks", "ctr_pct",
                            "cvr_pct", "computed_at"]], hide_index=True, use_container_width=True)


def render_quality(client: AdPulseClient, s: dict) -> None:
    st.subheader("Fraud & data quality")
    c = st.columns(3)
    kpi(c[0], "Suspicious users", format_number(s["suspicious_users"]), "≥ threshold clicks / minute")
    kpi(c[1], "Duplicate events", format_number(s["duplicate_events"]), "same event_id seen twice")
    kpi(c[2], "Invalid events", format_number(s["invalid_events"]), "failed validation rules")
    st.write("")
    left, right = st.columns([3, 2])
    sus = pd.DataFrame(client.suspicious(100))
    left.markdown("**⚠️ Suspicious click bursts**")
    if sus.empty:
        left.caption("Nothing flagged yet")
    else:
        left.dataframe(sus[["window_start", "user_id", "ip_address", "campaign_name", "click_count", "reason"]],
                       hide_index=True, use_container_width=True, height=330)
    inv = pd.DataFrame(client.invalid())
    if not inv.empty:
        f = px.bar(inv.sort_values("event_count"), x="event_count", y="validation_error", orientation="h",
                   title="Invalid events by reason", color_discrete_sequence=[ORANGE])
        right.plotly_chart(style(f, 360).update_layout(yaxis_title=""), use_container_width=True)
    dups = pd.DataFrame(client.duplicates(50))
    st.markdown("**🔁 Recent duplicate events (removed before counting)**")
    st.dataframe(dups, hide_index=True, use_container_width=True, height=250)


render()
