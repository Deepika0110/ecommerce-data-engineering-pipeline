import os

import pandas as pd
import plotly.express as px
import psycopg2
import streamlit as st

st.set_page_config(page_title="Ecommerce Data Pipeline", layout="wide")
st.title("📊 Ecommerce Data Pipeline Dashboard")

conn = psycopg2.connect(
    dbname=os.getenv("PGDATABASE", "ecommerce_db"),
    user=os.getenv("PGUSER", "deepikabode"),
    host=os.getenv("PGHOST", "localhost"),
    port=os.getenv("PGPORT", "5432"),
)

# -----------------------------
# KPI summary
# -----------------------------
summary = pd.read_sql(
    """
    SELECT
        COUNT(*) AS total_orders,
        COALESCE(SUM(order_total), 0) AS total_revenue
    FROM analytics.fact_orders
    """,
    conn,
)

orders = int(summary["total_orders"][0])
revenue_total = float(summary["total_revenue"][0])

col1, col2 = st.columns(2)
col1.metric("Total Orders", f"{orders:,}")
col2.metric("Total Revenue", f"${revenue_total:,.2f}")

# -----------------------------
# Daily revenue trend
# -----------------------------
revenue = pd.read_sql(
    """
    SELECT order_day, revenue
    FROM analytics.v_revenue_anomaly_daily
    ORDER BY order_day
    """,
    conn,
)

st.subheader("Daily Revenue")

fig = px.line(
    revenue,
    x="order_day",
    y="revenue",
    markers=True,
    title="Daily Revenue Trend",
)

fig.update_layout(
    xaxis_title="Date",
    yaxis_title="Revenue ($)",
    template="plotly_dark",
)

st.plotly_chart(fig, use_container_width=True)

# -----------------------------
# Revenue anomalies
# -----------------------------
st.subheader("Revenue Anomalies")

anomalies = pd.read_sql(
    """
    SELECT order_day, revenue, z_score
    FROM analytics.v_revenue_anomaly_daily
    WHERE is_anomaly = true
    ORDER BY order_day DESC
    """,
    conn,
)

if anomalies.empty:
    st.success("No revenue anomalies detected in the current data window.")
else:
    st.dataframe(anomalies, use_container_width=True)

# -----------------------------
# Pipeline health trend
# -----------------------------
st.subheader("Pipeline Health")

health = pd.read_sql(
    """
    SELECT
        run_timestamp,
        orders_quarantine_pct,
        payments_quarantine_pct
    FROM analytics.v_metrics_trend
    ORDER BY run_timestamp
    """,
    conn,
)

fig2 = px.line(
    health,
    x="run_timestamp",
    y=["orders_quarantine_pct", "payments_quarantine_pct"],
    markers=True,
    title="Data Quality Trend",
)

fig2.update_layout(
    xaxis_title="Run Timestamp",
    yaxis_title="Quarantine Percentage",
    template="plotly_dark",
)

st.plotly_chart(fig2, use_container_width=True)

conn.close()