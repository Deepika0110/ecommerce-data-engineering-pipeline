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

k1, k2 = st.columns(2)
k1.metric("Total Orders", f"{orders:,}")
k2.metric("Total Revenue", f"${revenue_total:,.2f}")

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
# New charts
# -----------------------------
st.subheader("Business Insights")

c1, c2 = st.columns(2)

# Top cities by revenue
city_revenue = pd.read_sql(
    """
    SELECT
        c.city,
        ROUND(SUM(f.order_total)::numeric, 2) AS revenue
    FROM analytics.fact_orders f
    JOIN analytics.dim_customer c
      ON f.customer_id = c.customer_id
    GROUP BY c.city
    ORDER BY revenue DESC
    LIMIT 10
    """,
    conn,
)

fig_city = px.bar(
    city_revenue,
    x="city",
    y="revenue",
    title="Top 10 Cities by Revenue",
)

fig_city.update_layout(
    xaxis_title="City",
    yaxis_title="Revenue ($)",
    template="plotly_dark",
)

c1.plotly_chart(fig_city, use_container_width=True)

# Payment status distribution
payment_status = pd.read_sql(
    """
    SELECT
        COALESCE(payment_status, 'unknown') AS payment_status,
        COUNT(*) AS count
    FROM analytics.fact_orders
    GROUP BY COALESCE(payment_status, 'unknown')
    ORDER BY count DESC
    """,
    conn,
)

fig_payment = px.pie(
    payment_status,
    names="payment_status",
    values="count",
    title="Payment Status Distribution",
)

fig_payment.update_layout(template="plotly_dark")

c2.plotly_chart(fig_payment, use_container_width=True)

# Orders per day
orders_day = pd.read_sql(
    """
    SELECT
        order_day,
        COUNT(*) AS order_count
    FROM analytics.fact_orders
    GROUP BY order_day
    ORDER BY order_day
    """,
    conn,
)

fig_orders = px.bar(
    orders_day,
    x="order_day",
    y="order_count",
    title="Orders Per Day",
)

fig_orders.update_layout(
    xaxis_title="Date",
    yaxis_title="Orders",
    template="plotly_dark",
)

st.plotly_chart(fig_orders, use_container_width=True)

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