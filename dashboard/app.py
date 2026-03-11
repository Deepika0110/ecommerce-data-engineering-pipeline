import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px

st.set_page_config(page_title="Ecommerce Data Pipeline", layout="wide")

st.title("📊 Ecommerce Data Pipeline Dashboard")

conn = psycopg2.connect(
    dbname="ecommerce_db",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

# Revenue trend
revenue = pd.read_sql("""
SELECT order_day, revenue
FROM analytics.v_revenue_daily
ORDER BY order_day
""", conn)

st.subheader("Daily Revenue")

fig = px.line(
    revenue,
    x="order_day",
    y="revenue",
    markers=True
)

st.plotly_chart(fig, use_container_width=True)

# Anomalies
st.subheader("Revenue Anomalies")

anomalies = pd.read_sql("""
SELECT order_day, revenue, z_score
FROM analytics.v_revenue_anomaly_daily
WHERE is_anomaly = true
ORDER BY order_day DESC
""", conn)

st.dataframe(anomalies)

# Pipeline metrics
st.subheader("Pipeline Data Quality Trend")

metrics = pd.read_sql("""
SELECT run_timestamp,
orders_quarantine_pct,
payments_quarantine_pct
FROM analytics.v_metrics_trend
ORDER BY run_timestamp
""", conn)

fig2 = px.line(
    metrics,
    x="run_timestamp",
    y=["orders_quarantine_pct", "payments_quarantine_pct"],
)

st.plotly_chart(fig2, use_container_width=True)

conn.close()
