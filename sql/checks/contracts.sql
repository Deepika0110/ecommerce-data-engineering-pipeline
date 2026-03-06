-- 1) fact_orders must not have duplicate primary keys
SELECT 'fact_orders_duplicate_order_id' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM (
  SELECT order_id
  FROM analytics.fact_orders
  GROUP BY order_id
  HAVING COUNT(*) > 1
) d;

-- 2) no null order_id in fact_orders
SELECT 'fact_orders_null_order_id' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM analytics.fact_orders
WHERE order_id IS NULL;

-- 3) quarantine rate should not exceed 5% (payments)
WITH latest AS (
  SELECT *
  FROM analytics.pipeline_metrics
  ORDER BY run_timestamp DESC
  LIMIT 1
)
SELECT 'payments_quarantine_rate' AS check_name,
       CASE WHEN (quarantined_payments::numeric / NULLIF(raw_payments_count,0)) <= 0.05 THEN 'PASS' ELSE 'FAIL' END AS status,
       quarantined_payments AS failing_rows
FROM latest;

-- 4) anomaly log should not explode (basic sanity)
WITH latest AS (
  SELECT started_at
  FROM analytics.pipeline_runs
  ORDER BY started_at DESC
  LIMIT 1
)
SELECT 'recent_anomalies_count' AS check_name,
       CASE WHEN COUNT(*) <= 5 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM analytics.revenue_anomaly_log;