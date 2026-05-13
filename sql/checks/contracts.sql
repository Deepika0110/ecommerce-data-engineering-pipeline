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

-- 5) fact table must have at least 100 rows
SELECT 'fact_orders_min_volume' AS check_name,
       CASE WHEN COUNT(*) >= 100 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM analytics.fact_orders;

-- 6) pipeline must have run successfully within the last {freshness_hours} hours
--    default 24h, override with FRESHNESS_HOURS env var (e.g. 168 for local dev)
SELECT 'pipeline_ran_recently' AS check_name,
       CASE WHEN MAX(finished_at) >= NOW() - INTERVAL '1 hour' * {freshness_hours} THEN 'PASS' ELSE 'FAIL' END AS status,
       0 AS failing_rows
FROM analytics.pipeline_runs
WHERE status = 'success';

-- 7) every customer_id in fact must exist in dim_customer
SELECT 'fact_orders_customer_ref' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM analytics.fact_orders f
WHERE NOT EXISTS (
  SELECT 1 FROM analytics.dim_customer d WHERE d.customer_id = f.customer_id
);

-- 8) paid payments with a non-empty order_id must reference an order in raw.orders
--    (payments with empty order_id are already caught by the quarantine "missing order_id" rule)
SELECT 'payments_order_ref' AS check_name,
       CASE WHEN COUNT(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS status,
       COUNT(*) AS failing_rows
FROM raw.payments p
WHERE p.payment_status = 'paid'
  AND p.order_id IS NOT NULL AND p.order_id <> ''
  AND NOT EXISTS (
    SELECT 1 FROM raw.orders o WHERE o.order_id = p.order_id
  );