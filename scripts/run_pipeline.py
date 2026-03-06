import os
import uuid
from datetime import datetime

import psycopg2

# ---------------- Config ----------------
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "7"))

DB_NAME = os.getenv("PGDATABASE", "ecommerce_db")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_PASSWORD = os.getenv("PGPASSWORD", "") or None
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))


def new_run_id() -> str:
    return "RUN_" + datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:6]


def run_sql(cur, sql: str):
    cur.execute(sql)


def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = False

    run_id = new_run_id()
    started_at = datetime.now()

    try:
        with conn.cursor() as cur:
            # ---------- Schemas ----------
            run_sql(cur, "CREATE SCHEMA IF NOT EXISTS raw;")
            run_sql(cur, "CREATE SCHEMA IF NOT EXISTS staging;")
            run_sql(cur, "CREATE SCHEMA IF NOT EXISTS analytics;")

            # ---------- Run tracking ----------
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS analytics.pipeline_runs (
                  run_id       TEXT PRIMARY KEY,
                  started_at   TIMESTAMP DEFAULT NOW(),
                  finished_at  TIMESTAMP,
                  status       TEXT NOT NULL,
                  window_days  INT,
                  notes        TEXT
                );
                """,
            )

            cur.execute(
                """
                INSERT INTO analytics.pipeline_runs (run_id, status, window_days)
                VALUES (%s, 'running', %s);
                """,
                (run_id, WINDOW_DAYS),
            )

            # ---------- Quarantine tables ----------
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS raw.quarantine_orders (
                  order_id       VARCHAR(50),
                  customer_id    VARCHAR(50),
                  order_date     TIMESTAMP,
                  order_total    NUMERIC(10,2),
                  reason         TEXT,
                  quarantined_at TIMESTAMP DEFAULT NOW()
                );
                """,
            )
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS raw.quarantine_payments (
                  payment_id     VARCHAR(50),
                  order_id       VARCHAR(50),
                  payment_status TEXT,
                  payment_amount NUMERIC(10,2),
                  reason         TEXT,
                  quarantined_at TIMESTAMP DEFAULT NOW()
                );
                """,
            )

            # deterministic each run
            run_sql(cur, "TRUNCATE TABLE raw.quarantine_orders;")
            run_sql(cur, "TRUNCATE TABLE raw.quarantine_payments;")

            # ---------- Quarantine rules: orders ----------
            # duplicates by order_id (keep earliest ingested, quarantine the rest)
            run_sql(
                cur,
                """
                WITH dups AS (
                  SELECT *,
                         ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingested_at) AS rn
                  FROM raw.orders
                )
                INSERT INTO raw.quarantine_orders (order_id, customer_id, order_date, order_total, reason)
                SELECT order_id, customer_id, order_date, order_total, 'duplicate order_id'
                FROM dups
                WHERE rn > 1;
                """,
            )

            # missing customer_id
            run_sql(
                cur,
                """
                INSERT INTO raw.quarantine_orders (order_id, customer_id, order_date, order_total, reason)
                SELECT order_id, customer_id, order_date, order_total, 'missing customer_id'
                FROM raw.orders
                WHERE customer_id IS NULL OR customer_id = '';
                """,
            )

            # negative totals
            run_sql(
                cur,
                """
                INSERT INTO raw.quarantine_orders (order_id, customer_id, order_date, order_total, reason)
                SELECT order_id, customer_id, order_date, order_total, 'negative order_total'
                FROM raw.orders
                WHERE order_total < 0;
                """,
            )

            # ---------- Quarantine rules: payments ----------
            run_sql(
                cur,
                """
                INSERT INTO raw.quarantine_payments (payment_id, order_id, payment_status, payment_amount, reason)
                SELECT payment_id, order_id, payment_status, payment_amount, 'missing order_id'
                FROM raw.payments
                WHERE order_id IS NULL OR order_id = '';
                """,
            )

            # paid must match order_total (quarantine payment rows that mismatch)
            run_sql(
                cur,
                """
                INSERT INTO raw.quarantine_payments (payment_id, order_id, payment_status, payment_amount, reason)
                SELECT p.payment_id, p.order_id, p.payment_status, p.payment_amount, 'paid amount != order_total'
                FROM raw.payments p
                JOIN raw.orders o ON o.order_id = p.order_id
                WHERE p.payment_status = 'paid'
                  AND p.payment_amount IS NOT NULL
                  AND o.order_total IS NOT NULL
                  AND p.payment_amount <> o.order_total;
                """,
            )

            # ---------- Rebuild staging ----------
            run_sql(cur, "DROP TABLE IF EXISTS staging.orders_clean;")
            run_sql(cur, "DROP TABLE IF EXISTS staging.payments_clean;")

            run_sql(
                cur,
                """
                CREATE TABLE staging.orders_clean AS
                SELECT *
                FROM raw.orders o
                WHERE NOT EXISTS (
                  SELECT 1 FROM raw.quarantine_orders q WHERE q.order_id = o.order_id
                );
                """,
            )

            run_sql(
                cur,
                """
                CREATE TABLE staging.payments_clean AS
                SELECT *
                FROM raw.payments p
                WHERE NOT EXISTS (
                  SELECT 1 FROM raw.quarantine_payments q WHERE q.payment_id = p.payment_id
                );
                """,
            )

            # ---------- Hard quality gate (optional strictness) ----------
            # Keep this lenient for your project; adjust later.
            cur.execute("SELECT COUNT(*) FROM raw.quarantine_orders;")
            quarantine_orders = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM raw.quarantine_payments;")
            quarantine_payments = cur.fetchone()[0]

            MAX_ALLOWED_ERRORS = 999999  # set to e.g. 20 if you want strict failing
            if quarantine_orders > MAX_ALLOWED_ERRORS or quarantine_payments > MAX_ALLOWED_ERRORS:
                raise Exception(
                    f"Data quality threshold breached: orders={quarantine_orders}, payments={quarantine_payments}"
                )

            # ---------- fact_orders (incremental rebuild for WINDOW_DAYS) ----------
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS analytics.fact_orders (
                  order_id       VARCHAR(50) PRIMARY KEY,
                  customer_id    VARCHAR(50),
                  order_day      DATE,
                  order_total    NUMERIC(10,2),
                  payment_status TEXT,
                  payment_amount NUMERIC(10,2)
                );
                """,
            )
            run_sql(cur, "CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON analytics.fact_orders(customer_id);")
            run_sql(cur, "CREATE INDEX IF NOT EXISTS idx_fact_orders_day ON analytics.fact_orders(order_day);")
            run_sql(cur, "CREATE INDEX IF NOT EXISTS idx_fact_orders_payment_status ON analytics.fact_orders(payment_status);")

            run_sql(
                cur,
                f"""
                DELETE FROM analytics.fact_orders
                WHERE order_day >= (CURRENT_DATE - INTERVAL '{WINDOW_DAYS} days')::date;
                """,
            )

            run_sql(
                cur,
                f"""
                INSERT INTO analytics.fact_orders (
                  order_id, customer_id, order_day, order_total, payment_status, payment_amount
                )
                SELECT
                  o.order_id,
                  o.customer_id,
                  DATE(o.order_date) AS order_day,
                  o.order_total,
                  p.payment_status,
                  p.payment_amount
                FROM staging.orders_clean o
                LEFT JOIN staging.payments_clean p
                  ON p.order_id = o.order_id
                WHERE DATE(o.order_date) >= (CURRENT_DATE - INTERVAL '{WINDOW_DAYS} days')::date
                ON CONFLICT (order_id) DO UPDATE
                SET
                  customer_id = EXCLUDED.customer_id,
                  order_day = EXCLUDED.order_day,
                  order_total = EXCLUDED.order_total,
                  payment_status = EXCLUDED.payment_status,
                  payment_amount = EXCLUDED.payment_amount;
                """,
            )

            # ---------- Dimensions (simple rebuilds) ----------
            # If these raw tables don't exist yet, comment these 3 blocks.
            run_sql(cur, "DROP TABLE IF EXISTS analytics.dim_customer;")
            run_sql(cur, "DROP TABLE IF EXISTS analytics.dim_product;")
            run_sql(cur, "DROP TABLE IF EXISTS analytics.dim_date;")

            run_sql(
                cur,
                """
                CREATE TABLE analytics.dim_customer AS
                SELECT
                  customer_id,
                  NULLIF(TRIM(name), '')  AS name,
                  NULLIF(TRIM(email), '') AS email,
                  NULLIF(TRIM(city), '')  AS city,
                  created_at
                FROM raw.customers;
                """,
            )

            run_sql(
                cur,
                """
                CREATE TABLE analytics.dim_product AS
                SELECT product_id, product_name, category, price
                FROM raw.products;
                """,
            )

            run_sql(
                cur,
                """
                CREATE TABLE analytics.dim_date AS
                SELECT DISTINCT
                  DATE(order_date) AS date_day,
                  EXTRACT(YEAR FROM order_date)::INT  AS year,
                  EXTRACT(MONTH FROM order_date)::INT AS month,
                  EXTRACT(DAY FROM order_date)::INT   AS day,
                  EXTRACT(DOW FROM order_date)::INT   AS day_of_week
                FROM staging.orders_clean;
                """,
            )

            # ---------- Revenue anomaly log (A+B: smart, volume-aware) ----------
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS analytics.revenue_anomaly_log (
                  run_id         TEXT,
                  detected_at    TIMESTAMP DEFAULT NOW(),
                  order_day      DATE,
                  revenue        NUMERIC(12,2),
                  orders         INT,
                  rev_avg_14d    NUMERIC(12,2),
                  rev_std_14d    NUMERIC(12,2),
                  pct_change_pct NUMERIC(8,2),
                  z_score        NUMERIC(8,2),
                  PRIMARY KEY (run_id, order_day)
                );
                """,
            )

            cur.execute(
                f"""
                WITH daily AS (
                  SELECT
                    order_day,
                    SUM(CASE WHEN payment_status = 'paid' THEN payment_amount ELSE 0 END) AS revenue,
                    COUNT(*) AS orders
                  FROM analytics.fact_orders
                  GROUP BY order_day
                ),
                stats AS (
                  SELECT
                    order_day,
                    revenue,
                    orders,
                    AVG(revenue) OVER (ORDER BY order_day ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS avg14,
                    STDDEV_SAMP(revenue) OVER (ORDER BY order_day ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS std14
                  FROM daily
                ),
                scored AS (
                  SELECT
                    order_day,
                    revenue,
                    orders,
                    avg14,
                    std14,
                    CASE WHEN avg14 IS NULL OR avg14 = 0 THEN NULL ELSE (revenue - avg14) / avg14 END AS pct_change,
                    CASE WHEN avg14 IS NULL OR std14 IS NULL OR std14 = 0 THEN NULL ELSE (revenue - avg14) / std14 END AS z
                  FROM stats
                )
                INSERT INTO analytics.revenue_anomaly_log
                (run_id, order_day, revenue, orders, rev_avg_14d, rev_std_14d, pct_change_pct, z_score)
                SELECT
                  %s,
                  order_day,
                  ROUND(revenue::numeric, 2),
                  orders,
                  ROUND(avg14::numeric, 2),
                  ROUND(std14::numeric, 2),
                  ROUND((pct_change * 100)::numeric, 2),
                  ROUND(z::numeric, 2)
                FROM scored
                WHERE order_day >= (CURRENT_DATE - INTERVAL '{WINDOW_DAYS} days')::date
                  AND z IS NOT NULL
                  AND orders >= 15
                  AND ABS(pct_change) >= 0.30
                  AND ABS(z) > 3
                ON CONFLICT (run_id, order_day) DO NOTHING;
                """,
                (run_id,),
            )

            # ---------- Metrics ----------
            run_sql(
                cur,
                """
                CREATE TABLE IF NOT EXISTS analytics.pipeline_metrics (
                  run_timestamp        TIMESTAMP DEFAULT NOW(),
                  window_days          INT,
                  raw_orders_count     INT,
                  quarantined_orders   INT,
                  clean_orders_count   INT,
                  raw_payments_count   INT,
                  quarantined_payments INT,
                  clean_payments_count INT
                );
                """,
            )

            cur.execute(
                """
                INSERT INTO analytics.pipeline_metrics (
                  window_days,
                  raw_orders_count, quarantined_orders, clean_orders_count,
                  raw_payments_count, quarantined_payments, clean_payments_count
                )
                SELECT
                  %s,
                  (SELECT COUNT(*) FROM raw.orders),
                  (SELECT COUNT(*) FROM raw.quarantine_orders),
                  (SELECT COUNT(*) FROM staging.orders_clean),
                  (SELECT COUNT(*) FROM raw.payments),
                  (SELECT COUNT(*) FROM raw.quarantine_payments),
                  (SELECT COUNT(*) FROM staging.payments_clean);
                """,
                (WINDOW_DAYS,),
            )

        # commit the whole run
        conn.commit()

        # mark success
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE analytics.pipeline_runs
                SET status='success', finished_at=NOW()
                WHERE run_id=%s;
                """,
                (run_id,),
            )
        conn.commit()

        duration = (datetime.now() - started_at).total_seconds()
        print(f"🟦 run_id={run_id}")
        print(f"✅ Pipeline run complete. ({duration:.2f}s)")

    except Exception as e:
        conn.rollback()

        # mark failed (best effort)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE analytics.pipeline_runs
                    SET status='failed', finished_at=NOW(), notes=%s
                    WHERE run_id=%s;
                    """,
                    (str(e), run_id),
                )
            conn.commit()
        except Exception:
            pass

        print("❌ Pipeline failed:", str(e))
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()