import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "ecommerce_db")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

ORDERS_DAILY = os.path.abspath("data/orders_daily.csv")
PAYMENTS_DAILY = os.path.abspath("data/payments_daily.csv")

def main():
    if not os.path.exists(ORDERS_DAILY) or not os.path.exists(PAYMENTS_DAILY):
        raise FileNotFoundError("Daily CSVs not found. Run: python scripts/generate_daily_orders.py")

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, host=DB_HOST, port=DB_PORT)
    conn.autocommit = False

    try:
        with conn.cursor() as cur:
            # load tables
            cur.execute("TRUNCATE raw.orders_load;")
            cur.execute("TRUNCATE raw.payments_load;")

            with open(ORDERS_DAILY, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY raw.orders_load(order_id,customer_id,order_date,order_total) FROM STDIN WITH (FORMAT csv, HEADER true);",
                    f,
                )

            with open(PAYMENTS_DAILY, "r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"COPY raw.payments_load(payment_id,order_id,payment_status,payment_amount) FROM STDIN WITH (FORMAT csv, HEADER true);",
                    f,
                )

            # upserts (PKs already exist)
            cur.execute("""
                INSERT INTO raw.orders (order_id, customer_id, order_date, order_total, source_file, batch_id)
                SELECT order_id, customer_id, order_date, order_total, 'orders_daily.csv', 'BATCH_DAILY'
                FROM raw.orders_load
                ON CONFLICT (order_id) DO NOTHING;
            """)

            cur.execute("""
                INSERT INTO raw.payments (payment_id, order_id, payment_status, payment_amount, source_file, batch_id)
                SELECT payment_id, order_id, payment_status, payment_amount, 'payments_daily.csv', 'BATCH_DAILY'
                FROM raw.payments_load
                ON CONFLICT (payment_id) DO NOTHING;
            """)

        conn.commit()
        print("✅ Daily load complete.")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    main()