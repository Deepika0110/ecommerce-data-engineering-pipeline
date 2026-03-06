import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "ecommerce_db")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD", "postgres")
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = int(os.getenv("PGPORT", "5432"))

DATA_DIR = "/data"

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = True

    with conn.cursor() as cur:
        # Schemas (safe if already exist)
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

        # Raw tables
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.customers (
          customer_id VARCHAR(50) PRIMARY KEY,
          name TEXT,
          email TEXT,
          city TEXT,
          created_at TIMESTAMP,
          ingested_at TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.products (
          product_id VARCHAR(50) PRIMARY KEY,
          product_name TEXT,
          category TEXT,
          price NUMERIC(10,2),
          ingested_at TIMESTAMP DEFAULT NOW()
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.orders (
          order_id VARCHAR(50),
          customer_id VARCHAR(50),
          order_date TIMESTAMP,
          order_total NUMERIC(10,2),
          ingested_at TIMESTAMP DEFAULT NOW(),
          source_file TEXT,
          batch_id TEXT,
          run_id TEXT
        );
        """)

        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw.payments (
          payment_id VARCHAR(50) PRIMARY KEY,
          order_id VARCHAR(50),
          payment_status TEXT,
          payment_amount NUMERIC(10,2),
          ingested_at TIMESTAMP DEFAULT NOW(),
          source_file TEXT,
          batch_id TEXT,
          run_id TEXT
        );
        """)

        # Load base CSVs only if empty
        cur.execute("SELECT COUNT(*) FROM raw.customers;")
        if cur.fetchone()[0] == 0:
            cur.execute(f"""
            COPY raw.customers(customer_id,name,email,city,created_at)
            FROM '{DATA_DIR}/customers.csv'
            WITH (FORMAT csv, HEADER true);
            """)

        cur.execute("SELECT COUNT(*) FROM raw.products;")
        if cur.fetchone()[0] == 0:
            cur.execute(f"""
            COPY raw.products(product_id,product_name,category,price)
            FROM '{DATA_DIR}/products.csv'
            WITH (FORMAT csv, HEADER true);
            """)

        cur.execute("SELECT COUNT(*) FROM raw.orders;")
        if cur.fetchone()[0] == 0:
            cur.execute(f"""
            COPY raw.orders(order_id,customer_id,order_date,order_total)
            FROM '{DATA_DIR}/orders.csv'
            WITH (FORMAT csv, HEADER true);
            """)
            cur.execute("UPDATE raw.orders SET source_file='orders.csv', batch_id='BOOTSTRAP';")

        cur.execute("SELECT COUNT(*) FROM raw.payments;")
        if cur.fetchone()[0] == 0:
            cur.execute(f"""
            COPY raw.payments(payment_id,order_id,payment_status,payment_amount)
            FROM '{DATA_DIR}/payments.csv'
            WITH (FORMAT csv, HEADER true);
            """)
            cur.execute("UPDATE raw.payments SET source_file='payments.csv', batch_id='BOOTSTRAP';")

    conn.close()
    print("✅ DB bootstrap complete (raw tables + base CSV load)")

if __name__ == "__main__":
    main()