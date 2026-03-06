import os
import time
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "ecommerce_db")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD", "postgres")
DB_HOST = os.getenv("PGHOST", "db")
DB_PORT = int(os.getenv("PGPORT", "5432"))

timeout_sec = 60
start = time.time()

while True:
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        conn.close()
        print("✅ DB is ready")
        break
    except Exception:
        if time.time() - start > timeout_sec:
            raise RuntimeError("DB not ready after 60s")
        print("⏳ Waiting for DB...")
        time.sleep(2)