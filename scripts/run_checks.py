import os
import psycopg2

DB_NAME = os.getenv("PGDATABASE", "ecommerce_db")
DB_USER = os.getenv("PGUSER", os.getenv("USER"))
DB_PASSWORD = os.getenv("PGPASSWORD", None)
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = int(os.getenv("PGPORT", "5432"))

CHECKS_FILE = "sql/checks/contracts.sql"

def main():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
    )
    conn.autocommit = True

    with open(CHECKS_FILE, "r", encoding="utf-8") as f:
        sql = f.read()

    failed = False
    with conn.cursor() as cur:
        # execute multiple statements by splitting on ';'
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        for stmt in statements:
            cur.execute(stmt)
            rows = cur.fetchall()
            for row in rows:
                print(row)
                if len(row) >= 2 and row[1] == "FAIL":
                    failed = True

    conn.close()

    if failed:
        raise SystemExit("❌ Contract checks failed")
    print("✅ All contract checks passed")

if __name__ == "__main__":
    main()