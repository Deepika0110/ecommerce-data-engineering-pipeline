# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Local development (requires a running PostgreSQL instance)

```bash
# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Initialize schemas (run once against a fresh DB)
psql -d ecommerce_db -f sql/init/001_create_schemas.sql

# Bootstrap raw tables and load base CSVs (idempotent — skips if tables already populated)
python scripts/bootstrap_db.py

# Run the full pipeline
python scripts/run_pipeline.py

# Run contract/quality checks (default freshness window: 24h)
python scripts/run_checks.py

# Run checks locally with a wider freshness window (pipeline_ran_recently won't fail)
FRESHNESS_HOURS=168 python scripts/run_checks.py

# Simulate a daily incremental load
python scripts/generate_daily_orders.py   # writes data/orders_daily.csv + payments_daily.csv
python scripts/load_daily.py              # upserts those CSVs into raw tables

# Launch the Streamlit dashboard
streamlit run dashboard/app.py
```

### Docker (self-contained, no local Postgres required)

```bash
docker compose up --build       # starts db, pipeline, dashboard (port 8501), and Metabase (port 3000)
docker exec -it ecommerce_db psql -U postgres -d ecommerce_db   # psql shell
```

### Environment variables

All scripts read DB connection from env vars with these defaults:

| Variable     | Default (local) | Default (Docker) |
|--------------|-----------------|------------------|
| `PGDATABASE` | `ecommerce_db`  | `ecommerce_db`   |
| `PGUSER`     | `$USER`         | `postgres`       |
| `PGPASSWORD` | *(empty)*       | `postgres`       |
| `PGHOST`     | `localhost`     | `db`             |
| `PGPORT`     | `5432`          | `5432`           |
| `WINDOW_DAYS`| `7`             | `7`              |

Docker maps the host port **5433 → 5432** inside the container to avoid conflicts.

## Architecture

The pipeline is a single-transaction Python script (`scripts/run_pipeline.py`) that drives all SQL directly via psycopg2. There is no ORM and no DAG framework — execution order is enforced by sequential `cur.execute()` calls within one transaction that is committed atomically or rolled back on failure.

### Data layers (PostgreSQL schemas)

```
raw        ← CSV ingestion landing zone; never modified after load
staging    ← validated, de-duplicated view of raw (rebuilt each run)
analytics  ← dimensional model + observability tables
```

### Pipeline execution order (run_pipeline.py)

1. **Schema creation** — idempotent `CREATE SCHEMA IF NOT EXISTS`
2. **Run tracking** — inserts a `pipeline_runs` row with `status='running'`
3. **Quarantine** — truncates then repopulates `raw.quarantine_orders` / `raw.quarantine_payments` based on three rules: duplicate `order_id`, missing `customer_id`, negative `order_total`, missing `order_id` in payments, `paid` amount ≠ `order_total`
4. **Staging rebuild** — drops and recreates `staging.orders_clean` / `staging.payments_clean` as `SELECT … WHERE NOT EXISTS (quarantine)`
5. **Quality gate** — configurable `MAX_ALLOWED_ERRORS` threshold (currently `999999`; tighten to enforce strict failure)
6. **Fact table (incremental)** — deletes the last `WINDOW_DAYS` rows from `analytics.fact_orders`, then re-inserts from staging using `ON CONFLICT DO UPDATE`
7. **Dimension tables** — full drops + rebuilds for `dim_customer`, `dim_product`, `dim_date`
8. **Anomaly detection** — inserts into `analytics.revenue_anomaly_log` for days where `|z-score| > 3`, `|pct_change| ≥ 30 %`, and `orders ≥ 15`
9. **Metrics snapshot** — appends row counts to `analytics.pipeline_metrics`
10. **Views** — `CREATE OR REPLACE VIEW` for `v_revenue_anomaly_daily` and `v_metrics_trend`
11. **Run status update** — sets `pipeline_runs.status = 'success'` (or `'failed'` on rollback)

### Bootstrap vs. daily load

- `bootstrap_db.py` — creates raw tables and loads the four base CSVs (`customers`, `products`, `orders`, `payments`) **once**; subsequent runs are no-ops because it checks `COUNT(*) = 0` before loading.
- `load_daily.py` — stages new rows through `raw.orders_load` / `raw.payments_load` and upserts into the main raw tables with `ON CONFLICT DO NOTHING`.
- `generate_daily_orders.py` — synthetic data generator that produces the `orders_daily.csv` and `payments_daily.csv` files consumed by `load_daily.py`.

### Contract checks

`sql/checks/contracts.sql` contains SQL assertions executed by `scripts/run_checks.py`. Each statement must return rows where the second column is either `'PASS'` or `'FAIL'`. Any `'FAIL'` row causes the script to exit non-zero.

### Dashboard

`dashboard/app.py` is a Streamlit app that connects directly to PostgreSQL and reads from the analytics schema. It queries `analytics.fact_orders`, `analytics.v_revenue_anomaly_daily`, `analytics.pipeline_metrics`, and `analytics.pipeline_runs`.

### CI

`.github/workflows/ci.yml` runs the full pipeline on every push/PR against a GitHub Actions PostgreSQL service container: initialize schemas → bootstrap → run pipeline → run checks.
