# Economic Pulse Lakehouse

A bronze→silver→gold medallion pipeline over Mexico's public economic
indicators (Banxico, INEGI) — the same shape as an AWS Glue/S3/Athena
lakehouse, run entirely local and free.

Built as portfolio project #4 in a 6-project data-engineering roadmap:
extract → object-store lakehouse → Spark transforms → SQL query layer →
BI visualization, each stage a real, separately-runnable piece.

## Architecture

```
   Banxico SIE API              INEGI Banco de Indicadores API
   (FX rate, INPC — daily/       (IGAE, unemployment — token
    monthly, token optional,      required, falls back to
    falls back to a flagged       flagged synthetic data
    synthetic series if unset)    if INEGI_TOKEN is unset)
          │                                │
          └────────────────┬───────────────┘
                            ▼
                   extract/run.py (Python)
                            │
                            ▼
              MinIO (S3-compatible) — bronze/
             raw JSON, one object per source per run
                            │
                            ▼
                  transform/run.py (PySpark, local mode)
              bronze → silver (conformed fact table, deduped)
              silver → gold   (change_pct, rolling_avg_30d;
                                + a wide, forward-filled daily
                                calendar table — same idea as
                                project #2's wide table, applied
                                to a daily indicator calendar)
                            │
                            ▼
         MinIO — silver/, gold/ (Parquet)     + local data/ copy
                            │
                            ▼
              query/duckdb_gold.py — DuckDB reads
                 the local gold Parquet directly
                            │
                            ▼
              tableau/build_hyper.py — DuckDB gold
                 tables -> a .hyper extract
                            │
                            ▼
            Tableau Public (manual publish — see
            tableau/REPORT_SPEC.md; Tableau Public
            has no publish API, same situation as
            project #1's Power BI step)
```

Orchestrated end to end by Airflow (`airflow/dags/economic_pulse_dag.py`,
Docker Compose, LocalExecutor) — daily schedule, same job-market-radar
Postgres+Airflow pattern from project #1.

## Why this design

**Local/free instead of AWS-hosted.** MinIO stands in for S3, PySpark
local mode for Glue, DuckDB for Athena — everything runs in Docker Compose
at $0, and swapping the storage/query layer back to real S3 + Glue +
Athena later is a config change (a different boto3 endpoint, DuckDB's
`httpfs` extension instead of local Parquet), not a rewrite.

**Two gold tables, on purpose.** `indicators_long` (one row per
indicator/date, with period-over-period `%` change and a 30-day rolling
average) is the natural shape for a Tableau line chart per indicator.
`indicators_wide` (one row per calendar day, one column per indicator,
forward-filled) is the same "an AI/BI tool can just `SELECT *`" idea as
project #2's wide table, applied here to a daily economic-indicator
calendar instead of a listings table.

**Fallback data, not a broken demo.** Both `extract/banxico.py` and
`extract/inegi.py` fall back to a flagged (`is_fallback: true`), API-shaped
synthetic response when their token is missing or the call fails — the
whole pipeline (extract → transform → query → Hyper extract) is buildable
and runnable today without waiting on INEGI's registration email, same
convention motor-analytics uses for `BANXICO_TOKEN`.

## Local development

Prerequisites: Docker, a free [Banxico SIE API
token](https://www.banxico.org.mx/SieAPIRest/service/v1/token) (optional —
falls back without one), a free [INEGI Indicadores API
token](https://www.inegi.org.mx/servicios/api_indicadores.html) (optional,
same fallback), and — only if your own network setup needs it — see
CLAUDE.md's "Docker build network calls" note before your first
`docker compose build`.

```bash
cp .env.example .env   # fill in tokens if you have them; safe to leave blank
docker compose up -d postgres minio
docker compose up airflow-init      # one-time: migrates the Airflow DB, creates the admin user
docker compose up -d airflow-webserver airflow-scheduler
```

Airflow UI: http://localhost:8081 (`AIRFLOW_ADMIN_USER`/`AIRFLOW_ADMIN_PASSWORD`
from `.env`). MinIO console: http://localhost:9001
(`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`). Trigger the `economic_pulse` DAG
from the UI, or run each stage by hand:

```bash
docker compose run --rm airflow-scheduler python -m extract.run
docker compose run --rm airflow-scheduler python -m transform.run
docker compose run --rm airflow-scheduler python -m tableau.build_hyper
```

## Running pieces individually

- **Host venv** (`.venv`, `requirements-dev.txt`) — pure-Python pieces
  only: `extract/banxico.py`, `extract/inegi.py`, `query/duckdb_gold.py`
  (once gold Parquet exists locally or in MinIO), and the non-Spark tests.
  PySpark/Airflow never run on the host — see CLAUDE.md.
- **`python -m query.duckdb_gold`-style ad hoc queries** — anything that
  imports `query/duckdb_gold.py` pulls a local copy of the gold Parquet
  from MinIO automatically if `data/gold/` isn't already populated.

## Tests

```bash
.venv/Scripts/python -m pytest              # host-runnable: parse, fallback, DuckDB
docker compose run --rm airflow-scheduler python -m pytest tests/test_spark_transform.py
```

The Spark-dependent tests are `importorskip`-guarded so the host run above
skips them cleanly instead of erroring.

## Data model summary

- **bronze** (MinIO, JSON): one object per source (`banxico`, `inegi`) per
  extract run — raw API responses, untouched.
- **silver** (`indicators`): `source, indicator_id, indicator_label,
  obs_date, value, is_fallback` — one clean row per (indicator, date),
  deduped across bronze pulls, real observations preferred over fallback.
- **gold `indicators_long`**: silver + `change_pct` (vs. the prior
  observation) + `rolling_avg_30d`.
- **gold `indicators_wide`**: one row per calendar day from the earliest
  to the latest observed date, one column per `indicator_label`, forward-filled.

## Known limitations

- `extract/inegi.py`'s `INDICATORS` list ships with only INEGI's own
  documentation example indicator ID (national population) as a
  placeholder — not IGAE or the unemployment rate. Confirmed pulling real
  data with a live `INEGI_TOKEN` (`is_fallback: false`, real observations
  back to 1910), but real IGAE/unemployment codes still need manual lookup
  — see that file's docstring for what's been tried and ruled out.
- Tableau Public's scheduled-refresh feature needs a paid/Server tier;
  refreshing the published viz with new data is a manual re-publish (see
  `tableau/REPORT_SPEC.md`'s Publish section) — an accepted tradeoff for
  staying at $0.
- `gold_wide`'s forward-fill leaves genuine leading nulls before an
  indicator's first real observation (not backfilled) — correct behavior,
  not a bug; see the comment in `transform/silver_to_gold.py`. Its
  calendar is also trimmed to a recent window (`RECENT_WINDOW_DAYS`, see
  that same file) after forward-fill — a sparse, decades-old indicator
  otherwise blows the row count up hugely; `indicators_long` keeps each
  indicator's full history regardless.
