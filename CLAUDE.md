# CLAUDE.md

Guidance for Claude Code (and future contributors) working in this repo.

## What this is

Portfolio project #4 of 6 in a data-engineering roadmap (see
`professional-website`'s `/portfolio` page and the "Data Pipeline Roadmap"
Claude Artifact from that planning session, card 04 "Economic Pulse
Lakehouse"). A bronze→silver→gold medallion pipeline over Banxico/INEGI
economic indicators, local/free stand-ins for an AWS Glue/S3/Athena
lakehouse. Full pipeline story and architecture diagram are in
[README.md](README.md) — read that first, it's not duplicated here.

## Repo layout

```
extract/    bronze stage — extract/banxico.py, extract/inegi.py (both
            token-optional, fall back to flagged synthetic data),
            extract/minio_client.py (boto3 S3 wrapper), extract/run.py
transform/  silver + gold stages — transform/parse.py (pure-Python bronze
            JSON parsing, unit-testable without Spark),
            transform/bronze_to_silver.py, transform/silver_to_gold.py
            (both PySpark), transform/spark_session.py, transform/run.py
query/      query/duckdb_gold.py — DuckDB over local gold Parquet (pulls a
            copy from MinIO first if not already local)
tableau/    tableau/build_hyper.py (DuckDB gold -> .hyper extract) +
            REPORT_SPEC.md, the one stage that isn't automated (Tableau
            Public has no publish API — same situation as project #1's
            Power BI step, except Tableau Public *does* host the result
            live once published, so there's no separate hosting step)
airflow/    dags/economic_pulse_dag.py orchestrates extract -> transform
            -> build_hyper_extract, daily
docker/     airflow.Dockerfile, ca-certs/ (see "Docker build network
            calls" below)
tests/      pytest — host-runnable (parse, extract fallback, DuckDB) plus
            tests/test_spark_transform.py (needs PySpark/JVM, Docker-only)
```

## Local dev environment

Host Python (3.12, via a `.venv`) runs `requirements-dev.txt` only:
`extract/banxico.py`/`extract/inegi.py` (plain `requests`), `boto3`,
`duckdb`, `pytest`. PySpark and Airflow only ever run inside Docker (which
bundles its own Python + a JDK) — never installed into the host venv, same
split job-market-radar uses for Airflow/dbt vs. its host Python.

**Relative paths need the right `WORKDIR`.** `transform/run.py`,
`query/duckdb_gold.py`, and `tableau/build_hyper.py` all default to
relative paths (`data/...`, `tableau/*.hyper`) meant to resolve against
the repo root — correct as-is for host-venv runs. The base `apache/airflow`
image's own `WORKDIR` is `/opt/airflow`, **not** the bind-mounted project
directory (`/opt/airflow/project`, from `docker-compose.yml`'s
`.:/opt/airflow/project` volume); without overriding it, those relative
paths resolve against the container's ephemeral filesystem and silently
vanish on the next `docker compose run --rm`, never reaching the host or
getting reused across runs. `docker/airflow.Dockerfile` sets
`WORKDIR /opt/airflow/project` to fix this — Airflow itself doesn't care
about process cwd (its own paths come from `AIRFLOW_HOME`), so this is
safe. If a path ever needs to be genuinely absolute regardless of cwd, use
an env var override (each module reads one, e.g. `LOCAL_DATA_DIR`) rather
than assuming this WORKDIR.

## Docker build network calls

This dev machine's Avast Antivirus HTTPS-scanning MITMs outbound TLS with
its own locally-generated root CA — the Windows OS trust store has it
(so host Python works via `truststore.inject_into_ssl()`, see
wide-table-motor-analytics-build/ecobici-pulse-build's own CLAUDE.md notes
for that pattern), but **containers don't inherit the Windows cert store**,
so `pip install` from *inside* any container's network path (both at
`docker build` time and at `docker compose run` time) fails
`CERTIFICATE_VERIFY_FAILED` against pypi.org — `apt-get` against Debian's
own mirrors is unaffected, only pip hits this.

Fixed properly, not bypassed: `docker/ca-certs/` holds that same Avast
root CA exported from the Windows cert store, installed into the image's
trust store via `update-ca-certificates` in `docker/airflow.Dockerfile`,
with `PIP_CERT`/`REQUESTS_CA_BUNDLE`/`SSL_CERT_FILE` pointed at the
resulting combined bundle so both `pip install` (build time) and this
project's own `requests` calls (`extract/banxico.py`/`extract/inegi.py`,
run time) verify for real against the CA actually terminating the
connection — never `--trusted-host`/disabled verification, which would be
a real regression anywhere else this image gets built (e.g. CI).

**`docker/ca-certs/*.crt` is machine-specific and gitignored** (only
`.gitkeep` is committed) — a fresh checkout on a machine without this
problem builds unmodified (the dir is empty, `update-ca-certificates` is a
no-op). To regenerate it on a machine that *does* need it:

```powershell
$cert = Get-ChildItem Cert:\LocalMachine\Root | Where-Object { $_.Subject -match "Avast|AVG" }
$b64 = [Convert]::ToBase64String($cert.RawData, 'InsertLineBreaks')
"-----BEGIN CERTIFICATE-----`n$b64`n-----END CERTIFICATE-----" |
  Out-File -FilePath docker\ca-certs\avast-root-ca.crt -Encoding ascii
```

Swap the `Where-Object` filter for whatever's actually intercepting on a
different machine (corporate proxy CA, a different AV product, etc.) — the
mechanism is generic, only the cert differs.

## Banxico / INEGI specifics

- Banxico series are verified real (reused from motor-analytics):
  `SF43718` (FIX FX rate, daily), `SP1` (INPC general, monthly). Token is
  free, not project-scoped — motor-analytics' `BANXICO_TOKEN` works here
  unchanged.
- **INEGI indicator IDs in `extract/inegi.py` are placeholders**, not IGAE
  or the unemployment rate — see that file's docstring. INEGI's API docs
  don't publish a clean indicator-ID catalog; look codes up at
  inegi.org.mx/app/indicadores ("Consultar API" on a chosen indicator)
  once `INEGI_TOKEN` exists.
- Both clients fall back to a flagged (`is_fallback: true`), response-shaped
  synthetic payload when their token is empty or the API call raises —
  `transform/parse.py`'s parsers don't need a separate code path for real
  vs. fallback data, and `transform/bronze_to_silver.py`'s dedup window
  prefers real rows over fallback ones whenever both exist for the same
  (indicator, date).

## Known gotchas / history

- The `apache/airflow` base image's entrypoint wraps a bare command as an
  `airflow` subcommand — `docker compose run --rm airflow-scheduler pytest
  ...` fails with `invalid choice: 'pytest'`, not "command not found".
  Always go through the interpreter explicitly: `python -m pytest ...`
  (same reason `requirements-airflow.txt` includes `pytest`, not just
  `requirements-dev.txt` — the Spark-dependent tests only run in this
  image).

- `transform/parse.py`'s INEGI parser must handle bare `"YYYY"`
  `TIME_PERIOD` values (annual indicators, e.g. the historical population
  placeholder), not just `"YYYY/MM"` (monthly) — the missing case used to
  silently drop *every* observation from an annual indicator (empty
  `month` string failed `int()`, caught by the same broad `except
  ValueError` that also legitimately skips malformed periods), so the
  indicator would just never appear anywhere downstream with no error at
  all. Caught by noticing INEGI rows were 0 in gold despite a real,
  `is_fallback=False` bronze pull.

- `transform/silver_to_gold.py`'s `build_gold_wide` must bound its output
  to a recent window (`RECENT_WINDOW_DAYS`, applied *after* forward-fill
  so old real observations still correctly seed it) — the dense daily
  calendar spans every indicator's full history otherwise, and mixing
  even one sparse, decades-old annual series with a daily/monthly one
  blows the row count up hugely (hit in practice: INEGI's 1910-onward
  placeholder turned a ~400-row table into 42,609). `indicators_long`
  keeps full history per indicator regardless of this table's window.

- DuckDB relations must come from `duckdb.sql(...)` (the module-level
  default connection), not a fresh `duckdb.connect()` per call —
  `query/duckdb_gold.py` hit `ConnectionException: Connection has already
  been closed` from a relation outliving its connection object, because an
  explicitly-created `DuckDBPyConnection` with no other live references
  gets garbage-collected (closing it) as soon as the function that created
  it returns, before the caller reads the relation.
- `transform/silver_to_gold.py`'s forward-fill window
  (`Window.orderBy("calendar_date")`, unpartitioned — there's nothing to
  partition by, it's one global daily calendar) logs Spark's "No Partition
  Defined for Window operation!" warning. Expected and harmless at this
  project's data scale (a few hundred rows); not a sign anything's wrong.
- Silver/gold MinIO uploads must clear the destination prefix first
  (`extract/minio_client.py`'s `clear_prefix`, called by
  `transform/run.py`'s `_sync_to_minio`) — S3-style object stores have no
  directory-overwrite semantics, so without this, every run's
  randomly-named `part-*.parquet` file lands *alongside* every previous
  run's instead of replacing it, and `read_parquet('*.parquet')` silently
  double-counts old data. The local copy doesn't have this problem
  (`_write_parquet` does a real `shutil.rmtree` first) — only caught this
  by inspecting MinIO's actual bucket contents after a second run, not
  from the (successful, silent) exit code.
- `extract/minio_client.py`'s `upload_directory` skips dotfiles
  (`.*.crc`, Spark/Hadoop's own write-integrity sidecars) — uploading them
  triggered a `botocore`/`urllib3` `HeaderParsingError` warning against
  MinIO's response for those specific 0-byte objects (auto-retries and
  still succeeds, but noisy, and nothing downstream reads them anyway).
