"""DuckDB query layer over the gold Parquet tables. Reads local Parquet
directly (no `httpfs` extension — see transform/spark_session.py for why);
pulls a fresh local copy from MinIO first if one isn't already on disk, so
this works standalone (e.g. from the Tableau Hyper build, or a host-venv
`python -c "from query.duckdb_gold import ..."` smoke test) without
requiring the caller to have just run transform/run.py in the same
process.
"""
from __future__ import annotations

import glob
import os

import duckdb

from extract import config
from extract.minio_client import download_directory, get_client

LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")
GOLD_LONG_PATH = os.path.join(LOCAL_DATA_DIR, "gold", "indicators_long")
GOLD_WIDE_PATH = os.path.join(LOCAL_DATA_DIR, "gold", "indicators_wide")


def _ensure_local(path: str, prefix: str) -> str:
    parquet_files = glob.glob(os.path.join(path, "*.parquet"))
    if not parquet_files:
        download_directory(config.MINIO_GOLD_BUCKET, prefix, path, get_client())
        parquet_files = glob.glob(os.path.join(path, "*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(
            f"No gold Parquet files found locally at {path} or in MinIO under "
            f"{config.MINIO_GOLD_BUCKET}/{prefix}/ — run `python -m transform.run` first."
        )
    return os.path.join(path, "*.parquet")


def gold_long_glob() -> str:
    return _ensure_local(GOLD_LONG_PATH, "indicators_long")


def gold_wide_glob() -> str:
    return _ensure_local(GOLD_WIDE_PATH, "indicators_wide")


def query_long(sql_suffix: str = "") -> "duckdb.DuckDBPyRelation":
    """`sql_suffix` is appended after `FROM read_parquet(...)`, e.g.
    `"WHERE indicator_label = 'fx_rate_usd_mxn' ORDER BY obs_date"`.

    Uses `duckdb.sql` (the module-level default connection) rather than a
    fresh `duckdb.connect()` per call — a connection object with no other
    references gets garbage-collected as soon as the function returns,
    which closes it out from under the caller's still-unread relation."""
    return duckdb.sql(f"SELECT * FROM read_parquet('{gold_long_glob()}') {sql_suffix}")


def query_wide(sql_suffix: str = "") -> "duckdb.DuckDBPyRelation":
    return duckdb.sql(f"SELECT * FROM read_parquet('{gold_wide_glob()}') {sql_suffix}")


def row_counts() -> dict[str, int]:
    return {
        "indicators_long": query_long().count("*").fetchone()[0],
        "indicators_wide": query_wide().count("*").fetchone()[0],
    }
