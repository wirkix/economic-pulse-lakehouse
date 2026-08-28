"""Host-runnable: builds tiny local Parquet fixtures with DuckDB itself (no
Spark needed to produce Parquet — see transform/spark_session.py's
docstring for why the real pipeline avoids Spark's S3A connector, which is
the same reasoning DuckDB's local-Parquet-only query layer follows) and
points query/duckdb_gold.py at them via monkeypatch instead of MinIO.
"""
import os

import duckdb

from query import duckdb_gold


def _write_parquet(con: duckdb.DuckDBPyConnection, sql: str, path: str) -> None:
    os.makedirs(path, exist_ok=True)
    con.sql(f"COPY ({sql}) TO '{os.path.join(path, 'part-0.parquet')}' (FORMAT PARQUET)")


def test_query_long_and_wide_read_local_fixtures(tmp_path, monkeypatch):
    long_path = tmp_path / "gold" / "indicators_long"
    wide_path = tmp_path / "gold" / "indicators_wide"
    monkeypatch.setattr(duckdb_gold, "GOLD_LONG_PATH", str(long_path))
    monkeypatch.setattr(duckdb_gold, "GOLD_WIDE_PATH", str(wide_path))

    con = duckdb.connect()
    _write_parquet(
        con,
        """
        SELECT * FROM (VALUES
            ('banxico', 'SF43718', 'fx_rate_usd_mxn', DATE '2026-06-01', 17.85::DOUBLE, NULL::DOUBLE, 17.85::DOUBLE, false),
            ('banxico', 'SF43718', 'fx_rate_usd_mxn', DATE '2026-06-02', 17.90::DOUBLE, 0.28::DOUBLE, 17.875::DOUBLE, false)
        ) AS t(source, indicator_id, indicator_label, obs_date, value, change_pct, rolling_avg_30d, is_fallback)
        """,
        str(long_path),
    )
    _write_parquet(
        con,
        """
        SELECT * FROM (VALUES
            (DATE '2026-06-01', 17.85::DOUBLE),
            (DATE '2026-06-02', 17.90::DOUBLE)
        ) AS t(calendar_date, fx_rate_usd_mxn)
        """,
        str(wide_path),
    )

    long_rel = duckdb_gold.query_long("ORDER BY obs_date")
    assert long_rel.fetchall()[0][4] == 17.85

    wide_rel = duckdb_gold.query_wide("ORDER BY calendar_date")
    assert wide_rel.columns == ["calendar_date", "fx_rate_usd_mxn"]

    counts = duckdb_gold.row_counts()
    assert counts == {"indicators_long": 2, "indicators_wide": 2}


def test_ensure_local_raises_clear_error_without_data_or_minio(tmp_path, monkeypatch):
    monkeypatch.setattr(duckdb_gold, "GOLD_LONG_PATH", str(tmp_path / "empty"))

    def _fail_download(*_args, **_kwargs):
        raise ConnectionError("no MinIO in this test")

    monkeypatch.setattr(duckdb_gold, "download_directory", _fail_download)

    try:
        duckdb_gold.gold_long_glob()
        assert False, "expected an error when no local data and MinIO is unreachable"
    except ConnectionError:
        pass
