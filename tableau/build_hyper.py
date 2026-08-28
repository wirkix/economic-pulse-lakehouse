"""Builds a Tableau `.hyper` extract from the DuckDB gold tables.

This is as far as this repo automates the Tableau step — Tableau Public has
no publish API/CLI (same situation job-market-radar hit with Power BI
Desktop), so the extract this script produces gets opened and published
*manually* in Tableau Public Desktop. See REPORT_SPEC.md for the exact
steps. Run via `docker compose run --rm airflow-scheduler python -m
tableau.build_hyper` (needs `tableauhyperapi`, which lives in
requirements-airflow.txt) or as the Airflow DAG's `build_hyper_extract`
task.
"""
from __future__ import annotations

import logging
import os

from tableauhyperapi import (
    Connection,
    CreateMode,
    HyperProcess,
    Inserter,
    SqlType,
    TableDefinition,
    TableName,
    Telemetry,
)

from extract.config import TABLEAU_HYPER_PATH
from query.duckdb_gold import query_long, query_wide

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tableau.build_hyper")

_TYPE_MAP = {
    "DOUBLE": SqlType.double(),
    "BIGINT": SqlType.big_int(),
    "INTEGER": SqlType.int(),
    "DATE": SqlType.date(),
    "BOOLEAN": SqlType.bool(),
    "VARCHAR": SqlType.text(),
}


def _hyper_type(duckdb_type: str):
    base = duckdb_type.split("(")[0].upper()
    return _TYPE_MAP.get(base, SqlType.text())


def _write_table(connection: Connection, table_name: str, relation) -> None:
    columns = [
        TableDefinition.Column(name, _hyper_type(str(dtype)))
        for name, dtype in zip(relation.columns, relation.types)
    ]
    table_def = TableDefinition(table_name=TableName("public", table_name), columns=columns)
    connection.catalog.create_table(table_def)
    with Inserter(connection, table_def) as inserter:
        inserter.add_rows(rows=relation.fetchall())
        inserter.execute()
    log.info("wrote table %s (%d rows)", table_name, relation.count("*").fetchone()[0])


def build(output_path: str = TABLEAU_HYPER_PATH) -> str:
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with HyperProcess(telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU) as hyper:
        with Connection(
            endpoint=hyper.endpoint, database=output_path, create_mode=CreateMode.CREATE_AND_REPLACE
        ) as connection:
            # A fresh .hyper database already has a "public" schema —
            # creating it again raises "schema 'public' already exists".
            _write_table(connection, "indicators_long", query_long())
            _write_table(connection, "indicators_wide", query_wide())
    log.info("wrote %s", output_path)
    return output_path


if __name__ == "__main__":
    build()
