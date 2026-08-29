"""Exports the DuckDB gold tables to CSV for Tableau Public.

**Not** a `.hyper` extract — an earlier version of this script built one
via `tableauhyperapi`, but Tableau Public Desktop's file connectors are
Excel, CSV/text, JSON, spatial, and statistical files only; `.hyper`
extracts need full (paid) Tableau Desktop, confirmed against Tableau's own
published Public-vs-Desktop feature comparison. A real design mistake in
the original version of this script, caught only once the user actually
tried opening the file in Tableau Public — not from anything in this
repo's own testing, since there's no way to script-verify what a GUI
app's file-open dialog accepts.

This is as far as this repo automates the Tableau step — Tableau Public
has no publish API/CLI either way (same situation job-market-radar hit
with Power BI Desktop), so these CSVs get opened and published *manually*
in Tableau Public Desktop. See REPORT_SPEC.md for the exact steps. Run via
`docker compose run --rm airflow-scheduler python -m tableau.build_extract`
or as the Airflow DAG's `build_tableau_extract` task.
"""
from __future__ import annotations

import logging
import os

from extract.config import TABLEAU_EXPORT_DIR
from query.duckdb_gold import query_long, query_wide

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("tableau.build_extract")


def _write_csv(relation, path: str) -> None:
    relation.write_csv(path)
    log.info("wrote %s (%d rows)", path, relation.count("*").fetchone()[0])


def build(output_dir: str = TABLEAU_EXPORT_DIR) -> list[str]:
    os.makedirs(output_dir, exist_ok=True)
    long_path = os.path.join(output_dir, "economic_pulse_indicators_long.csv")
    wide_path = os.path.join(output_dir, "economic_pulse_indicators_wide.csv")
    _write_csv(query_long(), long_path)
    _write_csv(query_wide(), wide_path)
    return [long_path, wide_path]


if __name__ == "__main__":
    build()
