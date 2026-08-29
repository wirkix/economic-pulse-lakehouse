"""Economic Pulse Lakehouse: extract (Banxico + INEGI -> MinIO bronze) ->
transform (PySpark bronze -> silver -> gold) -> build_tableau_extract
(DuckDB gold -> tableau/extract/*.csv, for Tableau Public — see
tableau/build_extract.py's docstring for why CSV, not .hyper). Daily
schedule — Banxico's FX series updates daily, INPC/INEGI series update
monthly, so a daily run just re-writes unchanged months/observations
until the next real release, which is cheap and simpler than tracking
each series' own cadence.
"""
from __future__ import annotations

import datetime as dt

from airflow.decorators import dag, task

default_args = {"owner": "alois", "retries": 1, "retry_delay": dt.timedelta(minutes=5)}


@dag(
    dag_id="economic_pulse",
    schedule="@daily",
    start_date=dt.datetime(2026, 8, 1),
    catchup=False,
    default_args=default_args,
    tags=["economic-pulse-lakehouse"],
)
def economic_pulse():
    @task
    def extract():
        from extract.run import run as extract_run

        return extract_run()

    @task
    def transform(_bronze_keys):
        from transform.run import run as transform_run

        transform_run()

    @task
    def build_tableau_extract(_):
        from tableau.build_extract import build

        return build()

    build_tableau_extract(transform(extract()))


economic_pulse()
