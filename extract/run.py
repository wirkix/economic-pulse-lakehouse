"""Bronze extract entrypoint: pulls Banxico + INEGI, lands raw JSON in
MinIO's `bronze` bucket. Run via `python -m extract.run` (host venv, for a
one-off local pull) or as the Airflow DAG's `extract` task (inside Docker).
"""
from __future__ import annotations

import datetime as dt
import logging

from extract import banxico, config, inegi
from extract.minio_client import ensure_buckets, get_client, put_json

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("extract.run")

# Banxico history depth for each bronze pull — enough for the silver layer
# to compute trailing YoY/rolling-average derived columns without needing a
# separate backfill job.
LOOKBACK_DAYS = 400


def run() -> list[str]:
    client = get_client()
    ensure_buckets(client)

    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=LOOKBACK_DAYS)

    keys: list[str] = []

    banxico_raw = banxico.fetch(config.BANXICO_TOKEN, start_date, end_date)
    log.info("banxico: is_fallback=%s", banxico_raw.get("is_fallback"))
    keys.append(put_json("banxico", banxico_raw, client))

    for indicator_raw in inegi.fetch_all(config.INEGI_TOKEN):
        log.info(
            "inegi[%s]: is_fallback=%s",
            indicator_raw.get("label"),
            indicator_raw.get("is_fallback"),
        )
        keys.append(put_json("inegi", indicator_raw, client))

    log.info("wrote %d bronze object(s): %s", len(keys), keys)
    return keys


if __name__ == "__main__":
    run()
