"""Transform entrypoint: bronze (MinIO) -> silver -> gold (both local
Parquet + MinIO). Run via `docker compose run --rm airflow-scheduler
python -m transform.run` (needs PySpark, so it runs inside the Airflow
image, not the host venv — see CLAUDE.md) or as the Airflow DAG's
`transform` task.
"""
from __future__ import annotations

import logging
import os
import shutil

from extract.minio_client import clear_prefix, ensure_buckets, get_client, list_json, upload_directory
from extract import config
from transform.bronze_to_silver import build_silver_df
from transform.silver_to_gold import build_gold_long, build_gold_wide
from transform.spark_session import get_spark

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("transform.run")

LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data")
SILVER_PATH = os.path.join(LOCAL_DATA_DIR, "silver", "indicators")
GOLD_LONG_PATH = os.path.join(LOCAL_DATA_DIR, "gold", "indicators_long")
GOLD_WIDE_PATH = os.path.join(LOCAL_DATA_DIR, "gold", "indicators_wide")


def _write_parquet(df, path: str) -> None:
    shutil.rmtree(path, ignore_errors=True)
    df.coalesce(1).write.mode("overwrite").parquet(path)


def _sync_to_minio(local_path: str, bucket: str, prefix: str, client) -> None:
    """Local overwrite (_write_parquet) already replaced the local copy;
    clear_prefix makes the MinIO copy a true mirror of it too, rather than
    each run's part-files piling up alongside every previous run's."""
    clear_prefix(bucket, prefix, client)
    upload_directory(local_path, bucket, prefix, client)


def run() -> None:
    client = get_client()
    ensure_buckets(client)

    banxico_records = list_json("banxico", client=client)
    inegi_records = list_json("inegi", client=client)
    log.info("bronze: %d banxico object(s), %d inegi object(s)", len(banxico_records), len(inegi_records))

    spark = get_spark()
    try:
        silver = build_silver_df(spark, banxico_records, inegi_records).cache()
        silver_count = silver.count()
        log.info("silver: %d row(s)", silver_count)
        _write_parquet(silver, SILVER_PATH)
        _sync_to_minio(SILVER_PATH, config.MINIO_SILVER_BUCKET, "indicators", client)

        gold_long = build_gold_long(silver)
        _write_parquet(gold_long, GOLD_LONG_PATH)
        _sync_to_minio(GOLD_LONG_PATH, config.MINIO_GOLD_BUCKET, "indicators_long", client)

        gold_wide = build_gold_wide(silver, spark)
        _write_parquet(gold_wide, GOLD_WIDE_PATH)
        _sync_to_minio(GOLD_WIDE_PATH, config.MINIO_GOLD_BUCKET, "indicators_wide", client)

        log.info("gold: wrote indicators_long + indicators_wide to %s and MinIO", LOCAL_DATA_DIR)
    finally:
        spark.stop()


if __name__ == "__main__":
    run()
