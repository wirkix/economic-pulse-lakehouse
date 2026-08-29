"""Thin boto3-S3 wrapper for MinIO: bucket bootstrap, bronze JSON
put/list, and local-Parquet-directory upload/download/clear helpers used
by transform/run.py and query/duckdb_gold.py. Bronze objects are raw JSON,
one per source per run, timestamped and never overwritten — an
append-only landing zone. Silver/gold are Parquet written locally by Spark
(see transform/spark_session.py for why not via Spark's own S3A client)
then synced to MinIO by this module; unlike bronze, each sync clears the
destination prefix first (see clear_prefix) since silver/gold represent
current state, not history — leaving old runs' part-files in place would
make every `read_parquet('*.parquet')` double-count past data.
"""
from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
from botocore.client import Config as BotoConfig

from extract import config


def get_client():
    return boto3.client(
        "s3",
        endpoint_url=config.MINIO_ENDPOINT,
        aws_access_key_id=config.MINIO_ACCESS_KEY,
        aws_secret_access_key=config.MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_buckets(client=None) -> None:
    client = client or get_client()
    existing = {b["Name"] for b in client.list_buckets().get("Buckets", [])}
    for bucket in (config.MINIO_BRONZE_BUCKET, config.MINIO_SILVER_BUCKET, config.MINIO_GOLD_BUCKET):
        if bucket not in existing:
            client.create_bucket(Bucket=bucket)


def bronze_key(source: str) -> str:
    """`<source>/<source>_<UTC-timestamp>_<8-hex>.json`. The 8-hex suffix
    matters: extract/run.py calls put_json once per INEGI indicator in a
    tight loop, easily landing two calls in the same second (second-
    precision timestamp alone isn't unique enough) — hit in practice, one
    indicator's real bronze pull silently overwrote another's under the
    same key, only noticed because the *second* indicator's data never
    made it past silver despite a logged successful fetch. Split out from
    put_json so the uniqueness property is unit-testable without a real
    MinIO client."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{source}/{source}_{ts}_{uuid.uuid4().hex[:8]}.json"


def put_json(source: str, payload: dict[str, Any], client=None) -> str:
    """Writes one bronze object at the key `bronze_key(source)` builds and
    returns it. Timestamped, not overwritten — bronze is an append-only
    raw landing zone; silver/gold decide what "latest" means."""
    client = client or get_client()
    key = bronze_key(source)
    client.put_object(
        Bucket=config.MINIO_BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(payload).encode("utf-8"),
        ContentType="application/json",
    )
    return key


def list_json(source: str, bucket: str = None, client=None) -> list[dict]:
    """Downloads and parses every bronze object under `<source>/` — bronze
    is small (a handful of JSON files per run) so listing everything rather
    than tracking "latest" is simplest and cheap."""
    client = client or get_client()
    bucket = bucket or config.MINIO_BRONZE_BUCKET
    out: list[dict] = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{source}/"):
        for obj in page.get("Contents", []):
            body = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            out.append(json.loads(body))
    return out


def download_directory(bucket: str, prefix: str, local_dir: str, client=None) -> int:
    """Inverse of upload_directory — used by query/duckdb_gold.py to pull
    gold Parquet down before a local, non-Spark process (DuckDB, the
    Tableau Hyper build) reads it."""
    client = client or get_client()
    os.makedirs(local_dir, exist_ok=True)
    count = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            rel_path = obj["Key"][len(prefix) + 1 :]
            local_path = os.path.join(local_dir, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            client.download_file(bucket, obj["Key"], local_path)
            count += 1
    return count


def clear_prefix(bucket: str, prefix: str, client=None) -> int:
    """Deletes every object under `<bucket>/<prefix>/` — call before
    upload_directory for silver/gold so each run's Parquet fully replaces
    the last one instead of accumulating alongside it (S3-style stores
    have no directory-overwrite semantics; this is the closest
    equivalent)."""
    client = client or get_client()
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        keys.extend({"Key": obj["Key"]} for obj in page.get("Contents", []))
    if keys:
        client.delete_objects(Bucket=bucket, Delete={"Objects": keys})
    return len(keys)


def upload_directory(local_dir: str, bucket: str, prefix: str, client=None) -> int:
    """Uploads every file under a local Parquet output directory (Spark
    writes one directory per table: real `part-*.parquet` files plus a
    `_SUCCESS` marker and hidden `.*.crc` checksum sidecars) to
    `<bucket>/<prefix>/`, preserving relative paths. Returns the file count.

    Skips the `.crc` sidecars: they're Spark/Hadoop's own write-integrity
    check, meaningless once copied elsewhere, and MinIO's response headers
    for these particular 0-byte objects trip a botocore/urllib3 header
    parser warning (harmless — request auto-retries and succeeds — but
    noisy) for reasons not worth chasing further for files nothing reads
    anyway."""
    client = client or get_client()
    count = 0
    for root, _dirs, files in os.walk(local_dir):
        for fname in files:
            if fname.startswith("."):
                continue
            local_path = os.path.join(root, fname)
            rel_path = os.path.relpath(local_path, local_dir).replace(os.sep, "/")
            client.upload_file(local_path, bucket, f"{prefix}/{rel_path}")
            count += 1
    return count
