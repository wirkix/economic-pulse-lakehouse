"""Central env-driven config. Loaded once via python-dotenv so both host
scripts (`python -m extract.run`) and the Airflow DAG (which sets these as
container env vars via docker-compose, not a .env read) see the same
variable names.
"""
from __future__ import annotations

import os

from dotenv import load_dotenv

load_dotenv()


def _env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


# --- MinIO ---
# Containers (Airflow) reach MinIO by its Compose service name; host scripts
# run outside that network and need the published localhost port instead.
# IN_DOCKER is set by docker-compose.yml on the Airflow containers only.
IN_DOCKER = os.getenv("IN_DOCKER") == "true"
MINIO_ENDPOINT = _env("MINIO_ENDPOINT") if IN_DOCKER else os.getenv(
    "MINIO_ENDPOINT_HOST", "http://localhost:9000"
)
MINIO_ACCESS_KEY = _env("MINIO_ACCESS_KEY", "economic_pulse")
MINIO_SECRET_KEY = _env("MINIO_SECRET_KEY", "change-me")
MINIO_BRONZE_BUCKET = os.getenv("MINIO_BRONZE_BUCKET", "bronze")
MINIO_SILVER_BUCKET = os.getenv("MINIO_SILVER_BUCKET", "silver")
MINIO_GOLD_BUCKET = os.getenv("MINIO_GOLD_BUCKET", "gold")

# --- Tokens (both optional — extract clients fall back to flagged
# synthetic data when unset, same convention as motor-analytics'
# BANXICO_TOKEN handling) ---
BANXICO_TOKEN = os.getenv("BANXICO_TOKEN", "")
INEGI_TOKEN = os.getenv("INEGI_TOKEN", "")

# --- Tableau (CSV, not .hyper — Tableau Public Desktop's file connectors
# don't include raw Hyper extracts, only full paid Desktop's do) ---
TABLEAU_EXPORT_DIR = os.getenv("TABLEAU_EXPORT_DIR", "tableau/extract")
