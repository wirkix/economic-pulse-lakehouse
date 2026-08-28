"""Local-mode SparkSession builder.

Deliberately **not** wired to MinIO via the Hadoop S3A connector: pulling
`hadoop-aws`/`aws-java-sdk-bundle` at runtime means Spark's Ivy resolver
hitting repo.maven.apache.org, a non-PyPI host this dev machine's Avast
HTTPS interception has repeatedly broken for other projects (see
CLAUDE.md). Instead, bronze/silver/gold move between MinIO and Spark as
plain local Parquet files (boto3 download/upload on either side of a Spark
read/write) — see transform/run.py. Same reasoning applies to DuckDB's
`httpfs` extension in query/duckdb_gold.py: it also reads local Parquet.
"""
from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "economic-pulse-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
