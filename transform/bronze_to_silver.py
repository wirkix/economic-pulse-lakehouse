"""Bronze -> silver: conforms Banxico + INEGI raw JSON into one clean fact
table, deduped across however many bronze pulls have landed.

I/O is entirely local Parquet (see transform/spark_session.py for why);
transform/run.py handles moving bytes to/from MinIO around this.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, StringType, StructField, StructType
from pyspark.sql.window import Window

from transform.parse import parse_bronze

SILVER_SCHEMA = StructType(
    [
        StructField("source", StringType(), False),
        StructField("indicator_id", StringType(), False),
        StructField("indicator_label", StringType(), False),
        StructField("obs_date", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("is_fallback", BooleanType(), False),
    ]
)


def build_silver_df(spark: SparkSession, banxico_records: list[dict], inegi_records: list[dict]) -> DataFrame:
    rows = parse_bronze(banxico_records, inegi_records)
    if not rows:
        return spark.createDataFrame([], SILVER_SCHEMA)

    df = spark.createDataFrame(rows, SILVER_SCHEMA).withColumn("obs_date", F.to_date("obs_date"))

    # Multiple bronze pulls can carry the same (indicator, date) — keep one
    # row per key, preferring a real observation over a fallback one; among
    # equally-real rows there's nothing to break ties on, so `first()` is
    # fine (they're value-identical, same source API for the same date).
    window = Window.partitionBy("source", "indicator_id", "obs_date").orderBy(F.col("is_fallback").asc())
    ranked = df.withColumn("rn", F.row_number().over(window))
    return ranked.filter(F.col("rn") == 1).drop("rn").orderBy("source", "indicator_id", "obs_date")
