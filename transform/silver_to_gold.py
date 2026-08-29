"""Silver -> gold: two gold tables, both read straight by DuckDB
(query/duckdb_gold.py) and by the Tableau Hyper extract build
(tableau/build_hyper.py).

- gold_indicators_long: one row per (indicator, obs_date), with
  period-over-period %% change and a 30-calendar-day rolling average —
  the natural shape for a Tableau line-chart-per-indicator view.
- gold_indicators_wide: one row per calendar day, one column per
  indicator, forward-filled — same "wide table an AI/BI tool can just
  SELECT *" idea as project #2 (motor-analytics), applied here to a daily
  economic-indicator calendar instead of a listings table.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def build_gold_long(silver: DataFrame) -> DataFrame:
    by_indicator = Window.partitionBy("source", "indicator_id").orderBy("obs_date")
    rolling_30d = (
        Window.partitionBy("source", "indicator_id")
        .orderBy(F.col("obs_date").cast("timestamp").cast("long"))
        .rangeBetween(-30 * 86400, 0)
    )

    prev_value = F.lag("value").over(by_indicator)
    return (
        silver.withColumn("prev_value", prev_value)
        .withColumn(
            "change_pct",
            F.when(prev_value.isNotNull() & (prev_value != 0), (F.col("value") - prev_value) / prev_value * 100),
        )
        .withColumn("rolling_avg_30d", F.avg("value").over(rolling_30d))
        .drop("prev_value")
        .orderBy("source", "indicator_id", "obs_date")
    )


RECENT_WINDOW_DAYS = 400  # matches extract/run.py's own LOOKBACK_DAYS


def build_gold_wide(silver: DataFrame, spark: SparkSession, recent_window_days: int = RECENT_WINDOW_DAYS) -> DataFrame:
    labels = [r["indicator_label"] for r in silver.select("indicator_label").distinct().collect()]
    if not labels:
        return spark.createDataFrame([], "calendar_date date")

    date_bounds = silver.agg(F.min("obs_date").alias("min_d"), F.max("obs_date").alias("max_d")).first()
    calendar = spark.sql(
        f"SELECT explode(sequence(to_date('{date_bounds['min_d']}'), "
        f"to_date('{date_bounds['max_d']}'), interval 1 day)) AS calendar_date"
    )

    pivoted = silver.groupBy("obs_date").pivot("indicator_label", labels).agg(F.first("value"))
    joined = calendar.join(pivoted, calendar.calendar_date == pivoted.obs_date, "left").drop("obs_date")

    ffill_window = Window.orderBy("calendar_date").rowsBetween(Window.unboundedPreceding, 0)
    wide = joined
    for label in labels:
        wide = wide.withColumn(label, F.last(F.col(label), ignorenulls=True).over(ffill_window))

    # Rows before the first real observation of a given indicator stay
    # null after forward-fill (nothing to carry forward yet) — that's
    # correct, not a bug: Tableau/DuckDB consumers should treat leading
    # nulls as "series hadn't started" rather than back-filled.
    #
    # The calendar itself, though, spans every indicator's full history —
    # mixing a daily/monthly series with even one sparse, decades-old
    # annual one (e.g. a historical population figure) blows this up to
    # tens of thousands of mostly-null rows (hit in practice: one 1910-
    # onward annual series turned a ~400-row table into 42,609). ffill is
    # computed over the *full* calendar first (so old real observations
    # still correctly seed the fill), then trimmed to the last
    # `recent_window_days` — this table is meant as a recent daily pulse,
    # not a full archive; indicators_long keeps full history per indicator
    # regardless of this table's window.
    cutoff = F.date_sub(F.lit(date_bounds["max_d"]), recent_window_days)
    return wide.filter(F.col("calendar_date") >= cutoff).orderBy("calendar_date")
