"""Needs PySpark + a JVM, so it only runs inside the Airflow container, not
the host venv (see CLAUDE.md "Local dev environment"):

    docker compose run --rm airflow-scheduler python -m pytest tests/test_spark_transform.py

`importorskip` makes the host `pytest` run skip this file cleanly instead
of erroring, so `pytest` (no args) stays a valid host-venv smoke test.
"""
import pytest

pyspark = pytest.importorskip("pyspark")

from transform.bronze_to_silver import build_silver_df  # noqa: E402
from transform.silver_to_gold import build_gold_long, build_gold_wide  # noqa: E402
from transform.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = get_spark("pytest")
    yield session
    session.stop()


BANXICO_FIXTURE = {
    "is_fallback": False,
    "bmx": {
        "series": [
            {
                "idSerie": "SF43718",
                "datos": [
                    {"fecha": "01/06/2026", "dato": "17.80"},
                    {"fecha": "02/06/2026", "dato": "17.90"},
                    {"fecha": "03/06/2026", "dato": "18.00"},
                ],
            }
        ]
    },
}


def test_build_silver_df_dedupes_prefers_real_over_fallback(spark):
    fallback = {**BANXICO_FIXTURE, "is_fallback": True}
    silver = build_silver_df(spark, [BANXICO_FIXTURE, fallback], [])
    rows = silver.collect()

    # 3 distinct dates, real rows won the dedup (not doubled to 6).
    assert len(rows) == 3
    assert all(r["is_fallback"] is False for r in rows)


def test_build_gold_long_computes_change_pct(spark):
    silver = build_silver_df(spark, [BANXICO_FIXTURE], [])
    gold_long = build_gold_long(silver).orderBy("obs_date").collect()

    assert gold_long[0]["change_pct"] is None  # first obs has no prior value
    # (17.90 - 17.80) / 17.80 * 100
    assert gold_long[1]["change_pct"] == pytest.approx(0.5618, abs=1e-3)


def test_build_gold_wide_forward_fills(spark):
    silver = build_silver_df(spark, [BANXICO_FIXTURE], [])
    wide = build_gold_wide(silver, spark).orderBy("calendar_date").collect()

    assert len(wide) == 3  # dense calendar across the 3 observed days
    assert [r["fx_rate_usd_mxn"] for r in wide] == [17.80, 17.90, 18.00]
