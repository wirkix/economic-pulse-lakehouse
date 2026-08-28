"""Pure-Python, host-runnable: no Spark/MinIO needed."""
import json
import pathlib

from transform.parse import parse_banxico_record, parse_bronze, parse_inegi_record

FIXTURES = pathlib.Path(__file__).parent / "fixtures"


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_parse_banxico_record_skips_missing_and_conforms_shape():
    rows = parse_banxico_record(_load("banxico_sample.json"))

    # 3 datos for SF43718 minus 1 "N/E" = 2, plus 2 for SP1 = 4 total.
    assert len(rows) == 4
    fx_rows = [r for r in rows if r["indicator_id"] == "SF43718"]
    assert len(fx_rows) == 2
    assert fx_rows[0] == {
        "source": "banxico",
        "indicator_id": "SF43718",
        "indicator_label": "fx_rate_usd_mxn",
        "obs_date": "2026-06-01",
        "value": 17.8501,
        "is_fallback": False,
    }


def test_parse_inegi_record_skips_blank_value():
    rows = parse_inegi_record(_load("inegi_sample.json"))

    assert len(rows) == 2  # third observation has "" and is dropped
    assert rows[0]["obs_date"] == "2026-04-01"
    assert rows[0]["indicator_label"] == "poblacion_total_placeholder"
    assert rows[0]["value"] == 129123456.0


def test_parse_bronze_combines_both_sources():
    rows = parse_bronze([_load("banxico_sample.json")], [_load("inegi_sample.json")])
    sources = {r["source"] for r in rows}
    assert sources == {"banxico", "inegi"}
    assert len(rows) == 6


def test_fallback_flag_propagates():
    fallback_record = _load("banxico_sample.json")
    fallback_record["is_fallback"] = True
    rows = parse_banxico_record(fallback_record)
    assert all(r["is_fallback"] for r in rows)
