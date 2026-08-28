"""Host-runnable: exercises the no-token fallback paths (no network calls)
and checks the fallback payloads round-trip through transform/parse.py's
parsers cleanly — the whole point of shaping fallbacks like real API
responses.
"""
import datetime as dt

from extract import banxico, inegi
from transform.parse import parse_banxico_record, parse_inegi_record


def test_banxico_fetch_falls_back_without_token():
    raw = banxico.fetch("", dt.date(2026, 1, 1), dt.date(2026, 1, 31))
    assert raw["is_fallback"] is True
    rows = parse_banxico_record(raw)
    assert len(rows) > 0
    assert all(r["is_fallback"] for r in rows)


def test_inegi_fetch_all_falls_back_without_token():
    records = inegi.fetch_all("")
    assert len(records) == len(inegi.INDICATORS)
    assert all(r["is_fallback"] for r in records)
    rows = parse_inegi_record(records[0])
    assert len(rows) > 0


def test_banxico_fallback_covers_full_date_range():
    start, end = dt.date(2026, 3, 1), dt.date(2026, 3, 10)
    raw = banxico.fallback_raw(start, end)
    fx_series = next(s for s in raw["bmx"]["series"] if s["idSerie"] == banxico.SERIES_FX)
    assert len(fx_series["datos"]) == (end - start).days + 1
