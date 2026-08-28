"""Pure-Python bronze-JSON -> flat-row parsing, kept Spark-free on purpose
so it's unit-testable without a JVM (see tests/test_parse.py). Both parsers
emit rows in the same conformed shape:

    {source, indicator_id, indicator_label, obs_date (ISO str), value (float
    or None), is_fallback (bool)}

`value=None` rows (Banxico's "N/E" missing-observation marker) are dropped
here rather than passed through as nulls — silver is meant to be a clean
fact table; forward-filling missing observations onto a dense calendar is
gold's job (see transform/silver_to_gold.py), same split motor-analytics
uses for its Banxico reference table.
"""
from __future__ import annotations

import datetime as dt

BANXICO_LABELS = {"SF43718": "fx_rate_usd_mxn", "SP1": "inpc_general"}


def parse_banxico_record(raw: dict) -> list[dict]:
    is_fallback = bool(raw.get("is_fallback", False))
    rows: list[dict] = []
    for series in raw.get("bmx", {}).get("series", []):
        indicator_id = series.get("idSerie", "")
        label = BANXICO_LABELS.get(indicator_id, indicator_id)
        for point in series.get("datos", []):
            raw_val = point.get("dato")
            if raw_val in (None, "", "N/E"):
                continue
            obs_date = dt.datetime.strptime(point["fecha"], "%d/%m/%Y").date()
            rows.append(
                {
                    "source": "banxico",
                    "indicator_id": indicator_id,
                    "indicator_label": label,
                    "obs_date": obs_date.isoformat(),
                    "value": float(raw_val),
                    "is_fallback": is_fallback,
                }
            )
    return rows


def parse_inegi_record(raw: dict) -> list[dict]:
    is_fallback = bool(raw.get("is_fallback", False))
    label = raw.get("label", "unknown")
    rows: list[dict] = []
    for series in raw.get("Series", []):
        indicator_id = series.get("INDICADOR", "")
        for obs in series.get("OBSERVATIONS", []):
            raw_val = obs.get("OBS_VALUE")
            period = obs.get("TIME_PERIOD")
            if raw_val in (None, "") or not period:
                continue
            year, _, month = period.partition("/")
            try:
                obs_date = dt.date(int(year), int(month), 1)
            except ValueError:
                continue
            rows.append(
                {
                    "source": "inegi",
                    "indicator_id": indicator_id,
                    "indicator_label": label,
                    "obs_date": obs_date.isoformat(),
                    "value": float(raw_val),
                    "is_fallback": is_fallback,
                }
            )
    return rows


def parse_bronze(banxico_records: list[dict], inegi_records: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for rec in banxico_records:
        rows.extend(parse_banxico_record(rec))
    for rec in inegi_records:
        rows.extend(parse_inegi_record(rec))
    return rows
