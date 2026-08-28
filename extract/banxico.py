"""Banxico SIE API client — bronze layer: fetches the raw multi-series
response and hands it back unparsed (schema conformance is transform's job,
not extract's).

Docs: https://www.banxico.org.mx/SieAPIRest/service/v1/doc/index.html
Free token: https://www.banxico.org.mx/SieAPIRest/service/v1/token
(same token motor-analytics uses — Banxico tokens aren't project-scoped)

Series pulled (both verified against the live API in motor-analytics):
  SF43718 — Tipo de cambio FIX, pesos por USD (daily)
  SP1     — INPC general (monthly)
"""
from __future__ import annotations

import datetime as dt

import requests

SERIES_FX = "SF43718"
SERIES_INPC = "SP1"
BASE_URL = "https://www.banxico.org.mx/SieAPIRest/service/v1/series"


def fetch_raw(token: str, start_date: dt.date, end_date: dt.date) -> dict:
    """Raises for network/HTTP errors — callers decide whether to fall
    back (see extract/run.py)."""
    series_ids = f"{SERIES_FX},{SERIES_INPC}"
    url = f"{BASE_URL}/{series_ids}/datos/{start_date:%Y-%m-%d}/{end_date:%Y-%m-%d}"
    resp = requests.get(url, headers={"Bmx-Token": token}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fallback_raw(start_date: dt.date, end_date: dt.date) -> dict:
    """Used when BANXICO_TOKEN is unset or the API call fails — shaped
    like a real Banxico response (same `bmx.series[].datos[]` structure)
    so transform's parser doesn't need a separate code path, but flagged
    at the top level so bronze consumers can tell it's synthetic."""
    calendar = [start_date + dt.timedelta(days=i) for i in range((end_date - start_date).days + 1)]
    return {
        "is_fallback": True,
        "bmx": {
            "series": [
                {
                    "idSerie": SERIES_FX,
                    "titulo": "Tipo de cambio FIX (fallback)",
                    "datos": [{"fecha": d.strftime("%d/%m/%Y"), "dato": "17.5"} for d in calendar],
                },
                {
                    "idSerie": SERIES_INPC,
                    "titulo": "INPC general (fallback)",
                    "datos": [
                        {"fecha": d.strftime("%d/%m/%Y"), "dato": "130.0"}
                        for d in calendar
                        if d.day == 1
                    ],
                },
            ]
        },
    }


def fetch(token: str, start_date: dt.date, end_date: dt.date) -> dict:
    if not token:
        return fallback_raw(start_date, end_date)
    try:
        raw = fetch_raw(token, start_date, end_date)
        raw["is_fallback"] = False
        return raw
    except requests.RequestException:
        return fallback_raw(start_date, end_date)
