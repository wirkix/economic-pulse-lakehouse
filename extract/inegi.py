"""INEGI "Banco de Indicadores" API client — bronze layer: fetches raw
per-indicator responses and hands them back unparsed.

Docs: https://www.inegi.org.mx/servicios/api_indicadores.html
Free token (email registration, separate from Banxico's):
https://www.inegi.org.mx/servicios/api_indicadores.html

URL shape (confirmed from INEGI's own doc example):
  https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/
    INDICATOR/{indicator_id}/{lang}/{area_geo}/{recent}/BISE/2.0/{token}?type=json

INDICATORS below ships with only INEGI's own documentation example ID
(1002000001, national population) as a placeholder — **not** IGAE or the
unemployment rate. The real codes for those aren't in INEGI's public docs
in a form worth guessing at; look them up at inegi.org.mx/app/indicadores
(search the indicator, "Consultar API" gives the exact ID) once INEGI_TOKEN
exists, and update INDICATORS then. Everything downstream (transform,
tests) is written against the shape of a response, not against which
indicator produced it, so swapping these in is a one-line change here.
"""
from __future__ import annotations

import requests

BASE_URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
AREA_GEO_NATIONAL = "00"

INDICATORS = [
    {"id": "1002000001", "label": "poblacion_total_placeholder"},
]


def fetch_one_raw(token: str, indicator_id: str) -> dict:
    """Raises for network/HTTP errors — callers decide whether to fall
    back (see extract/run.py)."""
    url = f"{BASE_URL}/{indicator_id}/es/{AREA_GEO_NATIONAL}/false/BISE/2.0/{token}"
    resp = requests.get(url, params={"type": "json"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fallback_raw(indicator_id: str, label: str) -> dict:
    """Used when INEGI_TOKEN is unset or the API call fails — shaped like
    a real INEGI response (`Series[].OBSERVATIONS[]`) so transform's
    parser doesn't need a separate code path, flagged at the top level."""
    return {
        "is_fallback": True,
        "label": label,
        "Series": [
            {
                "INDICADOR": indicator_id,
                "OBSERVATIONS": [
                    {"TIME_PERIOD": f"2026/0{m}", "OBS_VALUE": "100.0"} for m in range(1, 7)
                ],
            }
        ],
    }


def fetch_all(token: str) -> list[dict]:
    results = []
    for indicator in INDICATORS:
        if not token:
            results.append(fallback_raw(indicator["id"], indicator["label"]))
            continue
        try:
            raw = fetch_one_raw(token, indicator["id"])
            raw["is_fallback"] = False
            raw["label"] = indicator["label"]
        except requests.RequestException:
            raw = fallback_raw(indicator["id"], indicator["label"])
        results.append(raw)
    return results
