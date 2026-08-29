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
unemployment rate. Tried the widely-cited `inegiR` R package's hardcoded
IDs for those (444612 unemployment, 381016 GDP, 216064 price index) live
against this endpoint with a real token — all three 400 (confirmed via
`docker compose run --rm airflow-scheduler python -c "..."`, live INEGI
API, 2026-08-28) while the doc's own 10-digit example ID succeeds; an open
inegiR GitHub issue ("Versión 2.0 del API — la versión actual ya no
funciona") suggests that package's IDs target an older, incompatible API
version, not this one. Also tried the BIE web UI's own tree
(inegi.org.mx/app/indicadores) directly: its indicator checkboxes expose
numeric values too (e.g. IGAE "Series originales" is internally "603588")
but these are the *web UI's own* internal node IDs, a third, separate
namespace — also 400s against this endpoint. Neither inegiR's IDs nor the
BIE tree's own checkbox IDs are usable here; only IDs actually generated
by the UI's "Consultar API" action (a button on an indicator's own detail/
chart view, not exposed on the checkbox itself) are confirmed to match
this endpoint's ID space — that action wasn't reached in this pass (BIE's
site is JS-heavy enough that browser automation kept timing out screenshots
mid-flow; worth a fresh, patient attempt or just doing it by hand once).
So: real codes for THIS endpoint aren't in any public doc/package/tree
value in a form worth guessing further at — get them from that specific
"Consultar API" action and update INDICATORS then. Everything downstream
(transform, tests) is written against the shape of a response, not
against which indicator produced it, so swapping these in is a one-line
change here.
"""
from __future__ import annotations

import requests

BASE_URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
# Five digits, not two — "00" 400s; INEGI's own doc example uses "00000"
# for "all of Mexico" (confirmed live, 2026-08-28).
AREA_GEO_NATIONAL = "00000"

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
