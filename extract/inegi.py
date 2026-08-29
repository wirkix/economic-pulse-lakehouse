"""INEGI "Banco de Indicadores" API client — bronze layer: fetches raw
per-indicator responses and hands them back unparsed.

Docs: https://www.inegi.org.mx/servicios/api_indicadores.html
Free token (email registration, separate from Banxico's):
https://www.inegi.org.mx/servicios/api_indicadores.html

URL shape (confirmed from INEGI's own doc example):
  https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/
    INDICATOR/{indicator_id}/{lang}/{area_geo}/{recent}/BISE/2.0/{token}?type=json

INDICATORS below carries INEGI's own documentation example ID
(1002000001, national population, annual back to 1910) plus 6200093973
(desocupados_total), sourced from INEGI's own "Consultar API" action —
the only ID source that's actually confirmed to match this endpoint's ID
space. Two other sources were tried and ruled out first: the widely-cited
`inegiR` R package's hardcoded IDs (444612 unemployment, 381016 GDP,
216064 price index — all 400, confirmed live 2026-08-28; an open inegiR
GitHub issue, "Versión 2.0 del API — la versión actual ya no funciona",
suggests those target an older, incompatible API version) and the BIE web
UI's own tree-checkbox values (inegi.org.mx/app/indicadores exposes
numeric values on its checkboxes too, e.g. IGAE "Series originales" is
internally "603588" — also 400s, a third, separate ID namespace). Still
missing: IGAE itself (national economic-activity index) — same "Consultar
API" action, not yet done for that one. Everything downstream (transform,
tests) is written against the shape of a response, not against which
indicator produced it, so adding more is a one-line change here.

`6200093973`'s magnitude (~1.6M as of 2026-02, confirmed live) is
consistent with an absolute headcount (ENOE's "Población desocupada"),
**not** a percentage unemployment *rate* — labeled `desocupados_total`
below rather than `tasa_desocupacion` on that basis. Re-verify/relabel if
that turns out wrong.
"""
from __future__ import annotations

import requests

BASE_URL = "https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/INDICATOR"
# "00" and "00000" both work identically for "all of Mexico" (confirmed
# live, 2026-08-29, against a real ID) — an earlier version of this
# comment claimed "00" 400s; that was a bad inference from a paraphrased
# search result, never an actual test, and was wrong. Kept as "00000"
# since that's what INEGI's own doc example uses, not because "00" fails.
AREA_GEO_NATIONAL = "00000"

INDICATORS = [
    {"id": "1002000001", "label": "poblacion_total_placeholder"},
    {"id": "6200093973", "label": "desocupados_total"},
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
