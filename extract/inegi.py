"""INEGI "Banco de Indicadores" API client — bronze layer: fetches raw
per-indicator responses and hands them back unparsed.

Docs: https://www.inegi.org.mx/servicios/api_indicadores.html
Free token (email registration, separate from Banxico's):
https://www.inegi.org.mx/servicios/api_indicadores.html

URL shape (confirmed from INEGI's own doc example):
  https://www.inegi.org.mx/app/api/indicadores/desarrolladores/jsonxml/
    INDICATOR/{indicator_id}/{lang}/{area_geo}/{recent}/{source}/2.0/{token}?type=json

INDICATORS' IDs all came from INEGI's own "Consultar API" action (the
indicator's detail/chart view on inegi.org.mx/app/indicadores) — the only
ID source confirmed to match this endpoint's ID space. Two others were
tried and ruled out first: the widely-cited `inegiR` R package's hardcoded
IDs (444612 unemployment, 381016 GDP, 216064 price index — all 400,
confirmed live 2026-08-28; an open inegiR GitHub issue, "Versión 2.0 del
API — la versión actual ya no funciona", suggests those target an older,
incompatible API version) and the BIE web UI's own tree-checkbox values
(inegi.org.mx/app/indicadores exposes numeric values on its checkboxes
too, e.g. IGAE "Series originales" is internally "603588" — also 400s, a
third, separate ID namespace).

`source` is genuinely per-indicator, not a fixed constant — confirmed live
2026-08-29: 444603/737121/737145 need "BIE-BISE" and 400 on plain "BISE";
6200093973 is the reverse (needs "BISE", 400s on "BIE-BISE"). No pattern
found for which indicators need which; each INDICATORS entry carries its
own.

Chose one indicator per concept rather than every variant INEGI publishes:
- `desempleo_tasa` (444603) — ENOE unemployment *rate*, a real percentage
  (2.90% as of 2026-07, confirmed live). Kept alongside `desocupados_total`
  (6200093973) — that one's magnitude (~1.6-2.9M across history) is an
  absolute *headcount*, a different, complementary metric, not a
  duplicate.
- `igae_ivf` (737121) — IGAE's raw index level (Índice de Volumen Físico,
  base 2018=100; 107.45 as of 2026-06) — the natural level series for this
  project's own derived columns (change_pct, rolling_avg_30d in
  transform/silver_to_gold.py) to be computed from, same role fx_rate_usd_mxn
  plays for Banxico.
- `igae_variacion_anual` (737145) — IGAE's own official year-over-year %,
  worth having alongside this project's generic period-over-period
  change_pct since INEGI's YoY calculation is authoritative, not
  reconstructed.
- Deliberately skipped: 737169 (Índice de Volumen Físico Acumulado) and
  737193 (Variación Anual Acumulada) — cumulative-to-date framings, a
  different question (progress through the current year) than the
  monthly-pulse one this dashboard is answering; adds clutter more than
  insight here.
- Dropped the earlier `poblacion_total_placeholder` (1002000001,
  INEGI's own doc-example ID) now that real, on-topic indicators exist —
  it was only ever a stand-in for "some real INEGI series," not itself a
  meaningful economic-pulse metric.
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
    {"id": "6200093973", "label": "desocupados_total", "source": "BISE"},
    {"id": "444603", "label": "desempleo_tasa", "source": "BIE-BISE"},
    {"id": "737121", "label": "igae_ivf", "source": "BIE-BISE"},
    {"id": "737145", "label": "igae_variacion_anual", "source": "BIE-BISE"},
]


def fetch_one_raw(token: str, indicator_id: str, source: str) -> dict:
    """Raises for network/HTTP errors — callers decide whether to fall
    back (see extract/run.py)."""
    url = f"{BASE_URL}/{indicator_id}/es/{AREA_GEO_NATIONAL}/false/{source}/2.0/{token}"
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
            raw = fetch_one_raw(token, indicator["id"], indicator["source"])
            raw["is_fallback"] = False
            raw["label"] = indicator["label"]
        except requests.RequestException:
            raw = fallback_raw(indicator["id"], indicator["label"])
        results.append(raw)
    return results
