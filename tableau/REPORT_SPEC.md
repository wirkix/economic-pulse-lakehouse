# Tableau Public report spec

The pipeline (extract → PySpark bronze/silver/gold → `.hyper` extract) is
fully automated (`python -m tableau.build_hyper`, or the Airflow DAG's
`build_hyper_extract` task); this last step — authoring the actual
workbook and publishing it — isn't, because Tableau Public has no
publish CLI/API (same situation job-market-radar hit with Power BI
Desktop). Unlike that project's report, though, Tableau Public *does* host
the result live and gives it a public, embeddable URL — no separate
hosting step needed once this is done. This doc is the spec to build the
workbook from in ~20–30 minutes.

## Prerequisites

- A free Tableau Public account (tableau.com/products/public) and Tableau
  Public Desktop installed.
- `tableau/economic_pulse_gold.hyper` built and up to date (`python -m
  tableau.build_hyper`, or let the Airflow DAG do it).

## Connect

Tableau Public Desktop → Connect → To a File → More… → pick
`economic_pulse_gold.hyper`. Both tables (`indicators_long`,
`indicators_wide`) load as data sources — no relationship/join needed for
the sheets below (each uses one or the other, not both at once).

## Sheets

**1. FX rate trend** (`indicators_wide`)
- Line: `calendar_date` (continuous, day) × `fx_rate_usd_mxn`
- Second line, same axis, dashed: `fx_rate_usd_mxn` from
  `indicators_long` filtered to `indicator_label = fx_rate_usd_mxn`'s
  `rolling_avg_30d` — the 30-day rolling average against the raw daily
  rate.

**2. Inflation (INPC) YoY %** (`indicators_long`, filtered to
`indicator_label = inpc_general`)
- Bar or line: `obs_date` × `change_pct`.

**3. INEGI indicators** (`indicators_long`, filtered to `source = inegi`)
- Line: `obs_date` × `value`, colored by `indicator_label`. Currently
  `poblacion_total_placeholder` (real, but not an economic-pulse metric —
  INEGI's own doc-example ID) and `desocupados_total` (real, unemployment
  headcount — see `extract/inegi.py`'s docstring on why it's a headcount
  and not a rate). IGAE itself is still missing; add it here once found.
- While `is_fallback = true` for any series in view, add a text callout
  ("synthetic placeholder data — INEGI/Banxico token not yet configured")
  so a visitor never mistakes fallback data for the real thing.

**4. Snapshot KPIs** (`indicators_wide`, latest `calendar_date` row)
- Text/BAN tiles: latest `fx_rate_usd_mxn`, latest `inpc_general`, latest
  INEGI indicator value(s).

## Dashboard

Combine all four sheets into one dashboard ("Economic Pulse — México"),
tiled: KPIs across the top, FX trend and INPC YoY side by side below,
INEGI indicators full-width at the bottom. Add a caption noting the data
sources (Banxico SIE API, INEGI Banco de Indicadores) and the refresh
cadence (manual — see Publish below).

## Publish

File → Save to Tableau Public As… — publish the workbook (not just the
dashboard) under a name matching the portfolio card (e.g. "Economic Pulse
— México"). Tableau Public gives it a permanent public URL
(`public.tableau.com/app/profile/<you>/viz/<workbook>/<dashboard>`) — that
URL is the `demo` link for `professional-website`'s portfolio card, and
Tableau Public also provides an embed `<iframe>` snippet (Share → Embed
Code) if the card wants an inline preview instead of just a link.

**Refresh:** re-running `python -m tableau.build_hyper` overwrites the
local `.hyper` file; re-publishing (same File → Save to Tableau Public As…
flow, same workbook name) pushes the new data to the same public URL. No
auto-refresh — Tableau Public's scheduled-refresh feature needs a
paid/Server tier we're not using, consistent with this project's
zero-cost scope.
