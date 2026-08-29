# Tableau Public report spec

The pipeline (extract → PySpark bronze/silver/gold → CSV export) is fully
automated (`python -m tableau.build_extract`, or the Airflow DAG's
`build_tableau_extract` task); this last step — authoring the actual
workbook and publishing it — isn't, because Tableau Public has no
publish CLI/API (same situation job-market-radar hit with Power BI
Desktop). Unlike that project's report, though, Tableau Public *does* host
the result live and gives it a public, embeddable URL — no separate
hosting step needed once this is done. This doc is the spec to build the
workbook from in ~20–30 minutes.

The export is CSV, not a `.hyper` extract — Tableau Public Desktop's file
connectors don't include raw Hyper extracts, only full (paid) Tableau
Desktop's do (see `tableau/build_extract.py`'s docstring).

## Prerequisites

- A free Tableau Public account (tableau.com/products/public) and Tableau
  Public Desktop installed.
- `tableau/extract/*.csv` built and up to date (`python -m
  tableau.build_extract`, or let the Airflow DAG do it).

## Connect

Two separate connections, one per CSV — Tableau's Text File connector
takes one file per connection, and these don't need a
relationship/join for the sheets below (each uses one or the other, not
both at once):

- Tableau Public Desktop → Connect → To a File → Text File → pick
  `tableau/extract/economic_pulse_indicators_long.csv`.
- Connect → To a File → Text File again → pick
  `tableau/extract/economic_pulse_indicators_wide.csv`.

Check each connection's inferred column types once loaded — CSV has no
native date/boolean types, so `obs_date`/`calendar_date` and `is_fallback`
sometimes need a manual type override (right-click the column header in
the Data pane → Change Data Type) if Tableau guessed string instead of
Date / Boolean.

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

**3. IGAE trend** (`indicators_long`, filtered to `indicator_label =
igae_ivf`)
- Line: `obs_date` × `value` — the index level (base 2018=100), same role
  `fx_rate_usd_mxn` plays in sheet 1.
- Second line, dual axis (index points don't share a scale with a %):
  `indicator_label = igae_variacion_anual`'s `value` — INEGI's own
  official year-over-year %, not this project's generic `change_pct`.

**4. Employment** (`indicators_long`, filtered to `source = inegi` and
`indicator_label` in (`desempleo_tasa`, `desocupados_total`))
- Two separate charts, not one combined — `desempleo_tasa` is a
  percentage (~2-6% range) and `desocupados_total` is a headcount in the
  millions; sharing an axis would flatten the rate to an invisible line.
  - Bar or line: `obs_date` × `value`, filtered to `desempleo_tasa`.
  - Line: `obs_date` × `value`, filtered to `desocupados_total`.
- While `is_fallback = true` for any series in view, add a text callout
  ("synthetic placeholder data — INEGI/Banxico token not yet configured")
  so a visitor never mistakes fallback data for the real thing.

**5. Snapshot KPIs** (`indicators_wide`, latest `calendar_date` row)
- Text/BAN tiles: latest `fx_rate_usd_mxn`, latest `inpc_general`, latest
  `igae_ivf`, latest `desempleo_tasa`.

## Dashboard

Combine all five sheets into one dashboard ("Economic Pulse — México"),
tiled: KPIs across the top, FX trend and INPC YoY side by side below that,
IGAE trend and Employment side by side at the bottom. Add a caption noting
the data sources (Banxico SIE API, INEGI Banco de Indicadores) and the
refresh cadence (manual — see Publish below).

## Publish

File → Save to Tableau Public As… — publish the workbook (not just the
dashboard) under a name matching the portfolio card (e.g. "Economic Pulse
— México"). Tableau Public gives it a permanent public URL
(`public.tableau.com/app/profile/<you>/viz/<workbook>/<dashboard>`) — that
URL is the `demo` link for `professional-website`'s portfolio card, and
Tableau Public also provides an embed `<iframe>` snippet (Share → Embed
Code) if the card wants an inline preview instead of just a link.

**Refresh:** re-running `python -m tableau.build_extract` overwrites both
local CSVs in place (same paths); back in Tableau, Data → [each
connection] → Refresh picks up the new rows (no need to reconnect, since
the file paths don't change). Re-publishing (same File → Save to Tableau
Public As… flow, same workbook name) pushes the refreshed data to the
same public URL. No auto-refresh — Tableau Public's scheduled-refresh
feature needs a paid/Server tier we're not using, consistent with this
project's zero-cost scope.
