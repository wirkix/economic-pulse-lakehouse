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

**No relationship between the two connections is ever needed for this
spec** — a Tableau *worksheet* binds to exactly one data source (whichever
is active in the bottom-left Data pane when you build it); a *dashboard*
combines already-built worksheets, not data sources, so sheets from
different connections coexist on one dashboard with nothing to relate.
The rule that matters: a *single sheet* only needs a relationship/blend if
it must plot fields from **both** files on **one chart** at once. Every
sheet below avoids that — sheets 1–4 use `indicators_long` only (it
already carries `value` alongside its own `change_pct`/`rolling_avg_30d`
for the same row, so nothing from `indicators_wide` is needed there);
only sheet 5 uses `indicators_wide`, and it's the only sheet that does.

## Sheets

**1. FX rate trend** (`indicators_long`, filtered to `indicator_label =
fx_rate_usd_mxn`)
- Drag `value` to Rows — the raw daily rate.
- Drag `rolling_avg_30d` to Rows again, to the right of `value` — this
  creates a second pane with its own axis. Right-click that second axis →
  "Dual Axis", then right-click it again → "Synchronize Axis" so both
  lines share one scale. Format the `rolling_avg_30d` line dashed (click
  its mark card → the line-style dropdown) to distinguish it from the raw
  rate.
- `obs_date` (continuous, day) on Columns.

**2. Inflation (INPC) YoY %** (`indicators_long`, filtered to
`indicator_label = inpc_general`)
- Bar or line: `obs_date` × `change_pct`.

**3. IGAE trend** (`indicators_long`, filtered to `indicator_label` in
(`igae_ivf`, `igae_variacion_anual`))
- These are two *different rows* (different `indicator_label` values,
  same `value` column) that need to end up as two lines on one chart —
  unlike sheet 1, where both numbers already sit on the same row. Splitting
  a shared column by a dimension like this is the standard Tableau
  pattern for long/tall data: two calculated fields (right-click
  `indicators_long` in the Data pane → Create Calculated Field), then
  plot those two calc fields the same dual-axis way as sheet 1:
  ```
  IGAE Level := IF [indicator_label] = "igae_ivf" THEN [value] END
  IGAE YoY %  := IF [indicator_label] = "igae_variacion_anual" THEN [value] END
  ```
- Rows: `IGAE Level`, then `IGAE YoY %` as a second pane → dual axis (not
  synchronized this time — index points, roughly 90–115, and a
  percentage, roughly ±5, don't share a scale). Columns: `obs_date`.

**4. Employment** (`indicators_long`, filtered to `source = inegi` and
`indicator_label` in (`desempleo_tasa`, `desocupados_total`))
- Two separate sheets, not one combined chart — `desempleo_tasa` is a
  percentage (~2–6% range) and `desocupados_total` is a headcount in the
  millions; sharing an axis would flatten the rate to an invisible line.
  Simpler than sheet 3's split, since each of these two sheets only shows
  *one* indicator: just add an `indicator_label` filter to each ("Employment
  — rate" filtered to `desempleo_tasa`, "Employment — headcount" filtered
  to `desocupados_total`), no calculated field needed.
  - Bar or line: `obs_date` × `value`, filter: `indicator_label = desempleo_tasa`.
  - Line: `obs_date` × `value`, filter: `indicator_label = desocupados_total`.
- While `is_fallback = true` for any series in view, add a text callout
  ("synthetic placeholder data — INEGI/Banxico token not yet configured")
  so a visitor never mistakes fallback data for the real thing.

**5. Snapshot KPIs** (`indicators_wide`, latest `calendar_date` row) —
the one sheet that uses the wide table, because it's exactly what wide is
for: one row already has every indicator's latest value as its own
column, with no per-indicator filtering/pivoting needed to grab all of
them at once.
- Text/BAN tiles: latest `fx_rate_usd_mxn`, latest `inpc_general`, latest
  `igae_ivf`, latest `desempleo_tasa`.

## Dashboard

New Dashboard → drag each of the five already-built sheets onto the
canvas as its own tile (Dashboard pane, left side, lists every sheet in
the workbook regardless of which data source built it — this is the step
where sheets from `indicators_long` and `indicators_wide` end up on the
same dashboard with nothing to relate). Tiled: KPIs across the top, FX
trend and INPC YoY side by side below that, IGAE trend and Employment
side by side at the bottom. Add a caption noting the data sources
(Banxico SIE API, INEGI Banco de Indicadores) and the refresh cadence
(manual — see Publish below).

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
