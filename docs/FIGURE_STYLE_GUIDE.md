# Figure Style Guide

## Purpose

This file locks the visual conventions for PR2 so the report, dashboard, and presentation deck use the same visual language.

## Shared responsibilities

- `maps.py` owns map styling and map export behavior.
- `visualization.py` owns chart styling and summary panels.
- notebooks should not define their own competing color palettes.

## Core color rules

### PTN tier colors

| Tier | Color |
|---|---|
| Rapid Transit | `#0064B1` |
| Frequent Express | `#F37043` |
| Frequent | `#00B262` |
| Direct | `#026C7E` |
| Connector | `#052465` |
| Limited Span | `#8B6914` |
| Community | `#6B7280` |

### Headway colors

| Band | Color |
|---|---|
| `<10min` | `#1a9850` |
| `10-15min` | `#91cf60` |
| `15-30min` | `#fee08b` |
| `30-60min` | `#fc8d59` |
| `>60min` | `#d73027` |

### Comparison colors

- baseline or pre: `#d95f02`
- comparison or post: `#1b9e77`

### Change colors

- improvement: green
- worsening: red
- neutral: light gray

Always normalize change metrics before plotting so positive values mean improvement.

## Figure defaults

- DPI: `200`
- background: white
- `bbox_inches="tight"`
- single-panel chart: `10 x 6`
- two-panel chart: `14 x 6`
- four-panel summary: `14 x 10`
- square map: `10.5 x 10.5`

## Map defaults

- basemap: `CartoDB Positron`
- plotting CRS: `EPSG:3857`
- legend order for PTN tiers must follow `PTN_TIER_ORDER`

## Figure naming

Use the exact names registered in `ptn_analysis/reporting.py`.

## Employment-access figures

- Label this metric as `jobs proxy` or `employment destination proxy`, not exact jobs.
- Use the same improvement convention as other change charts:
  - positive change = better
  - green = improvement
  - red = worsening
