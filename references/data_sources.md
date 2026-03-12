# Data Sources and Notes

This document captures the data sources used by the pipeline and key ingestion notes.

## Core Sources

| Source | Endpoint | Notes |
|---|---|---|
| Winnipeg Transit GTFS | `https://gtfs.winnipegtransit.com/google_transit.zip` | Current schedule feed for core network structure. |
| Winnipeg Open Data Portal | `https://data.winnipeg.ca` | Operational + spatial datasets via Socrata SODA endpoints. |
| Transitland (historical GTFS) | `https://www.transit.land` | Used for historical pre/post/transition PTN comparisons when API access permits. |

## Open Data Datasets Used

| Dataset | ID | API Pattern |
|---|---|---|
| Neighbourhoods | `8k6x-xxsy` | `/api/v3/views/{id}/query.geojson` |
| Community Areas | `gfvw-fk34` | `/api/v3/views/{id}/query.geojson` |
| Pass-ups | `mer2-irmb` | `/api/v3/views/{id}/export.csv` |
| On-time Performance | `gp3k-am4u` | `/api/v3/views/{id}/export.csv` |
| Passenger Counts | `bv6q-du26` | `/api/v3/views/{id}/export.csv` |
| Cycling Network | `kjd9-dvf5` | `/api/v3/views/{id}/query.geojson` |
| Walkways | `jdeq-xf3y` | `/api/v3/views/{id}/query.geojson` |

## Why CSV vs GeoJSON

- Use `export.csv` for large tabular operational datasets (faster ingest and less memory overhead for wide/high-row tables).
- Use `query.geojson` for spatial layers where geometry must be loaded directly.
- Post-processing and aggregations are SQL-first in `ptn_analysis/data/sql/`.

## Reproducibility Notes

- Pipeline entrypoint: `make data`
- Validation entrypoint: `python -m ptn_analysis.validate all`
- Cached raw downloads are stored in `data/raw/`.
