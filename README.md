# Winnipeg 2025 Primary Transit Network Analysis

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

**COMP 4710 - Introduction to Data Mining**  
**University of Manitoba - Winter 2026**  
**Group 11**

## Project Overview

This project analyzes Winnipeg's redesigned Primary Transit Network (PTN), launched on **June 29, 2025**.

Core questions:
- **Network structure**: critical stops/transfers and graph properties
- **Coverage equity**: neighbourhood/community service availability
- **Service patterns**: frequency, headway, and reliability

## Team

| Member | Responsibility | Focus |
|--------|----------------|-------|
| Ahmed Hasan | Data Infrastructure | GTFS pipeline, frequency metrics |
| Cathy | Network Analysis | Graph construction, centrality |
| Sudipta | Coverage Analysis | Neighbourhood statistics |
| Stephenie | Visualization | Maps, charts, dashboard |

## Quick Start

```bash
git clone https://github.com/ahmedas22/ywg-ptn-analysis-4710-g11.git
cd ywg-ptn-analysis-4710-g11
make setup
source .venv/bin/activate
make data
make notebook
```

## Project Structure

- `ptn_analysis/analysis/` - analysis modules (network, coverage, visualization, frequency)
- `ptn_analysis/data/` - ingest/transform pipeline and database helpers
- `notebooks/` - analysis notebooks
- `data/` - raw inputs and processed DuckDB output
- `reports/figures/` - generated visuals

## Database Conventions

| Prefix | Purpose |
|--------|---------|
| `raw_` | source data tables |
| `agg_` | aggregated/derived metrics |
| `ref_` | GTFS ↔ Open Data mapping tables |
| `v_`   | analysis-ready views |

## Commands

```bash
make setup              # First-time environment setup
make data               # Run full data pipeline
make notebook           # Start Jupyter notebook server
make dashboard          # Launch Streamlit dashboard
make validate           # Validate GTFS data quality
make open-data          # Load operational datasets only
make active-mobility    # Load cycling/walkway datasets only
make lint               # Ruff checks
make format             # Ruff format
```

<<<<<<< HEAD
## Methodology

This project follows the [Cookiecutter Data Science](https://cookiecutter-data-science.drivendata.org/) template (v2) for reproducible analysis workflows.

### Data Pipeline

1. **Ingestion**: GTFS feeds downloaded and extracted; Open Data fetched via Winnipeg Open Data API
2. **Storage**: All data stored in local DuckDB database with spatial extension
3. **Transformation**: Network edges derived from stop sequences; coverage computed via spatial joins
4. **Analysis**: Team members implement domain-specific analysis using shared helper functions

### Key Libraries

| Library | Purpose |
|---------|---------|
| DuckDB | Local analytical database with spatial extension |
| NetworkX | Graph analysis and centrality computation |
| Kepler.gl | High-performance WebGL map visualization |
| Pandera | Data validation schemas |
| GeoPandas | Geospatial data manipulation |

## Timeline

| Milestone | Date | Deliverable |
|-----------|------|-------------|
| Progress Report 1 | Feb 5, 2026 | Data pipeline, initial analysis |
| Progress Report 2 | Mar 10, 2026 | Complete analysis, visualizations |
| Final Report | Apr 6, 2026 | Full report with findings |

## References

- Winnipeg Transit Master Plan: [winnipegtransit.com](https://winnipegtransit.com/en/major-announcements/winnipeg-transit-master-plan)
- General Transit Feed Specification: [gtfs.org](https://gtfs.org/)
- Cookiecutter Data Science: [drivendata.github.io](https://cookiecutter-data-science.drivendata.org/)
- NetworkX Documentation: [networkx.org](https://networkx.org/)
- DuckDB Spatial: [duckdb.org/docs/extensions/spatial](https://duckdb.org/docs/extensions/spatial.html)
=======
## Latest Pipeline Snapshot

Latest successful run loaded:
- **24 tables**
- **8,928,140 total rows**

Largest tables:
- `raw_open_data_on_time`: 5,329,339
- `raw_open_data_passenger_counts`: 2,275,201
- `raw_gtfs_stop_times`: 464,265

Historical loading is implemented and optional; availability depends on Transitland API access.

## Troubleshooting

- CSV parser error (`EOF inside string`): delete the affected file in `data/raw/` and rerun `make data`.
- Missing historical data: set `TRANSITLAND_API_KEY` in `.env` (core pipeline still runs without it).
>>>>>>> 941172c (chore: format codebase and simplify README)

## License

Educational project for COMP 4710 (University of Manitoba).
