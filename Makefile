# Winnipeg PTN Analysis - COMP 4710 Group 11

.PHONY: help setup notebook dashboard lint format clean data open-data status frequency test figures exports employment live-bootstrap live-snapshots validate

.DEFAULT_GOAL := help

PYTHON := ./.venv/bin/python
JUPYTER := ./.venv/bin/jupyter
STREAMLIT := ./.venv/bin/streamlit
RUFF := ./.venv/bin/ruff
export MPLCONFIGDIR := /tmp/matplotlib
export XDG_CACHE_HOME := /tmp
export JAVA_HOME ?= $(shell /usr/libexec/java_home -v 21 2>/dev/null || echo "")

help:
	@echo "Winnipeg PTN Analysis - Available commands:"
	@echo ""
	@echo "  Setup:"
	@echo "    make setup       First-time project setup"
	@echo ""
	@echo "  Daily workflow:"
	@echo "    make notebook    Start Jupyter notebook server"
	@echo "    make dashboard   Start Streamlit dashboard"
	@echo "    make status      Show data pipeline status"
	@echo ""
	@echo "  Code quality:"
	@echo "    make lint        Check code with Ruff"
	@echo "    make format      Auto-format code with Ruff"
	@echo "    make test        Run compile and import smoke checks"
	@echo ""
	@echo "  Data pipeline:"
	@echo "    make data        Full pipeline (GTFS + boundaries + Open Data + employment + live transit)"
	@echo "    make open-data   Load Open Data tables only"
	@echo "    make employment  Load jobs-proxy and place-of-work context tables"
	@echo "    make live-bootstrap  Cache wide Winnipeg Transit API metadata"
	@echo "    make live-snapshots  Refresh sampled Winnipeg Transit API snapshots"
	@echo "    make frequency DATE=YYYY-MM-DD   Materialize active trips"
	@echo ""
	@echo "  Validation:"
	@echo "    make validate    Run data quality checks independently"
	@echo ""
	@echo "  Reports:"
	@echo "    make figures           Generate PR1 figures (default)"
	@echo "    make figures REPORT=pr2  Generate PR2 figures"
	@echo "    make figures REPORT=all  Generate all report figures"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean       Clear local data cache"

setup:
	@mkdir -p data/raw data/cache data/interim data/processed data/external reports/pr1/figures reports/pr2/figures
	@test -f .env || cp .env.example .env
	uv venv --python 3.11
	uv sync --all-extras
	@echo ""
	@echo "Setup complete! Next steps:"
	@echo "  1. source .venv/bin/activate"
	@echo "  2. make data"

notebook:
	$(JUPYTER) notebook notebooks/

dashboard:
	$(STREAMLIT) run ptn_analysis/app.py --server.port 8501 --server.fileWatcherType none

lint:
	$(RUFF) check ptn_analysis/

format:
	$(RUFF) check --fix ptn_analysis/
	$(RUFF) format ptn_analysis/

test:
	MPLCONFIGDIR=/tmp/matplotlib $(PYTHON) -m compileall ptn_analysis
	MPLCONFIGDIR=/tmp/matplotlib $(PYTHON) -c "from ptn_analysis import CoverageAnalyzer, FrequencyAnalyzer, NetworkAnalyzer; print('import smoke ok')"
	MPLCONFIGDIR=/tmp/matplotlib $(PYTHON) -c "from ptn_analysis.reporting import REPORT_NOTEBOOKS, generate_figures; print('reporting import ok')"
	MPLCONFIGDIR=/tmp/matplotlib $(PYTHON) -c "from ptn_analysis.context.serving import Dashboard; print('app import ok')"
	@if [ -d tests ]; then $(PYTHON) -m pytest tests/ -v --tb=short -m "not integration"; else echo "No tests/ directory found; skipping pytest."; fi

clean:
	rm -f data/pipeline.log
	rm -rf data/interim/ data/processed/ data/exports/
	rm -rf data/raw/employment/ data/raw/gtfs_archive/ data/raw/open_data/ data/raw/api_cache/
	rm -f data/raw/google_transit.zip data/raw/ywg_census_da_CA21.geojson data/raw/manitoba-latest.osm.pbf
	rm -f data/raw/archive_index.html data/raw/**/*.part
	find notebooks -type f -name "*.executed.ipynb" -delete 2>/dev/null || true
	find notebooks -name "*.duckdb" -delete 2>/dev/null || true
	find reports/pr1/figures -mindepth 1 -delete 2>/dev/null || true
	find reports/pr2/figures -mindepth 1 -delete 2>/dev/null || true
	find reports -type f \( -name "*.aux" -o -name "*.log" -o -name "*.out" -o -name "*.fls" -o -name "*.fdb_latexmk" \) -delete 2>/dev/null || true
	find models/production -type f \( -name "*.pkl" -o -name "*.joblib" \) -delete 2>/dev/null || true
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -name ".DS_Store" -delete 2>/dev/null || true
	@mkdir -p data/raw data/cache data/interim data/processed data/external data/exports
	@echo "Fully cleared. Run 'make data' for a fresh rebuild."

data:
	$(PYTHON) -m ptn_analysis.data.make_dataset all
	@echo "Data pipeline complete!"

status:
	$(PYTHON) -m ptn_analysis.data.make_dataset status

DATE ?= $(shell date +%Y-%m-%d)
frequency:
	$(PYTHON) -m ptn_analysis.data.make_dataset service $(DATE)
	@echo "Active trips materialized for $(DATE)"

open-data:
	$(PYTHON) -m ptn_analysis.data.make_dataset open-data

employment:
	$(PYTHON) -m ptn_analysis.data.make_dataset employment

live-bootstrap:
	$(PYTHON) -m ptn_analysis.data.make_dataset live-bootstrap

live-snapshots:
	$(PYTHON) -m ptn_analysis.data.make_dataset live-snapshots

# Default report: pr1. Override with: make figures REPORT=pr2  or  make figures REPORT=all
REPORT ?= pr1

figures:
	$(PYTHON) -c "from ptn_analysis.context.reporting import generate_figures; generate_figures('$(REPORT)')"
	@echo "Figures generated for report: $(REPORT)"

# Compile LaTeX report to PDF (requires pdflatex): make pdf REPORT=pr1 or REPORT=pr2
pdf:
	@echo "Compiling $(REPORT)/main.tex..."
	@cd reports/$(REPORT) && pdflatex -interaction=nonstopmode main.tex > /dev/null 2>&1 && pdflatex -interaction=nonstopmode main.tex > /dev/null 2>&1
	@echo "PDF written to reports/$(REPORT)/main.pdf"

exports:
	$(PYTHON) -m ptn_analysis.data.make_dataset exports

validate:
	$(PYTHON) -m ptn_analysis.data.make_dataset validate
