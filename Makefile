# Winnipeg PTN Analysis - COMP 4710 Group 11

.PHONY: help setup notebook dashboard lint format clean data validate open-data active-mobility status frequency

.DEFAULT_GOAL := help

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
	@echo "    make validate    Validate GTFS data quality"
	@echo ""
	@echo "  Data pipeline:"
	@echo "    make data        Full pipeline (GTFS + boundaries + graph)"
	@echo "    make open-data   Load Winnipeg Open Data datasets"
	@echo "    make active-mobility   Load cycling and walkway datasets"
	@echo "    make frequency DATE=YYYY-MM-DD   Materialize active trips"
	@echo ""
	@echo "  Other:"
	@echo "    make clean       Clear local data cache"

setup:
	@mkdir -p data/raw data/interim data/processed data/external reports/figures
	@test -f .env || cp .env.example .env
	uv venv --python 3.11
	uv sync
	@echo ""
	@echo "Setup complete! Next steps:"
	@echo "  1. source .venv/bin/activate"
	@echo "  2. make data"

notebook:
	uv run jupyter notebook notebooks/

dashboard:
	uv run streamlit run ptn_analysis/app.py --server.port 8501

lint:
	uv run ruff check ptn_analysis/

format:
	uv run ruff check --fix ptn_analysis/
	uv run ruff format ptn_analysis/

clean:
	rm -f data/processed/*.duckdb
	rm -rf data/raw/gtfs/
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleared. Run 'make data' to reload data."

data:
	uv run python -m ptn_analysis.data.make_dataset all
	uv run python -m ptn_analysis.validate all
	@echo "Data pipeline complete!"

DATE ?= $(shell date +%Y-%m-%d)
frequency:
	uv run python -m ptn_analysis.data.make_dataset service $(DATE)
	@echo "Active trips materialized for $(DATE)"

open-data:
	uv run python -m ptn_analysis.data.make_dataset open-data

active-mobility:
	uv run python -m ptn_analysis.data.make_dataset active-mobility

validate:
	uv run python -m ptn_analysis.validate all

status:
	uv run python -m ptn_analysis.data.make_dataset status
