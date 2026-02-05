# Winnipeg PTN Analysis - COMP 4710 Group 11

.PHONY: help setup notebook dashboard lint format clean data open-data status frequency test

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
	@echo "    make test        Run test suite"
	@echo ""
	@echo "  Data pipeline:"
	@echo "    make data        Full pipeline (GTFS + boundaries + Open Data + graph)"
	@echo "    make open-data   Load Open Data tables only"
	@echo "    make frequency DATE=YYYY-MM-DD   Materialize active trips"
	@echo ""
	@echo "  Cleanup:"
	@echo "    make clean       Clear local data cache"

setup:
	@mkdir -p data/raw data/interim data/processed data/external reports/figures
	@test -f .env || cp .env.example .env
	uv venv --python 3.11
	uv sync --all-extras
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

test:
	uv run pytest tests/ -v

clean:
	rm -f data/processed/*.duckdb
	rm -rf data/raw/gtfs/
	rm -f data/raw/*.csv data/raw/*.geojson data/raw/*.zip
	find data/raw -maxdepth 1 -type d -name "????-????_*" -exec rm -rf {} +
	find data/raw -type f -name "*.part" -delete
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleared. Run 'make data' to reload."

data:
	uv run python -m ptn_analysis.data.make_dataset all
	@echo "Data pipeline complete!"

status:
	uv run python -m ptn_analysis.data.make_dataset status

DATE ?= $(shell date +%Y-%m-%d)
frequency:
	uv run python -m ptn_analysis.data.make_dataset service $(DATE)
	@echo "Active trips materialized for $(DATE)"

open-data:
	uv run python -m ptn_analysis.data.make_dataset open-data
