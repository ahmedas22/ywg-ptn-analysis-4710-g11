"""
Configuration module for Winnipeg PTN Analysis.

Provides centralized path constants, database path, and Winnipeg Open Data
dataset identifiers used throughout the project.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv

# Only load .env if it exists (avoids blocking on missing file)
_env_file = Path(__file__).resolve().parents[1] / ".env"
if _env_file.exists():
    load_dotenv(_env_file)

PROJ_ROOT: Path = Path(__file__).resolve().parents[1]

DATA_DIR: Path = PROJ_ROOT / "data"
RAW_DATA_DIR: Path = DATA_DIR / "raw"
PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"

GTFS_DIR: Path = RAW_DATA_DIR / "gtfs"
GTFS_ZIP_PATH: Path = RAW_DATA_DIR / "google_transit.zip"

REPORTS_DIR: Path = PROJ_ROOT / "reports"
FIGURES_DIR: Path = REPORTS_DIR / "figures"

# DuckDB path - normalize env override relative to project root, not caller cwd
_duckdb_env = os.getenv("DUCKDB_PATH")
if _duckdb_env:
    _duckdb_path = Path(_duckdb_env).expanduser()
    DUCKDB_PATH: Path = _duckdb_path if _duckdb_path.is_absolute() else (PROJ_ROOT / _duckdb_path)
else:
    DUCKDB_PATH: Path = PROCESSED_DATA_DIR / "wpg_transit.duckdb"

GTFS_URL: str = os.getenv("GTFS_URL", "https://gtfs.winnipegtransit.com/google_transit.zip")

WPG_OPEN_DATA_URL: str = "https://data.winnipeg.ca"

DATASETS: dict[str, str] = {
    "neighbourhoods": "8k6x-xxsy",
    "communities": "gfvw-fk34",
    "cycling": "kjd9-dvf5",
    "walkways": "jdeq-xf3y",
    "pass_ups": "mer2-irmb",
    "on_time": "gp3k-am4u",
    "passenger_counts": "bv6q-du26",
}


class WpgBoundsType(TypedDict):
    """Type definition for Winnipeg geographic bounds."""

    min_lat: float
    max_lat: float
    min_lon: float
    max_lon: float
    center_lat: float
    center_lon: float


WPG_BOUNDS: WpgBoundsType = {
    "min_lat": 49.75,
    "max_lat": 50.00,
    "min_lon": -97.35,
    "max_lon": -96.95,
    "center_lat": 49.8951,
    "center_lon": -97.1384,
}

MAPBOX_TOKEN: str = os.getenv("MAPBOX_TOKEN", "")

# PTN-specific constants
PTN_LAUNCH_DATE: str = "2025-06-29"  # Winnipeg PTN launched June 29, 2025

# Transitland API for historical GTFS archives
TRANSITLAND_API_URL: str = "https://transit.land/api/v2/rest"
TRANSITLAND_FEED_ID: str = "f-cbfg-winnipegtransit"
TRANSITLAND_API_KEY: str = os.getenv("TRANSITLAND_API_KEY", "")

# Historical GTFS storage
GTFS_ARCHIVE_DIR: Path = RAW_DATA_DIR / "gtfs_archive"

__all__ = [
    "PROJ_ROOT",
    "DATA_DIR",
    "RAW_DATA_DIR",
    "PROCESSED_DATA_DIR",
    "GTFS_DIR",
    "GTFS_ZIP_PATH",
    "REPORTS_DIR",
    "FIGURES_DIR",
    "DUCKDB_PATH",
    "GTFS_URL",
    "WPG_OPEN_DATA_URL",
    "DATASETS",
    "WPG_BOUNDS",
    "MAPBOX_TOKEN",
    "PTN_LAUNCH_DATE",
    "TRANSITLAND_API_URL",
    "TRANSITLAND_FEED_ID",
    "TRANSITLAND_API_KEY",
    "GTFS_ARCHIVE_DIR",
]

# Note: Loguru uses stderr by default, which works for both CLI and notebooks.
# Avoid modifying handlers at import time as it can suppress CLI output.
