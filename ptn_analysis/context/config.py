"""Configuration module for Winnipeg PTN Analysis.

Provides centralized path constants, database path, and Winnipeg Open Data
dataset identifiers used throughout the project.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TypedDict

from dotenv import load_dotenv

# Only load .env if it exists (avoids blocking on missing file)
_env_file = Path(__file__).resolve().parents[2] / ".env"
if _env_file.exists():
    load_dotenv(_env_file)

PROJ_ROOT: Path = Path(__file__).resolve().parents[2]

DATA_DIR: Path = PROJ_ROOT / "data"
RAW_DATA_DIR: Path = DATA_DIR / "raw"
CACHE_DATA_DIR: Path = DATA_DIR / "cache"
INTERIM_DATA_DIR: Path = DATA_DIR / "interim"
PROCESSED_DATA_DIR: Path = DATA_DIR / "processed"
OPEN_DATA_CACHE_DIR: Path = CACHE_DATA_DIR / "open_data"
TRANSIT_API_CACHE_DIR: Path = CACHE_DATA_DIR / "api" / "winnipeg_transit" / "v4"

GTFS_ZIP_PATH: Path = RAW_DATA_DIR / "google_transit.zip"

REPORTS_DIR: Path = PROJ_ROOT / "reports"
# Per-report figure directories: reports/<report_name>/figures/
# Use REPORTS_DIR / "<report>" / "figures" directly; no single FIGURES_DIR.

MODELS_DIR: Path = PROJ_ROOT / "models"
PRODUCTION_MODELS_DIR: Path = MODELS_DIR / "production"

# DuckDB path - normalize env override relative to project root, not caller cwd
_duckdb_env = os.getenv("DUCKDB_PATH")
if _duckdb_env:
    _duckdb_path = Path(_duckdb_env).expanduser()
    DUCKDB_PATH: Path = _duckdb_path if _duckdb_path.is_absolute() else (PROJ_ROOT / _duckdb_path)
else:
    DUCKDB_PATH: Path = INTERIM_DATA_DIR / "wpg_transit.duckdb"

_serving_duckdb_env = os.getenv("SERVING_DUCKDB_PATH")
if _serving_duckdb_env:
    _serving_duckdb_path = Path(_serving_duckdb_env).expanduser()
    SERVING_DUCKDB_PATH: Path = (
        _serving_duckdb_path
        if _serving_duckdb_path.is_absolute()
        else (PROJ_ROOT / _serving_duckdb_path)
    )
else:
    SERVING_DUCKDB_PATH: Path = PROCESSED_DATA_DIR / "wpg_transit_serving.duckdb"

GTFS_URL: str = os.getenv("GTFS_URL", "https://gtfs.winnipegtransit.com/google_transit.zip")

WPG_OPEN_DATA_URL: str = "https://data.winnipeg.ca"

DATASETS: dict[str, str] = {
    "neighbourhoods": "8k6x-xxsy",  # data.winnipeg.ca neighbourhood boundaries
    "communities": "gfvw-fk34",  # data.winnipeg.ca community areas
    "cycling": "kjd9-dvf5",  # data.winnipeg.ca cycling network paths
    "walkways": "jdeq-xf3y",  # data.winnipeg.ca pedestrian walkways
    "pass_ups": "mer2-irmb",  # data.winnipeg.ca transit pass-up incidents
    "on_time": "gp3k-am4u",  # data.winnipeg.ca on-time performance
    "passenger_counts": "bv6q-du26",  # data.winnipeg.ca passenger boarding counts
    "census_poverty_2021": "ige9-5jxk",  # 2021 neighbourhood-level poverty (census)
}

# SODA API pagination
OPEN_DATA_PAGE_LIMIT: int = 50_000

# Headway calculation window for gtfs-kit (standard transit analysis convention).
# Actual GTFS service runs 04:45–26:10; this window captures core-service headways.
SERVICE_DAY_START: str = "06:00:00"
SERVICE_DAY_END: str = "22:00:00"


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

DEFAULT_CITY_KEY: str = "ywg"

# PTN-specific constants
PTN_LAUNCH_DATE: str = "2025-06-29"  # Winnipeg PTN launched June 29, 2025
PTN_HEADWAY_TARGETS: dict[str, tuple[int, int]] = {
    "Rapid Transit": (4, 10),
    "Frequent Express": (5, 15),
    "Frequent": (10, 15),
    "Direct": (10, 15),
    "Connector": (15, 30),
    "Limited Span": (30, 60),
    "Community": (30, 60),
}

# Historical GTFS archive (wtlivewpg.com — community archive since 2010)
GTFS_ARCHIVE_URL: str = "https://wtlivewpg.com/Pages/gtfs"
GTFS_ARCHIVE_DIR: Path = RAW_DATA_DIR / "gtfs_archive"

# Winnipeg Transit API (live transit features)
WINNIPEG_TRANSIT_API_KEY: str = os.getenv("WINNIPEG_TRANSIT_API_KEY", "")

FEED_ID_CURRENT: str = "current"
FEED_ID_PRE_PTN: str = "pre_ptn"
PRE_PTN_ARCHIVE_COUNT: int = 4  # seasonal snapshots before PTN launch
DEFAULT_ANALYSIS_DATE: str = "2026-01-15"
H3_RESOLUTION: int = 8  # ~461 m hexagon side; TOD canonical scale

# Cited spatial constants
WALK_BUFFER_M: float = 400.0  # TCQSM (TRB, 2013) pedestrian catchment
BIKE_BUFFER_M: float = 500.0  # Conservative winter estimate; NACTO ideal = 800 m
CIRCUITY_FACTOR: float = 1.3  # Iacono et al. (2010) network circuity adjustment
WALKSCORE_DECAY_M: float = 400.0  # WalkScore Transit Score (2011) decay distance
MAX_WALK_MINUTES: float = 10.0  # TCQSM maximum acceptable walk to transit stop
WGS84_CRS: str = "EPSG:4326"
WINNIPEG_PROJECTED_CRS: str = "EPSG:32614"
WEB_MERCATOR_CRS: str = "EPSG:3857"

# Routing infrastructure (r5py + city2graph + osmnx)
OSM_PBF_PATH: Path = RAW_DATA_DIR / "manitoba-latest.osm.pbf"
OSM_PBF_URL: str = "https://download.geofabrik.de/north-america/canada/manitoba-latest.osm.pbf"
ROUTING_CACHE_DIR: Path = CACHE_DATA_DIR / "routing"
R5_DEPARTURE_TIME: str = "08:00:00"
R5_DEPARTURE_WINDOW_MINUTES: int = 10
R5_PERCENTILES: tuple[int, ...] = (25, 50, 75)
R5_ISOCHRONE_MINUTES: list[int] = [10, 20, 30, 45]
JOBS_ACCESS_MAX_TRAVEL_MINUTES: int = 45
PRE_PTN_ARCHIVE_DATES: list[str] = ["2024-09-01", "2024-12-15"]

EMPLOYMENT_DATA_DIR: Path = Path(
    os.getenv("EMPLOYMENT_DATA_DIR", str(DATA_DIR / "external"))
).expanduser()
EMPLOYMENT_CACHE_DIR: Path = INTERIM_DATA_DIR / "employment"
STATCAN_WDS_URL: str = os.getenv(
    "STATCAN_WDS_URL", "https://www150.statcan.gc.ca/t1/wds/rest"
)
STATCAN_POW_PRODUCT_IDS: tuple[str, ...] = ("98100491", "98100492")
CBP_DA_SOURCE_URL: str = os.getenv("CBP_DA_SOURCE_URL", "").strip()
CBP_DA_SOURCE_PATH: Path = Path(
    os.getenv("CBP_DA_SOURCE_PATH", str(EMPLOYMENT_DATA_DIR / "cbp_da_dec2022.csv"))
).expanduser()
CENSUS_POW_SOURCE_PATHS: tuple[Path, ...] = (
    Path(
        os.getenv(
            "CENSUS_POW_SOURCE_PATH_1",
            str(EMPLOYMENT_DATA_DIR / "census_pow_98100491.csv"),
        )
    ).expanduser(),
    Path(
        os.getenv(
            "CENSUS_POW_SOURCE_PATH_2",
            str(EMPLOYMENT_DATA_DIR / "census_pow_98100492.csv"),
        )
    ).expanduser(),
)


def normalize_gtfs_date(raw: str) -> str:
    """Convert YYYYMMDD to YYYY-MM-DD format (single source of truth).

    Args:
        raw: Date string in YYYYMMDD or YYYY-MM-DD format.

    Returns:
        Date string in YYYY-MM-DD format.
    """
    val = str(raw).strip()
    if len(val) == 8 and val.isdigit():
        return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
    return val


__all__ = [
    "PROJ_ROOT",
    "MODELS_DIR",
    "PRODUCTION_MODELS_DIR",
    "DATA_DIR",
    "RAW_DATA_DIR",
    "CACHE_DATA_DIR",
    "INTERIM_DATA_DIR",
    "PROCESSED_DATA_DIR",
    "OPEN_DATA_CACHE_DIR",
    "TRANSIT_API_CACHE_DIR",
    "GTFS_ZIP_PATH",
    "REPORTS_DIR",
    "DUCKDB_PATH",
    "SERVING_DUCKDB_PATH",
    "GTFS_URL",
    "WPG_OPEN_DATA_URL",
    "DATASETS",
    "OPEN_DATA_PAGE_LIMIT",
    "SERVICE_DAY_START",
    "SERVICE_DAY_END",
    "WPG_BOUNDS",
    "MAPBOX_TOKEN",
    "DEFAULT_CITY_KEY",
    "PTN_LAUNCH_DATE",
    "PTN_HEADWAY_TARGETS",
    "GTFS_ARCHIVE_URL",
    "GTFS_ARCHIVE_DIR",
    "normalize_gtfs_date",
    "WINNIPEG_TRANSIT_API_KEY",
    "FEED_ID_CURRENT",
    "FEED_ID_PRE_PTN",
    "DEFAULT_ANALYSIS_DATE",
    "H3_RESOLUTION",
    "WGS84_CRS",
    "WINNIPEG_PROJECTED_CRS",
    "WEB_MERCATOR_CRS",
    "EMPLOYMENT_DATA_DIR",
    "EMPLOYMENT_CACHE_DIR",
    "STATCAN_WDS_URL",
    "STATCAN_POW_PRODUCT_IDS",
    "CBP_DA_SOURCE_URL",
    "CBP_DA_SOURCE_PATH",
    "CENSUS_POW_SOURCE_PATHS",
    "WALK_BUFFER_M",
    "BIKE_BUFFER_M",
    "CIRCUITY_FACTOR",
    "WALKSCORE_DECAY_M",
    "MAX_WALK_MINUTES",
    "OSM_PBF_PATH",
    "OSM_PBF_URL",
    "ROUTING_CACHE_DIR",
    "R5_DEPARTURE_TIME",
    "R5_DEPARTURE_WINDOW_MINUTES",
    "R5_PERCENTILES",
    "R5_ISOCHRONE_MINUTES",
    "JOBS_ACCESS_MAX_TRAVEL_MINUTES",
    "PRE_PTN_ARCHIVE_DATES",
    "HEADWAY_TIER_COLORS",
    "HEADWAY_TIER_LIST",
    "PTN_TIER_COLORS",
    "PTN_TIER_ORDER",
    "FX_ROUTE_COLORS",
    "classify_ptn_tier",
    "get_route_display_color",
    "headway_tier",
]

# ---------------------------------------------------------------------------
# PTN tier colors and classification (shared across all notebooks/dashboard)
# ---------------------------------------------------------------------------

HEADWAY_TIER_COLORS: dict[str, str] = {
    "<10min": "#1a9850",
    "10-15min": "#91cf60",
    "15-30min": "#fee08b",
    "30-60min": "#fc8d59",
    ">60min": "#d73027",
}

# (threshold, label, color, line_weight) — ordered best-to-worst service
HEADWAY_TIER_LIST: list[tuple[float, str, str, int]] = [
    (10, "<10min", "#1a9850", 6),
    (15, "10-15min", "#91cf60", 5),
    (30, "15-30min", "#fee08b", 4),
    (60, "30-60min", "#fc8d59", 3),
    (float("inf"), ">60min", "#d73027", 2),
]

PTN_TIER_COLORS: dict[str, str] = {
    "Rapid Transit": "#0064B1",
    "Frequent Express": "#F37043",
    "Frequent": "#00B262",
    "Direct": "#026C7E",
    "Connector": "#052465",
    "Limited Span": "#8B6914",
    "Community": "#6B7280",
}

PTN_TIER_ORDER: list[str] = [
    "Rapid Transit",
    "Frequent Express",
    "Frequent",
    "Direct",
    "Connector",
    "Limited Span",
    "Community",
]

FX_ROUTE_COLORS: dict[str, str] = {
    "FX2": "#F37043",
    "FX3": "#F27EA6",
    "FX4": "#6450A1",
}

_CONNECTOR_ROUTES: set[str] = {
    "22", "28", "31", "37", "38", "39", "43", "48", "70", "74",
}

_LIMITED_SPAN_ROUTES: set[str] = {
    "690", "691", "694", "833", "881", "883", "884", "885",
    "886", "887", "888", "889", "895",
}


def classify_ptn_tier(route_short_name: str) -> tuple[str, str]:
    """Classify a route into a PTN tier and return its tier color.

    Logic must match the ``route_ptn_tiers`` view in ``views.sql``.

    Args:
        route_short_name: GTFS route_short_name (e.g. ``"BLUE"``, ``"FX2"``).

    Returns:
        Tuple of ``(tier_label, hex_color)``.
    """
    name = str(route_short_name).strip()

    if name == "BLUE":
        tier = "Rapid Transit"
    elif name.startswith("FX"):
        tier = "Frequent Express"
    elif name.startswith("F") and not name.startswith("FX"):
        tier = "Frequent"
    elif name.startswith("D"):
        tier = "Direct"
    elif name in _CONNECTOR_ROUTES:
        tier = "Connector"
    elif name in _LIMITED_SPAN_ROUTES:
        tier = "Limited Span"
    elif len(name) <= 2 and name.isdigit():
        tier = "Connector"
    elif len(name) == 3 and name.isdigit():
        tier = "Community"
    else:
        tier = "Community"

    return tier, PTN_TIER_COLORS[tier]


def get_route_display_color(route_short_name: str, route_color: str | None = None) -> str:
    """Get display color for a route, preferring GTFS route_color.

    Args:
        route_short_name: GTFS route_short_name.
        route_color: GTFS route_color (hex without ``#`` prefix, may be None).

    Returns:
        Hex color string with ``#`` prefix.
    """
    if route_color and route_color.strip():
        color = route_color.strip()
        return f"#{color}" if not color.startswith("#") else color

    name = str(route_short_name).strip()
    if name in FX_ROUTE_COLORS:
        return FX_ROUTE_COLORS[name]

    _, tier_color = classify_ptn_tier(name)
    return tier_color


def headway_tier(headway: float) -> tuple[str, str, int]:
    """Classify a headway value into a tier label, color, and line weight.

    Args:
        headway: Mean headway in minutes.

    Returns:
        (tier_label, hex_color, line_weight) tuple.
    """
    for threshold, label, color, weight in HEADWAY_TIER_LIST:
        if headway < threshold:
            return label, color, weight
    return ">60min", "#d73027", 2


# Note: Loguru uses stderr by default, which works for both CLI and notebooks.
# Avoid modifying handlers at import time as it can suppress CLI output.
