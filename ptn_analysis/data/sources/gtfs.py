"""GTFS download and load services."""

from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
import re

from loguru import logger

from ptn_analysis.context.config import (
    FEED_ID_CURRENT,
    GTFS_ARCHIVE_DIR,
    GTFS_ARCHIVE_URL,
    GTFS_URL,
    GTFS_ZIP_PATH,
    PTN_LAUNCH_DATE,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.http import Downloader
from ptn_analysis.data.sources.common import is_valid_zip

GTFS_TABLE_NAMES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
    "shapes",
    "feed_info",
    "fare_attributes",
    "fare_rules",
]

CITY_GTFS_DOWNLOAD_URLS = {"ywg": GTFS_URL}

_downloader = Downloader()


def current_url(city_key: str) -> str:
    """Return the configured GTFS URL for a city."""
    if city_key in CITY_GTFS_DOWNLOAD_URLS:
        return CITY_GTFS_DOWNLOAD_URLS[city_key]
    env_key = f"TRANSITLAND_GTFS_URL_{city_key.upper()}"
    transitland_url = os.getenv(env_key, "")
    if not transitland_url:
        transitland_url = os.getenv("TRANSITLAND_GTFS_URL", "")
    if transitland_url:
        return transitland_url
    raise ValueError(
        f"No GTFS URL is configured for city_key={city_key!r}. "
        f"Set {env_key} or TRANSITLAND_GTFS_URL."
    )


def download_current(city_key: str) -> Path:
    """Download the current GTFS ZIP."""
    _downloader.request(
        current_url(city_key),
        cache_path=GTFS_ZIP_PATH,
        response_format="bytes",
    )
    if not is_valid_zip(GTFS_ZIP_PATH):
        raise ValueError(f"Downloaded GTFS ZIP failed validation: {GTFS_ZIP_PATH}")
    return GTFS_ZIP_PATH


@lru_cache(maxsize=8)
def read_feed(gtfs_path: Path | None = None):
    """Load and cache a gtfs-kit feed object."""
    import gtfs_kit as gtfs_kit

    source_path = gtfs_path or GTFS_ZIP_PATH
    if not source_path.exists():
        raise FileNotFoundError(f"GTFS not found at {source_path}")
    logger.info(f"Loading GTFS feed from {source_path}")
    return gtfs_kit.read_feed(str(source_path), dist_units="km")


def load_feed_tables(
    city_key: str,
    db_instance: TransitDB,
    feed,
    feed_id: str,
) -> dict[str, int]:
    """Insert GTFS feed tables into DuckDB."""
    results: dict[str, int] = {}
    for table_name in GTFS_TABLE_NAMES:
        frame = getattr(feed, table_name, None)
        if frame is None or frame.empty:
            logger.warning(f"Skipping {table_name}: no data")
            results[table_name] = 0
            continue
        load_frame = frame.copy()
        load_frame.insert(0, "feed_id", feed_id)
        physical_table_name = db_instance.table_name(table_name, city_key)
        if db_instance.relation_exists(physical_table_name):
            db_instance.execute(
                f"DELETE FROM {physical_table_name} WHERE feed_id = :feed_id",
                {"feed_id": feed_id},
            )
            db_instance.load_table(physical_table_name, load_frame, mode="append")
        else:
            db_instance.load_table(physical_table_name, load_frame, mode="replace")
        results[table_name] = len(load_frame)
        logger.info(f"Loaded {len(load_frame):,} rows -> {physical_table_name}")
    return results


def load_current(city_key: str, db_instance: TransitDB) -> dict[str, int]:
    """Load the current GTFS feed into DuckDB."""
    return load_feed_tables(city_key, db_instance, read_feed(), FEED_ID_CURRENT)


def available_archives() -> list[str]:
    """Fetch all available historical archive dates (reverse-sorted)."""
    index_path = GTFS_ARCHIVE_DIR / "archive_index.html"
    if index_path.exists():
        html_text = index_path.read_text(encoding="utf-8")
    else:
        index_path.parent.mkdir(parents=True, exist_ok=True)
        html_text = _downloader.request(
            f"{GTFS_ARCHIVE_URL}/",
            cache_path=index_path,
            response_format="text",
        )
    archive_dates = re.findall(r"(\d{4}-\d{2}-\d{2})\.zip", html_text)
    archive_dates.sort(reverse=True)
    return archive_dates


def download_archive(archive_date: str) -> Path:
    """Download one historical archive ZIP."""
    cache_path = GTFS_ARCHIVE_DIR / f"{archive_date}.zip"
    _downloader.request(
        f"{GTFS_ARCHIVE_URL}/{archive_date}.zip",
        cache_path=cache_path,
        response_format="bytes",
    )
    if not is_valid_zip(cache_path):
        raise ValueError(f"Downloaded archive failed validation: {cache_path}")
    return cache_path


def is_pre_ptn(archive_date: str) -> bool:
    """Return whether an archive date is before the PTN launch."""
    return archive_date < PTN_LAUNCH_DATE


def pick_archive(pre_ptn: bool) -> str | None:
    """Pick the most recent archive matching the pre/post PTN flag."""
    for archive_date in available_archives():
        if is_pre_ptn(archive_date) == pre_ptn:
            return archive_date
    return None


def load_archive(
    city_key: str,
    db_instance: TransitDB,
    archive_date: str,
    feed_id: str,
    feed=None,
) -> dict[str, int]:
    """Load one historical GTFS archive into DuckDB."""
    loaded_feed = feed
    if loaded_feed is None:
        loaded_feed = read_feed(download_archive(archive_date))
    return load_feed_tables(city_key, db_instance, loaded_feed, feed_id)
