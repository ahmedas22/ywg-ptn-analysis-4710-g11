"""GTFS download and load services."""

from __future__ import annotations

from functools import lru_cache
import os
from pathlib import Path
import re
import zipfile

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
from ptn_analysis.context.http import DataClient


def is_valid_zip(path: Path) -> bool:
    """Return whether a file is a readable ZIP archive."""
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path, "r") as zf:
            zf.namelist()
        return True
    except Exception:
        return False

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

_downloader = DataClient()


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
            # Align columns: add missing cols as NA so append works by name
            existing_cols = [
                r[0] for r in db_instance.query(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{physical_table_name}' ORDER BY ordinal_position"
                ).values
            ]
            for col in existing_cols:
                if col not in load_frame.columns:
                    load_frame[col] = None
            load_frame = load_frame[existing_cols]
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


# ── Manifest-driven GTFS resolution ───────────────────────────────────


def resolve_and_download(snapshot_id: str, city_key: str = "ywg") -> Path:
    """Download a GTFS feed using the manifest provider chain.

    Tries providers in order (mobility_data → wtlivewpg → direct).
    Saves to legacy-compatible paths so builders/routing work unchanged.

    Args:
        snapshot_id: Manifest snapshot ID (e.g. "current", "2024-09-01").
        city_key: City namespace.

    Returns:
        Path to the downloaded GTFS ZIP.
    """
    from ptn_analysis.context.config import load_gtfs_manifest

    manifest = load_gtfs_manifest()
    entry = next(
        (f for f in manifest.get("feeds", [])
         if f["snapshot_id"] == snapshot_id and f["city_key"] == city_key),
        None,
    )

    # Determine destination path (legacy-compatible)
    if snapshot_id == "current":
        dest = GTFS_ZIP_PATH
    else:
        dest = GTFS_ARCHIVE_DIR / f"{snapshot_id}.zip"

    if dest.exists() and dest.stat().st_size > 0:
        logger.info(f"GTFS {snapshot_id} cached at {dest}")
        return dest

    if entry is None:
        logger.warning(f"No manifest entry for {snapshot_id}/{city_key}, using direct download")
        if snapshot_id == "current":
            return download_current(city_key)
        return download_archive(snapshot_id)

    # Try providers in order
    for provider in entry.get("providers", []):
        try:
            ptype = provider["type"]
            if ptype == "mobility_data":
                path = _download_via_mobility_data(
                    provider, dest, city_key, manifest,
                    target_service_date=entry.get("target_service_date", "latest"),
                )
            elif ptype == "wtlivewpg":
                path = _download_via_wtlivewpg(provider, dest)
            elif ptype == "direct":
                path = _download_via_direct(provider, dest)
            else:
                continue
            if path and path.exists() and is_valid_zip(path):
                logger.info(f"GTFS {snapshot_id} downloaded via {ptype}")
                return path
        except Exception as e:
            logger.warning(f"Provider {provider['type']} failed for {snapshot_id}: {e}")

    raise RuntimeError(f"All providers failed for GTFS {snapshot_id}/{city_key}")


def _download_via_mobility_data(
    provider: dict, dest: Path, city_key: str, manifest: dict,
    target_service_date: str = "latest",
) -> Path | None:
    """Download GTFS via MobilityData API.

    Args:
        provider: Provider config from manifest entry.
        dest: Local destination path.
        city_key: City namespace.
        manifest: Full manifest dict (for pinned feed IDs).
        target_service_date: The entry's target_service_date for dataset selection.
    """
    from ptn_analysis.data.sources.mobility_data import MobilityDataClient

    client = MobilityDataClient()
    if not client.available:
        logger.debug("MobilityData: no refresh token, skipping")
        return None

    # Use pinned feed_id or discover
    feed_id = provider.get("feed_id")
    if not feed_id:
        pinned = manifest.get("pinned_feed_ids", {})
        feed_id = pinned.get(city_key)
    if not feed_id:
        feed_id = client.discover_feed_id(city_key)
        if not feed_id:
            return None

    # Use the ENTRY's target_service_date, not the provider block
    selector = provider.get("selector", "latest")
    if selector == "latest" or target_service_date in ("current", "latest"):
        dataset = client.find_dataset_for_date(feed_id, "latest")
    else:
        dataset = client.find_dataset_for_date(feed_id, target_service_date)

    if dataset is None:
        return None

    return client.download_dataset(dataset, dest)


def _download_via_wtlivewpg(provider: dict, dest: Path) -> Path:
    """Download GTFS from wtlivewpg.com archive."""
    archive_date = provider.get("archive_date")
    if not archive_date:
        raise ValueError("wtlivewpg provider requires archive_date")
    dest.parent.mkdir(parents=True, exist_ok=True)
    _downloader.request(
        f"{GTFS_ARCHIVE_URL}/{archive_date}.zip",
        cache_path=dest,
        response_format="bytes",
    )
    return dest


def _download_via_direct(provider: dict, dest: Path) -> Path:
    """Download GTFS from a direct URL."""
    url = provider.get("url")
    if not url:
        raise ValueError("direct provider requires url")
    dest.parent.mkdir(parents=True, exist_ok=True)
    _downloader.request(url, cache_path=dest, response_format="bytes")
    return dest


def manifest_feeds(city_key: str = "ywg", era: str | None = None) -> list[dict]:
    """Return manifest feed entries filtered by city and optionally era.

    Args:
        city_key: City namespace.
        era: Filter to "pre_ptn", "post_ptn", or None for all.

    Returns:
        List of manifest feed entry dicts.
    """
    from ptn_analysis.context.config import load_gtfs_manifest

    manifest = load_gtfs_manifest()
    feeds = [f for f in manifest.get("feeds", []) if f["city_key"] == city_key]
    if era:
        feeds = [f for f in feeds if f.get("era") == era]
    return feeds
