"""Data loading utilities using gtfs-kit and pooch."""

from __future__ import annotations

from collections.abc import Callable
from functools import lru_cache
from pathlib import Path

import gtfs_kit as gk
from loguru import logger
import pooch

from ptn_analysis.config import GTFS_ZIP_PATH

__all__ = ["load_gtfs_feed", "get_feed_date_range", "download_with_cache"]


@lru_cache(maxsize=1)
def load_gtfs_feed(gtfs_path: Path | None = None) -> gk.Feed:
    """Load and cache gtfs-kit Feed object.

    Args:
        gtfs_path: Path to GTFS ZIP file. Defaults to GTFS_ZIP_PATH.

    Returns:
        Cached gtfs-kit Feed object with all GTFS tables loaded.

    Raises:
        FileNotFoundError: If GTFS ZIP not found at specified path.
    """
    source = gtfs_path or GTFS_ZIP_PATH
    if not source.exists():
        raise FileNotFoundError(f"GTFS not found at {source}. Run 'make data' first.")

    logger.info(f"Loading GTFS Feed from {source}")
    feed = gk.read_feed(str(source), dist_units="km")
    logger.info(
        f"Loaded feed: {len(feed.stops)} stops, {len(feed.routes)} routes, {len(feed.trips)} trips"
    )
    return feed


def clear_feed_cache() -> None:
    """Clear the cached Feed object."""
    load_gtfs_feed.cache_clear()
    logger.debug("Feed cache cleared")


def get_feed_date_range() -> tuple[str, str]:
    """Get feed validity date range.

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format.

    Raises:
        ValueError: If no service dates found in feed.
    """
    feed = load_gtfs_feed()
    feed_info = feed.feed_info

    if feed_info is not None and not feed_info.empty:
        start = str(feed_info["feed_start_date"].iloc[0])
        end = str(feed_info["feed_end_date"].iloc[0])
        return f"{start[:4]}-{start[4:6]}-{start[6:8]}", f"{end[:4]}-{end[4:6]}-{end[6:8]}"

    dates = feed.get_dates()
    if dates:
        start, end = dates[0], dates[-1]
        return f"{start[:4]}-{start[4:6]}-{start[6:8]}", f"{end[:4]}-{end[4:6]}-{end[6:8]}"

    raise ValueError("No service dates found in feed")


def download_with_cache(
    url: str,
    output_path: Path,
    *,
    description: str = "Downloading",
    headers: dict[str, str] | None = None,
    timeout_seconds: float = 600.0,
    validator: Callable[[Path], bool] | None = None,
    force_redownload: bool = False,
    progressbar: bool = True,
) -> Path:
    """Download URL to a local file if not already cached.

    Uses pooch for robust download handling with progress bars.

    Args:
        url: Source URL.
        output_path: Destination path.
        description: Progress bar label.
        headers: Optional HTTP headers.
        timeout_seconds: HTTP timeout in seconds.
        validator: Optional file validator; failed validation triggers re-download.
        force_redownload: If True, ignore cache and fetch a fresh copy.
        progressbar: If True, show pooch progress bar.

    Returns:
        Path to cached/downloaded file.
    """
    if output_path.exists() and output_path.stat().st_size > 0:
        if force_redownload:
            logger.info(f"Refreshing cached file: {output_path}")
            output_path.unlink()
        elif validator is None or validator(output_path):
            logger.debug(f"Using cached: {output_path}")
            return output_path
        else:
            logger.warning(f"Cached file failed validation, re-downloading: {output_path}")
            output_path.unlink()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"{description}: {url}")
    temp_path = output_path.with_suffix(f"{output_path.suffix}.part")
    if temp_path.exists():
        temp_path.unlink()

    downloader = pooch.HTTPDownloader(
        headers=headers or {},
        progressbar=progressbar,
        timeout=timeout_seconds,
    )

    downloaded_path = pooch.retrieve(
        url=url,
        known_hash=None,
        fname=temp_path.name,
        path=str(output_path.parent),
        downloader=downloader,
    )

    downloaded = Path(downloaded_path)
    if validator is not None and not validator(downloaded):
        downloaded.unlink(missing_ok=True)
        raise ValueError(f"Downloaded file failed validation: {output_path.name}")

    downloaded.replace(output_path)
    logger.info(f"Downloaded to {output_path}")
    return output_path
