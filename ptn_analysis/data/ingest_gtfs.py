"""GTFS ingestion helpers, including historical Transitland feeds."""

from dataclasses import dataclass
from pathlib import Path
import zipfile

import httpx
from loguru import logger
import pandas as pd
from tqdm import tqdm

from ptn_analysis.config import (
    GTFS_ARCHIVE_DIR,
    GTFS_DIR,
    GTFS_URL,
    GTFS_ZIP_PATH,
    PTN_LAUNCH_DATE,
    TRANSITLAND_API_KEY,
    TRANSITLAND_API_URL,
    TRANSITLAND_FEED_ID,
)
from ptn_analysis.data.db import bulk_insert_df, drop_create, validate_identifier
from ptn_analysis.data.ingest_shared import (
    download_with_cache,
    parse_yyyymmdd_columns,
    select_existing_columns,
)
from ptn_analysis.data.schemas import GTFS_TABLES, GTFSTableConfig

BATCH_SIZE = 5000
MAX_FILE_SIZE_MB = 100


def _enforce_max_file_size(file_path: Path) -> None:
    """Raise if file exceeds configured size limit.

    Args:
        file_path: Path to file to validate.
    """
    file_size_mb = file_path.stat().st_size / (1024 * 1024)
    if file_size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(
            f"File {file_path.name} ({file_size_mb:.1f}MB) exceeds {MAX_FILE_SIZE_MB}MB limit"
        )


def _safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract ZIP with path traversal protection.

    Args:
        zip_path: Path to ZIP archive.
        extract_dir: Destination extraction directory.

    Raises:
        ValueError: If a member path escapes the extraction root.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.namelist():
            if ".." in member or member.startswith("/") or member.startswith("\\"):
                raise ValueError(f"Unsafe path in ZIP archive: {member}")
            resolved = (extract_dir / member).resolve()
            if not str(resolved).startswith(str(extract_dir.resolve())):
                raise ValueError(f"Path escapes extraction directory: {member}")
        archive.extractall(extract_dir)


def download_gtfs() -> Path:
    """Download current Winnipeg GTFS archive.

    Returns:
        Path to downloaded ZIP file.
    """
    logger.info(f"Downloading GTFS from {GTFS_URL}")
    download_with_cache(
        GTFS_URL,
        GTFS_ZIP_PATH,
        description="Downloading GTFS",
        follow_redirects=True,
    )
    logger.info(f"Downloaded GTFS to {GTFS_ZIP_PATH}")
    return GTFS_ZIP_PATH


def extract_gtfs() -> Path:
    """Extract GTFS archive to raw data directory.

    Returns:
        Path to GTFS extraction directory.
    """
    logger.info(f"Extracting GTFS to {GTFS_DIR}")
    _safe_extract_zip(GTFS_ZIP_PATH, GTFS_DIR)
    files = list(GTFS_DIR.glob("*.txt"))
    logger.info(f"Extracted {len(files)} files: {[f.name for f in files]}")
    return GTFS_DIR


def load_gtfs_table(config: GTFSTableConfig) -> int:
    """Load one GTFS table into DuckDB.

    Args:
        config: GTFS table configuration.

    Returns:
        Number of loaded rows.
    """
    file_path = GTFS_DIR / config.filename
    if not file_path.exists():
        raise FileNotFoundError(f"{config.filename} not found at {file_path}")

    _enforce_max_file_size(file_path)

    logger.info(f"Loading {config.filename}")
    drop_create("raw", config.table_name, config.ddl)

    if config.use_chunking:
        total_rows = 0
        for chunk in tqdm(
            pd.read_csv(file_path, chunksize=BATCH_SIZE, dtype=config.dtypes),
            desc=f"Loading {config.log_name}",
        ):
            available_columns = select_existing_columns(config.columns, list(chunk.columns))
            chunk = chunk[available_columns]
            chunk = parse_yyyymmdd_columns(chunk, config.date_columns)
            bulk_insert_df(chunk, "raw", config.table_name, log_insert=False)
            total_rows += len(chunk)
        logger.info(f"Loaded {total_rows:,} {config.log_name}")
        return total_rows

    df = pd.read_csv(file_path, dtype=config.dtypes)
    available_columns = select_existing_columns(config.columns, list(df.columns))
    df = df[available_columns]
    df = parse_yyyymmdd_columns(df, config.date_columns)
    bulk_insert_df(df, "raw", config.table_name, log_insert=False)
    logger.info(f"Loaded {len(df):,} {config.log_name}")
    return len(df)


@dataclass(frozen=True)
class GTFSFeedVersion:
    """Transitland GTFS feed version metadata."""

    sha1: str
    fetched_at: str
    earliest_service_date: str
    latest_service_date: str
    download_url: str

    @property
    def is_pre_ptn(self) -> bool:
        """Return whether feed is fully before PTN launch."""
        return self.latest_service_date < PTN_LAUNCH_DATE

    @property
    def is_post_ptn(self) -> bool:
        """Return whether feed is fully after PTN launch."""
        return self.earliest_service_date >= PTN_LAUNCH_DATE


def fetch_transitland_feed_versions(limit: int = 50) -> list[GTFSFeedVersion]:
    """Fetch available historical feed versions from Transitland.

    Args:
        limit: Maximum number of versions to request.

    Returns:
        List of feed versions.

    Raises:
        ValueError: If Transitland API key is missing.
    """
    if not TRANSITLAND_API_KEY:
        raise ValueError(
            "TRANSITLAND_API_KEY not set. Sign up for free academic access at "
            "https://www.transit.land/ and set the environment variable."
        )

    response = httpx.get(
        f"{TRANSITLAND_API_URL}/feed_versions",
        params={"feed_onestop_id": TRANSITLAND_FEED_ID, "limit": limit},
        headers={"apikey": TRANSITLAND_API_KEY},
        timeout=30.0,
    )
    response.raise_for_status()

    versions: list[GTFSFeedVersion] = []
    for item in response.json().get("feed_versions", []):
        versions.append(
            GTFSFeedVersion(
                sha1=item["sha1"],
                fetched_at=item.get("fetched_at", ""),
                earliest_service_date=item.get("earliest_calendar_date", ""),
                latest_service_date=item.get("latest_calendar_date", ""),
                download_url=item.get("url", ""),
            )
        )

    logger.info(f"Found {len(versions)} feed versions")
    return versions


def download_historical_gtfs(feed_version: GTFSFeedVersion) -> Path:
    """Download one historical GTFS archive.

    Args:
        feed_version: Transitland feed version metadata.

    Returns:
        Path to downloaded ZIP archive.
    """
    if not feed_version.download_url:
        raise ValueError(f"No download URL for feed version {feed_version.sha1}")

    GTFS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"gtfs_{feed_version.earliest_service_date}_{feed_version.latest_service_date}.zip"
    output_path = GTFS_ARCHIVE_DIR / filename

    if output_path.exists():
        logger.info(f"Using cached GTFS: {output_path}")
        return output_path

    headers = {"apikey": TRANSITLAND_API_KEY} if TRANSITLAND_API_KEY else {}
    download_with_cache(
        feed_version.download_url,
        output_path,
        description="Downloading historical GTFS",
        headers=headers,
        timeout_seconds=60.0,
    )

    logger.info(f"Downloaded to {output_path}")
    return output_path


def get_pre_ptn_feed_versions() -> list[GTFSFeedVersion]:
    """Get historical feed versions fully before PTN launch.

    Returns:
        Feed versions before June 29, 2025.
    """
    return [
        version for version in fetch_transitland_feed_versions(limit=100) if version.is_pre_ptn
    ]


def get_post_ptn_feed_versions() -> list[GTFSFeedVersion]:
    """Get historical feed versions fully after PTN launch.

    Returns:
        Feed versions after June 29, 2025.
    """
    return [
        version for version in fetch_transitland_feed_versions(limit=100) if version.is_post_ptn
    ]


def get_ptn_transition_feed_versions() -> list[GTFSFeedVersion]:
    """Get versions that span the PTN launch date.

    Returns:
        Feed versions crossing June 29, 2025.
    """
    return [
        version
        for version in fetch_transitland_feed_versions(limit=100)
        if version.earliest_service_date < PTN_LAUNCH_DATE <= version.latest_service_date
    ]


def load_historical_gtfs_to_schema(
    feed_version: GTFSFeedVersion, schema_suffix: str
) -> dict[str, int]:
    """Load historical GTFS into suffixed raw tables.

    Args:
        feed_version: Transitland feed version to load.
        schema_suffix: Table suffix such as "pre_ptn".

    Returns:
        Mapping from GTFS logical table name to loaded row count.
    """
    validate_identifier(schema_suffix, "schema_suffix")

    zip_path = download_historical_gtfs(feed_version)
    _enforce_max_file_size(zip_path)
    extract_dir = GTFS_ARCHIVE_DIR / f"extracted_{schema_suffix}"

    logger.info(f"Extracting {zip_path.name} to {extract_dir}")
    _safe_extract_zip(zip_path, extract_dir)

    results: dict[str, int] = {}
    for config in GTFS_TABLES:
        file_path = extract_dir / config.filename
        if not file_path.exists():
            logger.warning(f"Skipping {config.filename}: not found in archive")
            results[config.log_name] = 0
            continue

        _enforce_max_file_size(file_path)

        table_name = f"gtfs_{config.table_name.replace('gtfs_', '')}_{schema_suffix}"
        logger.info(f"Loading {config.filename} to raw_{table_name}")

        if config.use_chunking:
            chunks = []
            for chunk in pd.read_csv(file_path, chunksize=BATCH_SIZE, dtype=config.dtypes):
                available_columns = select_existing_columns(config.columns, list(chunk.columns))
                chunk = chunk[available_columns]
                chunk = parse_yyyymmdd_columns(chunk, config.date_columns)
                chunks.append(chunk)
            df = pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
        else:
            df = pd.read_csv(file_path, dtype=config.dtypes)
            available_columns = select_existing_columns(config.columns, list(df.columns))
            df = df[available_columns]
            df = parse_yyyymmdd_columns(df, config.date_columns)

        if df.empty:
            results[config.log_name] = 0
            continue

        bulk_insert_df(df, "raw", table_name, if_exists="replace", log_insert=False)
        results[config.log_name] = len(df)
        logger.info(f"Loaded {len(df):,} rows to raw_{table_name}")

    return results
