"""Data ingestion for GTFS and Winnipeg Open Data.

This module handles downloading, extracting, and loading all data sources
into DuckDB tables. Uses gtfs-kit for GTFS parsing.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import csv
from datetime import date
import json
import math
import os
from pathlib import Path
import tempfile
from typing import TypedDict
from urllib.parse import urlencode
import zipfile

import gtfs_kit as gk
import httpx
from loguru import logger

from ptn_analysis.config import (
    DATASETS,
    GTFS_ARCHIVE_DIR,
    GTFS_DIR,
    GTFS_URL,
    GTFS_ZIP_PATH,
    PTN_LAUNCH_DATE,
    RAW_DATA_DIR,
    TRANSITLAND_API_KEY,
    TRANSITLAND_API_URL,
    TRANSITLAND_FEED_ID,
    WPG_OPEN_DATA_URL,
)
from ptn_analysis.data.db import bulk_insert_df, get_duckdb, validate_identifier
from ptn_analysis.data.loaders import download_with_cache, load_gtfs_feed
from ptn_analysis.data.transform import run_sql

__all__ = [
    # Current GTFS
    "download_gtfs",
    "extract_gtfs",
    "load_gtfs",
    # Open Data
    "load_boundaries",
    "load_all_open_data",
    # Historical GTFS (Transitland)
    "fetch_feed_versions",
    "download_historical_gtfs",
    "load_historical_gtfs",
    "GTFSFeedVersion",
    "is_pre_ptn",
    "is_post_ptn",
]

# Limits
MAX_FILE_SIZE_MB = 100
# Socrata SODA max page size for reliable GeoJSON pulls.
SODA_PAGE_LIMIT = 50_000
MAX_PARALLEL_PAGE_DOWNLOADS = max(
    1,
    min(20, int(os.getenv("OPEN_DATA_PARALLEL_DOWNLOADS", "10"))),
)

# GTFS tables to load from gtfs-kit Feed object
GTFS_TABLES = [
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
    "shapes",
    "feed_info",
]

# Open Data sources - boundary=True means polygon boundary table
# use_gtfs_dates=True filters using GTFS feed validity period
# use_gtfs_dates=False loads all available data (for datasets ending before PTN)
OPEN_DATA_SOURCES = [
    {
        "key": "pass_ups",
        "table": "passups",
        "log": "pass-ups",
        "date_col": "time",
        "use_gtfs_dates": True,
    },
    {
        "key": "passenger_counts",
        "table": "passenger_counts",
        "log": "passenger counts",
        "date_col": "schedule_period_end_date",
        "use_gtfs_dates": False,  # Dataset ends June 2024, before current GTFS period
    },
    {"key": "cycling", "table": "cycling_paths", "log": "cycling network"},
    {"key": "walkways", "table": "walkways", "log": "walkways"},
    {
        "key": "on_time",
        "table": "ontime_performance",
        "log": "on-time performance",
        "date_col": "scheduled_time",
        "use_gtfs_dates": True,
    },
    {
        "key": "neighbourhoods",
        "table": "neighbourhoods",
        "log": "neighbourhoods",
        "boundary": True,
    },
    {"key": "communities", "table": "community_areas", "log": "community areas", "boundary": True},
]


# =============================================================================
# Helper functions
# =============================================================================


def _enforce_file_size(path: Path) -> None:
    """Raise ValueError if file exceeds size limit.

    Args:
        path: Path to file to check.

    Raises:
        ValueError: If file exceeds MAX_FILE_SIZE_MB.
    """
    size_mb = path.stat().st_size / (1024 * 1024)
    if size_mb > MAX_FILE_SIZE_MB:
        raise ValueError(f"{path.name} ({size_mb:.1f}MB) exceeds {MAX_FILE_SIZE_MB}MB limit")


def _safe_extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract ZIP with path traversal protection.

    Args:
        zip_path: Path to ZIP file.
        extract_dir: Destination directory.

    Raises:
        ValueError: If archive contains unsafe paths or path escapes directory.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as archive:
        for member in archive.namelist():
            # Block path traversal attacks
            if ".." in member or member.startswith("/") or member.startswith("\\"):
                raise ValueError(f"Unsafe path in archive: {member}")

            resolved = (extract_dir / member).resolve()
            if not str(resolved).startswith(str(extract_dir.resolve())):
                raise ValueError(f"Path escapes directory: {member}")

        archive.extractall(extract_dir)


def _get_open_data_headers() -> dict[str, str]:
    """Build request headers for Winnipeg Open Data API.

    Returns:
        Headers dict with Accept and optional X-App-Token.
    """
    headers = {"Accept": "application/json"}
    token = os.getenv("WPG_OPEN_DATA_APP_TOKEN")
    if token:
        headers["X-App-Token"] = token
    return headers


def _is_valid_zip(path: Path) -> bool:
    """Check whether a path points to a valid ZIP archive.

    Args:
        path: Candidate ZIP file path.

    Returns:
        True when the file exists, is non-empty, and opens as a ZIP.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path, "r") as archive:
            archive.namelist()
        return True
    except Exception:
        return False


def _is_valid_geojson(path: Path) -> bool:
    """Check whether a path points to a valid GeoJSON FeatureCollection.

    Args:
        path: Candidate GeoJSON file path.

    Returns:
        True when the file exists, is non-empty, and decodes to a
        ``FeatureCollection`` object.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        return isinstance(payload, dict) and payload.get("type") == "FeatureCollection"
    except Exception:
        return False


def _zip_date_range(zip_path: Path) -> tuple[str | None, str | None]:
    """Extract service start/end dates from GTFS ZIP metadata.

    Args:
        zip_path: Path to a GTFS ZIP archive.

    Returns:
        Tuple of ``(earliest_date, latest_date)`` in ``YYYY-MM-DD`` format,
        or ``(None, None)`` when dates cannot be resolved.
    """
    if not _is_valid_zip(zip_path):
        return (None, None)

    def _normalize(raw: str | None) -> str | None:
        if not raw:
            return None
        value = str(raw).strip()
        if len(value) >= 8 and value[:8].isdigit():
            return f"{value[:4]}-{value[4:6]}-{value[6:8]}"
        if len(value) == 10 and value[4] == "-" and value[7] == "-":
            return value
        return None

    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            if "feed_info.txt" in archive.namelist():
                with archive.open("feed_info.txt", "r") as f:
                    reader = csv.DictReader((line.decode("utf-8-sig") for line in f))
                    row = next(reader, None)
                    if row:
                        start = _normalize(row.get("feed_start_date"))
                        end = _normalize(row.get("feed_end_date"))
                        if start and end:
                            return (start, end)

            if "calendar.txt" in archive.namelist():
                starts: list[str] = []
                ends: list[str] = []
                with archive.open("calendar.txt", "r") as f:
                    reader = csv.DictReader((line.decode("utf-8-sig") for line in f))
                    for row in reader:
                        start = _normalize(row.get("start_date"))
                        end = _normalize(row.get("end_date"))
                        if start:
                            starts.append(start)
                        if end:
                            ends.append(end)
                if starts and ends:
                    return (min(starts), max(ends))
    except Exception:
        return (None, None)

    return (None, None)


def _get_historical_gtfs_start() -> str | None:
    """Get the earliest date from all historical GTFS archives.

    Scans all feed_info_* and calendar_* tables (excluding main tables)
    to find the minimum start date across all loaded historical archives.

    Returns:
        Earliest date in YYYY-MM-DD format, or None if no historical data.
    """
    con = get_duckdb()

    def _normalize_date(raw: str | None) -> str | None:
        if not raw:
            return None
        val = str(raw).strip()
        if len(val) == 8 and val.isdigit():
            return f"{val[:4]}-{val[4:6]}-{val[6:8]}"
        if len(val) >= 10 and val[4] == "-" and val[7] == "-":
            return val[:10]
        return None

    dates: list[str] = []

    # Find all historical feed_info tables (feed_info_*)
    try:
        tables = con.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name LIKE 'feed_info_%'
            """
        ).fetchall()
        for (table_name,) in tables:
            try:
                result = con.execute(
                    f"SELECT feed_start_date FROM {table_name} LIMIT 1"
                ).fetchone()
                if result:
                    normalized = _normalize_date(result[0])
                    if normalized:
                        dates.append(normalized)
            except Exception:
                pass
    except Exception:
        pass

    # Find all historical calendar tables (calendar_*)
    try:
        tables = con.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name LIKE 'calendar_%'
              AND table_name != 'calendar_dates'
            """
        ).fetchall()
        for (table_name,) in tables:
            try:
                result = con.execute(
                    f"SELECT MIN(start_date) FROM {table_name}"
                ).fetchone()
                if result:
                    normalized = _normalize_date(result[0])
                    if normalized:
                        dates.append(normalized)
            except Exception:
                pass
    except Exception:
        pass

    return min(dates) if dates else None


def _resolve_open_data_date_range(use_gtfs_dates: bool = True) -> tuple[str, str]:
    """Resolve inclusive date window used for operational Open Data pulls.

    Logic:
        - use_gtfs_dates=False: Full historical range (2010-today) for datasets
          that end before PTN (e.g., passenger_counts ending June 2024).
        - use_gtfs_dates=True + historical GTFS exists: Use historical feed's
          start date for pre/post PTN comparison analysis.
        - use_gtfs_dates=True + no historical: PTN_LAUNCH_DATE to today.

    Args:
        use_gtfs_dates: If True, starts from PTN launch date (or earlier if
            historical GTFS data exists for comparison analysis).
            If False, returns full historical range.

    Returns:
        Tuple of ``(start_date, end_date)`` in ``YYYY-MM-DD`` format.
    """
    today = date.today().isoformat()

    if not use_gtfs_dates:
        # Load all available historical data (e.g., passenger_counts ending June 2024)
        logger.info("Using full historical date range (2010 to today)")
        return ("2010-01-01", today)

    # Check if historical GTFS exists and get its start date
    historical_start = _get_historical_gtfs_start()
    if historical_start:
        logger.info(f"Historical GTFS detected - fetching Open Data from {historical_start}")
        return (historical_start, today)

    # Default: Start from PTN launch date (June 29, 2025)
    logger.info(f"Using PTN launch date as start: {PTN_LAUNCH_DATE}")
    return (PTN_LAUNCH_DATE, today)


def _quote_sql_ident(name: str) -> str:
    """Quote SQL identifier for safe interpolation.

    Args:
        name: Identifier name to quote.

    Returns:
        Double-quoted identifier with escaped quotes.
    """
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _build_soda_where(date_col: str | None, use_gtfs_dates: bool = True) -> str | None:
    """Build SODA where clause for date-filtered operational datasets.

    Args:
        date_col: Dataset datetime column name.
        use_gtfs_dates: If True, uses GTFS feed validity period. If False, uses full historical range.

    Returns:
        SQL predicate string when ``date_col`` is provided, else ``None``.
    """
    if not date_col:
        return None
    start, end = _resolve_open_data_date_range(use_gtfs_dates)
    return f"{date_col} >= '{start}' AND {date_col} <= '{end}'"


def _build_soda_url(
    dataset_id: str,
    *,
    extension: str,
    where: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    select: str | None = None,
) -> str:
    """Build SODA URL for a specific endpoint and query.

    Args:
        dataset_id: Socrata dataset identifier.
        extension: SODA extension (for example ``geojson`` or ``json``).
        where: Optional SODA ``$where`` clause.
        limit: Optional page size.
        offset: Optional row offset for pagination.
        select: Optional SODA ``$select`` expression.

    Returns:
        Complete request URL with encoded query parameters.
    """
    base = f"{WPG_OPEN_DATA_URL}/resource/{dataset_id}.{extension}"
    query: dict[str, str | int] = {}
    if limit is not None:
        query["$limit"] = limit
    if offset is not None:
        query["$offset"] = offset
    if where:
        query["$where"] = where
    if select:
        query["$select"] = select
    if not query:
        return base
    return f"{base}?{urlencode(query)}"


def _get_soda_count(dataset_id: str, *, where: str | None = None, headers: dict[str, str]) -> int:
    """Get row count for a SODA dataset query.

    Args:
        dataset_id: Socrata dataset identifier.
        where: Optional SODA ``$where`` clause.
        headers: HTTP request headers.

    Returns:
        Number of rows returned by the query.
    """
    count_url = _build_soda_url(
        dataset_id,
        extension="json",
        where=where,
        select="count(*)",
    )
    response = httpx.get(count_url, headers=headers, timeout=120.0)
    response.raise_for_status()
    payload = response.json()
    if isinstance(payload, list) and payload:
        return int(payload[0].get("count", 0))
    return 0


def _load_geojson(path: Path) -> dict:
    """Load a GeoJSON file and return the decoded JSON object.

    Args:
        path: GeoJSON file path.

    Returns:
        Parsed JSON payload.
    """
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _download_operational_geojson(
    dataset_id: str,
    log_name: str,
    date_col: str,
    output_path: Path,
    use_gtfs_dates: bool = True,
) -> None:
    """Download all date-filtered GeoJSON pages and merge to one file.

    Args:
        dataset_id: Socrata dataset identifier.
        log_name: Human-readable dataset label for logs.
        date_col: Datetime column used for date filtering.
        output_path: Final merged GeoJSON destination path.
        use_gtfs_dates: If True, filters using GTFS feed period. If False, uses full historical range.

    Returns:
        None. Writes a merged FeatureCollection to ``output_path``.
    """

    def _download_page(offset: int, temp_dir: Path) -> tuple[int, list[dict], dict | None]:
        """Download one operational-data page.

        Args:
            offset: Pagination offset.
            temp_dir: Temporary download directory.

        Returns:
            Tuple of ``(offset, features, crs)`` from the downloaded page.
        """
        page_url = _build_soda_url(
            dataset_id,
            extension="geojson",
            where=where,
            limit=SODA_PAGE_LIMIT,
            offset=offset,
        )
        page_path = temp_dir / f"{dataset_id}_{offset}.geojson"
        download_with_cache(
            page_url,
            page_path,
            description=f"Downloading {log_name} (offset={offset})",
            headers=headers,
            validator=_is_valid_geojson,
            force_redownload=True,
            progressbar=False,
        )
        payload = _load_geojson(page_path)
        return (offset, payload.get("features", []), payload.get("crs"))

    where = _build_soda_where(date_col, use_gtfs_dates)
    headers = _get_open_data_headers()
    total_records = _get_soda_count(dataset_id, where=where, headers=headers)
    if total_records == 0:
        output_path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        logger.info(f"{log_name}: merged 0 total records")
        return

    total_pages = math.ceil(total_records / SODA_PAGE_LIMIT)
    offsets = [page * SODA_PAGE_LIMIT for page in range(total_pages)]
    crs: dict | None = None
    merged_count = 0
    output_temp = output_path.with_suffix(f"{output_path.suffix}.part")
    if output_temp.exists():
        output_temp.unlink()

    with tempfile.TemporaryDirectory(prefix=f"{dataset_id}_", dir=RAW_DATA_DIR) as tmp:
        temp_dir = Path(tmp)
        with output_temp.open("w", encoding="utf-8") as out:
            out.write('{"type":"FeatureCollection","features":[')
            first_feature = True

            for i in range(0, len(offsets), MAX_PARALLEL_PAGE_DOWNLOADS):
                batch_offsets = offsets[i : i + MAX_PARALLEL_PAGE_DOWNLOADS]
                page_results: dict[int, tuple[list[dict], dict | None]] = {}

                with ThreadPoolExecutor(max_workers=MAX_PARALLEL_PAGE_DOWNLOADS) as pool:
                    futures = {
                        pool.submit(_download_page, offset, temp_dir): offset
                        for offset in batch_offsets
                    }
                    for future in as_completed(futures):
                        offset, features, page_crs = future.result()
                        page_results[offset] = (features, page_crs)
                        logger.info(
                            f"{log_name}: fetched {len(features):,} records at offset {offset:,}"
                        )

                for offset in sorted(batch_offsets):
                    features, page_crs = page_results[offset]
                    if crs is None and page_crs is not None:
                        crs = page_crs
                    for feature in features:
                        if not first_feature:
                            out.write(",")
                        json.dump(feature, out, separators=(",", ":"))
                        first_feature = False
                        merged_count += 1

            out.write("]")
            if crs is not None:
                out.write(',"crs":')
                json.dump(crs, out, separators=(",", ":"))
            out.write("}")

    output_temp.replace(output_path)
    logger.info(f"{log_name}: merged {merged_count:,} total records")


# =============================================================================
# Current GTFS
# =============================================================================


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
        validator=_is_valid_zip,
    )
    return GTFS_ZIP_PATH


def extract_gtfs() -> Path:
    """Extract GTFS archive to raw data directory.

    Returns:
        Path to extraction directory.

    Raises:
        ValueError: If archive contains unsafe paths.
    """
    logger.info(f"Extracting GTFS to {GTFS_DIR}")
    _safe_extract_zip(GTFS_ZIP_PATH, GTFS_DIR)
    files = list(GTFS_DIR.glob("*.txt"))
    logger.info(f"Extracted {len(files)} files")
    return GTFS_DIR


def load_gtfs() -> dict[str, int]:
    """Load all GTFS tables into DuckDB using gtfs-kit.

    Returns:
        Dict mapping table name to row count.
    """
    run_sql("schema.sql")
    feed = load_gtfs_feed()

    results = {}
    for table in GTFS_TABLES:
        df = getattr(feed, table, None)

        if df is None or df.empty:
            logger.warning(f"Skipping {table}: no data")
            results[table] = 0
            continue

        bulk_insert_df(df, "", table, if_exists="replace", log_insert=False)
        results[table] = len(df)
        logger.info(f"Loaded {len(df):,} rows → {table}")

    return results


# =============================================================================
# Winnipeg Open Data
# =============================================================================


def load_open_data_geojson(
    key: str,
    table: str,
    log_name: str,
    date_col: str | None = None,
    boundary: bool = False,
    use_gtfs_dates: bool = True,
) -> int:
    """Load GeoJSON Open Data dataset into DuckDB with all columns.

    For boundary tables, extracts standardized id/name/area/geometry columns.
    For regular tables, keeps all original columns plus geometry.

    Args:
        key: Dataset key in DATASETS config.
        table: Target table name.
        log_name: Human-readable name for logging.
        date_col: Column for SODA date filtering.
        boundary: If True, extract standardized boundary columns.
        use_gtfs_dates: If True, filters using GTFS feed period. If False, uses full historical range.

    Returns:
        Number of rows loaded.
    """
    dataset_id = DATASETS.get(key)
    if not dataset_id:
        logger.warning(f"Skipping {log_name}: dataset not configured")
        return 0

    geojson_path = RAW_DATA_DIR / f"{dataset_id}.geojson"
    if date_col:
        _download_operational_geojson(dataset_id, log_name, date_col, geojson_path, use_gtfs_dates)
    else:
        geojson_url = _build_soda_url(
            dataset_id, extension="geojson", limit=SODA_PAGE_LIMIT, offset=0
        )
        download_with_cache(
            geojson_url,
            geojson_path,
            description=f"Downloading {log_name}",
            headers=_get_open_data_headers(),
            validator=_is_valid_geojson,
        )

    # Load into DuckDB
    conn = get_duckdb()
    logger.info(f"Loading {log_name} → {table}")
    geojson_posix = geojson_path.as_posix()

    if boundary:
        # Boundary tables: standardize to id, name, area_km2, geometry
        describe = conn.execute(f"DESCRIBE SELECT * FROM ST_Read('{geojson_posix}')").fetchdf()
        cols = describe["column_name"].tolist()

        name_col = next((c for c in ["name", "NAME"] if c in cols), None)
        area_col = next((c for c in ["area_km2", "AREA_KM2"] if c in cols), None)

        name_expr = _quote_sql_ident(name_col) if name_col else "'Unknown'"
        area_expr = f"TRY_CAST({_quote_sql_ident(area_col)} AS DOUBLE)" if area_col else "0.0"

        sql = f"""
            CREATE TABLE {table} AS
            SELECT row_number() OVER () AS id, {name_expr} AS name, {area_expr} AS area_km2, geom AS geometry
            FROM ST_Read('{geojson_posix}')
        """
    else:
        # Regular tables: keep all columns
        sql = f"""
            CREATE TABLE {table} AS
            SELECT *, geom AS geometry FROM ST_Read('{geojson_posix}')
        """

    conn.execute(f"DROP TABLE IF EXISTS {table}")
    conn.execute(sql)

    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    logger.info(f"Loaded {count:,} rows → {table}")
    return count


def load_all_open_data() -> dict[str, int]:
    """Load all Open Data tables (operational data, not boundaries).

    Returns:
        Dict mapping table name to row count.
    """
    results = {}
    failures: list[str] = []
    for source in OPEN_DATA_SOURCES:
        if source.get("boundary"):
            continue
        try:
            results[source["table"]] = load_open_data_geojson(
                source["key"],
                source["table"],
                source["log"],
                date_col=source.get("date_col"),
                use_gtfs_dates=source.get("use_gtfs_dates", True),
            )
        except Exception as exc:
            logger.exception(f"Failed loading {source['table']}: {exc}")
            failures.append(source["table"])

    if failures:
        tables = ", ".join(failures)
        raise RuntimeError(f"Open Data load incomplete. Failed tables: {tables}")
    return results


def load_boundaries() -> dict[str, int]:
    """Load boundary datasets (neighbourhoods, communities).

    Returns:
        Dict mapping table name to row count.
    """
    results = {}
    for source in OPEN_DATA_SOURCES:
        if not source.get("boundary"):
            continue
        results[source["table"]] = load_open_data_geojson(
            source["key"],
            source["table"],
            source["log"],
            boundary=True,
        )
    return results


# =============================================================================
# Historical GTFS (Transitland)
# =============================================================================


class GTFSFeedVersion(TypedDict):
    """Transitland GTFS feed version metadata."""

    sha1: str
    fetched_at: str
    earliest_date: str  # YYYY-MM-DD
    latest_date: str  # YYYY-MM-DD
    url: str


def is_pre_ptn(version: GTFSFeedVersion) -> bool:
    """Check if feed is fully before PTN launch (June 29, 2025).

    Args:
        version: Feed version metadata.

    Returns:
        True if feed's latest date is before PTN launch.
    """
    return version["latest_date"] < PTN_LAUNCH_DATE


def is_post_ptn(version: GTFSFeedVersion) -> bool:
    """Check if feed is fully after PTN launch (June 29, 2025).

    Args:
        version: Feed version metadata.

    Returns:
        True if feed's earliest date is on or after PTN launch.
    """
    return version["earliest_date"] >= PTN_LAUNCH_DATE


def fetch_feed_versions(limit: int = 50) -> list[GTFSFeedVersion]:
    """Fetch available feed versions from Transitland API.

    Args:
        limit: Maximum number of versions to fetch.

    Returns:
        List of feed versions sorted by date.

    Raises:
        ValueError: If TRANSITLAND_API_KEY not set.
        httpx.HTTPStatusError: If API request fails.
    """
    if not TRANSITLAND_API_KEY:
        raise ValueError("TRANSITLAND_API_KEY not set")

    resp = httpx.get(
        f"{TRANSITLAND_API_URL}/feed_versions",
        params={"feed_onestop_id": TRANSITLAND_FEED_ID, "limit": limit},
        headers={"apikey": TRANSITLAND_API_KEY},
        timeout=30.0,
    )
    resp.raise_for_status()

    versions: list[GTFSFeedVersion] = []
    for item in resp.json().get("feed_versions", []):
        versions.append(
            {
                "sha1": item["sha1"],
                "fetched_at": item.get("fetched_at", ""),
                "earliest_date": item.get("earliest_calendar_date", ""),
                "latest_date": item.get("latest_calendar_date", ""),
                "url": item.get("url", ""),
            }
        )

    return versions


def download_historical_gtfs(version: GTFSFeedVersion) -> Path:
    """Download historical GTFS archive from Transitland.

    Args:
        version: Feed version to download.

    Returns:
        Path to downloaded ZIP file.

    Raises:
        ValueError: If version has no download URL.
    """
    if not version["url"]:
        raise ValueError(f"No URL for version {version['sha1']}")

    GTFS_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"gtfs_{version['earliest_date']}_{version['latest_date']}.zip"
    output = GTFS_ARCHIVE_DIR / filename

    if output.exists():
        logger.info(f"Using cached: {output}")
        return output

    headers = {}
    if TRANSITLAND_API_KEY:
        headers["apikey"] = TRANSITLAND_API_KEY

    download_with_cache(
        version["url"],
        output,
        description="Downloading historical GTFS",
        headers=headers,
        validator=_is_valid_zip,
    )
    return output


def load_historical_gtfs(version: GTFSFeedVersion, suffix: str) -> dict[str, int]:
    """Load historical GTFS into suffixed tables (e.g., stops_pre_ptn).

    Args:
        version: Feed version to load.
        suffix: Table name suffix (e.g., "pre_ptn").

    Returns:
        Dict mapping base table name to row count.

    Raises:
        ValueError: If suffix contains invalid characters or file too large.
    """
    validate_identifier(suffix, "suffix")
    zip_path = download_historical_gtfs(version)
    _enforce_file_size(zip_path)

    logger.info(f"Loading historical feed with suffix '{suffix}'")
    feed = gk.read_feed(str(zip_path), dist_units="km")

    results = {}
    for table in GTFS_TABLES:
        df = getattr(feed, table, None)
        target_table = f"{table}_{suffix}"

        if df is None or df.empty:
            results[table] = 0
            continue

        bulk_insert_df(df, "", target_table, if_exists="replace", log_insert=False)
        results[table] = len(df)
        logger.info(f"Loaded {len(df):,} rows → {target_table}")

    return results
