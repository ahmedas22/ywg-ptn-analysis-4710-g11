"""Generic Socrata-style Open Data source service."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from hashlib import md5
import json
from pathlib import Path
from urllib.parse import urlencode

import geopandas as gpd
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    CACHE_DATA_DIR,
    DATASETS,
    OPEN_DATA_PAGE_LIMIT,
    PTN_LAUNCH_DATE,
    RAW_DATA_DIR,
    WGS84_CRS,
    WINNIPEG_PROJECTED_CRS,
    WPG_OPEN_DATA_URL,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.http import Downloader
from ptn_analysis.data.sources.common import load_geojson_table, open_data_headers

OPEN_DATA_DATASET_WORKERS = 6
BOUNDARY_TABLE_NAMES = {"neighbourhoods", "community_areas"}

_downloader = Downloader()

CITY_OPEN_DATA_CONFIG = {
    "ywg": {
        "portal_url": WPG_OPEN_DATA_URL,
        "boundary_datasets": [
            {
                "base_table_name": "neighbourhoods",
                "dataset_id": DATASETS["neighbourhoods"],
                "use_ptn_dates": False,
                "format": "geojson",
            },
            {
                "base_table_name": "community_areas",
                "dataset_id": DATASETS["communities"],
                "use_ptn_dates": False,
                "format": "geojson",
            },
        ],
        "datasets": [
            {
                "base_table_name": "passups",
                "dataset_id": DATASETS["pass_ups"],
                "date_column": "time",
                "use_ptn_dates": False,
                "format": "json",
            },
            {
                "base_table_name": "passenger_counts",
                "dataset_id": DATASETS["passenger_counts"],
                "date_column": "schedule_period_end_date",
                "use_ptn_dates": False,
                "format": "json",
            },
            {
                "base_table_name": "cycling_paths",
                "dataset_id": DATASETS["cycling"],
                "format": "geojson",
            },
            {
                "base_table_name": "walkways",
                "dataset_id": DATASETS["walkways"],
                "format": "geojson",
            },
            {
                "base_table_name": "ontime_performance",
                "dataset_id": DATASETS["on_time"],
                "date_column": "scheduled_time",
                "use_ptn_dates": False,
                "format": "json",
            },
            {
                "base_table_name": "census_poverty_2021",
                "dataset_id": DATASETS["census_poverty_2021"],
                "format": "geojson",
            },
        ],
    }
}


JSON_PAGE_LIMIT = OPEN_DATA_PAGE_LIMIT
OPEN_DATA_FETCH_WORKERS = 12
OPEN_DATA_RAW_DIR = RAW_DATA_DIR / "open_data"
OPEN_DATA_CACHE_DIR = CACHE_DATA_DIR / "open_data"


def dataset_raw_dir(source, base_table_name: str) -> Path:
    """Build the stable raw-data directory for one dataset."""
    return OPEN_DATA_RAW_DIR / source.city_key / base_table_name


def merged_cache_path(source, base_table_name: str, extension: str) -> Path:
    """Build the merged raw-data path for one dataset."""
    cache_dir = dataset_raw_dir(source, base_table_name)
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"merged.{extension}"


def page_cache_path(source, base_table_name: str, offset: int, extension: str) -> Path:
    """Build a stable page-cache path for one dataset page."""
    cache_dir = OPEN_DATA_CACHE_DIR / source.city_key / base_table_name / "pages"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{offset}.{extension}"


def count_cache_path(source, base_table_name: str, where_clause: str | None) -> Path:
    """Build a stable cache path for one dataset count query."""
    digest_source = where_clause or "all_rows"
    digest = md5(digest_source.encode("utf-8")).hexdigest()[:12]
    cache_dir = OPEN_DATA_CACHE_DIR / source.city_key / base_table_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"count_{digest}.json"


def jsonl_part_path(cache_path: Path) -> Path:
    """Build the temporary merged JSON-lines cache path."""
    return cache_path.with_name(f"{cache_path.name}.part")


def is_valid_jsonl_cache(cache_path: Path) -> bool:
    """Return whether a merged JSON-lines cache is usable."""
    part_path = jsonl_part_path(cache_path)
    if part_path.exists():
        return False
    return cache_path.exists() and cache_path.stat().st_size > 0


def append_jsonl_cache(cache_path: Path, rows: list[dict]) -> None:
    """Append one JSON page to the merged JSON-lines cache."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.open("a", encoding="utf-8") as cache_file:
        for row in rows:
            cache_file.write(json.dumps(row, separators=(",", ":")))
            cache_file.write("\n")


def load_jsonl_cache(table_name: str, cache_path: Path, db_instance: TransitDB) -> int:
    """Load a merged JSON-lines cache into DuckDB."""
    if not cache_path.exists() or cache_path.stat().st_size == 0:
        db_instance.drop_relation_if_exists(table_name)
        return 0
    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT *
        FROM read_json_auto('{cache_path.as_posix()}', format='newline_delimited')
        """
    )
    return db_instance.count(table_name) or 0


def prune_page_caches(source, base_table_name: str, extension: str, keep_offset: int) -> None:
    """Delete page-cache fragments after a successful merge."""
    keep_path = page_cache_path(source, base_table_name, keep_offset, extension)
    cache_dir = OPEN_DATA_CACHE_DIR / source.city_key / base_table_name / "pages"
    if not cache_dir.exists():
        return
    for cache_path in cache_dir.glob(f"*.{extension}"):
        if cache_path == keep_path:
            continue
        cache_path.unlink(missing_ok=True)


def parse_row_count(payload: list[dict] | dict | None) -> int:
    """Extract a Socrata count value from a response payload."""
    if not payload or not isinstance(payload, list):
        return 0
    first_row = payload[0]
    if not isinstance(first_row, dict):
        return 0
    try:
        return int(first_row.get("row_count", 0))
    except (TypeError, ValueError):
        return 0


def is_row_count_payload(payload: list[dict] | dict | None) -> bool:
    """Return whether a payload matches the Socrata count response shape."""
    if not isinstance(payload, list) or len(payload) != 1:
        return False
    first_row = payload[0]
    if not isinstance(first_row, dict):
        return False
    return "row_count" in first_row


def where_clause(date_column: str | None, use_ptn_dates: bool) -> str | None:
    """Build a `$where` clause for date-filtered datasets."""
    if date_column is None or not use_ptn_dates:
        return None
    return f"{date_column} between '{PTN_LAUNCH_DATE}' and '{date.today().isoformat()}'"


def dataset_url(
    portal_url: str,
    dataset_id: str,
    ext: str,
    select: str | None = None,
    where_clause: str | None = None,
    limit: int | None = None,
    offset: int | None = None,
    order: str | None = None,
) -> str:
    """Build one Socrata dataset URL."""
    params: dict[str, object] = {}
    if select is not None:
        params["$select"] = select
    if where_clause is not None:
        params["$where"] = where_clause
    if limit is not None:
        params["$limit"] = limit
    if offset is not None:
        params["$offset"] = offset
    if order is not None:
        params["$order"] = order
    query_string = urlencode(params)
    base_url = f"{portal_url}/resource/{dataset_id}.{ext}"
    return base_url if not query_string else f"{base_url}?{query_string}"


def fetch_dataset_payload(source, url: str, cache_path: Path, response_format: str) -> list[dict] | dict:
    """Fetch and cache one Socrata payload."""
    return source.downloader.request(
        url,
        cache_path=cache_path,
        headers=source.headers,
        response_format=response_format,
    )


def fetch_dataset_count(
    source,
    dataset_id: str,
    cache_key: str,
    where_clause_value: str | None = None,
) -> int:
    """Fetch a Socrata dataset row count with a stable cache location."""
    portal_url = source.config()["portal_url"]
    url = dataset_url(
        portal_url,
        dataset_id,
        ext="json",
        select="COUNT(*) AS row_count",
        where_clause=where_clause_value,
    )
    cache_path_value = count_cache_path(source, cache_key, where_clause_value)
    payload = fetch_dataset_payload(
        source,
        url=url,
        cache_path=cache_path_value,
        response_format="json",
    )
    if is_row_count_payload(payload):
        return parse_row_count(payload)

    logger.warning(
        "Discarding stale or invalid row-count cache for "
        f"{cache_key} and fetching a fresh count payload."
    )
    cache_path_value.unlink(missing_ok=True)
    payload = fetch_dataset_payload(
        source,
        url=url,
        cache_path=cache_path_value,
        response_format="json",
    )
    return parse_row_count(payload)


def fetch_json_page(
    source,
    portal_url: str,
    dataset_id: str,
    base_table_name: str,
    where_clause_value: str | None,
    offset: int,
) -> list[dict]:
    """Fetch one JSON page from a Socrata dataset."""
    payload = fetch_dataset_payload(
        source,
        url=dataset_url(
            portal_url,
            dataset_id,
            ext="json",
            where_clause=where_clause_value,
            limit=JSON_PAGE_LIMIT,
            offset=offset,
            order=":id",
        ),
        cache_path=page_cache_path(source, base_table_name, offset, "json"),
        response_format="json",
    )
    if not isinstance(payload, list):
        raise ValueError(f"Unexpected JSON payload for {dataset_id}: {type(payload)!r}")
    return payload


def fetch_json_batch(
    source,
    portal_url: str,
    dataset_id: str,
    base_table_name: str,
    where_clause_value: str | None,
    offsets: list[int],
) -> dict[int, list[dict]]:
    """Fetch one batch of JSON pages in parallel."""
    page_rows_by_offset: dict[int, list[dict]] = {}
    with ThreadPoolExecutor(max_workers=OPEN_DATA_FETCH_WORKERS) as executor:
        future_map = {}
        for offset in offsets:
            future = executor.submit(
                fetch_json_page,
                source,
                portal_url,
                dataset_id,
                base_table_name,
                where_clause_value,
                offset,
            )
            future_map[future] = offset
        for future in as_completed(future_map):
            offset = future_map[future]
            page_rows_by_offset[offset] = future.result()
    return page_rows_by_offset


def store_json_page(cache_path: Path, page_rows: list[dict], total_rows: int) -> int:
    """Append one JSON page to the merged raw cache."""
    append_jsonl_cache(cache_path, page_rows)
    return total_rows + len(page_rows)


def prepare_json_cache(source, dataset: dict, progress_callback=None) -> Path:
    """Prepare one merged JSON-lines cache in its dataset folder."""
    portal_url = source.config()["portal_url"]
    dataset_id = dataset["dataset_id"]
    base_table_name = dataset["base_table_name"]
    clause = where_clause(dataset.get("date_column"), bool(dataset.get("use_ptn_dates", True)))
    cache_path_value = merged_cache_path(source, base_table_name, "jsonl")
    cache_part_path = jsonl_part_path(cache_path_value)

    if is_valid_jsonl_cache(cache_path_value):
        logger.info(f"Using merged JSON cache: {cache_path_value}")
        return cache_path_value

    expected_count = 0
    try:
        expected_count = fetch_dataset_count(
            source,
            dataset_id,
            base_table_name,
            where_clause_value=clause,
        )
        if expected_count > 0:
            logger.info(f"Expected {expected_count:,} rows for {base_table_name}")
    except Exception as exc:
        logger.debug(f"Could not fetch Socrata row count for {base_table_name}: {exc}")

    if cache_path_value.exists():
        cache_path_value.unlink()
    if cache_part_path.exists():
        cache_part_path.unlink()

    def _report(total, expected, name):
        pct = (total / expected * 100) if expected else 0
        msg = f"{name}: {total:,}/{expected:,} ({pct:.0f}%)"
        logger.info(f"  {msg}")
        if progress_callback:
            progress_callback(msg)

    total_rows = 0
    last_offset = 0
    if expected_count > 0:
        next_offset = 0
        keep_loading = True
        while keep_loading and next_offset < expected_count:
            batch_offsets = []
            for _ in range(OPEN_DATA_FETCH_WORKERS):
                if next_offset >= expected_count:
                    break
                batch_offsets.append(next_offset)
                next_offset += JSON_PAGE_LIMIT
            page_rows_by_offset = fetch_json_batch(
                source,
                portal_url,
                dataset_id,
                base_table_name,
                clause,
                batch_offsets,
            )
            for current_offset in batch_offsets:
                page_rows = page_rows_by_offset.get(current_offset, [])
                if not page_rows:
                    keep_loading = False
                    break
                total_rows = store_json_page(cache_part_path, page_rows, total_rows)
                last_offset = current_offset
                _report(total_rows, expected_count, base_table_name)
                if len(page_rows) < JSON_PAGE_LIMIT:
                    keep_loading = False
                    break
    else:
        next_offset = 0
        while True:
            page_rows = fetch_json_page(
                source,
                portal_url,
                dataset_id,
                base_table_name,
                clause,
                next_offset,
            )
            if not page_rows:
                break
            total_rows = store_json_page(cache_part_path, page_rows, total_rows)
            last_offset = next_offset
            if len(page_rows) < JSON_PAGE_LIMIT:
                break
            next_offset += JSON_PAGE_LIMIT

    if total_rows > 0 and cache_part_path.exists():
        cache_part_path.replace(cache_path_value)

    prune_page_caches(source, base_table_name, "json", keep_offset=last_offset)
    return cache_path_value


GEOJSON_PAGE_LIMIT =  min(OPEN_DATA_PAGE_LIMIT, 1000)


def prepare_geojson_cache_for_dataset(
    source,
    dataset: dict,
    boundary_table_names: set[str],
) -> Path:
    """Prepare one GeoJSON raw cache in its dataset folder."""
    portal_url = source.config()["portal_url"]
    dataset_id = dataset["dataset_id"]
    base_table_name = dataset["base_table_name"]
    cache_path = merged_cache_path(source, base_table_name, "geojson")
    if base_table_name in boundary_table_names:
        return prepare_boundary_geojson_cache(
            source,
            cache_path=cache_path,
            dataset_id=dataset_id,
            base_table_name=base_table_name,
            portal_url=portal_url,
        )
    return prepare_geojson_cache(
        source,
        cache_path=cache_path,
        dataset_id=dataset_id,
        base_table_name=base_table_name,
        portal_url=portal_url,
        date_column=dataset.get("date_column"),
        use_ptn_dates=bool(dataset.get("use_ptn_dates", True)),
    )


def prepare_boundary_geojson_cache(source, cache_path, dataset_id: str, base_table_name: str, portal_url: str):
    """Fetch one full boundary GeoJSON file and cache it locally."""
    reusable_cache = existing_geojson_cache(cache_path)
    if reusable_cache is not None:
        return reusable_cache
    payload = fetch_dataset_payload(
        source,
        url=dataset_url(portal_url, dataset_id, ext="geojson", order=":id"),
        cache_path=cache_path,
        response_format="json",
    )
    if isinstance(payload, dict) and payload.get("type") == "FeatureCollection":
        cache_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
    if not is_valid_geojson_cache(cache_path):
        raise ValueError(
            f"Boundary GeoJSON cache for {dataset_id} is empty or invalid: {cache_path}"
        )
    return cache_path


def prepare_geojson_cache(
    source,
    cache_path,
    dataset_id: str,
    base_table_name: str,
    portal_url: str,
    date_column: str | None,
    use_ptn_dates: bool,
):
    """Return a valid GeoJSON cache path for one dataset."""
    reusable_cache = existing_geojson_cache(cache_path)
    if reusable_cache is not None:
        return reusable_cache
    fetch_all_geojson_pages(
        source,
        portal_url,
        dataset_id,
        base_table_name,
        cache_path,
        date_column,
        use_ptn_dates,
    )
    if not is_valid_geojson_cache(cache_path):
        raise ValueError(f"GeoJSON cache for {dataset_id} is empty or invalid: {cache_path}")
    return cache_path


def existing_geojson_cache(cache_path):
    """Return an existing usable GeoJSON cache path when available."""
    if is_valid_geojson_cache(cache_path):
        logger.info(f"Using cached GeoJSON: {cache_path.name}")
        return cache_path
    return None


def is_valid_geojson_cache(cache_path) -> bool:
    """Return whether a GeoJSON cache contains at least one feature."""
    if not cache_path.exists() or cache_path.stat().st_size == 0:
        return False
    try:
        payload = json.loads(cache_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False
    if payload.get("type") != "FeatureCollection":
        return False
    features = payload.get("features")
    if not isinstance(features, list):
        return False
    return len(features) > 0


def load_boundary_table(table_name: str, cache_path, db_instance: TransitDB) -> int:
    """Load a boundary dataset and standardize key columns."""
    boundary_frame = gpd.read_file(cache_path)
    if boundary_frame.crs is None:
        boundary_frame = boundary_frame.set_crs(WGS84_CRS)
    else:
        boundary_frame = boundary_frame.to_crs(WGS84_CRS)

    name_column = None
    for candidate in ["name", "NAME"]:
        if candidate in boundary_frame.columns.tolist():
            name_column = candidate
            break

    projected_frame = boundary_frame.to_crs(WINNIPEG_PROJECTED_CRS)
    normalized_frame = gpd.GeoDataFrame(
        {
            "id": range(1, len(boundary_frame) + 1),
            "name": (
                boundary_frame[name_column].fillna("Unknown").astype(str)
                if name_column is not None
                else pd.Series(["Unknown"] * len(boundary_frame))
            ),
            "area_km2": projected_frame.geometry.area / 1_000_000,
            "geometry": boundary_frame.geometry,
        },
        geometry="geometry",
        crs=WGS84_CRS,
    )
    db_instance.load_table(table_name, normalized_frame, mode="replace")
    return db_instance.count(table_name) or 0


def fetch_all_geojson_pages(
    source,
    portal_url: str,
    dataset_id: str,
    base_table_name: str,
    cache_path,
    date_column: str | None,
    use_ptn_dates: bool,
) -> int:
    """Fetch and merge all GeoJSON pages for one dataset."""
    clause = where_clause(date_column, use_ptn_dates)
    merged_features: list[dict] = []
    crs_payload: dict | None = None
    offset = 0

    while True:
        payload = fetch_dataset_payload(
            source,
            url=dataset_url(
                portal_url,
                dataset_id,
                ext="geojson",
                where_clause=clause,
                limit=GEOJSON_PAGE_LIMIT,
                offset=offset,
                order=":id",
            ),
            cache_path=page_cache_path(source, base_table_name, offset, "geojson"),
            response_format="json",
        )
        if not isinstance(payload, dict):
            raise ValueError(f"Unexpected GeoJSON payload for {dataset_id}: {type(payload)!r}")
        features = payload.get("features", [])
        if not features:
            if offset == 0:
                cache_path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
                return 0
            break
        merged_features.extend(features)
        if crs_payload is None:
            crs_payload = payload.get("crs")
        if len(features) < GEOJSON_PAGE_LIMIT:
            break
        offset += GEOJSON_PAGE_LIMIT

    merged_payload: dict[str, object] = {"type": "FeatureCollection", "features": merged_features}
    if crs_payload is not None:
        merged_payload["crs"] = crs_payload
    cache_path.write_text(json.dumps(merged_payload, separators=(",", ":")), encoding="utf-8")
    prune_page_caches(source, base_table_name, "geojson", keep_offset=offset)
    return len(merged_features)


def load_prepared_geojson_dataset(
    table_name: str, cache_path, boundary_table_names: set[str], db_instance: TransitDB
) -> int:
    """Load a prepared GeoJSON dataset into DuckDB."""
    if (
        table_name.split("_", 1)[-1] in boundary_table_names
        or table_name.endswith("neighbourhoods")
        or table_name.endswith("community_areas")
    ):
        return load_boundary_table(table_name, cache_path, db_instance)
    return load_geojson_table(table_name, cache_path, db_instance)


class _SourceContext:
    """Internal context for helper function compatibility."""

    __slots__ = ("city_key", "downloader", "headers")

    def __init__(self, city_key: str) -> None:
        self.city_key = city_key
        self.downloader = _downloader
        self.headers = open_data_headers(city_key)

    def config(self) -> dict:
        return get_config(self.city_key)


def get_config(city_key: str) -> dict:
    """Return the portal configuration for a city."""
    city_config = CITY_OPEN_DATA_CONFIG.get(city_key)
    if city_config is None:
        raise ValueError(
            f"No Open Data portal configuration for city_key={city_key!r}."
        )
    return city_config


def load_boundaries(city_key: str, db_instance: TransitDB) -> dict[str, int]:
    """Load configured boundary datasets."""
    ctx = _SourceContext(city_key)
    return _load_dataset_group(ctx, db_instance, get_config(city_key)["boundary_datasets"])


def load_all(city_key: str, db_instance: TransitDB, progress_callback=None) -> dict[str, int]:
    """Load all configured non-boundary datasets."""
    ctx = _SourceContext(city_key)
    return _load_dataset_group(ctx, db_instance, get_config(city_key)["datasets"], progress_callback=progress_callback)


def _load_dataset_group(
    ctx: _SourceContext, db_instance: TransitDB, datasets: list[dict], progress_callback=None
) -> dict[str, int]:
    prepared_caches = _prepare_dataset_caches(ctx, datasets, progress_callback=progress_callback)
    results: dict[str, int] = {}
    for dataset in datasets:
        name = dataset["base_table_name"]
        if progress_callback:
            progress_callback(f"loading {name}...")
        cache_path = prepared_caches[name]
        results[name] = _load_prepared_dataset(
            ctx, db_instance, dataset, cache_path
        )
    return results


def _prepare_dataset_caches(ctx: _SourceContext, datasets: list[dict], progress_callback=None) -> dict[str, Path]:
    prepared_caches: dict[str, Path] = {}
    with ThreadPoolExecutor(max_workers=OPEN_DATA_DATASET_WORKERS) as executor:
        future_map = {}
        for dataset in datasets:
            future = executor.submit(_prepare_dataset_cache, ctx, dataset, progress_callback)
            future_map[future] = dataset["base_table_name"]
        for future in as_completed(future_map):
            base_table_name = future_map[future]
            prepared_caches[base_table_name] = future.result()
    return prepared_caches


def _prepare_dataset_cache(ctx: _SourceContext, dataset: dict, progress_callback=None) -> Path:
    if dataset.get("format", "geojson") == "json":
        return prepare_json_cache(ctx, dataset, progress_callback=progress_callback)
    return prepare_geojson_cache_for_dataset(ctx, dataset, BOUNDARY_TABLE_NAMES)


def _load_prepared_dataset(
    ctx: _SourceContext, db_instance: TransitDB, dataset: dict, cache_path: Path
) -> int:
    table_name = db_instance.table_name(dataset["base_table_name"], ctx.city_key)
    if dataset.get("format", "geojson") == "json":
        return load_jsonl_cache(table_name, cache_path, db_instance)
    if dataset["base_table_name"] in BOUNDARY_TABLE_NAMES:
        return load_boundary_table(table_name, cache_path, db_instance)
    return load_geojson_table(table_name, cache_path, db_instance)
