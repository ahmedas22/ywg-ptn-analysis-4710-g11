"""Winnipeg Open Data ingestion for tabular and spatial datasets."""

from dataclasses import dataclass
import os
from pathlib import Path
import re
from typing import Callable

from loguru import logger
import pandas as pd
from tqdm import tqdm

from ptn_analysis.config import DATASETS, RAW_DATA_DIR, WPG_OPEN_DATA_URL
from ptn_analysis.data.db import bulk_insert_df, get_duckdb
from ptn_analysis.data.ingest_shared import download_with_cache
from ptn_analysis.data.schemas import BoundaryTableConfig

PAGE_SIZE = 50_000


@dataclass(frozen=True)
class OpenDataLoadSpec:
    """Configuration for loading one Winnipeg Open Data dataset.

    Args:
        result_key: Result dictionary key returned by bulk loaders.
        dataset_key: Lookup key in DATASETS.
        table_name: Target table name without raw_ prefix.
        log_name: Human-readable label for logs.
        is_geojson: Whether dataset is spatial and should load from GeoJSON.
        transformers: Optional column transformers for tabular data.
    """

    result_key: str
    dataset_key: str
    table_name: str
    log_name: str
    is_geojson: bool = False
    transformers: dict[str, Callable] | None = None


OPEN_DATA_SPECS: tuple[OpenDataLoadSpec, ...] = (
    OpenDataLoadSpec("pass_ups", "pass_ups", "open_data_pass_ups", "pass-up data"),
    OpenDataLoadSpec(
        "on_time",
        "on_time",
        "open_data_on_time",
        "on-time performance data",
        transformers={"deviation": lambda x: pd.to_numeric(x, errors="coerce")},
    ),
    OpenDataLoadSpec("passenger_counts", "passenger_counts", "open_data_passenger_counts", "passenger count data"),
    OpenDataLoadSpec("cycling", "cycling", "open_data_cycling_network", "cycling network data", is_geojson=True),
    OpenDataLoadSpec("walkways", "walkways", "open_data_walkways", "walkways data", is_geojson=True),
)

ACTIVE_MOBILITY_SPECS = tuple(spec for spec in OPEN_DATA_SPECS if spec.is_geojson)
STANDARD_OPEN_DATA_SPECS = tuple(spec for spec in OPEN_DATA_SPECS if not spec.is_geojson)


def _resolve_dataset_id(dataset_key: str, log_name: str) -> str | None:
    """Return configured dataset id.

    Args:
        dataset_key: Lookup key in DATASETS.
        log_name: Label for warning messages.

    Returns:
        Dataset id, or None when missing.
    """
    dataset_id = DATASETS.get(dataset_key)
    if dataset_id:
        return dataset_id

    logger.warning(f"Skipping {log_name}: dataset id is not configured")
    return None


def _build_request_headers() -> dict[str, str]:
    """Build request headers for Winnipeg Open Data APIs.

    Returns:
        Request headers with optional app token.
    """
    headers = {"Accept": "application/json"}
    app_token = os.getenv("WPG_OPEN_DATA_APP_TOKEN")
    if app_token:
        headers["X-App-Token"] = app_token
    return headers


def _dataset_cache_path(dataset_id: str, extension: str) -> Path:
    """Build path under data/raw for a dataset artifact.

    Args:
        dataset_id: Winnipeg Open Data dataset id.
        extension: File extension without dot.

    Returns:
        Path to artifact in RAW_DATA_DIR.
    """
    return RAW_DATA_DIR / f"{dataset_id}.{extension}"


def _apply_transformers(df: pd.DataFrame, transformers: dict[str, Callable] | None) -> pd.DataFrame:
    """Apply configured column transformers.

    Args:
        df: Input DataFrame.
        transformers: Mapping of column name to transform callable.

    Returns:
        DataFrame with transformed columns.
    """
    if not transformers:
        return df

    for column, transform in transformers.items():
        if column in df.columns:
            df[column] = transform(df[column])
    return df


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize DataFrame column names to snake_case.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with normalized column names.
    """
    normalized = {}
    for column in df.columns:
        name = str(column).strip().lower()
        name = re.sub(r"[^a-z0-9]+", "_", name)
        name = re.sub(r"_+", "_", name).strip("_")
        normalized[column] = name
    return df.rename(columns=normalized)


def _load_csv_table(dataset_id: str, spec: OpenDataLoadSpec, limit: int | None = None) -> int:
    """Load tabular dataset into DuckDB from export.csv.

    Args:
        dataset_id: Winnipeg Open Data dataset id.
        spec: Dataset load specification.
        limit: Optional row cap.

    Returns:
        Inserted row count.
    """
    if limit is not None and limit <= 0:
        logger.warning("Non-positive limit provided; returning 0 rows")
        return 0

    csv_url = f"{WPG_OPEN_DATA_URL}/api/v3/views/{dataset_id}/export.csv"
    csv_path = download_with_cache(
        csv_url,
        _dataset_cache_path(dataset_id, "csv"),
        description=f"Export {spec.log_name} ({dataset_id})",
        headers=_build_request_headers(),
    )

    if limit is not None:
        df = pd.read_csv(csv_path, nrows=limit, low_memory=False)
        df = _normalize_column_names(df)
        df = _apply_transformers(df, spec.transformers)
        bulk_insert_df(df, "raw", spec.table_name, if_exists="replace", log_insert=False)
        logger.info(f"Loaded {len(df):,} rows to raw_{spec.table_name}")
        return len(df)

    loaded_rows = 0
    with tqdm(
        total=None,
        unit="rows",
        unit_scale=True,
        mininterval=0.5,
        smoothing=0.1,
        desc=f"Load {spec.log_name} ({dataset_id})",
    ) as progress:
        for chunk_number, chunk in enumerate(pd.read_csv(csv_path, chunksize=PAGE_SIZE, low_memory=False), start=1):
            chunk = _normalize_column_names(chunk)
            chunk = _apply_transformers(chunk, spec.transformers)
            if chunk_number == 1:
                bulk_insert_df(chunk, "raw", spec.table_name, if_exists="replace", log_insert=False)
            else:
                bulk_insert_df(chunk, "raw", spec.table_name, if_exists="append", log_insert=False)

            loaded_rows += len(chunk)
            progress.update(len(chunk))

    logger.info(f"Loaded {loaded_rows:,} rows to raw_{spec.table_name}")
    return loaded_rows


def _quote_ident(name: str) -> str:
    """Quote identifier for SQL generation.

    Args:
        name: Identifier name.

    Returns:
        Double-quoted identifier.
    """
    return f'"{name.replace(chr(34), chr(34) * 2)}"'


def _load_geojson_table(
    dataset_id: str,
    table_name: str,
    name_fields: list[str] | None = None,
    area_fields: list[str] | None = None,
) -> int:
    """Load GeoJSON dataset into DuckDB using ST_Read.

    Args:
        dataset_id: Winnipeg Open Data dataset id.
        table_name: Target table without raw_ prefix.
        name_fields: Optional boundary name candidates.
        area_fields: Optional boundary area candidates.

    Returns:
        Loaded row count.
    """
    geojson_url = f"{WPG_OPEN_DATA_URL}/api/v3/views/{dataset_id}/query.geojson"
    geojson_path = download_with_cache(
        geojson_url,
        _dataset_cache_path(dataset_id, "geojson"),
        description=f"GeoJSON {table_name} ({dataset_id})",
        headers=_build_request_headers(),
    )

    conn = get_duckdb()
    full_table_name = f"raw_{table_name}"

    # Boundary tables have standardized columns. Other spatial datasets keep raw properties.
    select_sql = """
        SELECT
            row_number() OVER () AS id,
            NULL::VARCHAR AS properties_json,
            geom AS geometry
        FROM ST_Read('{path}')
    """

    if name_fields is not None and area_fields is not None:
        describe_sql = f"DESCRIBE SELECT * FROM ST_Read('{geojson_path.as_posix()}')"
        columns = conn.execute(describe_sql).fetchdf()["column_name"].tolist()

        available_names = [column for column in name_fields if column in columns]
        available_areas = [column for column in area_fields if column in columns]

        if available_names:
            name_expr = "COALESCE(" + ", ".join(_quote_ident(column) for column in available_names) + ")"
        else:
            name_expr = "'Unknown'"

        if available_areas:
            area_expr = "COALESCE(" + ", ".join(
                f"TRY_CAST({_quote_ident(column)} AS DOUBLE)" for column in available_areas
            ) + ", 0.0)"
        else:
            area_expr = "0.0"

        select_sql = f"""
            SELECT
                row_number() OVER () AS id,
                {name_expr} AS name,
                {area_expr} AS area_km2,
                geom AS geometry
            FROM ST_Read('{{path}}')
        """

    conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")
    conn.execute(f"CREATE TABLE {full_table_name} AS {select_sql.format(path=geojson_path.as_posix())}")

    row = conn.execute(f"SELECT COUNT(*) FROM {full_table_name}").fetchone()
    loaded_rows = row[0] if row else 0
    logger.info(f"Loaded {loaded_rows:,} rows to {full_table_name}")
    return loaded_rows


def _load_open_data_spec(spec: OpenDataLoadSpec, limit: int | None = None) -> int:
    """Load one Open Data spec into raw tables.

    Args:
        spec: Dataset load specification.
        limit: Optional row cap for tabular datasets.

    Returns:
        Loaded row count.
    """
    logger.info(f"Loading {spec.log_name}")
    dataset_id = _resolve_dataset_id(spec.dataset_key, spec.log_name)
    if dataset_id is None:
        return 0

    if spec.is_geojson:
        return _load_geojson_table(dataset_id, spec.table_name)

    return _load_csv_table(dataset_id, spec, limit=limit)


def _spec_matches_filter(spec: OpenDataLoadSpec, include: set[str] | None = None, exclude: set[str] | None = None) -> bool:
    """Return whether a dataset spec passes include/exclude filters.

    Args:
        spec: Dataset specification.
        include: Optional include tokens (dataset key or dataset id).
        exclude: Optional exclude tokens (dataset key or dataset id).

    Returns:
        True if spec should be loaded.
    """
    dataset_id = DATASETS.get(spec.dataset_key, "")
    include_ok = include is None or spec.dataset_key in include or dataset_id in include
    exclude_hit = exclude is not None and (spec.dataset_key in exclude or dataset_id in exclude)
    return include_ok and not exclude_hit


def load_boundary_table(config: BoundaryTableConfig) -> int:
    """Load one boundary table.

    Args:
        config: Boundary table configuration.

    Returns:
        Loaded row count.
    """
    logger.info(f"Loading {config.log_name} boundaries")
    dataset_id = _resolve_dataset_id(config.dataset_key, config.log_name)
    if dataset_id is None:
        return 0

    return _load_geojson_table(
        dataset_id,
        table_name=config.table_name,
        name_fields=config.name_fields,
        area_fields=config.area_fields,
    )


def load_standard_open_data_tables(
    limit: int | None = None,
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> dict[str, int]:
    """Load standard tabular Open Data datasets.

    Args:
        limit: Optional row cap for each dataset.
        include: Optional include tokens (dataset key or dataset id).
        exclude: Optional exclude tokens (dataset key or dataset id).

    Returns:
        Mapping from dataset key to loaded row count.
    """
    selected = [spec for spec in STANDARD_OPEN_DATA_SPECS if _spec_matches_filter(spec, include, exclude)]
    return {spec.result_key: _load_open_data_spec(spec, limit=limit) for spec in selected}


def load_active_mobility_datasets(
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> dict[str, int]:
    """Load cycling and walkway spatial datasets.

    Args:
        include: Optional include tokens (dataset key or dataset id).
        exclude: Optional exclude tokens (dataset key or dataset id).

    Returns:
        Mapping from dataset key to loaded row count.
    """
    selected = [spec for spec in ACTIVE_MOBILITY_SPECS if _spec_matches_filter(spec, include, exclude)]
    return {spec.result_key: _load_open_data_spec(spec) for spec in selected}
