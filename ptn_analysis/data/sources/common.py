"""Shared geo helpers for source modules."""

from __future__ import annotations

import os
from pathlib import Path
import re
import zipfile

from ptn_analysis.context.db import TransitDB


# ---------------------------------------------------------------------------
# Geo helpers
# ---------------------------------------------------------------------------


def is_valid_zip(path: Path) -> bool:
    """Return whether a file is a readable ZIP archive.

    Args:
        path: File path.

    Returns:
        True when the file is a readable ZIP archive.
    """
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with zipfile.ZipFile(path, "r") as zip_file:
            zip_file.namelist()
        return True
    except Exception:
        return False


def open_data_headers(city_key: str | None = None) -> dict[str, str]:
    """Build Socrata-style request headers.

    Args:
        city_key: Optional city namespace.

    Returns:
        Header dictionary.
    """
    headers: dict[str, str] = {"Accept": "application/json"}
    token = ""
    if city_key == "ywg":
        token = os.getenv("WPG_OPEN_DATA_APP_TOKEN", "")
    if not token and city_key:
        token = os.getenv(f"SOCRATA_APP_TOKEN_{city_key.upper()}", "")
    if not token:
        token = os.getenv("SOCRATA_APP_TOKEN", "")
    if token:
        headers["X-App-Token"] = token
    return headers


def load_geojson_table(
    table_name: str, path: Path, db_instance: TransitDB, select: str = "*, geom AS geometry"
) -> int:
    """Load a GeoJSON file into DuckDB.

    Args:
        table_name: Destination table name.
        path: Local GeoJSON path.
        db_instance: TransitDB to write into.
        select: Select clause for ``ST_Read``.

    Returns:
        Loaded row count.
    """
    if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
        raise ValueError(f"Invalid table name: {table_name!r}")
    db_instance.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT {select} FROM ST_Read('{path.as_posix()}')"
    )
    row_count = db_instance.count(table_name)
    if row_count is None:
        return 0
    return row_count
