"""DuckDB connection and query helpers."""

from __future__ import annotations

import re
from typing import Any
import uuid

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger
import pandas as pd

from ptn_analysis.config import DUCKDB_PATH

__all__ = [
    "get_duckdb",
    "resolve_con",
    "query_df",
    "query_scalar",
    "bulk_insert_df",
]

_IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_conn: DuckDBPyConnection | None = None


def get_duckdb() -> DuckDBPyConnection:
    """Get cached DuckDB connection with spatial extension.

    Returns:
        DuckDB connection with spatial extension loaded.
    """
    global _conn
    if _conn is None:
        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _conn = duckdb.connect(str(DUCKDB_PATH))
        try:
            _conn.execute("LOAD spatial;")
        except duckdb.CatalogException:
            _conn.execute("INSTALL spatial; LOAD spatial;")
        logger.info(f"Connected to DuckDB at {DUCKDB_PATH}")
    return _conn


def resolve_con(con: DuckDBPyConnection | None = None) -> DuckDBPyConnection:
    """Return provided connection or default cached connection.

    Args:
        con: Optional existing connection.

    Returns:
        The provided connection if not None, otherwise the cached connection.
    """
    if con is not None:
        return con
    return get_duckdb()


def validate_identifier(name: str, kind: str = "identifier") -> None:
    """Validate SQL identifier for safety.

    Only allows lowercase letters, numbers, and underscores.
    Must start with letter or underscore.

    Args:
        name: Identifier to validate.
        kind: Type of identifier for error messages.

    Raises:
        ValueError: If name doesn't match the allowed pattern.
    """
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid {kind} name: {name}")


def query_df(
    sql: str,
    con: DuckDBPyConnection | None = None,
    params: list[Any] | dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Execute SQL and return results as DataFrame.

    Args:
        sql: SQL query string.
        con: Optional DuckDB connection. Uses cached connection if None.
        params: Optional query parameters for parameterized queries.

    Returns:
        Query results as a pandas DataFrame.
    """
    conn = resolve_con(con)
    if params:
        return conn.execute(sql, params).df()
    return conn.sql(sql).df()


def query_scalar(
    sql: str,
    con: DuckDBPyConnection | None = None,
    params: list[Any] | dict[str, Any] | None = None,
) -> Any:
    """Execute SQL and return first value from first row.

    Args:
        sql: SQL query string.
        con: Optional DuckDB connection. Uses cached connection if None.
        params: Optional query parameters for parameterized queries.

    Returns:
        First value from first row, or None if no results.
    """
    conn = resolve_con(con)
    if params:
        row = conn.execute(sql, params).fetchone()
    else:
        row = conn.sql(sql).fetchone()
    if row:
        return row[0]
    return None


def bulk_insert_df(
    df: pd.DataFrame,
    schema: str,
    table: str,
    con: DuckDBPyConnection | None = None,
    if_exists: str = "append",
    log_insert: bool = True,
) -> int:
    """Insert DataFrame into DuckDB table.

    Args:
        df: DataFrame to insert.
        schema: Schema prefix (empty string for no prefix).
        table: Target table name.
        con: Optional DuckDB connection. Uses cached connection if None.
        if_exists: How to handle existing table - "append" or "replace".
        log_insert: Whether to log the insertion.

    Returns:
        Number of rows inserted.

    Raises:
        ValueError: If table or schema name contains invalid characters.
    """
    conn = resolve_con(con)

    # Build full table name
    if schema:
        validate_identifier(schema, "schema")
        full_name = f"{schema}_{table}"
    else:
        full_name = table

    validate_identifier(table, "table")

    temp_name = f"_temp_df_{uuid.uuid4().hex[:8]}"
    try:
        conn.register(temp_name, df)
        if if_exists == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {full_name}")
            conn.execute(f"CREATE TABLE {full_name} AS SELECT * FROM {temp_name}")
        else:
            conn.execute(f"INSERT INTO {full_name} SELECT * FROM {temp_name}")
    finally:
        try:
            conn.unregister(temp_name)
        except duckdb.CatalogException:
            pass

    if log_insert:
        logger.info(f"Inserted {len(df):,} rows → {full_name}")
    return len(df)
