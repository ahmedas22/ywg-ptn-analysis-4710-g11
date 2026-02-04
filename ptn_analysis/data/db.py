"""DuckDB helpers for the PTN data pipeline.

This module provides a small functional API for connection management,
query execution, and table lifecycle operations.
"""

import atexit
import re
from typing import Any

import duckdb
from duckdb import DuckDBPyConnection
from loguru import logger
import pandas as pd

from ptn_analysis.config import DUCKDB_PATH

__all__ = [
    "get_duckdb",
    "close_duckdb",
    "reset_duckdb",
    "resolve_con",
    "query_df",
    "query_scalar",
    "query_all",
    "validate_identifier",
    "validate_table_name",
    "validate_schema_name",
    "table_exists",
    "count_rows",
    "drop_table",
    "drop_create",
    "replace_table_as",
    "create_index",
    "bulk_insert_df",
]

IDENTIFIER_PATTERN = re.compile(r"^[a-z_][a-z0-9_]*$")
_CONN: DuckDBPyConnection | None = None


def get_duckdb() -> DuckDBPyConnection:
    """Return a cached DuckDB connection with spatial loaded."""
    global _CONN
    if _CONN is None:
        DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _CONN = duckdb.connect(str(DUCKDB_PATH))
        _CONN.execute("INSTALL spatial; LOAD spatial;")
        logger.info(f"Connected to DuckDB at {DUCKDB_PATH}")
    return _CONN


def close_duckdb() -> None:
    """Close cached DuckDB connection if open."""
    global _CONN
    if _CONN is not None:
        _CONN.close()
        _CONN = None
        logger.debug("DuckDB connection closed")


def reset_duckdb() -> None:
    """Reset database connection cache."""
    close_duckdb()


atexit.register(close_duckdb)


def resolve_con(con: DuckDBPyConnection | None = None) -> DuckDBPyConnection:
    """Return provided connection or the default cached connection."""
    return con if con is not None else get_duckdb()


def validate_identifier(name: str, kind: str = "identifier") -> None:
    """Validate SQL identifier for dynamic SQL safety."""
    if not IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid {kind} name: {name}")


def validate_table_name(table_name: str) -> None:
    """Validate table name for SQL safety."""
    validate_identifier(table_name, "table")


def validate_schema_name(schema_name: str) -> None:
    """Validate schema prefix for SQL safety."""
    validate_identifier(schema_name, "schema")


def convert_schema_refs(sql: str) -> str:
    """Convert schema-qualified refs to physical table names.

    The pipeline stores tables as ``raw_*``, ``agg_*``, and ``ref_*`` in
    DuckDB. This helper rewrites SQL fragments like ``raw.gtfs_stops`` to
    ``raw_gtfs_stops`` while leaving quoted identifiers untouched.

    Args:
        sql: SQL text that may include logical schema prefixes.

    Returns:
        SQL with logical schema prefixes rewritten to physical table names.
    """
    pattern = re.compile(r"\b(raw|agg|ref)\.([a-z_][a-z0-9_]*)\b")
    return pattern.sub(r"\1_\2", sql)


def query_df(
    sql: str,
    con: DuckDBPyConnection | None = None,
    params: list[Any] | dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Execute SQL and return a DataFrame."""
    conn = resolve_con(con)
    prepared_sql = convert_schema_refs(sql)
    if params:
        return conn.execute(prepared_sql, params).fetchdf()
    return conn.execute(prepared_sql).fetchdf()


def query_scalar(
    sql: str,
    con: DuckDBPyConnection | None = None,
    params: list[Any] | dict[str, Any] | None = None,
):
    """Execute SQL and return first value from first row."""
    conn = resolve_con(con)
    prepared_sql = convert_schema_refs(sql)
    row = (
        conn.execute(prepared_sql, params).fetchone()
        if params
        else conn.execute(prepared_sql).fetchone()
    )
    return row[0] if row else None


def query_all(
    sql: str,
    con: DuckDBPyConnection | None = None,
    params: list[Any] | dict[str, Any] | None = None,
) -> list[tuple]:
    """Execute SQL and return all rows."""
    conn = resolve_con(con)
    prepared_sql = convert_schema_refs(sql)
    if params:
        return conn.execute(prepared_sql, params).fetchall()
    return conn.execute(prepared_sql).fetchall()


def table_exists(schema: str, table: str, con: DuckDBPyConnection | None = None) -> bool:
    """Return whether a table exists."""
    full_name = f"{schema}_{table}"
    conn = resolve_con(con)
    try:
        conn.execute(f"SELECT 1 FROM {full_name} LIMIT 0")
        return True
    except Exception:
        return False


def count_rows(schema: str, table: str, con: DuckDBPyConnection | None = None) -> int:
    """Return row count for a table, or -1 if unavailable."""
    full_name = f"{schema}_{table}"
    conn = resolve_con(con)
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {full_name}").fetchone()
        return row[0] if row else 0
    except Exception as exc:
        logger.opt(exception=exc).debug(f"Count failed for {full_name}")
        return -1


def drop_table(schema: str, table: str, con: DuckDBPyConnection | None = None) -> None:
    """Drop table if it exists."""
    validate_schema_name(schema)
    validate_table_name(table)
    conn = resolve_con(con)
    conn.execute(f"DROP TABLE IF EXISTS {schema}_{table}")


def drop_create(schema: str, table: str, ddl: str, con: DuckDBPyConnection | None = None) -> None:
    """Drop table and create it using provided DDL."""
    validate_schema_name(schema)
    validate_table_name(table)
    conn = resolve_con(con)
    drop_table(schema, table, conn)
    prepared_ddl = ddl.replace(f"{schema}.{table}", f"{schema}_{table}")
    conn.execute(prepared_ddl)
    logger.info(f"Created {schema}_{table}")


def replace_table_as(
    schema: str,
    table: str,
    select_sql: str,
    con: DuckDBPyConnection | None = None,
    params: dict[str, Any] | list[Any] | None = None,
) -> None:
    """Replace table with CREATE TABLE AS SELECT output."""
    validate_schema_name(schema)
    validate_table_name(table)
    conn = resolve_con(con)
    full_name = f"{schema}_{table}"
    drop_table(schema, table, conn)
    prepared_sql = convert_schema_refs(select_sql)
    if params:
        conn.execute(f"CREATE TABLE {full_name} AS\n{prepared_sql}", params)
    else:
        conn.execute(f"CREATE TABLE {full_name} AS\n{prepared_sql}")
    logger.info(f"Replaced {full_name}")


def create_index(
    schema: str,
    table: str,
    index_name: str,
    columns: str,
    con: DuckDBPyConnection | None = None,
) -> None:
    """Create index if not exists."""
    validate_schema_name(schema)
    validate_table_name(table)
    validate_identifier(index_name, "index")
    conn = resolve_con(con)
    conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema}_{table}({columns})")


def bulk_insert_df(
    df: pd.DataFrame,
    schema: str,
    table: str,
    con: DuckDBPyConnection | None = None,
    if_exists: str = "append",
    log_insert: bool = True,
) -> int:
    """Insert DataFrame into table and return inserted row count.

    Args:
        df: DataFrame to insert.
        schema: Schema prefix (raw, agg, ref).
        table: Table name without schema prefix.
        con: Optional existing DuckDB connection.
        if_exists: Insert mode ("append" or "replace").
        log_insert: Whether to emit per-insert row logs.

    Returns:
        Number of inserted rows.
    """
    conn = resolve_con(con)
    full_name = f"{schema}_{table}"
    try:
        conn.register("_temp_df", df)
        if if_exists == "replace":
            conn.execute(f"DROP TABLE IF EXISTS {full_name}")
            conn.execute(f"CREATE TABLE {full_name} AS SELECT * FROM _temp_df")
        else:
            conn.execute(f"INSERT INTO {full_name} SELECT * FROM _temp_df")
    finally:
        try:
            conn.unregister("_temp_df")
        except Exception:
            pass
    if log_insert:
        logger.info(f"Inserted {len(df):,} rows to {full_name}")
    return len(df)
