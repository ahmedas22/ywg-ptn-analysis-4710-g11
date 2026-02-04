"""Data transformation steps executed from SQL files."""

from datetime import datetime
from pathlib import Path
import re

from loguru import logger

from ptn_analysis.data.db import count_rows, get_duckdb, query_all, validate_identifier

DAY_COLUMNS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
SQL_DIR = Path(__file__).with_name("sql")


def _run_sql_file(filename: str) -> None:
    """Execute all SQL statements from a file under ``data/sql``.

    Args:
        filename: SQL file name located in ``ptn_analysis/data/sql``.
    """
    sql_text = (SQL_DIR / filename).read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    conn = get_duckdb()
    for statement in statements:
        conn.execute(statement)


def _run_sql_template(filename: str, replacements: dict[str, str]) -> None:
    """Execute a templated SQL file with placeholder replacement.

    Args:
        filename: SQL template file name in ``ptn_analysis/data/sql``.
        replacements: Placeholder values for ``{{key}}`` template tokens.
    """
    sql_text = (SQL_DIR / filename).read_text(encoding="utf-8")
    for key, value in replacements.items():
        sql_text = sql_text.replace(f"{{{{{key}}}}}", value)
    unresolved = re.findall(r"\{\{[^{}]+\}\}", sql_text)
    if unresolved:
        raise ValueError(
            f"Unresolved SQL template placeholders in {filename}: {sorted(set(unresolved))}"
        )

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]

    conn = get_duckdb()
    for statement in statements:
        conn.execute(statement)


def get_day_of_week_column(date: datetime) -> str:
    """Get GTFS calendar day column for a date.

    Args:
        date: Target date.

    Returns:
        Day column name matching GTFS calendar format.
    """
    return DAY_COLUMNS[date.weekday()]


def parse_target_date(target_date: str) -> tuple[str, str]:
    """Parse target date and derive GTFS date/day identifiers.

    Args:
        target_date: Date string in ``YYYY-MM-DD`` format.

    Returns:
        Tuple of ``(target_date, day_column)``.

    Raises:
        ValueError: If date format is invalid.
    """
    try:
        date_obj = datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {target_date}. Use YYYY-MM-DD.") from exc

    day_column = get_day_of_week_column(date_obj)
    return target_date, day_column


def build_edges_table() -> int:
    """Build ``raw_gtfs_edges`` from GTFS stop times and trips.

    Returns:
        Number of rows in ``raw_gtfs_edges`` after build.
    """
    logger.info("Building network edges from stop_times")
    _run_sql_file("build_edges.sql")
    edge_count = count_rows("raw", "gtfs_edges")
    logger.info(f"Created {edge_count:,} edges")
    return edge_count


def create_aggregated_edges() -> int:
    """Build ``raw_gtfs_edges_weighted`` from ``raw_gtfs_edges``.

    Returns:
        Number of rows in ``raw_gtfs_edges_weighted`` after build.
    """
    logger.info("Creating weighted edges")
    _run_sql_file("build_weighted_edges.sql")
    edge_count = count_rows("raw", "gtfs_edges_weighted")
    logger.info(f"Created {edge_count:,} weighted edges")
    return edge_count


def get_feed_date_range() -> tuple[str, str]:
    """Return GTFS feed date range from ``raw_gtfs_feed_info``.

    Returns:
        Tuple of ``(start_date, end_date)`` formatted as ``YYYY-MM-DD``.

    Raises:
        ValueError: If feed metadata is unavailable.
    """
    result = query_all(
        """
        SELECT feed_start_date, feed_end_date
        FROM raw_gtfs_feed_info
        LIMIT 1
        """
    )
    if not result:
        raise ValueError("No feed_info found - run 'make data' first")

    start_raw, end_raw = result[0][0], result[0][1]
    try:
        start_fmt = datetime.strptime(str(start_raw), "%Y%m%d").strftime("%Y-%m-%d")
        end_fmt = datetime.strptime(str(end_raw), "%Y%m%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid feed date range values: {start_raw}, {end_raw}") from exc
    return start_fmt, end_fmt


def materialize_active_trips(target_date: str) -> int:
    """Create ``agg_active_trips`` for a target service date.

    Args:
        target_date: Service date in ``YYYY-MM-DD`` format.

    Returns:
        Number of rows in ``agg_active_trips``.
    """
    target_date_sql, day_column = parse_target_date(target_date)
    validate_identifier(day_column, "calendar_day")

    logger.info(f"Materializing active trips for {target_date} ({day_column})")
    _run_sql_template(
        "materialize_active_trips.sql",
        {
            "target_date": target_date,
            "date_gtfs": target_date_sql,
            "day_column": day_column,
        },
    )

    active_count = count_rows("agg", "active_trips")
    logger.info(f"Materialized {active_count:,} active trips for {target_date}")
    return active_count


def create_coverage_aggs() -> None:
    """Create coverage aggregate tables from spatial joins."""
    logger.info("Creating coverage aggregation tables")
    _run_sql_file("coverage_aggs.sql")


def create_route_summary_aggs() -> None:
    """Create route and stop performance aggregate tables."""
    logger.info("Creating route summary aggregation tables")
    _run_sql_file("route_summary_aggs.sql")


def create_reference_tables() -> None:
    """Create GTFS-to-Open-Data mapping tables."""
    logger.info("Creating reference mapping tables")
    _run_sql_file("reference_tables.sql")


def create_performance_views() -> None:
    """Create analysis-ready performance views."""
    logger.info("Creating performance views")
    _run_sql_file("performance_views.sql")


def create_database_indexes() -> None:
    """Create indexes for join-heavy analysis."""
    logger.info("Creating indexes")
    _run_sql_file("indexes.sql")
