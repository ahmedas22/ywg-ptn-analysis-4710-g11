"""SQL execution and data transformation functions."""

from datetime import datetime
from pathlib import Path
import re

from loguru import logger

from ptn_analysis.data.db import bulk_insert_df, get_duckdb
from ptn_analysis.data.loaders import load_gtfs_feed

SQL_DIR = Path(__file__).with_name("sql")


def run_sql(filename: str, **replacements: str) -> None:
    """Execute SQL file with optional template replacement.

    Args:
        filename: SQL file name relative to sql/ directory.
        **replacements: Template {{key}} replacements.

    Raises:
        FileNotFoundError: If SQL file does not exist.
        ValueError: If unresolved placeholders remain after replacement.
        RuntimeError: If SQL execution fails.
    """
    sql_path = SQL_DIR / filename
    if not sql_path.exists():
        raise FileNotFoundError(f"SQL file not found: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")

    for key, value in replacements.items():
        sql_text = sql_text.replace(f"{{{{{key}}}}}", value)

    unresolved = re.findall(r"\{\{[^{}]+\}\}", sql_text)
    if unresolved:
        raise ValueError(f"Unresolved placeholders in {filename}: {sorted(set(unresolved))}")

    statements = [stmt.strip() for stmt in sql_text.split(";") if stmt.strip()]
    conn = get_duckdb()

    conn.execute("BEGIN TRANSACTION")
    try:
        for stmt in statements:
            conn.execute(stmt)
        conn.execute("COMMIT")
        logger.debug(f"Executed {filename}")
    except Exception as exc:
        conn.execute("ROLLBACK")
        raise RuntimeError(f"SQL failed in {filename}: {exc}") from exc


def create_tables() -> None:
    """Create all GTFS tables from DDL schema."""
    logger.info("Creating database tables")
    run_sql("schema.sql")


def build_stop_connections() -> None:
    """Build stop_connections table from stop_times and trips."""
    logger.info("Building stop connections")
    run_sql("build_edges.sql")


def build_weighted_connections() -> None:
    """Build stop_connections_weighted from stop_connections."""
    logger.info("Building weighted connections")
    run_sql("build_weighted_edges.sql")


def materialize_daily_service(target_date: str) -> None:
    """Create daily_service table for a target service date using gtfs-kit.

    Uses gtfs-kit's restrict_to_dates() which correctly handles:
    - calendar.txt service patterns (M-F, Sat, Sun)
    - calendar_dates.txt exceptions (holidays, special service)

    Args:
        target_date: Service date in YYYY-MM-DD format.
    """
    # Validate date format
    try:
        datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid date format: {target_date}. Use YYYY-MM-DD.") from exc

    logger.info(f"Materializing daily service for {target_date}")

    # Load feed and restrict to target date
    feed = load_gtfs_feed()
    date_str = target_date.replace("-", "")  # gtfs-kit uses YYYYMMDD format
    restricted_feed = feed.restrict_to_dates([date_str])

    conn = get_duckdb()
    conn.execute("DROP TABLE IF EXISTS daily_service")

    if restricted_feed.trips is None or restricted_feed.trips.empty:
        logger.warning(f"No active trips found for {target_date}")
        # Create empty table with correct schema
        conn.execute(
            """
            CREATE TABLE daily_service (
                trip_id VARCHAR,
                route_id VARCHAR,
                service_id VARCHAR,
                trip_headsign VARCHAR,
                direction_id INTEGER,
                service_date VARCHAR
            )
            """
        )
    else:
        # Add service_date column to DataFrame before inserting
        trips_df = restricted_feed.trips.copy()
        trips_df["service_date"] = target_date
        logger.info(f"Found {len(trips_df)} active trips for {target_date}")

        # Use DuckDB replacement scan - references DataFrame by variable name
        conn.execute(
            """
            CREATE TABLE daily_service AS
            SELECT DISTINCT
                trip_id,
                route_id,
                service_id,
                trip_headsign,
                direction_id,
                service_date
            FROM trips_df
            """
        )

    conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_route ON daily_service(route_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_daily_service ON daily_service(service_id)")
    logger.info("Daily service table created")


def create_views() -> None:
    """Create analysis views (coverage, performance)."""
    logger.info("Creating analysis views")
    run_sql("views.sql")


def create_indexes() -> None:
    """Create database indexes for query performance."""
    logger.info("Creating indexes")
    run_sql("indexes.sql")


def materialize_gtfs_metrics() -> dict[str, int]:
    """Materialize gtfs-kit route/stop metrics tables in DuckDB.

    Metrics are computed from the current GTFS feed for every available service
    date and stored as ``gtfs_route_stats`` and ``gtfs_stop_stats``.

    Returns:
        Dictionary containing inserted row counts for metric tables.
    """
    logger.info("Computing gtfs-kit metrics for all service dates")
    feed = load_gtfs_feed()
    dates = feed.get_dates()
    if not dates:
        raise ValueError("No service dates found in GTFS feed")

    route_stats = feed.compute_route_stats(
        dates=dates,
        headway_start_time="06:00:00",
        headway_end_time="22:00:00",
        split_directions=True,
    )
    route_stats["date"] = (
        route_stats["date"]
        .astype(str)
        .str.replace(
            r"(\d{4})(\d{2})(\d{2})",
            r"\1-\2-\3",
            regex=True,
        )
    )
    bulk_insert_df(route_stats, "", "gtfs_route_stats", if_exists="replace", log_insert=False)

    stop_stats = feed.compute_stop_stats(
        dates=dates,
        headway_start_time="06:00:00",
        headway_end_time="22:00:00",
        split_directions=True,
    )
    stop_stats["date"] = (
        stop_stats["date"]
        .astype(str)
        .str.replace(
            r"(\d{4})(\d{2})(\d{2})",
            r"\1-\2-\3",
            regex=True,
        )
    )
    bulk_insert_df(stop_stats, "", "gtfs_stop_stats", if_exists="replace", log_insert=False)

    conn = get_duckdb()
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtfs_route_stats_date ON gtfs_route_stats(date)")
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_gtfs_route_stats_route ON gtfs_route_stats(route_id)"
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtfs_stop_stats_date ON gtfs_stop_stats(date)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gtfs_stop_stats_stop ON gtfs_stop_stats(stop_id)")

    results = {"gtfs_route_stats": len(route_stats), "gtfs_stop_stats": len(stop_stats)}
    logger.info(
        f"Loaded {results['gtfs_route_stats']:,} rows → gtfs_route_stats; "
        f"{results['gtfs_stop_stats']:,} rows → gtfs_stop_stats"
    )
    return results
