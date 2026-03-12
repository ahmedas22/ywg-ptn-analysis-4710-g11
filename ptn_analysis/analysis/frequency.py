"""Frequency analysis for transit service metrics.

This module provides frequency/headway metrics by querying pre-computed
gtfs-kit statistics tables (gtfs_route_stats, gtfs_stop_stats) stored in DuckDB.
"""

from __future__ import annotations

from duckdb import DuckDBPyConnection
from loguru import logger
import pandas as pd

from ptn_analysis.data.db import resolve_con

# Service window for gtfs-kit headway calculations (used in transform.py)
SERVICE_DAY_START = "06:00:00"
SERVICE_DAY_END = "22:00:00"


def _get_default_service_date(con: DuckDBPyConnection) -> str:
    """Get default service date from gtfs_route_stats or feed_info.

    Args:
        con: DuckDB connection.

    Returns:
        Service date in YYYY-MM-DD format.
    """
    # Try gtfs_route_stats first
    try:
        result = con.execute(
            "SELECT MIN(date) FROM gtfs_route_stats WHERE date IS NOT NULL"
        ).fetchone()
        if result and result[0]:
            return result[0]
    except Exception:
        pass

    # Fall back to feed_info
    try:
        result = con.execute("SELECT feed_start_date FROM feed_info").fetchone()
        if result and result[0]:
            raw = str(result[0])
            if len(raw) == 8:
                return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
            return raw
    except Exception:
        pass

    return "2026-01-15"  # Fallback date


def get_route_direction_labels(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Return human-readable direction labels per route and direction_id.

    Direction labels are derived from the most common ``trip_headsign`` for each
    ``(route_id, direction_id)`` pair.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: route_id, route_name, direction_id, direction_label.
    """
    con = resolve_con(con)
    query = """
        WITH label_rank AS (
            SELECT
                t.route_id,
                r.route_short_name AS route_name,
                t.direction_id,
                NULLIF(TRIM(t.trip_headsign), '') AS trip_headsign,
                COUNT(*) AS trips,
                ROW_NUMBER() OVER (
                    PARTITION BY t.route_id, t.direction_id
                    ORDER BY COUNT(*) DESC, NULLIF(TRIM(t.trip_headsign), '')
                ) AS rn
            FROM trips t
            JOIN routes r ON t.route_id = r.route_id
            GROUP BY t.route_id, r.route_short_name, t.direction_id, NULLIF(TRIM(t.trip_headsign), '')
        )
        SELECT
            route_id,
            route_name,
            direction_id,
            COALESCE(trip_headsign, 'Direction ' || CAST(direction_id AS VARCHAR)) AS direction_label
        FROM label_rank
        WHERE rn = 1
        ORDER BY route_name, direction_id
    """
    return con.execute(query).fetchdf()


def compute_route_frequency(
    service_date: str | None = None,
    split_directions: bool = False,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Compute route-level frequency metrics from pre-computed gtfs_route_stats.

    Uses the gtfs_route_stats table which is materialized by transform.py using
    gtfs-kit's compute_route_stats().

    Args:
        service_date: Date in ``YYYY-MM-DD`` format. If not provided, uses
            the earliest available date in gtfs_route_stats.
        split_directions: If True, return separate rows per direction_id.
            If False, aggregate across directions.
        con: Optional DuckDB connection.

    Returns:
        DataFrame containing route-level trip counts, headways, and service metrics.
        When split_directions=False, no direction_id column is included.
    """
    con = resolve_con(con)

    # Check if gtfs_route_stats exists
    has_stats = (
        con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'gtfs_route_stats'
            """
        ).fetchone()[0]
        > 0
    )

    if not has_stats:
        logger.warning("gtfs_route_stats table not found. Run 'make data' to materialize metrics.")
        return pd.DataFrame()

    if service_date is None:
        service_date = _get_default_service_date(con)
        logger.info(f"Using default service date: {service_date}")

    if split_directions:
        query = """
            SELECT
                route_id,
                route_short_name,
                direction_id,
                num_trips,
                mean_headway,
                min_headway,
                max_headway,
                peak_num_trips,
                service_duration,
                service_speed,
                start_time,
                end_time,
                service_distance,
                mean_trip_distance,
                mean_trip_duration
            FROM gtfs_route_stats
            WHERE date = ?
            ORDER BY route_short_name, direction_id
        """
    else:
        # Aggregate across directions - no direction_id in output
        query = """
            SELECT
                route_id,
                ANY_VALUE(route_short_name) AS route_short_name,
                SUM(num_trips) AS num_trips,
                AVG(mean_headway) AS mean_headway,
                MIN(min_headway) AS min_headway,
                MAX(max_headway) AS max_headway,
                SUM(peak_num_trips) AS peak_num_trips,
                MAX(service_duration) AS service_duration,
                AVG(service_speed) AS service_speed,
                MIN(start_time) AS start_time,
                MAX(end_time) AS end_time,
                SUM(service_distance) AS service_distance,
                AVG(mean_trip_distance) AS mean_trip_distance,
                AVG(mean_trip_duration) AS mean_trip_duration
            FROM gtfs_route_stats
            WHERE date = ?
            GROUP BY route_id
            ORDER BY route_short_name
        """

    stats = con.execute(query, [service_date]).fetchdf()
    logger.info(f"Retrieved frequency for {len(stats)} route groups")
    return stats


def compute_stop_headways(
    stop_id: str,
    service_date: str | None = None,
    split_directions: bool = True,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Compute stop-level headway statistics from pre-computed gtfs_stop_stats.

    Uses the gtfs_stop_stats table which is materialized by transform.py using
    gtfs-kit's compute_stop_stats().

    Args:
        stop_id: GTFS stop identifier.
        service_date: Date in ``YYYY-MM-DD`` format. If not provided, uses
            the earliest available date in gtfs_stop_stats.
        split_directions: If True, return separate stats per direction.
            If False, aggregate across directions.
        con: Optional DuckDB connection.

    Returns:
        DataFrame with stop-level departure and headway metrics.
    """
    con = resolve_con(con)

    # Check if gtfs_stop_stats exists
    has_stats = (
        con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'gtfs_stop_stats'
            """
        ).fetchone()[0]
        > 0
    )

    if not has_stats:
        logger.warning("gtfs_stop_stats table not found. Run 'make data' to materialize metrics.")
        return pd.DataFrame()

    if service_date is None:
        service_date = _get_default_service_date(con)
        logger.info(f"Using default service date: {service_date}")

    if split_directions:
        query = """
            SELECT
                stop_id,
                direction_id,
                num_routes,
                num_trips,
                mean_headway,
                min_headway,
                max_headway,
                start_time,
                end_time
            FROM gtfs_stop_stats
            WHERE stop_id = ? AND date = ?
        """
        return con.execute(query, [stop_id, service_date]).fetchdf()
    else:
        query = """
            SELECT
                stop_id,
                SUM(num_trips) AS num_trips,
                MAX(num_routes) AS num_routes,
                AVG(mean_headway) AS mean_headway,
                MIN(min_headway) AS min_headway,
                MAX(max_headway) AS max_headway,
                MIN(start_time) AS start_time,
                MAX(end_time) AS end_time
            FROM gtfs_stop_stats
            WHERE stop_id = ? AND date = ?
            GROUP BY stop_id
        """
        return con.execute(query, [stop_id, service_date]).fetchdf()


def get_frequency_summary(
    service_date: str | None = None,
    con: DuckDBPyConnection | None = None,
) -> dict[str, float]:
    """Get network-wide frequency summary statistics.

    Args:
        service_date: Date in YYYY-MM-DD format.
        con: Optional DuckDB connection.

    Returns:
        Dictionary with summary metrics:
            - total_routes: Number of routes operating
            - total_trips: Total trips across network
            - mean_headway_minutes: Network average headway
            - routes_under_15min: Routes with <15min headway
            - routes_under_30min: Routes with <30min headway
    """
    route_freq = compute_route_frequency(service_date, split_directions=False, con=con)

    if route_freq.empty:
        return {
            "total_routes": 0,
            "total_trips": 0,
            "mean_headway_minutes": 0.0,
            "routes_under_15min": 0,
            "routes_under_30min": 0,
        }

    return {
        "total_routes": len(route_freq),
        "total_trips": int(route_freq["num_trips"].sum()),
        "mean_headway_minutes": float(route_freq["mean_headway"].mean())
        if "mean_headway" in route_freq.columns
        else 0.0,
        "routes_under_15min": int((route_freq["mean_headway"] < 15).sum())
        if "mean_headway" in route_freq.columns
        else 0,
        "routes_under_30min": int((route_freq["mean_headway"] < 30).sum())
        if "mean_headway" in route_freq.columns
        else 0,
    }


def get_hourly_profile(
    route_id: str | None = None,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Get hourly departure profile from DuckDB.

    Args:
        route_id: Optional filter by route.
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: service_hour, trips_departing.
    """
    con = resolve_con(con)

    if route_id:
        query = """
            SELECT
                CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 as service_hour,
                COUNT(DISTINCT st.trip_id) as trips_departing
            FROM stop_times st
            JOIN trips t ON st.trip_id = t.trip_id
            WHERE st.stop_sequence = 1
              AND t.route_id = $1
            GROUP BY service_hour
            ORDER BY service_hour
        """
        return con.execute(query, [route_id]).fetchdf()
    else:
        query = """
            SELECT
                CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 as service_hour,
                COUNT(DISTINCT st.trip_id) as trips_departing
            FROM stop_times st
            WHERE st.stop_sequence = 1
            GROUP BY service_hour
            ORDER BY service_hour
        """
        return con.execute(query).fetchdf()


def get_departures_by_hour_by_route(
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Get hourly departure counts for each route using pre-computed view.

    Uses the hourly_departures_by_route view for efficient retrieval.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: route_id, route_short_name, route_long_name,
        hour, departures.
    """
    con = resolve_con(con)

    # Check if view exists
    has_view = (
        con.execute(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = 'hourly_departures_by_route'
            """
        ).fetchone()[0]
        > 0
    )

    if has_view:
        return con.execute(
            "SELECT * FROM hourly_departures_by_route ORDER BY route_short_name, hour"
        ).fetchdf()

    # Fallback: compute directly if view doesn't exist
    logger.warning("hourly_departures_by_route view not found. Computing directly.")
    query = """
        SELECT
            r.route_id,
            r.route_short_name,
            r.route_long_name,
            CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 AS hour,
            COUNT(DISTINCT st.trip_id) AS departures
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        WHERE st.stop_sequence = 1
        GROUP BY r.route_id, r.route_short_name, r.route_long_name, hour
        ORDER BY r.route_short_name, hour
    """
    return con.execute(query).fetchdf()


def get_route_performance(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Get route frequency combined with Open Data performance metrics.

    Uses route_performance view which joins:
    - GTFS routes
    - Pass-up counts (route_passups view)
    - On-time deviation (route_ontime view)

    Note:
        Open Data spans 2010-present while GTFS is current schedule.
        Historical route changes (especially PTN launch June 2025)
        may affect data quality for older records.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: route_id, route_short_name, route_long_name,
        passup_count, days_with_passups, avg_deviation_seconds, ontime_measurements.
    """
    con = resolve_con(con)
    return con.execute("SELECT * FROM route_performance ORDER BY passup_count DESC").fetchdf()
