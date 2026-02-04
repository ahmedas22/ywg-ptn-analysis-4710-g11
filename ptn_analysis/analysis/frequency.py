"""Frequency analysis for transit service metrics."""

from duckdb import DuckDBPyConnection
import pandas as pd

from ptn_analysis.data.db import resolve_con


def parse_gtfs_time(time_str: str) -> tuple[int, int, int]:
    """Parse GTFS time string to (hour, minute, second).

    Args:
        time_str: Time in HH:MM:SS format (e.g., "25:30:00").

    Returns:
        Tuple of (hour, minute, second) as integers.

    Raises:
        ValueError: If time_str is not in valid HH:MM:SS format.
    """
    if not time_str or not isinstance(time_str, str):
        raise ValueError(f"Invalid GTFS time: {time_str}")
    parts = time_str.split(":")
    if len(parts) != 3:
        raise ValueError(f"Invalid GTFS time format: {time_str}")
    try:
        hour, minute, second = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError as e:
        raise ValueError(f"Invalid GTFS time: {time_str}") from e
    if minute < 0 or minute > 59 or second < 0 or second > 59:
        raise ValueError(f"Invalid GTFS time: {time_str}")
    if hour < 0:
        raise ValueError(f"Invalid GTFS time: {time_str}")
    return hour, minute, second


def gtfs_time_to_minutes(time_str: str) -> int:
    """Convert GTFS time string to minutes since midnight.

    Args:
        time_str: Time in HH:MM:SS format.

    Returns:
        Minutes since midnight (can exceed 1440 for next-day times).
    """
    h, m, s = parse_gtfs_time(time_str)
    return h * 60 + m


def _has_active_trips_table(con: DuckDBPyConnection) -> bool:
    """Check if agg_active_trips table exists in DuckDB."""
    try:
        result = con.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = 'agg_active_trips'
        """).fetchone()
        return result is not None and result[0] > 0
    except Exception:
        return False


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
            FROM raw_gtfs_trips t
            JOIN raw_gtfs_routes r ON t.route_id = r.route_id
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


def compute_trips_per_hour(
    service_date: str | None = None,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Compute route departures by service hour.

    Args:
        service_date: Optional date in YYYY-MM-DD format to filter by.
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: route_id, route_name, service_hour, direction_id,
        direction_label, trips_departing.

    Raises:
        ValueError: If service_date is provided but agg_active_trips table is unavailable.
    """
    con = resolve_con(con)

    if service_date:
        if not _has_active_trips_table(con):
            raise ValueError(
                f"Cannot filter by service_date '{service_date}': agg_active_trips table "
                f"not found. Run 'make frequency DATE={service_date}' to materialize active trips."
            )
        # Use active trips table with date filter
        query = """
            WITH trip_counts AS (
                SELECT
                    t.route_id,
                    CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 AS service_hour,
                    t.direction_id,
                    COUNT(DISTINCT t.trip_id) AS trips_departing
                FROM raw_gtfs_stop_times st
                JOIN agg_active_trips t ON st.trip_id = t.trip_id
                WHERE st.stop_sequence = 1
                  AND t.service_date = $1
                GROUP BY t.route_id, service_hour, t.direction_id
            ),
            direction_labels AS (
                SELECT
                    t.route_id,
                    t.direction_id,
                    NULLIF(TRIM(t.trip_headsign), '') AS trip_headsign,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.route_id, t.direction_id
                        ORDER BY COUNT(*) DESC, NULLIF(TRIM(t.trip_headsign), '')
                    ) AS rn
                FROM agg_active_trips t
                WHERE t.service_date = $1
                GROUP BY t.route_id, t.direction_id, NULLIF(TRIM(t.trip_headsign), '')
            )
            SELECT
                tc.route_id,
                r.route_short_name AS route_name,
                tc.service_hour,
                tc.direction_id,
                COALESCE(dl.trip_headsign, 'Direction ' || CAST(tc.direction_id AS VARCHAR)) AS direction_label,
                tc.trips_departing
            FROM trip_counts tc
            JOIN raw_gtfs_routes r ON r.route_id = tc.route_id
            LEFT JOIN direction_labels dl
                ON dl.route_id = tc.route_id
               AND dl.direction_id = tc.direction_id
               AND dl.rn = 1
            ORDER BY route_name, tc.direction_id, tc.service_hour
        """
        return con.execute(query, [service_date]).fetchdf()
    else:
        query = """
            WITH trip_counts AS (
                SELECT
                    t.route_id,
                    CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 AS service_hour,
                    t.direction_id,
                    COUNT(DISTINCT t.trip_id) AS trips_departing
                FROM raw_gtfs_stop_times st
                JOIN raw_gtfs_trips t ON st.trip_id = t.trip_id
                WHERE st.stop_sequence = 1
                GROUP BY t.route_id, service_hour, t.direction_id
            ),
            direction_labels AS (
                SELECT
                    t.route_id,
                    t.direction_id,
                    NULLIF(TRIM(t.trip_headsign), '') AS trip_headsign,
                    ROW_NUMBER() OVER (
                        PARTITION BY t.route_id, t.direction_id
                        ORDER BY COUNT(*) DESC, NULLIF(TRIM(t.trip_headsign), '')
                    ) AS rn
                FROM raw_gtfs_trips t
                GROUP BY t.route_id, t.direction_id, NULLIF(TRIM(t.trip_headsign), '')
            )
            SELECT
                tc.route_id,
                r.route_short_name AS route_name,
                tc.service_hour,
                tc.direction_id,
                COALESCE(dl.trip_headsign, 'Direction ' || CAST(tc.direction_id AS VARCHAR)) AS direction_label,
                tc.trips_departing
            FROM trip_counts tc
            JOIN raw_gtfs_routes r ON r.route_id = tc.route_id
            LEFT JOIN direction_labels dl
                ON dl.route_id = tc.route_id
               AND dl.direction_id = tc.direction_id
               AND dl.rn = 1
            ORDER BY route_name, tc.direction_id, tc.service_hour
        """
        return con.execute(query).fetchdf()


def compute_headways(
    route_id: str,
    stop_id: str,
    service_date: str | None = None,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Compute headways (time between arrivals) at a stop for a route.

    Args:
        route_id: Route identifier.
        stop_id: Stop identifier.
        service_date: Optional date to filter active trips.
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: arrival_time, headway_minutes, direction_id.

    Raises:
        ValueError: If service_date is provided but agg_active_trips table is unavailable.
    """
    con = resolve_con(con)

    if service_date:
        if not _has_active_trips_table(con):
            raise ValueError(
                f"Cannot filter by service_date '{service_date}': agg_active_trips table "
                f"not found. Run 'make frequency DATE={service_date}' to materialize active trips."
            )
        query = """
            SELECT
                st.arrival_time,
                t.direction_id
            FROM raw_gtfs_stop_times st
            JOIN agg_active_trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = $1
              AND st.stop_id = $2
              AND t.service_date = $3
            ORDER BY t.direction_id, st.arrival_time
        """
        df = con.execute(query, [route_id, stop_id, service_date]).fetchdf()
    else:
        query = """
            SELECT
                st.arrival_time,
                t.direction_id
            FROM raw_gtfs_stop_times st
            JOIN raw_gtfs_trips t ON st.trip_id = t.trip_id
            WHERE t.route_id = $1
              AND st.stop_id = $2
            ORDER BY t.direction_id, st.arrival_time
        """
        df = con.execute(query, [route_id, stop_id]).fetchdf()

    if df.empty:
        return pd.DataFrame(columns=["arrival_time", "headway_minutes", "direction_id"])

    df["minutes"] = df["arrival_time"].apply(gtfs_time_to_minutes)

    headways = []
    for direction in df["direction_id"].unique():
        dir_df = df[df["direction_id"] == direction].sort_values("minutes")
        dir_df = dir_df.copy()
        dir_df["headway_minutes"] = dir_df["minutes"].diff()
        headways.append(dir_df)

    result = pd.concat(headways, ignore_index=True)
    return result[["arrival_time", "headway_minutes", "direction_id"]].dropna()


def get_frequency_summary(con: DuckDBPyConnection | None = None) -> dict:
    """Get network-level frequency summary metrics.

    Args:
        con: Optional DuckDB connection.

    Returns:
        Dictionary with keys:
            total_trip_departures,
            total_routes,
            average_trips_per_route,
            peak_service_hour,
            peak_hour_trip_departures,
            midday_average_hourly_departures,
            peak_to_midday_ratio.
    """
    con = resolve_con(con)

    trips_per_hour = compute_trips_per_hour(con=con)

    if trips_per_hour.empty:
        return {
            "total_trip_departures": 0,
            "total_routes": 0,
            "average_trips_per_route": 0,
            "peak_service_hour": 0,
            "peak_hour_trip_departures": 0,
            "midday_average_hourly_departures": 0,
            "peak_to_midday_ratio": 0,
        }

    hourly_departures = trips_per_hour.groupby("service_hour")["trips_departing"].sum().reset_index()

    peak_index = hourly_departures["trips_departing"].idxmax()
    peak_service_hour = int(hourly_departures.loc[peak_index, "service_hour"])
    peak_hour_trip_departures = int(hourly_departures.loc[peak_index, "trips_departing"])

    midday_window = hourly_departures[
        (hourly_departures["service_hour"] >= 10) & (hourly_departures["service_hour"] <= 15)
    ]
    midday_average_hourly_departures = (
        float(midday_window["trips_departing"].mean()) if not midday_window.empty else 0
    )

    total_routes = trips_per_hour["route_id"].nunique()

    total_trip_departures = int(trips_per_hour["trips_departing"].sum())

    return {
        "total_trip_departures": total_trip_departures,
        "total_routes": total_routes,
        "average_trips_per_route": total_trip_departures / total_routes if total_routes > 0 else 0,
        "peak_service_hour": peak_service_hour,
        "peak_hour_trip_departures": peak_hour_trip_departures,
        "midday_average_hourly_departures": midday_average_hourly_departures,
        "peak_to_midday_ratio": (
            peak_hour_trip_departures / midday_average_hourly_departures
            if midday_average_hourly_departures > 0
            else 0
        ),
    }


def compute_route_frequency(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Compute route-level frequency metrics.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns:
            route_id,
            route_name,
            total_trip_departures,
            peak_period_trip_departures,
            peak_period_avg_headway_minutes,
            midday_avg_headway_minutes,
            service_span_hours.
    """
    con = resolve_con(con)

    query = """
        WITH trip_times AS (
            SELECT
                t.route_id,
                r.route_short_name as route_name,
                CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) as hour,
                MIN(st.departure_time) as first_departure,
                MAX(st.departure_time) as last_departure
            FROM raw_gtfs_stop_times st
            JOIN raw_gtfs_trips t ON st.trip_id = t.trip_id
            JOIN raw_gtfs_routes r ON t.route_id = r.route_id
            WHERE st.stop_sequence = 1
            GROUP BY t.route_id, r.route_short_name, t.trip_id, hour
        ),
        route_stats AS (
            SELECT
                route_id,
                route_name,
                COUNT(*) as total_trip_departures,
                SUM(CASE WHEN hour BETWEEN 7 AND 9 OR hour BETWEEN 16 AND 18 THEN 1 ELSE 0 END)
                    as peak_period_trip_departures,
                SUM(CASE WHEN hour BETWEEN 10 AND 15 THEN 1 ELSE 0 END)
                    as midday_trip_departures,
                MIN(first_departure) as first_trip,
                MAX(last_departure) as last_trip
            FROM trip_times
            GROUP BY route_id, route_name
        )
        SELECT
            route_id,
            route_name,
            total_trip_departures,
            peak_period_trip_departures,
            CASE
                WHEN peak_period_trip_departures > 0
                THEN ROUND(360.0 / peak_period_trip_departures, 1)
                ELSE NULL
            END as peak_period_avg_headway_minutes,
            CASE
                WHEN midday_trip_departures > 0
                THEN ROUND(360.0 / midday_trip_departures, 1)
                ELSE NULL
            END as midday_avg_headway_minutes,
            ROUND(
                (
                    (
                        CAST(SPLIT_PART(last_trip, ':', 1) AS INTEGER) * 60 +
                        CAST(SPLIT_PART(last_trip, ':', 2) AS INTEGER)
                    ) -
                    (
                        CAST(SPLIT_PART(first_trip, ':', 1) AS INTEGER) * 60 +
                        CAST(SPLIT_PART(first_trip, ':', 2) AS INTEGER)
                    )
                ) / 60.0,
                2
            ) as service_span_hours
        FROM route_stats
        ORDER BY total_trip_departures DESC
    """

    return con.execute(query).fetchdf()


def get_hourly_profile(
    route_id: str | None = None,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Get hourly departure profile.

    Args:
        route_id: Optional route to filter (None = all routes).
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
            FROM raw_gtfs_stop_times st
            JOIN raw_gtfs_trips t ON st.trip_id = t.trip_id
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
            FROM raw_gtfs_stop_times st
            WHERE st.stop_sequence = 1
            GROUP BY service_hour
            ORDER BY service_hour
        """
        return con.execute(query).fetchdf()


def get_route_performance(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Get route frequency combined with Open Data performance metrics.

    Uses v_route_performance view which joins:
    - GTFS routes (via ref_route_mapping)
    - Pass-up counts (agg_route_passups_summary)
    - On-time deviation (agg_route_ontime_summary)

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with columns: route_id, route_short_name, route_long_name,
        passup_count, days_with_passups, avg_deviation_seconds, ontime_measurements.
    """
    con = resolve_con(con)
    return con.execute("SELECT * FROM v_route_performance ORDER BY passup_count DESC").fetchdf()
