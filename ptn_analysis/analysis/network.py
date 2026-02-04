"""Network analysis for Cathy."""

from duckdb import DuckDBPyConnection
import pandas as pd

from ptn_analysis.data.db import query_df


def get_edges_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load network edges with trip counts as weights.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with from_stop_id, to_stop_id, trip_count (as weight), route_count.
    """
    return query_df(
        """
        SELECT from_stop_id, to_stop_id, trip_count AS weight, route_count
        FROM raw_gtfs_edges_weighted
        """,
        con,
    )


def get_stops_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop data with coordinates.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, stop_lat, stop_lon.
    """
    return query_df(
        """
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM raw_gtfs_stops
        """,
        con,
    )


def get_routes_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load route definitions.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with route_id, route_short_name, route_long_name, route_type.
    """
    return query_df(
        """
        SELECT route_id, route_short_name, route_long_name, route_type
        FROM raw_gtfs_routes
        """,
        con,
    )


# -----------------------------------------------------------------------------
# Stubs for Cathy - See GitHub issue #2 for implementation hints
# -----------------------------------------------------------------------------


def build_network_graph(con: DuckDBPyConnection | None = None):
    """Build directed weighted NetworkX graph from GTFS edges.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        NetworkX DiGraph with stops as nodes, edges weighted by trip_count.
    """
    raise NotImplementedError("Cathy: Implement this function")


def compute_degree_centrality(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Compute in/out/total degree centrality for all stops.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, in_degree, out_degree, total_degree.
    """
    raise NotImplementedError("Cathy: Implement this function")


def get_network_stats(con: DuckDBPyConnection | None = None) -> dict:
    """Get basic network statistics.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        Dictionary with node_count, edge_count, density, components, avg_degree.
    """
    raise NotImplementedError("Cathy: Implement this function")


def get_top_hubs(n: int = 20, con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Return top n hubs by total degree with coordinates.

    Args:
        n: Number of top hubs to return.
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, total_degree, stop_lat, stop_lon.
    """
    raise NotImplementedError("Cathy: Implement this function")


def get_hub_performance(top_n: int = 20, con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Get top hub stops with passenger boarding metrics.

    Args:
        top_n: Number of top hubs to include.
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, total_degree, total_boardings.
    """
    raise NotImplementedError("Cathy: Implement this function")


def compute_betweenness_centrality(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Compute betweenness centrality to identify critical transfer stops.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, betweenness.
    """
    raise NotImplementedError("Cathy: Implement this function")


def detect_communities(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Run Louvain community detection on the stop graph.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, community_id.
    """
    raise NotImplementedError("Cathy: Implement this function")
