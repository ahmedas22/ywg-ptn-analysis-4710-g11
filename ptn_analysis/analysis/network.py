"""Network analysis module for Cathy.

Provides graph construction and analysis using NetworkX.
Uses stop_connections_weighted table from DuckDB as edge source.
"""
import networkx as nx
from duckdb import DuckDBPyConnection
import pandas as pd

from ptn_analysis.data.db import query_df


def get_edges_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load network edges with trip counts as weights.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with from_stop_id, to_stop_id, weight (trip_count), route_count.
    """
    return query_df(
        """
        SELECT from_stop_id, to_stop_id, trip_count AS weight, route_count
        FROM stop_connections_weighted
        """,
        con,
    )


def get_stops_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop data with coordinates.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with stop_id, stop_name, stop_lat, stop_lon.
    """
    return query_df(
        """
        SELECT stop_id, stop_name, stop_lat, stop_lon
        FROM stops
        """,
        con,
    )


def get_routes_df(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load route definitions.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with route_id, route_short_name, route_long_name, route_type.
    """
    return query_df(
        """
        SELECT route_id, route_short_name, route_long_name, route_type
        FROM routes
        """,
        con,
    )


# =============================================================================
# STUBS FOR CATHY
# =============================================================================



"""Build directed weighted NetworkX graph from edges.
Args:
    con: Optional DuckDB connection.
Returns:
    NetworkX DiGraph with stops as nodes, edges weighted by trip_count.
"""
def build_network_graph(con: DuckDBPyConnection | None = None) -> nx.DiGraph:
    """Build directed weighted NetworkX graph from edges."""
    edges = get_edges_df(con)
    G = nx.from_pandas_edgelist(
        edges,
        source="from_stop_id",
        target="to_stop_id",
        edge_attr=["weight", "route_count"],
        create_using=nx.DiGraph,
    )
    return G



"""Compute in/out/total degree centrality for all stops.
Args:
    con: Optional DuckDB connection.
Returns:
    DataFrame with stop_id, in_degree, out_degree, total_degree.
"""
def compute_degree_centrality(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Compute in/out/total degree for all stops."""
    G = build_network_graph(con)
    data = []
    for node in G.nodes:
        in_deg = G.in_degree(node)
        out_deg = G.out_degree(node)
        data.append(
            {
                "stop_id": node,
                "in_degree": in_deg,
                "out_degree": out_deg,
                "total_degree": in_deg + out_deg,
            }
        )
    return pd.DataFrame(data)




"""Get basic network statistics.
Args:
    con: Optional DuckDB connection.
Returns:
    Dict with node_count, edge_count, density, avg_degree.
"""
def get_network_stats(con: DuckDBPyConnection | None = None) -> dict:
    """Get basic network statistics."""
    G = build_network_graph(con)
    node_count = G.number_of_nodes()
    edge_count = G.number_of_edges()
    density = nx.density(G)
    avg_degree = (
        sum(dict(G.degree()).values()) / node_count if node_count > 0 else 0
    )
    return {
        "node_count": node_count,
        "edge_count": edge_count,
        "density": density,
        "avg_degree": avg_degree,
    }



"""Return top n hubs by total degree with coordinates.
Args:
    n: Number of top hubs to return.
    con: Optional DuckDB connection.
Returns:
    DataFrame with stop_id, stop_name, total_degree, stop_lat, stop_lon.
"""
def get_top_hubs(n: int = 20, con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Return top n hubs by total degree with coordinates."""
    degrees = compute_degree_centrality(con)
    stops = get_stops_df(con)
    hubs = (
        degrees.merge(stops, on="stop_id", how="left")
        .sort_values("total_degree", ascending=False)
        .head(n)
        .reset_index(drop=True)
    )
    return hubs[
        ["stop_id", "stop_name", "total_degree", "stop_lat", "stop_lon"]
    ]


"""Get top hub stops with passenger boarding metrics.
Args:
    top_n: Number of top hubs to include.
    con: Optional DuckDB connection.
Returns:
    DataFrame with stop_id, stop_name, total_degree, total_boardings.
"""
def get_hub_performance(top_n: int = 20, con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    # Top hub stops by network degree
    hubs = get_top_hubs(top_n, con)
    # Route-level passenger boardings
    route_boardings = query_df(
        """
        SELECT
            route_number,
            AVG(CAST(average_boardings AS DOUBLE)) AS avg_boardings
        FROM passenger_counts
        GROUP BY route_number
        """,
        con,
    )
    # Routes serving each stop
    stop_routes = query_df(
        """
        SELECT DISTINCT
            st.stop_id,
            r.route_short_name AS route_number
        FROM stop_times st
        JOIN trips t ON st.trip_id = t.trip_id
        JOIN routes r ON t.route_id = r.route_id
        """,
        con,
    )
    # Combine stop → routes → boardings
    stop_boardings = (
        stop_routes
        .merge(route_boardings, on="route_number", how="left")
        .groupby("stop_id", as_index=False)
        .agg(total_boardings=("avg_boardings", "sum"))
    )
    # Merge with hub metadata
    return (
        hubs
        .merge(stop_boardings, on="stop_id", how="left")
        .fillna({"total_boardings": 0})
        .sort_values("total_boardings", ascending=False)
        .reset_index(drop=True)
    )



"""Compute betweenness centrality for critical transfer stops.
Args:
    con: Optional DuckDB connection.
Returns:
    DataFrame with stop_id, stop_name, betweenness.
"""
def compute_betweenness_centrality(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Compute betweenness centrality for critical transfer stops."""
    G = build_network_graph(con)
    stops = get_stops_df(con)
    betweenness = nx.betweenness_centrality(G)
    df = pd.DataFrame(
        [
            {"stop_id": k, "betweenness": v}
            for k, v in betweenness.items()
        ]
    )
    return df.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")



"""Run Louvain community detection on the stop graph.
Args:
    con: Optional DuckDB connection.
Returns:
    DataFrame with stop_id, stop_name, community_id.
"""
def detect_communities(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Run Louvain community detection on the stop graph."""
    G = build_network_graph(con)
    stops = get_stops_df(con)
    # Louvain requires an undirected graph
    G_undirected = G.to_undirected()
    communities = nx.community.louvain_communities(G_undirected)
    records = []
    for community_id, nodes in enumerate(communities):
        for node in nodes:
            records.append(
                {
                    "stop_id": node,
                    "community_id": community_id,
                }
            )
    df = pd.DataFrame(records)
    return df.merge(stops[["stop_id", "stop_name"]], on="stop_id", how="left")
