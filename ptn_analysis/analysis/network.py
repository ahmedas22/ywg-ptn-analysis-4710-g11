"""Network graph construction and analysis."""

from __future__ import annotations

from loguru import logger
import networkx as nx
import pandas as pd

from ptn_analysis.analysis.base import AnalyzerBase
from ptn_analysis.context.db import TransitDB


class NetworkAnalyzer(AnalyzerBase):
    """Analyze stop-to-stop transit network structure.

    Args:
        city_key: City namespace.
        feed_id: Feed identifier.
        db_instance: Database handle.
    """

    def __init__(
        self,
        city_key: str,
        feed_id: str,
        db_instance: TransitDB,
    ) -> None:
        """Initialize the network analyzer.

        Args:
            city_key: City namespace.
            feed_id: Feed identifier.
            db_instance: Database handle.
        """
        super().__init__(city_key, feed_id, db_instance)
        self._graph: nx.DiGraph | None = None

    def __repr__(self) -> str:
        return (
            f"NetworkAnalyzer(city_key={self._city_key!r}, feed_id={self._feed_id!r}, "
            f"graph_loaded={self._graph is not None})"
        )

    @staticmethod
    def _empty_edges_frame() -> pd.DataFrame:
        """Return an empty edge table with the expected schema."""
        return pd.DataFrame(
            columns=[
                "feed_id",
                "from_stop_id",
                "to_stop_id",
                "travel_time_sec",
                "frequency",
            ]
        )

    @staticmethod
    def _empty_stops_frame() -> pd.DataFrame:
        """Return an empty stop table with the expected schema."""
        return pd.DataFrame(columns=["stop_id", "stop_name", "stop_lat", "stop_lon"])

    @staticmethod
    def _empty_routes_frame() -> pd.DataFrame:
        """Return an empty route table with the expected schema."""
        return pd.DataFrame(columns=["route_id", "route_short_name", "route_long_name", "route_type"])

    @staticmethod
    def _empty_degree_frame() -> pd.DataFrame:
        """Return an empty degree-centrality table with the expected schema."""
        return pd.DataFrame(
            {
                "stop_id": pd.Series(dtype="object"),
                "in_degree": pd.Series(dtype="int64"),
                "out_degree": pd.Series(dtype="int64"),
                "total_degree": pd.Series(dtype="int64"),
            }
        )

    @staticmethod
    def _empty_top_hubs_frame() -> pd.DataFrame:
        """Return an empty top-hubs table with plotting-friendly dtypes."""
        return pd.DataFrame(
            {
                "stop_id": pd.Series(dtype="object"),
                "stop_name": pd.Series(dtype="object"),
                "total_degree": pd.Series(dtype="float64"),
                "stop_lat": pd.Series(dtype="float64"),
                "stop_lon": pd.Series(dtype="float64"),
            }
        )

    @property
    def graph(self) -> nx.DiGraph:
        """Return the cached directed network graph.

        Returns:
            Directed NetworkX graph.
        """
        if self._graph is None:
            counts_table = self._table("stop_connection_counts")
            if not self._db.relation_exists(counts_table):
                logger.warning(f"Missing relation: {counts_table}. Returning an empty network graph.")
                self._graph = nx.DiGraph()
                return self._graph
            edges = self._db.query(
                f"""
                SELECT
                    from_stop_id,
                    to_stop_id,
                    frequency,
                    travel_time_sec
                FROM {counts_table}
                WHERE feed_id = :feed_id
                """,
                {"feed_id": self._feed_id},
            )
            self._graph = nx.from_pandas_edgelist(
                edges,
                source="from_stop_id",
                target="to_stop_id",
                edge_attr=["frequency", "travel_time_sec"],
                create_using=nx.DiGraph,
            )
        return self._graph

    def edges_df(self) -> pd.DataFrame:
        """Load weighted edge rows for the selected feed.

        Returns:
            DataFrame of weighted network edges.
        """
        counts_table = self._table("stop_connection_counts")
        if not self._db.relation_exists(counts_table):
            logger.warning(f"Missing relation: {counts_table}. Returning empty edge data.")
            return self._empty_edges_frame()
        return self._db.query(
            f"""
            SELECT
                feed_id,
                from_stop_id,
                to_stop_id,
                travel_time_sec,
                frequency
            FROM {counts_table}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )

    def stops_df(self) -> pd.DataFrame:
        """Load stop coordinates for the selected feed.

        Returns:
            Stop DataFrame.
        """
        stops_table = self._table("stops")
        if not self._db.relation_exists(stops_table):
            logger.warning(f"Missing relation: {stops_table}. Returning empty stop data.")
            return self._empty_stops_frame()
        return self._db.query(
            f"SELECT stop_id, stop_name, stop_lat, stop_lon FROM {stops_table} WHERE feed_id = :feed_id",
            {"feed_id": self._feed_id},
        )

    def routes_df(self) -> pd.DataFrame:
        """Load route definitions for the selected feed.

        Returns:
            Route DataFrame.
        """
        routes_table = self._table("routes")
        if not self._db.relation_exists(routes_table):
            logger.warning(f"Missing relation: {routes_table}. Returning empty route data.")
            return self._empty_routes_frame()
        return self._db.query(
            f"""
            SELECT route_id, route_short_name, route_long_name, route_type
            FROM {routes_table}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )

    def degree_centrality(self) -> pd.DataFrame:
        """Compute in-degree, out-degree, and total degree.

        # Asymmetric in/out-degree reflects directionality of stop connections;
        # total_degree is the symmetric hub-strength proxy used for ranking.

        Returns:
            Degree centrality DataFrame.
        """
        graph = self.graph
        if graph.number_of_nodes() == 0:
            return self._empty_degree_frame()
        in_degree = dict(graph.in_degree())
        out_degree = dict(graph.out_degree())
        rows: list[dict[str, int | str]] = []
        for node_id in graph.nodes:
            rows.append(
                {
                    "stop_id": node_id,
                    "in_degree": in_degree[node_id],
                    "out_degree": out_degree[node_id],
                    "total_degree": in_degree[node_id] + out_degree[node_id],
                }
            )
        return pd.DataFrame(rows)

    def betweenness_centrality(self) -> pd.DataFrame:
        """Compute betweenness centrality for the selected feed.

        # Brandes' algorithm O(VE) for unweighted graphs — efficient for
        # transit stop graphs of typical city scale (V~4K, E~10K).

        Returns:
            Betweenness centrality DataFrame.
        """
        if self.graph.number_of_nodes() == 0:
            return pd.DataFrame(columns=["stop_id", "betweenness", "stop_name"])
        centrality = nx.betweenness_centrality(self.graph)
        rows: list[dict[str, float | str]] = []
        for stop_id, value in centrality.items():
            rows.append({"stop_id": stop_id, "betweenness": value})
        frame = pd.DataFrame(rows)
        return frame.merge(self.stops_df()[["stop_id", "stop_name"]], on="stop_id", how="left")

    def detect_communities(self) -> pd.DataFrame:
        """Run Louvain community detection on the undirected stop graph.

        # Louvain method: O(n log n) modularity maximization.
        # Converts directed graph to undirected for community structure detection.

        Returns:
            Community membership DataFrame.
        """
        if self.graph.number_of_nodes() == 0:
            return pd.DataFrame(columns=["stop_id", "community_id", "stop_name"])
        communities = nx.community.louvain_communities(self.graph.to_undirected())
        rows: list[dict[str, int | str]] = []
        for community_id, nodes in enumerate(communities):
            for node_id in nodes:
                rows.append({"stop_id": node_id, "community_id": community_id})
        frame = pd.DataFrame(rows)
        return frame.merge(self.stops_df()[["stop_id", "stop_name"]], on="stop_id", how="left")

    def weighted_betweenness_centrality(self) -> pd.DataFrame:
        """Compute betweenness centrality weighted by travel time.

        Uses travel_time_sec as edge weight. Shorter travel times
        indicate stronger connections in the weighted graph.

        Returns:
            Weighted betweenness centrality DataFrame with stop_id, weighted_betweenness, stop_name.
        """
        if self.graph.number_of_nodes() == 0:
            return pd.DataFrame(columns=["stop_id", "weighted_betweenness", "stop_name"])
        centrality = nx.betweenness_centrality(self.graph, weight="travel_time_sec")
        rows: list[dict[str, float | str]] = []
        for stop_id, value in centrality.items():
            rows.append({"stop_id": stop_id, "weighted_betweenness": value})
        frame = pd.DataFrame(rows)
        return frame.merge(self.stops_df()[["stop_id", "stop_name"]], on="stop_id", how="left")

    def pagerank(self, alpha: float = 0.85) -> pd.DataFrame:
        """Compute PageRank centrality for all stops.

        Args:
            alpha: Damping factor (default 0.85).

        Returns:
            DataFrame with stop_id, stop_name, pagerank_score columns.
        """
        graph = self.graph
        if graph.number_of_nodes() == 0:
            return pd.DataFrame(columns=["stop_id", "pagerank_score"])
        pr = nx.pagerank(graph, alpha=alpha)
        frame = pd.DataFrame(list(pr.items()), columns=["stop_id", "pagerank_score"])
        frame = frame.sort_values("pagerank_score", ascending=False).reset_index(drop=True)
        return frame.merge(self.stops_df()[["stop_id", "stop_name"]], on="stop_id", how="left")

    def top_hubs(self, n: int = 20, weighted: bool = False) -> pd.DataFrame:
        """Return top hub stops by degree or weighted degree.

        Args:
            n: Number of hubs to return.
            weighted: When True, rank by weighted degree (1/travel_time_sec).

        Returns:
            Top-hub DataFrame.
        """
        if weighted:
            graph = self.graph
            if graph.number_of_nodes() == 0:
                return self._empty_top_hubs_frame()
            rows: list[dict[str, object]] = []
            for node_id in graph.nodes:
                edges = list(graph.in_edges(node_id, data=True)) + list(graph.out_edges(node_id, data=True))
                weighted_degree = sum(
                    1.0 / e[2]["travel_time_sec"]
                    for e in edges
                    if e[2].get("travel_time_sec") and e[2]["travel_time_sec"] > 0
                )
                rows.append({"stop_id": node_id, "total_degree": weighted_degree})
            degree_frame = pd.DataFrame(rows)
        else:
            degree_frame = self.degree_centrality()
        stop_frame = self.stops_df()
        if degree_frame.empty:
            return self._empty_top_hubs_frame()
        hub_frame = degree_frame.merge(stop_frame, on="stop_id", how="left")
        hub_frame = hub_frame.sort_values("total_degree", ascending=False).head(n).reset_index(drop=True)
        return hub_frame[["stop_id", "stop_name", "total_degree", "stop_lat", "stop_lon"]]

    def build_top_hub_table(self, n: int = 20) -> pd.DataFrame:
        """Build the top-hub table for the selected feed.

        Args:
            n: Number of hubs to return.

        Returns:
            Top-hub table.
        """
        return self.top_hubs(n=n)

    def build_network_metrics_table(self) -> pd.DataFrame:
        """Build one-row network metrics table for the selected feed.

        Returns:
            DataFrame with basic graph metrics including travel-time statistics.
        """
        stats = self.stats()
        counts_table = self._table("stop_connection_counts")
        if self._db.relation_exists(counts_table):
            tt_stats = self._db.query(
                f"""
                SELECT
                    AVG(travel_time_sec) AS mean_edge_travel_seconds,
                    MEDIAN(travel_time_sec) AS median_edge_travel_seconds
                FROM {counts_table}
                WHERE feed_id = :feed_id AND travel_time_sec IS NOT NULL
                """,
                {"feed_id": self._feed_id},
            )
            if not tt_stats.empty:
                stats["mean_edge_travel_seconds"] = tt_stats["mean_edge_travel_seconds"].iloc[0]
                stats["median_edge_travel_seconds"] = tt_stats["median_edge_travel_seconds"].iloc[0]
        return pd.DataFrame([{"feed_id": self._feed_id, **stats}])

    def build_network_comparison_table(self, baseline_feed_id: str) -> pd.DataFrame:
        """Build pre/post network metrics comparison.

        Args:
            baseline_feed_id: Feed identifier used as the baseline.

        Returns:
            Long-form comparison DataFrame.
        """
        comparison_analyzer = NetworkAnalyzer(self._city_key, baseline_feed_id, self._db)
        baseline_stats = comparison_analyzer.stats()
        comparison_stats = self.stats()
        rows: list[dict[str, float | str | None]] = []
        for metric_name in sorted(comparison_stats):
            baseline_value = baseline_stats.get(metric_name)
            comparison_value = comparison_stats.get(metric_name)
            absolute_change = None
            if baseline_value is not None and comparison_value is not None:
                absolute_change = comparison_value - baseline_value
            rows.append(
                {
                    "metric_name": metric_name,
                    "baseline_feed_id": baseline_feed_id,
                    "comparison_feed_id": self._feed_id,
                    "baseline_value": baseline_value,
                    "comparison_value": comparison_value,
                    "absolute_change": absolute_change,
                }
            )
        return pd.DataFrame(rows)

    def build_transfer_burden_matrix(self, top_n: int = 15, weighted: bool = False) -> pd.DataFrame:
        """Build a hub-to-hub shortest-path matrix.

        Args:
            top_n: Number of hubs to include.
            weighted: When True, compute travel-time weighted shortest paths.

        Returns:
            Long-form transfer burden DataFrame.
        """
        undirected_graph = self.graph.to_undirected()
        hub_frame = self.top_hubs(top_n)
        hub_names = dict(zip(hub_frame["stop_id"], hub_frame["stop_name"]))
        rows: list[dict[str, object]] = []
        weight_attr = "travel_time_sec" if weighted else None
        for origin_stop_id in hub_frame["stop_id"].tolist():
            for destination_stop_id in hub_frame["stop_id"].tolist():
                if origin_stop_id == destination_stop_id:
                    hop_count = 0
                    travel_seconds = 0.0
                else:
                    try:
                        hop_count = nx.shortest_path_length(undirected_graph, origin_stop_id, destination_stop_id)
                        travel_seconds = (
                            nx.shortest_path_length(undirected_graph, origin_stop_id, destination_stop_id, weight=weight_attr)
                            if weighted else None
                        )
                    except nx.NetworkXNoPath:
                        hop_count = None
                        travel_seconds = None
                row = {
                    "origin_stop_id": origin_stop_id,
                    "origin_stop_name": hub_names.get(origin_stop_id),
                    "destination_stop_id": destination_stop_id,
                    "destination_stop_name": hub_names.get(destination_stop_id),
                    "path_hop_count": hop_count,
                }
                if weighted:
                    row["path_travel_seconds"] = travel_seconds
                rows.append(row)
        return pd.DataFrame(rows)

    def build_network_communities_table(self) -> pd.DataFrame:
        """Build a canonical network community membership table.

        Returns:
            Community membership table with feed metadata.
        """
        community_table = self.detect_communities().copy()
        if community_table.empty:
            return community_table
        community_table.insert(0, "feed_id", self._feed_id)
        return community_table

    def build_network_export_tables(
        self,
        baseline_feed_id: str,
        top_n: int = 20,
    ) -> dict[str, pd.DataFrame]:
        """Build the network tables expected by PR2 notebooks.

        Args:
            baseline_feed_id: Feed identifier used as the baseline.
            top_n: Number of hubs to include.

        Returns:
            Dictionary of export-ready network tables.
        """
        network_metrics = self.build_network_comparison_table(baseline_feed_id=baseline_feed_id)
        top_hubs = self.build_top_hub_table(n=top_n).copy()
        if not top_hubs.empty:
            top_hubs.insert(0, "feed_id", self._feed_id)

        transfer_burden = self.build_transfer_burden_matrix(top_n=min(top_n, 15)).copy()
        if not transfer_burden.empty:
            transfer_burden.insert(0, "feed_id", self._feed_id)

        return {
            "network_metrics_prepost": network_metrics,
            "top_hubs_current": top_hubs,
            "hub_transfer_burden": transfer_burden,
            "network_communities_current": self.build_network_communities_table(),
        }

    def build_hub_ranking_change_table(
        self,
        baseline_feed_id: str = "avg_pre_ptn",
        top_n: int = 20,
    ) -> pd.DataFrame:
        """Compare hub rankings between baseline and current feed.

        Args:
            baseline_feed_id: Feed ID for the comparison baseline.
            top_n: Number of top hubs to include from each feed.

        Returns:
            DataFrame with stop_id, stop_name, baseline_rank, current_rank,
            rank_change, total_degree_current, total_degree_baseline.
        """
        current_hubs = self.top_hubs(n=top_n).copy()
        if current_hubs.empty:
            return pd.DataFrame()
        current_hubs["current_rank"] = range(1, len(current_hubs) + 1)
        current_hubs = current_hubs.rename(columns={"total_degree": "total_degree_current"})

        baseline = NetworkAnalyzer(self._city_key, baseline_feed_id, self._db)
        baseline_hubs = baseline.top_hubs(n=top_n).copy()
        if not baseline_hubs.empty:
            baseline_hubs["baseline_rank"] = range(1, len(baseline_hubs) + 1)
            baseline_hubs = baseline_hubs.rename(columns={"total_degree": "total_degree_baseline"})

        merged = current_hubs.merge(
            baseline_hubs[["stop_id", "baseline_rank", "total_degree_baseline"]]
            if not baseline_hubs.empty else pd.DataFrame(columns=["stop_id", "baseline_rank", "total_degree_baseline"]),
            on="stop_id",
            how="outer",
        )
        merged["rank_change"] = merged["baseline_rank"] - merged["current_rank"]
        if "stop_name" not in merged.columns:
            merged["stop_name"] = None
        return merged[
            ["stop_id", "stop_name", "baseline_rank", "current_rank",
             "rank_change", "total_degree_current", "total_degree_baseline"]
        ].sort_values("current_rank").reset_index(drop=True)

    def community_boundary_alignment(self) -> pd.DataFrame:
        """Compare Louvain communities against official neighbourhood boundaries.

        Returns:
            DataFrame with community_id, neighbourhood, overlap_ratio, stop_count.
        """
        communities = self.detect_communities()
        if communities.empty:
            return pd.DataFrame()

        # Spatial join: map each stop to its neighbourhood via ywg_stops + ywg_neighbourhoods
        stops_tbl = self._table("stops")
        nb_tbl = self._table("neighbourhoods")
        if not (self._db.relation_exists(stops_tbl) and self._db.relation_exists(nb_tbl)):
            return pd.DataFrame()

        stop_nb = self._db.query(
            f"""
            SELECT s.stop_id, n.name AS neighbourhood
            FROM {stops_tbl} s
            JOIN {nb_tbl} n
                ON ST_Contains(n.geometry, ST_Point(s.stop_lon, s.stop_lat))
            WHERE s.feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )
        if stop_nb.empty:
            return pd.DataFrame()

        merged = communities.merge(stop_nb, on="stop_id", how="left")
        merged = merged.dropna(subset=["neighbourhood"])
        if merged.empty:
            return pd.DataFrame()

        community_totals = merged.groupby("community_id")["stop_id"].count().rename("community_total")
        grouped = (
            merged.groupby(["community_id", "neighbourhood"])["stop_id"]
            .count()
            .rename("stop_count")
            .reset_index()
        )
        grouped = grouped.merge(community_totals, on="community_id")
        grouped["overlap_ratio"] = (grouped["stop_count"] / grouped["community_total"]).round(4)
        return grouped[
            ["community_id", "neighbourhood", "stop_count", "overlap_ratio"]
        ].sort_values(["community_id", "overlap_ratio"], ascending=[True, False]).reset_index(drop=True)

    def weighted_centrality_comparison(self, top_n: int = 20) -> pd.DataFrame:
        """Return top_n stops with both unweighted and weighted betweenness.

        Args:
            top_n: Number of top stops to return (by unweighted betweenness).

        Returns:
            DataFrame with stop_id, stop_name, betweenness, weighted_betweenness.
        """
        unweighted = self.betweenness_centrality()[["stop_id", "betweenness"]]
        weighted = self.weighted_betweenness_centrality()[["stop_id", "weighted_betweenness"]]
        merged = unweighted.merge(weighted, on="stop_id")
        stops = self.stops_df()[["stop_id", "stop_name"]]
        merged = merged.merge(stops, on="stop_id")
        return merged.nlargest(top_n, "betweenness")[
            ["stop_id", "stop_name", "betweenness", "weighted_betweenness"]
        ]

    def build_resilience_metrics_table(self) -> pd.DataFrame:
        """Compute network resilience metrics for the current feed.

        Returns:
            Single-row DataFrame with feed_id, num_components,
            largest_component_size, largest_component_pct, avg_shortest_path.
        """
        graph = self.graph
        if graph.number_of_nodes() == 0:
            return pd.DataFrame()

        undirected = graph.to_undirected()
        components = list(nx.connected_components(undirected))
        num_components = len(components)
        largest = max(components, key=len)
        total_nodes = graph.number_of_nodes()
        largest_pct = round(100.0 * len(largest) / total_nodes, 2)

        avg_path = None
        if len(largest) > 1:
            try:
                subgraph = undirected.subgraph(largest)
                avg_path = round(nx.average_shortest_path_length(subgraph), 4)
            except nx.NetworkXError:
                pass

        return pd.DataFrame([{
            "feed_id": self._feed_id,
            "num_components": num_components,
            "largest_component_size": len(largest),
            "largest_component_pct": largest_pct,
            "avg_shortest_path": avg_path,
        }])

    def build_critical_stops_table(self, top_n: int = 20) -> pd.DataFrame:
        """Identify critical stops whose removal most fragments the network.

        Args:
            top_n: Number of critical stops to return.

        Returns:
            DataFrame with feed_id, stop_id, stop_name, betweenness, criticality_rank.
        """
        bc = self.betweenness_centrality()
        if bc.empty:
            return pd.DataFrame()
        bc = bc.sort_values("betweenness", ascending=False).head(top_n).reset_index(drop=True)
        bc.insert(0, "feed_id", self._feed_id)
        bc["criticality_rank"] = range(1, len(bc) + 1)
        return bc[["feed_id", "stop_id", "stop_name", "betweenness", "criticality_rank"]]

    def stats(self) -> dict[str, float | int]:
        """Return basic network statistics.

        Returns:
            Dictionary of network metrics.
        """
        graph = self.graph
        node_count = graph.number_of_nodes()
        average_degree = 0.0
        if node_count > 0:
            average_degree = sum(dict(graph.degree()).values()) / node_count
        result = {
            "node_count": node_count,
            "edge_count": graph.number_of_edges(),
            "density": nx.density(graph),
            "avg_degree": average_degree,
        }
        try:
            result["clustering_coefficient"] = nx.average_clustering(
                graph.to_undirected()
            )
        except Exception:
            result["clustering_coefficient"] = 0.0
        try:
            result["assortativity"] = nx.degree_assortativity_coefficient(graph)
        except Exception:
            result["assortativity"] = 0.0
        return result
