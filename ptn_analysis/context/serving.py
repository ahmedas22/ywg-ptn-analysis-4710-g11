"""Dashboard data layer — serving-DB-bound, no Streamlit dependency."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

import geopandas as gpd
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    DEFAULT_CITY_KEY,
    FEED_ID_CURRENT,
    PTN_HEADWAY_TARGETS,
    WGS84_CRS,
)
from ptn_analysis.context.db import TransitDB


class MapLoaderLike(Protocol):
    """Minimal map-loader contract injected by the app layer."""

    def load_stops(self) -> pd.DataFrame:
        """Load stop features for the dashboard map."""

    def load_connections(self) -> pd.DataFrame:
        """Load stop connection lines for the dashboard map."""

    def load_neighbourhoods(self) -> pd.DataFrame:
        """Load neighbourhood geometries for the dashboard map."""


SummaryStatsFn = Callable[..., dict[str, float | int]]


class Dashboard:
    """DB-bound data loader for the Streamlit dashboard.

    Args:
        db_instance: Serving TransitDB to query.
        city_key: City namespace prefix.
        feed_id: Primary GTFS feed identifier.
        baseline_feed_id: Baseline feed for pre/post comparisons.
    """

    def __init__(
        self,
        db_instance: TransitDB,
        city_key: str = DEFAULT_CITY_KEY,
        feed_id: str = FEED_ID_CURRENT,
        baseline_feed_id: str = "avg_pre_ptn",
    ) -> None:
        self._db = db_instance
        self._city_key = city_key
        self._feed_id = feed_id
        self._baseline_feed_id = baseline_feed_id

    def _table(self, base: str) -> str:
        return self._db.table_name(base, self._city_key)

    def _transit_table(self, base: str) -> str:
        return self._db.transit_table_name(base, self._city_key)

    def _query(self, relation: str, sql: str, params: dict | None = None) -> pd.DataFrame:
        """Query relation; return empty DataFrame if it doesn't exist."""
        if not self._db.relation_exists(relation):
            return pd.DataFrame()
        return self._db.query(sql, params or {})

    def missing_relations(self) -> list[str]:
        """Return names of required relations that are missing from the serving DB."""
        required = [
            "stops", "stop_connection_counts", "neighbourhoods",
            "neighbourhood_stop_count_density",
            "neighbourhood_stop_count_density_comparison",
            "neighbourhood_transit_access_metrics",
            "neighbourhood_jobs_access_metrics",
            "neighbourhood_jobs_access_comparison_metrics",
            "neighbourhood_priority_metrics",
            "route_schedule_metrics", "route_schedule_facts",
            "network_metrics", "top_hubs",
        ]
        return [r for base in required if not self._db.relation_exists(r := self._table(base))]

    # --- individual table loaders ---

    def load_coverage(self) -> pd.DataFrame:
        r = self._table("neighbourhood_transit_access_metrics")
        return self._query(
            r,
            f"SELECT * FROM {r} WHERE feed_id = :feed_id ORDER BY stop_count DESC",
            {"feed_id": self._feed_id},
        )

    def load_jobs_access(self) -> pd.DataFrame:
        r = self._table("neighbourhood_jobs_access_metrics")
        return self._query(
            r,
            f"SELECT * FROM {r} WHERE feed_id = :feed_id "
            f"ORDER BY jobs_access_score DESC, jobs_proxy_score DESC",
            {"feed_id": self._feed_id},
        )

    def load_jobs_access_comparison(self) -> pd.DataFrame:
        r = self._table("neighbourhood_jobs_access_comparison_metrics")
        return self._query(
            r,
            f"SELECT * FROM {r} "
            f"WHERE baseline_feed_id = :baseline AND comparison_feed_id = :feed_id "
            f"ORDER BY jobs_access_change DESC NULLS LAST, jobs_proxy_score DESC",
            {"baseline": self._baseline_feed_id, "feed_id": self._feed_id},
        )

    def load_priority_matrix(self) -> pd.DataFrame:
        r = self._table("neighbourhood_priority_metrics")
        return self._query(
            r, f"SELECT * FROM {r} WHERE feed_id = :feed_id", {"feed_id": self._feed_id}
        )

    def load_route_frequency(self) -> pd.DataFrame:
        r = self._table("route_schedule_metrics")
        return self._query(
            r,
            f"SELECT * FROM {r} WHERE feed_id = :feed_id ORDER BY route_short_name",
            {"feed_id": self._feed_id},
        )

    def load_route_facts(self) -> pd.DataFrame:
        r = self._table("route_schedule_facts")
        return self._query(
            r,
            f"SELECT * FROM {r} WHERE feed_id = :feed_id ORDER BY route_short_name",
            {"feed_id": self._feed_id},
        )

    def load_network_metrics(self) -> pd.DataFrame:
        r = self._table("network_metrics")
        return self._query(
            r, f"SELECT * FROM {r} WHERE feed_id = :feed_id", {"feed_id": self._feed_id}
        )

    def load_top_hubs(self, top_n: int = 20) -> pd.DataFrame:
        r = self._table("top_hubs")
        return self._query(
            r,
            f"SELECT * FROM {r} WHERE feed_id = :feed_id ORDER BY total_degree DESC LIMIT :n",
            {"feed_id": self._feed_id, "n": top_n},
        )

    def load_service_status(self) -> pd.DataFrame:
        r = self._transit_table("service_status")
        df = self._query(r, f"SELECT * FROM {r}")
        if not df.empty and "query_time" in df.columns:
            df = df.sort_values("query_time", ascending=False).reset_index(drop=True)
        return df

    def load_service_advisories(self) -> pd.DataFrame:
        r = self._transit_table("service_advisories")
        return self._query(r, f"SELECT * FROM {r} ORDER BY priority ASC, updated_at DESC")

    def load_trip_delay_summary(self) -> pd.DataFrame:
        r = self._transit_table("trip_delay_summary")
        return self._query(
            r,
            f"SELECT * FROM {r} ORDER BY ABS(mean_arrival_delay_seconds) DESC NULLS LAST",
        )

    def load_stop_features(self) -> pd.DataFrame:
        r = self._transit_table("stop_features")
        return self._query(
            r,
            f"SELECT stop_key, stop_number, stop_name, feature_name, feature_count "
            f"FROM {r} ORDER BY stop_number, feature_name",
        )

    def load_route_comparison(self) -> pd.DataFrame:
        """Load pre/post route schedule comparison if the relation exists."""
        r = self._table("route_schedule_comparison_metrics")
        return self._query(
            r,
            f"SELECT * FROM {r} "
            f"WHERE baseline_feed_id = :baseline AND comparison_feed_id = :feed_id "
            f"ORDER BY route_short_name",
            {"baseline": self._baseline_feed_id, "feed_id": self._feed_id},
        )

    def load_all(
        self,
        *,
        map_loader: MapLoaderLike,
        summary_stats_fn: SummaryStatsFn,
    ) -> dict[str, pd.DataFrame | dict[str, float | int]]:
        """Load all dashboard tables in one call.

        Collaborators are injected by the app layer so this module stays free
        of analysis-layer imports.
        """
        coverage = self.load_coverage()
        route_facts = self.load_route_facts()
        return {
            "summary_stats": summary_stats_fn(
                city_key=self._city_key, feed_id=self._feed_id, db_instance=self._db
            ),
            "stops": map_loader.load_stops(),
            "connections": map_loader.load_connections(),
            "neighbourhoods": map_loader.load_neighbourhoods(),
            "coverage": coverage,
            "underserved": _underserved(coverage),
            "jobs_access": self.load_jobs_access(),
            "jobs_access_comparison": self.load_jobs_access_comparison(),
            "priority_matrix": self.load_priority_matrix(),
            "route_frequency": self.load_route_frequency(),
            "route_facts": route_facts,
            "ptn_summary": _ptn_summary(route_facts),
            "network_metrics": self.load_network_metrics(),
            "top_hubs": self.load_top_hubs(),
            "service_status": self.load_service_status(),
            "service_advisories": self.load_service_advisories(),
            "trip_delay_summary": self.load_trip_delay_summary(),
            "stop_features": self.load_stop_features(),
            "route_comparison": self.load_route_comparison(),
        }


# ---------------------------------------------------------------------------
# Pure data helpers (no DB access)
# ---------------------------------------------------------------------------


def _underserved(coverage: pd.DataFrame) -> pd.DataFrame:
    if coverage.empty:
        return coverage
    cutoff = coverage["stop_density_per_km2"].quantile(0.25)
    return coverage.loc[coverage["stop_density_per_km2"] <= cutoff].sort_values(
        "stop_density_per_km2"
    )


def _ptn_summary(route_facts: pd.DataFrame) -> pd.DataFrame:
    if route_facts.empty or "ptn_tier" not in route_facts.columns:
        return pd.DataFrame()

    def meets_target(row: pd.Series) -> bool:
        targets = PTN_HEADWAY_TARGETS.get(row["ptn_tier"])
        return targets is not None and not pd.isna(row["mean_headway_minutes"]) and bool(
            row["mean_headway_minutes"] <= targets[1]
        )

    tbl = route_facts.copy()
    tbl["meets_target"] = tbl.apply(meets_target, axis=1)
    summary = (
        tbl.groupby("ptn_tier")
        .agg(
            route_count=("route_id", "count"),
            avg_headway_minutes=("mean_headway_minutes", "mean"),
            median_headway_minutes=("mean_headway_minutes", "median"),
            avg_speed_kmh=("scheduled_speed_kmh", "mean"),
            pct_meeting_target=("meets_target", "mean"),
        )
        .reset_index()
    )
    summary[["avg_headway_minutes", "median_headway_minutes", "avg_speed_kmh"]] = summary[
        ["avg_headway_minutes", "median_headway_minutes", "avg_speed_kmh"]
    ].round(1)
    summary["pct_meeting_target"] = (summary["pct_meeting_target"] * 100).round(0)
    return summary


# ---------------------------------------------------------------------------
# MapDataLoader — city/feed-bound map data queries
# ---------------------------------------------------------------------------


class MapDataLoader:
    """Map data loader — city/feed-bound, matching the analyzer pattern.

    Args:
        city_key: City namespace prefix.
        feed_id: GTFS feed identifier.
        db_instance: TransitDB to query.
    """

    def __init__(
        self,
        city_key: str,
        feed_id: str,
        db_instance: TransitDB,
    ) -> None:
        self._city_key = city_key
        self._feed_id = feed_id
        self._db = db_instance

    def _table(self, base_name: str) -> str:
        return self._db.table_name(base_name, self._city_key)

    def __repr__(self) -> str:
        return f"MapDataLoader(city_key={self._city_key!r}, feed_id={self._feed_id!r})"

    @staticmethod
    def _empty_stops_frame() -> pd.DataFrame:
        """Return an empty stop table with the expected schema."""
        return pd.DataFrame(
            {
                "stop_id": pd.Series(dtype="object"),
                "stop_name": pd.Series(dtype="object"),
                "stop_lat": pd.Series(dtype="float64"),
                "stop_lon": pd.Series(dtype="float64"),
                "frequency": pd.Series(dtype="float64"),
            }
        )

    @staticmethod
    def _empty_connections_frame() -> pd.DataFrame:
        """Return an empty connection table with the expected schema."""
        return pd.DataFrame(
            {
                "from_stop_id": pd.Series(dtype="object"),
                "to_stop_id": pd.Series(dtype="object"),
                "stop_connection_count": pd.Series(dtype="float64"),
                "from_lat": pd.Series(dtype="float64"),
                "from_lon": pd.Series(dtype="float64"),
                "to_lat": pd.Series(dtype="float64"),
                "to_lon": pd.Series(dtype="float64"),
            }
        )

    @staticmethod
    def _empty_neighbourhood_frame() -> gpd.GeoDataFrame:
        """Return an empty neighbourhood GeoDataFrame with the expected schema."""
        return gpd.GeoDataFrame(
            {
                "neighbourhood": pd.Series(dtype="object"),
                "area_km2": pd.Series(dtype="float64"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
            },
            geometry=gpd.GeoSeries([], name="geometry", crs=WGS84_CRS),
            crs=WGS84_CRS,
        )

    def load_stops(self) -> pd.DataFrame:
        """Load stop rows for map display.

        Returns:
            DataFrame with stop_id, stop_name, stop_lat, stop_lon, frequency.
        """
        stops_table = self._table("stops")
        connection_counts_table = self._table("stop_connection_counts")
        if not self._db.relation_exists(stops_table):
            logger.warning(f"Missing relation: {stops_table}. Returning empty stop map data.")
            return self._empty_stops_frame()
        if not self._db.relation_exists(connection_counts_table):
            logger.warning(
                f"Missing relation: {connection_counts_table}. Returning stop map data with zeroed metrics."
            )
            df = self._db.query(
                f"""
                SELECT stop_id, stop_name, stop_lat, stop_lon
                FROM {stops_table}
                WHERE feed_id = :feed_id
                """,
                {"feed_id": self._feed_id},
            )
            if df.empty:
                return self._empty_stops_frame()
            df["frequency"] = 0
            return df
        df = self._db.query(
            f"""
            SELECT stops.stop_id,
                   stops.stop_name,
                   stops.stop_lat,
                   stops.stop_lon,
                   COALESCE(connection_summary.frequency, 0) AS frequency
            FROM {stops_table} stops
            LEFT JOIN (
                SELECT from_stop_id,
                       SUM(frequency) AS frequency
                FROM {connection_counts_table}
                WHERE feed_id = :feed_id
                GROUP BY from_stop_id
            ) connection_summary
                ON stops.stop_id = connection_summary.from_stop_id
            WHERE stops.feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )
        return df

    def load_connections(self) -> pd.DataFrame:
        """Load stop-connection rows for line maps.

        Returns:
            DataFrame with from/to stop IDs, coordinates, connection count, and route count.
        """
        stops_table = self._table("stops")
        connection_counts_table = self._table("stop_connection_counts")
        if not self._db.relation_exists(stops_table) or not self._db.relation_exists(
            connection_counts_table
        ):
            logger.warning(
                f"Missing map connection relations for {self._city_key}/{self._feed_id}. Returning empty connection map data."
            )
            return self._empty_connections_frame()
        return self._db.query(
            f"""
            SELECT connection_counts.from_stop_id,
                   connection_counts.to_stop_id,
                   connection_counts.frequency AS stop_connection_count,
                   from_stops.stop_lat AS from_lat,
                   from_stops.stop_lon AS from_lon,
                   to_stops.stop_lat AS to_lat,
                   to_stops.stop_lon AS to_lon
            FROM {connection_counts_table} connection_counts
            JOIN {stops_table} from_stops
                ON connection_counts.from_stop_id = from_stops.stop_id
               AND from_stops.feed_id = :feed_id
            JOIN {stops_table} to_stops
                ON connection_counts.to_stop_id = to_stops.stop_id
               AND to_stops.feed_id = :feed_id
            WHERE connection_counts.feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )

    def load_neighbourhoods(self) -> gpd.GeoDataFrame:
        """Load neighbourhood geometry joined to stop-density metrics.

        Returns:
            GeoDataFrame with neighbourhood name, area, stop count, density, and geometry.

        Raises:
            FileNotFoundError: If required relations are missing.
            RuntimeError: If the neighbourhood geometry cannot be decoded.
        """
        neighbourhoods_table = self._table("neighbourhoods")
        density_table = self._table("neighbourhood_stop_count_density")
        if not self._db.relation_exists(neighbourhoods_table):
            logger.warning(
                f"Missing relation: {neighbourhoods_table}. Returning empty neighbourhood map data."
            )
            return self._empty_neighbourhood_frame()
        try:
            if self._db.relation_exists(density_table):
                gdf = self._db.query(
                    f"""
                    SELECT neighbourhoods.name AS neighbourhood,
                           neighbourhoods.area_km2,
                           density.stop_count,
                           density.stop_density_per_km2,
                           ST_AsWKB(neighbourhoods.geometry::GEOMETRY) AS geometry
                    FROM {neighbourhoods_table} neighbourhoods
                    LEFT JOIN {density_table} density
                        ON neighbourhoods.id = density.neighbourhood_id
                       AND density.feed_id = :feed_id
                    ORDER BY neighbourhoods.name
                    """,
                    {"feed_id": self._feed_id},
                    geo=True,
                )
            else:
                logger.warning(
                    f"Missing relation: {density_table}. Returning neighbourhood geometry without density metrics."
                )
                gdf = self._db.query(
                    f"""
                    SELECT neighbourhoods.name AS neighbourhood,
                           neighbourhoods.area_km2,
                           0::DOUBLE AS stop_count,
                           0::DOUBLE AS stop_density_per_km2,
                           ST_AsWKB(neighbourhoods.geometry) AS geometry
                    FROM {neighbourhoods_table} neighbourhoods
                    ORDER BY neighbourhoods.name
                    """,
                    geo=True,
                )
            gdf[["stop_count", "stop_density_per_km2"]] = gdf[
                ["stop_count", "stop_density_per_km2"]
            ].fillna(0)
            return gdf
        except ValueError as exc:
            logger.error(f"Could not decode neighbourhood map data: {exc}")
            raise RuntimeError("Neighbourhood map data could not be decoded.") from exc

