"""Visualization module for Stephenie.

Provides data loaders for Kepler.gl maps (dashboard) and Folium maps (notebooks).
"""

from typing import Any

from duckdb import DuckDBPyConnection
import folium
import geopandas as gpd
from loguru import logger
import pandas as pd

from ptn_analysis.analysis.coverage import get_stops_per_neighbourhood
from ptn_analysis.config import DATASETS, WPG_BOUNDS, WPG_OPEN_DATA_URL
from ptn_analysis.data.db import query_df


def get_kepler_config(
    center_lat: float | None = None,
    center_lon: float | None = None,
    zoom: float = 11,
) -> dict[str, Any]:
    """Generate Kepler.gl configuration centered on Winnipeg.

    Used by app.py dashboard.

    Args:
        center_lat: Map center latitude. Defaults to Winnipeg center.
        center_lon: Map center longitude. Defaults to Winnipeg center.
        zoom: Initial zoom level.

    Returns:
        Kepler.gl configuration dictionary.
    """
    if center_lat is None:
        center_lat = WPG_BOUNDS["center_lat"]
    if center_lon is None:
        center_lon = WPG_BOUNDS["center_lon"]

    return {
        "version": "v1",
        "config": {
            "mapState": {
                "latitude": center_lat,
                "longitude": center_lon,
                "zoom": zoom,
                "pitch": 0,
                "bearing": 0,
            },
            "mapStyle": {
                "styleType": "light",
                "visibleLayerGroups": {
                    "label": True,
                    "road": True,
                    "building": True,
                    "water": True,
                    "land": True,
                },
            },
        },
    }


def get_folium_map(
    center_lat: float | None = None,
    center_lon: float | None = None,
    zoom: int = 12,
    tiles: str = "CartoDB positron",
) -> folium.Map:
    """Create a Folium map centered on Winnipeg.

    Used by Jupyter notebooks for open-source basemaps.

    Args:
        center_lat: Map center latitude. Defaults to Winnipeg center.
        center_lon: Map center longitude. Defaults to Winnipeg center.
        zoom: Initial zoom level.
        tiles: Tile provider (CartoDB positron, OpenStreetMap, etc).

    Returns:
        Folium Map object.
    """
    if center_lat is None:
        center_lat = WPG_BOUNDS["center_lat"]
    if center_lon is None:
        center_lon = WPG_BOUNDS["center_lon"]

    return folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles=tiles)


def get_neighbourhood_coverage(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load neighbourhood coverage metrics.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with neighbourhood, area_km2, stop_count, stops_per_km2.
    """
    return get_stops_per_neighbourhood(con)


def get_stops_with_coords(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stops with coordinates and route counts.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with stop_id, stop_name, stop_lat, stop_lon, route_count.
    """
    return query_df(
        """
        SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon,
               COALESCE(e.route_count, 0) AS route_count
        FROM stops s
        LEFT JOIN (
            SELECT from_stop_id, SUM(route_count) AS route_count
            FROM stop_connections_weighted
            GROUP BY from_stop_id
        ) e ON s.stop_id = e.from_stop_id
        """,
        con,
    )


def get_edges_with_routes(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load edges with stop coordinates for line visualization.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with from/to stop IDs, lat/lon, trip_count, route_count.
    """
    return query_df(
        """
        SELECT
            e.from_stop_id,
            e.to_stop_id,
            s1.stop_lat AS from_lat,
            s1.stop_lon AS from_lon,
            s2.stop_lat AS to_lat,
            s2.stop_lon AS to_lon,
            e.trip_count,
            e.route_count
        FROM stop_connections_weighted e
        JOIN stops s1 ON e.from_stop_id = s1.stop_id
        JOIN stops s2 ON e.to_stop_id = s2.stop_id
        """,
        con,
    )


def get_neighbourhood_geodata(con: DuckDBPyConnection | None = None) -> gpd.GeoDataFrame:
    """Load neighbourhood geometries with coverage metrics.

    Args:
        con: Optional DuckDB connection.

    Returns:
        GeoDataFrame with geometry and coverage columns.
    """
    stats = get_neighbourhood_coverage(con).copy()

    dataset_id = DATASETS["neighbourhoods"]
    url = f"{WPG_OPEN_DATA_URL}/api/v3/views/{dataset_id}/query.geojson"

    try:
        logger.info(f"Fetching neighbourhood geometry from {url}")
        gdf = gpd.read_file(url)

        name_col = next((c for c in gdf.columns if c.lower() == "name"), None)
        if not name_col:
            logger.warning("Could not find name column in neighbourhood GeoJSON")
            return gpd.GeoDataFrame(stats)

        gdf = gdf.rename(columns={name_col: "neighbourhood"})
        gdf["match_name"] = gdf["neighbourhood"].str.upper()
        stats["match_name"] = stats["neighbourhood"].str.upper()

        merged = gdf.merge(
            stats[["match_name", "stop_count", "stops_per_km2"]],
            on="match_name",
            how="left",
        )

        merged["stop_count"] = merged["stop_count"].fillna(0)
        merged["stops_per_km2"] = merged["stops_per_km2"].fillna(0)
        merged = merged.drop(columns=["match_name"])

        return merged

    except Exception as e:
        logger.error(f"Error loading neighbourhood geometry: {e}")
        return gpd.GeoDataFrame()


# =============================================================================
# STUBS FOR STEPHENIE
# =============================================================================


def create_coverage_bar_chart(
    top_n: int = 20,
    output_path: str = "reports/figures/coverage_bar.png",
) -> None:
    """Save top-N neighbourhood stop-count bar chart.

    Args:
        top_n: Number of neighbourhoods to include.
        output_path: Path for PNG output.
    """
    raise NotImplementedError("Stephenie: implement using matplotlib")


def create_coverage_distribution_plot(
    output_path: str = "reports/figures/coverage_dist.png",
) -> None:
    """Save histogram of neighbourhood stop counts.

    Args:
        output_path: Path for PNG output.
    """
    raise NotImplementedError("Stephenie: implement using matplotlib")


def export_summary_stats(con: DuckDBPyConnection | None = None) -> dict:
    """Return report-ready summary statistics.

    Args:
        con: Optional DuckDB connection.

    Returns:
        Dict with network, feed period, and coverage metrics.
    """
    raise NotImplementedError("Stephenie: implement")


def create_route_performance_chart(
    top_n: int = 20,
    output_path: str = "reports/figures/route_performance.png",
) -> None:
    """Save chart comparing pass-up counts and route performance.

    Args:
        top_n: Number of routes to include.
        output_path: Path for PNG output.
    """
    raise NotImplementedError("Stephenie: implement")


def create_unified_map(
    layers: list[str] | None = None,
    output_path: str = "reports/figures/transit_map.html",
) -> None:
    """Build unified Kepler.gl map with selected layers.

    Args:
        layers: Optional layer names to include.
        output_path: Path for HTML output.
    """
    raise NotImplementedError("Stephenie: implement using keplergl")
