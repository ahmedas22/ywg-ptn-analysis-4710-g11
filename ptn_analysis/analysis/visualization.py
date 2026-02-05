"""Visualization helpers for Stephenie."""

from duckdb import DuckDBPyConnection
import geopandas as gpd
from loguru import logger
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt
from ptn_analysis.config import DATASETS, WPG_OPEN_DATA_URL
from ptn_analysis.data.db import query_df


def get_neighbourhood_coverage(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load neighbourhood coverage metrics.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with neighbourhood, area_km2, stop_count, stops_per_km2.
    """
    return query_df(
        """
        SELECT neighbourhood, area_km2, stop_count, stops_per_km2
        FROM agg_stops_per_neighbourhood
        ORDER BY stop_count DESC
        """,
        con,
    )


def get_stops_with_coords(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stops with coordinates and route counts.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with stop_id, stop_name, stop_lat, stop_lon, route_count.
    """
    return query_df(
        """
        SELECT s.stop_id, s.stop_name, s.stop_lat, s.stop_lon,
               COALESCE(e.route_count, 0) AS route_count
        FROM raw_gtfs_stops s
        LEFT JOIN (
            SELECT from_stop_id, SUM(route_count) AS route_count
            FROM raw_gtfs_edges_weighted
            GROUP BY from_stop_id
        ) e ON s.stop_id = e.from_stop_id
        """,
        con,
    )


def get_edges_with_routes(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load edges with stop coordinates.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with from/to stop IDs, from/to lat/lon, trip_count, route_count.
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
        FROM raw_gtfs_edges_weighted e
        JOIN raw_gtfs_stops s1 ON e.from_stop_id = s1.stop_id
        JOIN raw_gtfs_stops s2 ON e.to_stop_id = s2.stop_id
        """,
        con,
    )


def get_neighbourhood_geodata(con: DuckDBPyConnection | None = None) -> gpd.GeoDataFrame:
    """Load neighbourhood geometries and join coverage metrics.

    Args:
        con: Optional DuckDB connection. Uses default if None.

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
            logger.warning(
                "Could not find name column in neighbourhood GeoJSON")
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


def create_coverage_bar_chart(
    top_n: int = 20,
    output_path: str = "reports/figures/coverage_bar.png",
) -> None:
    """Save top-N neighbourhood stop-count bar chart."""
    df = get_neighbourhood_coverage().copy()
    if df.empty:
        logger.warning(
            "No neighbourhood coverage data available for bar chart.")
        return

    df = df.sort_values("stop_count", ascending=False).head(top_n)

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(9, 6))
    plt.barh(df["neighbourhood"], df["stop_count"])
    plt.gca().invert_yaxis()
    plt.title(f"Top {top_n} Neighbourhoods by Stop Count")
    plt.xlabel("Stop Count")
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()

    logger.info(f"Saved coverage bar chart to {out}")


def create_coverage_distribution_plot(
    output_path: str = "reports/figures/coverage_dist.png",
) -> None:
    """Save histogram of neighbourhood stop counts."""
    df = get_neighbourhood_coverage().copy()
    if df.empty:
        logger.warning(
            "No neighbourhood coverage data available for distribution plot.")
        return

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    plt.figure(figsize=(8, 5))
    plt.hist(df["stop_count"], bins=20)
    plt.title("Distribution of Stop Counts Across Neighbourhoods")
    plt.xlabel("Stop Count")
    plt.ylabel("Number of Neighbourhoods")
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()

    logger.info(f"Saved coverage distribution plot to {out}")


def export_summary_stats(con: DuckDBPyConnection | None = None) -> dict:
    """Return report-ready summary statistics."""
    # Basic network stats
    stops_n = query_df("SELECT COUNT(*) AS n FROM raw_gtfs_stops", con)["n"][0]
    edges_n = query_df(
        "SELECT COUNT(*) AS n FROM raw_gtfs_edges_weighted", con)["n"][0]

    # Coverage stats
    cov = get_neighbourhood_coverage(con)
    if cov.empty:
        cov_stats = {
            "neighbourhoods_n": 0,
            "total_stops_in_neighbourhoods": 0,
            "stops_per_km2_min": None,
            "stops_per_km2_median": None,
            "stops_per_km2_max": None,
        }
    else:
        cov_stats = {
            "neighbourhoods_n": int(cov["neighbourhood"].nunique()),
            "total_stops_in_neighbourhoods": int(cov["stop_count"].sum()),
            "stops_per_km2_min": float(cov["stops_per_km2"].min()),
            "stops_per_km2_median": float(cov["stops_per_km2"].median()),
            "stops_per_km2_max": float(cov["stops_per_km2"].max()),
        }

    stats = {
        "num_stops": int(stops_n),
        "num_edges": int(edges_n),
        **cov_stats,
    }

    return stats


def create_route_performance_chart(
    top_n: int = 20,
    output_path: str = "reports/figures/route_performance.png",
) -> None:
    """Save chart comparing pass-up counts and route performance."""
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # View was provided in your spec
    df = query_df(
        f"""
        SELECT route_short_name, passup_count, avg_deviation_seconds
        FROM v_route_performance
        WHERE passup_count > 0
        ORDER BY passup_count DESC
        LIMIT {int(top_n)}
        """
    )

    if df.empty:
        logger.warning(
            "No route performance rows (passup_count > 0); skipping chart.")
        return

    # Sort for clean horizontal bars
    df = df.sort_values("passup_count", ascending=True)

    fig, ax1 = plt.subplots(figsize=(10, 6))
    ax1.barh(df["route_short_name"], df["passup_count"])
    ax1.set_xlabel("Pass-up count")
    ax1.set_ylabel("Route")

    # Second axis for deviation (optional, but useful)
    ax2 = ax1.twiny()
    ax2.plot(df["avg_deviation_seconds"], df["route_short_name"], marker="o")
    ax2.set_xlabel("Avg deviation (seconds)")

    plt.title(f"Top {len(df)} Routes by Pass-ups (with Avg Deviation)")
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()

    logger.info(f"Saved route performance chart to {out}")


def create_unified_map(
    layers: list[str] | None = None,
    output_path: str = "reports/figures/transit_map.html",
) -> None:
    """Build unified map with selected layers and save to HTML.

    Uses Kepler.gl if installed; falls back to Folium if not.
    """
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    if layers is None:
        layers = ["stops", "edges", "coverage"]

    stops = get_stops_with_coords() if "stops" in layers else pd.DataFrame()
    edges = get_edges_with_routes() if "edges" in layers else pd.DataFrame()
    gdf = get_neighbourhood_geodata() if "coverage" in layers else None


    try:
        from keplergl import KeplerGl
        from ptn_analysis.config import WPG_BOUNDS
        from ptn_analysis.app import get_kepler_config

        config = get_kepler_config(
            WPG_BOUNDS["center_lat"], WPG_BOUNDS["center_lon"])
        m = KeplerGl(height=700, config=config)

        if not stops.empty:
            m.add_data(data=stops, name="stops")

        if not edges.empty:
            m.add_data(data=edges, name="edges")

        if gdf is not None and hasattr(gdf, "geometry") and not gdf.empty:
            # Kepler works well with GeoJSON strings
            m.add_data(data=gdf.to_json(), name="coverage")

        m.save_to_html(file_name=str(out), read_only=True)
        logger.info(f"Saved unified Kepler map to {out}")
        return

    except Exception as e:
        logger.warning(
            f"Kepler.gl unavailable or failed ({e}); falling back to Folium.")

    # ---- Folium fallback (still interactive + saved HTML) ----
    import folium

    # Winnipeg center (fallback)
    center_lat, center_lon = 49.8951, -97.1384
    fmap = folium.Map(location=[center_lat, center_lon],
                      zoom_start=11, tiles="CartoDB positron")

    # Stops layer
    if not stops.empty and {"stop_lat", "stop_lon"}.issubset(stops.columns):
        fg_stops = folium.FeatureGroup(name="Stops", show=True)
        for _, r in stops.iterrows():
            folium.CircleMarker(
                location=[r["stop_lat"], r["stop_lon"]],
                radius=3,
                fill=True,
                fill_opacity=0.7,
                popup=f"{r.get('stop_name', '')} ({r.get('stop_id', '')})",
            ).add_to(fg_stops)
        fg_stops.add_to(fmap)

    # Edges layer
    if not edges.empty and {"from_lat", "from_lon", "to_lat", "to_lon"}.issubset(edges.columns):
        fg_edges = folium.FeatureGroup(name="Edges", show=True)
        for _, r in edges.iterrows():
            folium.PolyLine(
                locations=[[r["from_lat"], r["from_lon"]],
                           [r["to_lat"], r["to_lon"]]],
                weight=1,
                opacity=0.35,
            ).add_to(fg_edges)
        fg_edges.add_to(fmap)

    # Coverage layer
    if gdf is not None and hasattr(gdf, "geometry") and not gdf.empty:
        fg_cov = folium.FeatureGroup(name="Coverage", show=True)

        tooltip_fields = []
        aliases = []
        for f, a in [
            ("neighbourhood", "Neighbourhood"),
            ("stop_count", "Stop count"),
            ("stops_per_km2", "Stops/km²"),
        ]:
            if f in gdf.columns:
                tooltip_fields.append(f)
                aliases.append(a)

        folium.GeoJson(
            gdf,
            name="Coverage",
            tooltip=folium.GeoJsonTooltip(
                fields=tooltip_fields, aliases=aliases, localize=True),
        ).add_to(fg_cov)

        fg_cov.add_to(fmap)

    folium.LayerControl(collapsed=False).add_to(fmap)
    fmap.save(str(out))
    logger.info(f"Saved unified Folium map to {out}")
