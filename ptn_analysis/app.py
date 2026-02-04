"""Winnipeg PTN Dashboard - Interactive transit network visualization.

Uses Kepler.gl for high-performance map rendering with multiple layers.
Requires MAPBOX_TOKEN environment variable for base maps.

Usage:
    streamlit run ptn_analysis/app.py --server.port 8501
    # Or: make dashboard
"""

from typing import Any

from keplergl import KeplerGl
import matplotlib.pyplot as plt
import streamlit as st
from streamlit_keplergl import keplergl_static

from ptn_analysis.config import DUCKDB_PATH, MAPBOX_TOKEN, WPG_BOUNDS


def ensure_database() -> bool:
    """Initialize DuckDB if missing (first-run bootstrap).

    Returns:
        True if database is ready, False if initialization failed.
    """
    if DUCKDB_PATH.exists():
        return True

    st.info("Initializing database for first run...")
    try:
        from ptn_analysis.data.make_dataset import boundaries, graph, gtfs

        with st.spinner("Building local DuckDB (first run may take a few minutes)..."):
            gtfs()
            boundaries()
            graph()
        return True
    except Exception as e:
        st.error(f"Database initialization failed: {e}")
        return False


def get_kepler_config(center_lat: float, center_lon: float, zoom: float = 11) -> dict[str, Any]:
    """Generate Kepler.gl configuration for transit visualization.

    Layers configured:
    1. Neighbourhoods (Polygon) - Filled by coverage (stops/km2)
    2. Network Edges (Line) - Thickness/Color by trip/route count
    3. Transit Stops (Point) - Radius by route_count

    Args:
        center_lat: Map center latitude.
        center_lon: Map center longitude.
        zoom: Initial zoom level.

    Returns:
        Kepler.gl configuration dictionary.
    """
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
                "styleType": "dark",
                "topLayerGroups": {},
                "visibleLayerGroups": {
                    "label": True,
                    "road": True,
                    "border": False,
                    "building": True,
                    "water": True,
                    "land": True,
                    "3d building": False,
                },
            },
            "visState": {
                "filters": [],
                "layers": [
                    {
                        "id": "neighbourhoods",
                        "type": "geojson",
                        "config": {
                            "dataId": "Neighbourhoods",
                            "label": "Neighbourhood Coverage",
                            "color": [23, 184, 190],
                            "highlightColor": [252, 242, 26, 255],
                            "columns": {"geojson": "geometry"},
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.4,
                                "strokeOpacity": 0.8,
                                "thickness": 0.5,
                                "strokeColor": [221, 178, 124],
                                "colorRange": {
                                    "name": "Global Warming",
                                    "type": "sequential",
                                    "category": "Uber",
                                    "colors": [
                                        "#5A1846",
                                        "#900C3F",
                                        "#C70039",
                                        "#E3611C",
                                        "#F1920E",
                                        "#FFC300",
                                    ],
                                },
                                "filled": True,
                                "stroked": True,
                                "enable3d": False,
                                "wireframe": False,
                            },
                            "textLabel": [
                                {
                                    "field": {"name": "neighbourhood", "type": "string"},
                                    "color": [255, 255, 255],
                                    "size": 18,
                                    "offset": [0, 0],
                                    "anchor": "middle",
                                    "alignment": "center",
                                }
                            ],
                        },
                        "visualChannels": {
                            "colorField": {"name": "stops_per_km2", "type": "real"},
                            "colorScale": "quantile",
                            "strokeColorField": None,
                            "strokeColorScale": "quantile",
                            "sizeField": None,
                            "sizeScale": "linear",
                        },
                    },
                    {
                        "id": "edges",
                        "type": "line",
                        "config": {
                            "dataId": "Network Edges",
                            "label": "Transit Network",
                            "color": [77, 193, 156],
                            "columns": {
                                "lat0": "from_lat",
                                "lng0": "from_lon",
                                "lat1": "to_lat",
                                "lng1": "to_lon",
                            },
                            "isVisible": True,
                            "visConfig": {
                                "opacity": 0.4,
                                "thickness": 1.0,
                                "colorRange": {
                                    "name": "Global Warming",
                                    "type": "sequential",
                                    "category": "Uber",
                                    "colors": [
                                        "#5A1846",
                                        "#900C3F",
                                        "#C70039",
                                        "#E3611C",
                                        "#F1920E",
                                        "#FFC300",
                                    ],
                                },
                                "sizeRange": [0, 10],
                                "targetColor": None,
                            },
                        },
                        "visualChannels": {
                            "colorField": {"name": "trip_count", "type": "integer"},
                            "colorScale": "quantize",
                            "sizeField": {"name": "route_count", "type": "integer"},
                            "sizeScale": "linear",
                        },
                    },
                    {
                        "id": "stops",
                        "type": "point",
                        "config": {
                            "dataId": "Transit Stops",
                            "label": "Stops",
                            "color": [255, 255, 255],
                            "columns": {
                                "lat": "stop_lat",
                                "lng": "stop_lon",
                                "altitude": None,
                            },
                            "isVisible": True,
                            "visConfig": {
                                "radius": 5,
                                "fixedRadius": False,
                                "opacity": 0.8,
                                "outline": False,
                                "thickness": 2,
                                "strokeColor": None,
                                "colorRange": {
                                    "name": "ColorBrewer YlGnBu-6",
                                    "type": "sequential",
                                    "category": "ColorBrewer",
                                    "colors": [
                                        "#ffffcc",
                                        "#c7e9b4",
                                        "#7fcdbb",
                                        "#41b6c4",
                                        "#2c7fb8",
                                        "#253494",
                                    ],
                                },
                                "radiusRange": [2, 20],
                                "filled": True,
                            },
                        },
                        "visualChannels": {
                            "colorField": {"name": "route_count", "type": "integer"},
                            "colorScale": "quantile",
                            "strokeColorField": None,
                            "strokeColorScale": "quantile",
                            "sizeField": {"name": "route_count", "type": "integer"},
                            "sizeScale": "linear",
                        },
                    },
                ],
                "interactionConfig": {
                    "tooltip": {
                        "fieldsToShow": {
                            "Neighbourhoods": [
                                {"name": "neighbourhood", "format": None},
                                {"name": "stops_per_km2", "format": None},
                                {"name": "stop_count", "format": None},
                            ],
                            "Network Edges": [
                                {"name": "trip_count", "format": None},
                                {"name": "route_count", "format": None},
                            ],
                            "Transit Stops": [
                                {"name": "stop_name", "format": None},
                                {"name": "route_count", "format": None},
                            ],
                        },
                        "compareMode": False,
                        "compareType": "absolute",
                        "enabled": True,
                    },
                    "brush": {"size": 0.5, "enabled": False},
                    "geocoder": {"enabled": False},
                    "coordinate": {"enabled": False},
                },
                "layerBlending": "normal",
                "splitMaps": [],
                "animationConfig": {"currentTime": None, "speed": 1},
            },
        },
    }


# Bootstrap database on first run
if not ensure_database():
    st.stop()

st.set_page_config(
    page_title="Winnipeg PTN Dashboard",
    page_icon="🚌",
    layout="wide",
)

st.title("🚌 Winnipeg Primary Transit Network")
st.markdown("**COMP 4710 Group 11** - Network Analysis Dashboard")

if not MAPBOX_TOKEN:
    st.error("MAPBOX_TOKEN is required. Set it in your `.env` file.")
    st.stop()


def _load_analysis_functions():
    """Import analysis callables lazily.

    Returns:
        Tuple of analysis function callables used by the dashboard.
    """
    from ptn_analysis.analysis import (
        get_edges_with_routes,
        get_neighbourhood_coverage,
        get_neighbourhood_geodata,
        get_stops_with_coords,
    )

    return get_stops_with_coords, get_edges_with_routes, get_neighbourhood_coverage, get_neighbourhood_geodata


def _coverage_category(stops_per_km2: float) -> str:
    """Map stop density to categorical coverage label.

    Args:
        stops_per_km2: Stop density value.

    Returns:
        Coverage category (High/Medium/Low).
    """
    if stops_per_km2 >= 5:
        return "High"
    if stops_per_km2 >= 1:
        return "Medium"
    return "Low"


@st.cache_data
def load_data():
    """Load and cache all visualization data."""
    get_stops_with_coords, get_edges_with_routes, get_neighbourhood_coverage, get_neighbourhood_geodata = (
        _load_analysis_functions()
    )
    stops = get_stops_with_coords()
    edges = get_edges_with_routes()
    coverage = get_neighbourhood_coverage()
    coverage_gdf = get_neighbourhood_geodata()

    coverage["coverage_category"] = coverage["stops_per_km2"].apply(_coverage_category)

    return stops, edges, coverage, coverage_gdf


try:
    stops, edges, coverage, coverage_gdf = load_data()
    data_loaded = True
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.info("Run `make data` to load data into local DuckDB.")
    data_loaded = False

if data_loaded:
    st.sidebar.header("Network Statistics")
    st.sidebar.metric("Transit Stops", f"{len(stops):,}")
    st.sidebar.metric("Network Edges", f"{len(edges):,}")
    st.sidebar.metric("Neighbourhoods", f"{len(coverage):,}")

    st.sidebar.subheader("Coverage Distribution")
    for cat in ["High", "Medium", "Low"]:
        count = len(coverage[coverage["coverage_category"] == cat])
        st.sidebar.write(f"{cat}: {count} neighbourhoods")

    tab1, tab2, tab3 = st.tabs(["🗺️ Map", "📊 Coverage", "📈 Network"])

    with tab1:
        st.subheader("Transit Network Map")

        config = get_kepler_config(
            center_lat=WPG_BOUNDS["center_lat"],
            center_lon=WPG_BOUNDS["center_lon"],
        )

        kepler_map = KeplerGl(height=600, config=config)

        if coverage_gdf is not None and not coverage_gdf.empty:
            kepler_map.add_data(data=coverage_gdf, name="Neighbourhoods")

        kepler_map.add_data(data=edges, name="Network Edges")

        required_columns = ["stop_id", "stop_name", "stop_lat", "stop_lon", "route_count"]
        missing_columns = [column for column in required_columns if column not in stops.columns]
        if missing_columns:
            st.error(f"Missing expected stop columns: {missing_columns}")
            st.stop()
        stops_for_kepler = stops[required_columns].copy()
        kepler_map.add_data(data=stops_for_kepler, name="Transit Stops")

        keplergl_static(kepler_map, center_map=True)

    with tab2:
        st.subheader("Neighbourhood Coverage")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Top 15 by Stop Count**")
            top_coverage = coverage.nlargest(15, "stop_count")[
                ["neighbourhood", "stop_count", "stops_per_km2", "coverage_category"]
            ]
            st.dataframe(top_coverage, use_container_width=True)

        with col2:
            st.markdown("**Coverage Statistics**")
            st.metric("Total Stops", f"{coverage['stop_count'].sum():,}")
            st.metric("Mean Stops/Neighbourhood", f"{coverage['stop_count'].mean():.1f}")
            st.metric("Median Stops/Neighbourhood", f"{coverage['stop_count'].median():.1f}")
            st.metric("Zero-Stop Areas", f"{(coverage['stop_count'] == 0).sum()}")

        st.markdown("**Stop Count Distribution**")
        fig, ax = plt.subplots(figsize=(10, 6))
        top20 = coverage.nlargest(20, "stop_count")
        colors = {"High": "green", "Medium": "orange", "Low": "red"}
        bar_colors = [colors.get(c, "gray") for c in top20["coverage_category"]]
        ax.barh(top20["neighbourhood"], top20["stop_count"], color=bar_colors)
        ax.set_xlabel("Number of Stops")
        ax.invert_yaxis()
        st.pyplot(fig)

    with tab3:
        st.subheader("Network Statistics")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Edge Statistics**")
            st.metric("Total Edges", f"{len(edges):,}")
            st.metric("Avg Trips per Edge", f"{edges['trip_count'].mean():.1f}")
            st.metric("Max Trips on Edge", f"{edges['trip_count'].max():,}")

        with col2:
            st.markdown("**Top Connections**")
            top_edges = edges.nlargest(10, "trip_count")[
                ["from_stop_id", "to_stop_id", "trip_count", "route_count"]
            ]
            st.dataframe(top_edges, use_container_width=True)

        st.markdown("**Trip Count Distribution**")
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.hist(edges["trip_count"], bins=50, edgecolor="black")
        ax.set_xlabel("Trips per Edge")
        ax.set_ylabel("Count")
        ax.set_yscale("log")
        st.pyplot(fig)

st.markdown("---")
st.markdown(
    "Data sources: [Winnipeg Transit GTFS](https://gtfs.winnipegtransit.com/) | "
    "[Winnipeg Open Data](https://data.winnipeg.ca/)"
)
