"""Routing infrastructure — r5py, city2graph, and osmnx pipeline steps.

Heavy computation lives here; analysis modules read precomputed tables.
All functions take explicit parameters (no singletons, no module state).
"""

from __future__ import annotations

import datetime
from pathlib import Path

import geopandas as gpd
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    JOBS_ACCESS_MAX_TRAVEL_MINUTES,
    OSM_PBF_PATH,
    OSM_PBF_URL,
    R5_DEPARTURE_TIME,
    R5_DEPARTURE_WINDOW_MINUTES,
    R5_ISOCHRONE_MINUTES,
    R5_PERCENTILES,
    ROUTING_CACHE_DIR,
    WGS84_CRS,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.http import Downloader


# ---------------------------------------------------------------------------
# OSM PBF download
# ---------------------------------------------------------------------------


def download_osm_pbf(
    url: str = OSM_PBF_URL,
    dest_path: Path = OSM_PBF_PATH,
    force_refresh: bool = False,
) -> Path:
    """Download the Manitoba OSM PBF extract from Geofabrik.

    Args:
        url: Geofabrik download URL.
        dest_path: Local destination path.
        force_refresh: Re-download even if the file exists.

    Returns:
        Path to the local PBF file.
    """
    if dest_path.exists() and not force_refresh:
        logger.info(f"OSM PBF already cached at {dest_path}")
        return dest_path

    logger.info(f"Downloading OSM PBF from {url}")
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    downloader = Downloader()
    downloader.request(
        url,
        cache_path=dest_path,
        response_format="bytes",
        force_refresh=force_refresh,
        timeout=600.0,
    )
    logger.info(f"OSM PBF saved to {dest_path} ({dest_path.stat().st_size / 1e6:.1f} MB)")
    return dest_path


# ---------------------------------------------------------------------------
# r5py transport network
# ---------------------------------------------------------------------------


def build_transport_network(osm_path: Path, gtfs_paths: list[Path]):
    """Build an r5py TransportNetwork from OSM + GTFS.

    Args:
        osm_path: Path to the OSM PBF file.
        gtfs_paths: Paths to GTFS zip files.

    Returns:
        r5py.TransportNetwork instance.
    """
    import r5py

    logger.info(f"Building r5py TransportNetwork: osm={osm_path.name}, gtfs={len(gtfs_paths)} feeds")
    network = r5py.TransportNetwork(str(osm_path), [str(p) for p in gtfs_paths])
    return network


# ---------------------------------------------------------------------------
# Travel time matrices
# ---------------------------------------------------------------------------


def build_travel_time_matrix(
    network,
    origins: gpd.GeoDataFrame,
    destinations: gpd.GeoDataFrame,
    modes: list[str],
    departure_date: str,
    departure_time: str = R5_DEPARTURE_TIME,
    max_minutes: int = JOBS_ACCESS_MAX_TRAVEL_MINUTES,
    percentiles: tuple[int, ...] = R5_PERCENTILES,
    departure_window_minutes: int = R5_DEPARTURE_WINDOW_MINUTES,
) -> pd.DataFrame:
    """Compute a travel time matrix using r5py.

    Args:
        network: r5py TransportNetwork.
        origins: GeoDataFrame with point geometry and ``id`` column.
        destinations: GeoDataFrame with point geometry and ``id`` column.
        modes: Transport modes (e.g. ``["WALK"]`` or ``["TRANSIT", "WALK"]``).
        departure_date: Date string ``YYYY-MM-DD``.
        departure_time: Time string ``HH:MM:SS``.
        max_minutes: Maximum travel time cutoff.
        percentiles: Travel time percentiles to compute.
        departure_window_minutes: Departure time window for percentile spread.

    Returns:
        DataFrame with ``from_id``, ``to_id``, and travel time columns.
    """
    import r5py

    transport_modes = [getattr(r5py.TransportMode, m) for m in modes]
    departure = datetime.datetime.fromisoformat(f"{departure_date}T{departure_time}")

    logger.info(
        f"Computing travel time matrix: {len(origins)} origins x {len(destinations)} destinations, "
        f"modes={modes}, max={max_minutes}min"
    )
    matrix = r5py.TravelTimeMatrix(
        network,
        origins=origins,
        destinations=destinations,
        transport_modes=transport_modes,
        departure=departure,
        departure_time_window=datetime.timedelta(minutes=departure_window_minutes),
        max_time=datetime.timedelta(minutes=max_minutes),
        percentiles=list(percentiles),
    )
    # r5py >=1.1: TravelTimeMatrix is a DataFrame subclass
    return pd.DataFrame(matrix)


# ---------------------------------------------------------------------------
# Isochrones
# ---------------------------------------------------------------------------


def _make_city_grid(
    bounds: dict,
    resolution_m: int = 500,
) -> gpd.GeoDataFrame:
    """Create a regular point grid clipped to city bounds.

    Args:
        bounds: Dict with ``min_lat``, ``max_lat``, ``min_lon``, ``max_lon``,
            ``center_lat`` keys (e.g. ``WPG_BOUNDS``).
        resolution_m: Grid spacing in metres.

    Returns:
        GeoDataFrame with ``id`` column and point geometry in WGS84.
    """
    import numpy as np
    from shapely.geometry import Point

    # Convert metres to degrees (approximate)
    lat_step = resolution_m / 111_000
    lon_step = resolution_m / (111_000 * np.cos(np.radians(bounds["center_lat"])))

    lats = np.arange(bounds["min_lat"], bounds["max_lat"], lat_step)
    lons = np.arange(bounds["min_lon"], bounds["max_lon"], lon_step)

    points = []
    for i, lat in enumerate(lats):
        for j, lon in enumerate(lons):
            points.append({"id": f"g_{i}_{j}", "geometry": Point(lon, lat)})

    gdf = gpd.GeoDataFrame(points, crs=WGS84_CRS)
    logger.info(f"City grid: {len(gdf)} points ({len(lats)}x{len(lons)}) at {resolution_m}m")
    return gdf


def build_isochrones(
    network,
    origins: gpd.GeoDataFrame,
    modes: list[str],
    departure_date: str,
    departure_time: str = R5_DEPARTURE_TIME,
    cutoffs: list[int] | None = None,
    bounds: dict | None = None,
    grid_resolution_m: int = 500,
) -> gpd.GeoDataFrame:
    """Compute isochrone polygons clipped to city bounds.

    When *bounds* is provided the function builds a local point grid
    (instead of letting r5py grid the full OSM extent), computes a
    travel-time matrix to those grid points, and constructs convex-hull
    polygons from the reachable set for each origin / cutoff pair.

    Args:
        network: r5py TransportNetwork.
        origins: GeoDataFrame with point geometry and ``id`` column.
        modes: Transport modes.
        departure_date: Date string ``YYYY-MM-DD``.
        departure_time: Time string ``HH:MM:SS``.
        cutoffs: Isochrone cutoff minutes.
        bounds: City bounding box dict (e.g. ``WPG_BOUNDS``).  When
            ``None`` falls back to the raw r5py Isochrones (slow on
            large PBF files).
        grid_resolution_m: Grid spacing in metres (default 500).

    Returns:
        GeoDataFrame with ``id``, ``travel_time``, and polygon geometry.
    """
    if cutoffs is None:
        cutoffs = R5_ISOCHRONE_MINUTES

    # --- fast path: bounded grid + TravelTimeMatrix + convex hull ---
    if bounds is not None:
        grid_gdf = _make_city_grid(bounds, resolution_m=grid_resolution_m)
        max_cutoff = max(cutoffs)

        logger.info(
            f"Computing clipped isochrones: {len(origins)} origins, "
            f"{len(grid_gdf)} grid pts, cutoffs={cutoffs}"
        )
        matrix = build_travel_time_matrix(
            network,
            origins=origins,
            destinations=grid_gdf,
            modes=modes,
            departure_date=departure_date,
            departure_time=departure_time,
            max_minutes=max_cutoff,
            percentiles=(50,),
        )

        # r5py names the column travel_time (1 percentile) or travel_time_p50 etc.
        tt_col = next(
            (c for c in matrix.columns if c.startswith("travel_time")),
            None,
        )
        if tt_col is None:
            logger.warning("No travel_time column in matrix — skipping isochrones")
            return _empty_isochrone()

        rows: list[dict] = []
        for origin_id in origins["id"]:
            origin_rows = matrix[matrix["from_id"] == origin_id]
            for cutoff in cutoffs:
                reachable_ids = origin_rows.loc[
                    origin_rows[tt_col] <= cutoff, "to_id"
                ]
                reachable_pts = grid_gdf[grid_gdf["id"].isin(reachable_ids)]
                if len(reachable_pts) >= 3:
                    hull = reachable_pts.union_all().convex_hull
                    rows.append(
                        {"id": origin_id, "travel_time": cutoff, "geometry": hull}
                    )

        if not rows:
            return _empty_isochrone()
        return gpd.GeoDataFrame(rows, crs=WGS84_CRS)

    # --- fallback: raw r5py Isochrones (grids full PBF extent) ---
    import r5py

    transport_modes = [getattr(r5py.TransportMode, m) for m in modes]
    departure = datetime.datetime.fromisoformat(f"{departure_date}T{departure_time}")

    logger.info(
        f"Computing isochrones (unclipped): {len(origins)} origins, cutoffs={cutoffs}"
    )
    isochrones = r5py.Isochrones(
        network,
        origins=origins,
        transport_modes=transport_modes,
        departure=departure,
        isochrones=cutoffs,
    )
    return gpd.GeoDataFrame(isochrones)


# ---------------------------------------------------------------------------
# city2graph edges + graph construction
# ---------------------------------------------------------------------------


def build_city2graph_edges(
    gtfs_path: Path,
    calendar_start: str,
    calendar_end: str,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build transit edges from GTFS using city2graph.

    Args:
        gtfs_path: Path to GTFS zip.
        calendar_start: Calendar start date ``YYYYMMDD``.
        calendar_end: Calendar end date ``YYYYMMDD``.

    Returns:
        Tuple of (nodes_gdf, edges_gdf).
    """
    import city2graph as c2g

    logger.info(f"Building city2graph transit graph: {gtfs_path.name} [{calendar_start}..{calendar_end}]")
    gtfs = c2g.load_gtfs(str(gtfs_path))
    nodes_gdf, edges_gdf = c2g.travel_summary_graph(
        gtfs, calendar_start=calendar_start, calendar_end=calendar_end,
    )
    logger.info(f"city2graph: {len(nodes_gdf)} nodes, {len(edges_gdf)} edges")
    return nodes_gdf, edges_gdf


def build_contiguity_graph(
    neighbourhoods: gpd.GeoDataFrame,
    contiguity: str = "queen",
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build neighbourhood contiguity graph using city2graph.

    Args:
        neighbourhoods: Neighbourhood polygons GeoDataFrame.
        contiguity: Contiguity type (``"queen"`` or ``"rook"``).

    Returns:
        Tuple of (nodes_gdf, edges_gdf).
    """
    import city2graph as c2g

    logger.info(f"Building {contiguity} contiguity graph for {len(neighbourhoods)} neighbourhoods")
    nodes_gdf, edges_gdf = c2g.contiguity_graph(neighbourhoods, contiguity=contiguity)
    return nodes_gdf, edges_gdf


def build_bridge_nodes(
    neighbourhood_nodes: gpd.GeoDataFrame,
    stop_nodes: gpd.GeoDataFrame,
    k: int = 5,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Build stop-to-neighbourhood bridge edges using city2graph.

    Args:
        neighbourhood_nodes: Neighbourhood node GeoDataFrame.
        stop_nodes: Stop node GeoDataFrame.
        k: Number of nearest neighbours for KNN bridging.

    Returns:
        Tuple of (bridge_nodes_gdf, bridge_edges_gdf).
    """
    import city2graph as c2g

    logger.info(f"Building bridge nodes: {len(stop_nodes)} stops -> {len(neighbourhood_nodes)} neighbourhoods")
    bridge_nodes, bridge_edges = c2g.bridge_nodes(
        {"neighbourhood": neighbourhood_nodes, "stop": stop_nodes},
        proximity_method="knn",
        k=k,
    )
    return bridge_nodes, bridge_edges


# ---------------------------------------------------------------------------
# osmnx helpers
# ---------------------------------------------------------------------------


def download_building_footprints(place: str = "Winnipeg, Canada") -> gpd.GeoDataFrame:
    """Download building footprints from OSM via osmnx.

    Args:
        place: Geocodable place name.

    Returns:
        GeoDataFrame of building footprint polygons.
    """
    import osmnx as ox

    logger.info(f"Downloading building footprints for {place}")
    buildings = ox.features_from_place(place, tags={"building": True})
    return buildings


def download_city_boundary(place: str = "Winnipeg, Canada") -> gpd.GeoDataFrame:
    """Download city boundary polygon from OSM via osmnx.

    Args:
        place: Geocodable place name.

    Returns:
        GeoDataFrame with city boundary polygon.
    """
    import osmnx as ox

    return ox.geocode_to_gdf(place)


# ---------------------------------------------------------------------------
# city2graph heterogeneous graph isochrones
# ---------------------------------------------------------------------------


def _empty_isochrone() -> gpd.GeoDataFrame:
    """Return an empty isochrone GeoDataFrame with the expected schema."""
    return gpd.GeoDataFrame(
        {
            "id": pd.Series(dtype="object"),
            "travel_time": pd.Series(dtype="int64"),
        },
        geometry=gpd.GeoSeries(crs=WGS84_CRS),
        crs=WGS84_CRS,
    )


def build_hetero_isochrone(
    hetero_graph,
    origin_node: str,
    cutoff_minutes: float,
) -> gpd.GeoDataFrame:
    """Build an isochrone on the city2graph heterogeneous graph.

    Args:
        hetero_graph: city2graph heterogeneous graph (NetworkX or rustworkx).
        origin_node: Node ID in the heterogeneous graph.
        cutoff_minutes: Maximum travel time in minutes.

    Returns:
        GeoDataFrame with the isochrone polygon.
    """
    import city2graph as c2g

    try:
        isochrone_gdf = c2g.create_isochrone(
            hetero_graph,
            origin=origin_node,
            cutoff=cutoff_minutes * 60.0,
            weight="travel_time_sec",
        )
        return isochrone_gdf
    except Exception as exc:
        logger.warning(f"city2graph isochrone failed for {origin_node}: {exc}")
        return _empty_isochrone()


# ---------------------------------------------------------------------------
# Feed asset registry
# ---------------------------------------------------------------------------


class FeedAssetRegistry:
    """Maps feed_id to GTFS zip path by scanning DB + filesystem.

    Args:
        db: TransitDB instance.
        city_key: City namespace.
        gtfs_archive_dir: Directory containing archived GTFS zips.
        current_gtfs_path: Path to the current GTFS zip.
    """

    def __init__(
        self,
        db: TransitDB,
        city_key: str,
        gtfs_archive_dir: Path,
        current_gtfs_path: Path,
    ) -> None:
        self._db = db
        self._city_key = city_key
        self._archive_dir = gtfs_archive_dir
        self._current_path = current_gtfs_path

    def resolve(self, feed_id: str) -> Path | None:
        """Resolve a feed_id to its GTFS zip path.

        Args:
            feed_id: Feed identifier.

        Returns:
            Path to the GTFS zip, or None if not found.
        """
        if feed_id == "current":
            if self._current_path.exists():
                return self._current_path
            return None

        # Alias: pre_ptn → pick the most recent pre-PTN archive
        if feed_id == "pre_ptn":
            from ptn_analysis.data.sources.gtfs import pick_archive, download_archive

            archive_date = pick_archive(pre_ptn=True)
            if archive_date is None:
                return None
            archive_path = self._archive_dir / f"{archive_date}.zip"
            if archive_path.exists():
                return archive_path
            return None

        # Check archive directory for date-based feed IDs
        for pattern in [f"*{feed_id}*", f"google_transit_{feed_id}.zip"]:
            matches = list(self._archive_dir.glob(pattern))
            if matches:
                return matches[0]

        return None

    def available_feeds(self) -> list[str]:
        """List feed_ids that have GTFS zips available.

        Returns:
            Sorted list of available feed identifiers.
        """
        feeds = []
        if self._current_path.exists():
            feeds.append("current")
        for zip_path in sorted(self._archive_dir.glob("*.zip")):
            stem = zip_path.stem.replace("google_transit_", "")
            feeds.append(stem)
        return sorted(set(feeds))
