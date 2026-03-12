"""Serving-database and flat-file export helpers."""

from __future__ import annotations

from pathlib import Path

import duckdb
import geopandas as gpd
import pandas as pd

from ptn_analysis.context.config import FEED_ID_CURRENT, GTFS_ZIP_PATH, WGS84_CRS

SERVING_EXPORT_DEFINITIONS: list[tuple[str, bool, str]] = [
    ("stops", False, "WHERE feed_id = 'current'"),
    ("stop_connection_counts", False, "WHERE feed_id = 'current'"),
    ("neighbourhoods", False, ""),
    ("routes", False, "WHERE feed_id = 'current'"),
    ("gtfs_route_stats", False, "WHERE feed_id = 'current'"),
    ("gtfs_stop_stats", False, "WHERE feed_id = 'current'"),
    ("feed_info", False, "WHERE feed_id = 'current'"),
    ("route_schedule_metrics", False, "WHERE feed_id = 'current'"),
    ("route_schedule_facts", False, "WHERE feed_id = 'current'"),
    ("route_classification_features", False, "WHERE feed_id = 'current'"),
    ("route_reliability_metrics", False, "WHERE feed_id = 'current'"),
    ("route_capacity_priority", False, "WHERE feed_id = 'current'"),
    ("neighbourhood_stop_count_density", False, "WHERE feed_id = 'current'"),
    ("neighbourhood_stop_count_density_comparison", False, ""),
    ("neighbourhood_transit_access_metrics", False, "WHERE feed_id = 'current'"),
    ("neighbourhood_jobs_access_metrics", False, "WHERE feed_id = 'current'"),
    ("neighbourhood_jobs_access_comparison_metrics", False, ""),
    ("neighbourhood_priority_metrics", False, "WHERE feed_id = 'current'"),
    ("community_area_jobs_access_metrics", False, "WHERE feed_id = 'current'"),
    ("network_metrics", False, "WHERE feed_id = 'current'"),
    ("network_comparison_metrics", False, ""),
    ("top_hubs", False, "WHERE feed_id = 'current'"),
    ("transfer_burden_matrix", False, "WHERE feed_id = 'current'"),
    ("network_communities", False, "WHERE feed_id = 'current'"),
    ("feed_regime_registry", False, ""),
    ("transit_service_status", True, ""),
    ("transit_service_advisories", True, ""),
    ("transit_trip_delay_summary", True, ""),
    ("h3_stop_service_metrics", False, "WHERE feed_id = 'current'"),
    ("h3_live_delay_metrics", False, ""),
]

STATUS_RELATIONS: list[tuple[str, bool]] = [
    ("stops", False),
    ("routes", False),
    ("trips", False),
    ("gtfs_route_stats", False),
    ("gtfs_stop_stats", False),
    ("stop_connection_counts", False),
    ("neighbourhood_stop_count_density", False),
    ("neighbourhood_stop_count_density_comparison", False),
    ("route_schedule_metrics", False),
    ("route_schedule_facts", False),
    ("neighbourhood_jobs_access_metrics", False),
    ("h3_stop_service_metrics", False),
    ("h3_live_delay_metrics", False),
    ("service_status", True),
    ("effective_stops", True),
    ("route_stops", True),
    ("stop_schedules", True),
    ("trip_schedules", True),
]


def status_relation_names(db_instance, city_key: str) -> list[str]:
    """Return the core status relation names.

    Args:
        db_instance: Database handle.
        city_key: City namespace.

    Returns:
        Physical relation names.
    """
    relation_names = []
    for base_name, is_transit in STATUS_RELATIONS:
        if is_transit:
            relation_names.append(db_instance.transit_table_name(base_name, city_key))
        else:
            relation_names.append(db_instance.table_name(base_name, city_key))
    return relation_names


def render_storage_rows(db_instance, serving_db) -> list[tuple[str, str]]:
    """Build storage-status rows for a Rich status table.

    Args:
        db_instance: Working database handle.
        serving_db: Serving database handle.

    Returns:
        Storage label/value rows.
    """
    rows: list[tuple[str, str]] = []
    if GTFS_ZIP_PATH.exists():
        size_mb = GTFS_ZIP_PATH.stat().st_size / (1024 * 1024)
        rows.append(("google_transit.zip", f"{size_mb:.1f} MB"))
    if db_instance.path.exists():
        rows.append(("interim_db", f"{db_instance.path.stat().st_size / (1024 * 1024):.1f} MB"))
    if serving_db.path.exists():
        rows.append(("serving_db", f"{serving_db.path.stat().st_size / (1024 * 1024):.1f} MB"))
    return rows


def export_serving_duckdb(db_instance, serving_db, city_key: str) -> dict[str, int]:
    """Build the curated serving database used by Streamlit and report notebooks.

    Args:
        db_instance: Working database handle.
        serving_db: Serving database handle.
        city_key: City namespace.

    Returns:
        Exported row counts keyed by relation name.
    """
    serving_path = serving_db.path
    serving_path.parent.mkdir(parents=True, exist_ok=True)
    serving_path.unlink(missing_ok=True)
    results: dict[str, int] = {}
    with duckdb.connect(str(serving_path)) as serving_connection:
        try:
            serving_connection.execute("LOAD spatial;")
        except Exception:
            serving_connection.execute("INSTALL spatial; LOAD spatial;")

        for base_name, is_transit, filter_sql in SERVING_EXPORT_DEFINITIONS:
            source_name = (
                db_instance.transit_table_name(base_name.removeprefix("transit_"), city_key)
                if is_transit
                else db_instance.table_name(base_name, city_key)
            )
            if not db_instance.relation_exists(source_name):
                continue
            query_sql = f"SELECT * FROM {source_name} {filter_sql}".strip()
            export_frame = db_instance.query(query_sql)
            serving_connection.register("export_frame", export_frame)
            serving_connection.execute(f"CREATE TABLE {source_name} AS SELECT * FROM export_frame")
            serving_connection.unregister("export_frame")
            results[source_name] = len(export_frame)
    return results


def export_flat_files(db_instance, export_dir: Path, city_key: str) -> dict[str, int]:
    """Export consolidated analysis-ready datasets.

    Produces four files:
    - neighbourhood_analysis.parquet — one row per neighbourhood, all metrics joined
    - route_analysis.parquet — one row per route, all metrics joined
    - stops.geojson — spatial stop layer (current feed)
    - neighbourhoods.geojson — spatial neighbourhood polygons with metrics

    Args:
        db_instance: Working database handle.
        export_dir: Output directory.
        city_key: City namespace.

    Returns:
        Export counts keyed by logical dataset name.
    """
    from loguru import logger

    export_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, int] = {}
    feed_params = {"feed_id": FEED_ID_CURRENT}

    # --- 1. neighbourhood_analysis: one row per neighbourhood, all metrics ---
    neighbourhood_tables = []
    for base_name, cols in [
        ("neighbourhood_stop_count_density", None),
        ("neighbourhood_transit_access_metrics", None),
        ("neighbourhood_jobs_access_metrics", None),
        ("neighbourhood_priority_metrics", None),
    ]:
        rel = db_instance.table_name(base_name, city_key)
        if not db_instance.relation_exists(rel):
            continue
        df = db_instance.query(
            f"SELECT * FROM {rel} WHERE feed_id = :feed_id", feed_params
        )
        if cols:
            df = df[cols]
        neighbourhood_tables.append(df)

    if neighbourhood_tables:
        import functools
        merged = functools.reduce(
            lambda left, right: left.merge(
                right.drop(columns=[c for c in right.columns if c in left.columns and c != "neighbourhood"], errors="ignore"),
                on="neighbourhood",
                how="outer",
            ),
            neighbourhood_tables,
        )
        merged.to_parquet(export_dir / "neighbourhood_analysis.parquet", index=False)
        results["neighbourhood_analysis"] = len(merged)
        logger.info(f"Exported neighbourhood_analysis: {len(merged)} rows")

    # --- 2. route_analysis: one row per route, all metrics ---
    route_tables = []
    for base_name in [
        "route_schedule_metrics",
        "route_schedule_facts",
        "route_reliability_metrics",
        "route_capacity_priority",
        "route_classification_features",
    ]:
        rel = db_instance.table_name(base_name, city_key)
        if not db_instance.relation_exists(rel):
            continue
        df = db_instance.query(
            f"SELECT * FROM {rel} WHERE feed_id = :feed_id", feed_params
        )
        route_tables.append(df)

    if route_tables:
        import functools
        join_key = "route_id" if "route_id" in route_tables[0].columns else "route_short_name"
        merged = functools.reduce(
            lambda left, right: left.merge(
                right.drop(columns=[c for c in right.columns if c in left.columns and c != join_key], errors="ignore"),
                on=join_key,
                how="outer",
            ),
            route_tables,
        )
        merged.to_parquet(export_dir / "route_analysis.parquet", index=False)
        results["route_analysis"] = len(merged)
        logger.info(f"Exported route_analysis: {len(merged)} rows")

    # --- 3. stops.geojson — spatial stop layer ---
    stops_rel = db_instance.table_name("stops", city_key)
    if db_instance.relation_exists(stops_rel):
        current_stops = db_instance.query(
            f"SELECT * FROM {stops_rel} WHERE feed_id = :feed_id", feed_params
        )
        if not current_stops.empty and "stop_lon" in current_stops.columns:
            stop_gdf = gpd.GeoDataFrame(
                current_stops,
                geometry=gpd.points_from_xy(current_stops["stop_lon"], current_stops["stop_lat"]),
                crs=WGS84_CRS,
            )
            stop_gdf.to_file(export_dir / "stops.geojson", driver="GeoJSON")
            results["stops"] = len(current_stops)
            logger.info(f"Exported stops.geojson: {len(current_stops)} stops")

    # --- 4. neighbourhoods.geojson — polygons with metrics ---
    neigh_rel = db_instance.table_name("neighbourhoods", city_key)
    if db_instance.relation_exists(neigh_rel):
        neigh_gdf = db_instance.query(
            f"SELECT * FROM {neigh_rel}",
            geo=True,
        )
        if not neigh_gdf.empty and "neighbourhood_analysis" in results:
            neigh_analysis = pd.read_parquet(export_dir / "neighbourhood_analysis.parquet")
            neigh_gdf = neigh_gdf.merge(
                neigh_analysis, left_on="name", right_on="neighbourhood", how="left",
            )
        if not neigh_gdf.empty:
            neigh_gdf.to_file(export_dir / "neighbourhoods.geojson", driver="GeoJSON")
            results["neighbourhoods"] = len(neigh_gdf)
            logger.info(f"Exported neighbourhoods.geojson: {len(neigh_gdf)} polygons")

    return results
