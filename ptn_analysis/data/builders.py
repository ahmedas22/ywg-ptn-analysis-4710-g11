"""Extracted pipeline builder functions for SRP.

Heavy computation extracted from DatasetPipeline. Each function takes
explicit (db, city_key) parameters instead of self.
"""

from __future__ import annotations

import re

from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    FEED_ID_CURRENT,
    GTFS_ARCHIVE_DIR,
    GTFS_ZIP_PATH,
    JOBS_ACCESS_MAX_TRAVEL_MINUTES,
    R5_ISOCHRONE_MINUTES,
    R5_PERCENTILES,
    WPG_BOUNDS,
)
from ptn_analysis.context.db import TransitDB

_SAFE_SQL_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:-]+$")

# ── Accessibility (r5py) ─────────────────────────────────

def build_accessibility_tables(db: TransitDB, city_key: str, progress_callback=None) -> dict[str, int]:
    """Build r5py travel time matrices and isochrone tables.

    Requires Java 21 and an OSM PBF file. Degrades gracefully when
    r5py is unavailable (no Java) or the OSM PBF is missing.

    Returns:
        Row counts keyed by logical table name, or empty dict if skipped.
    """
    results: dict[str, int] = {}

    try:
        from ptn_analysis.data.sources.routing import (
            FeedAssetRegistry,
            build_isochrones,
            build_transport_network,
            build_travel_time_matrix,
            download_osm_pbf,
        )
    except ImportError as exc:
        logger.warning(f"Skipping accessibility tables: r5py not installed ({exc})")
        return results

    from ptn_analysis.context.config import (
        DEFAULT_ANALYSIS_DATE,
    )

    # Download OSM PBF if missing
    try:
        osm_path = download_osm_pbf()
    except Exception as exc:
        logger.warning(f"OSM PBF download failed — skipping accessibility tables: {exc}")
        return results

    if not osm_path.exists():
        logger.warning("OSM PBF not found — skipping accessibility tables")
        return results

    registry = FeedAssetRegistry(
        db=db,
        city_key=city_key,
        gtfs_archive_dir=GTFS_ARCHIVE_DIR,
        current_gtfs_path=GTFS_ZIP_PATH,
    )

    # Build one transport network per feed that has a GTFS zip
    # Use the most recent pre-PTN date (not alias) for r5py
    from ptn_analysis.data.sources import gtfs as gtfs_mod
    pre_ptn_date = gtfs_mod.pick_archive(pre_ptn=True)
    r5py_feeds = [FEED_ID_CURRENT]
    if pre_ptn_date:
        r5py_feeds.append(pre_ptn_date)
    for feed_id in r5py_feeds:
        # Sanitize feed_id for table names (hyphens not allowed)
        safe_id = feed_id.replace("-", "_")
        # Skip if tables already populated (r5py is expensive)
        walk_table = db.table_name(f"walk_matrix_{safe_id}", city_key)
        transit_table = db.table_name(f"transit_matrix_{safe_id}", city_key)
        if db.relation_exists(walk_table) and db.relation_exists(transit_table):
            walk_n = db.count(walk_table) or 0
            transit_n = db.count(transit_table) or 0
            if walk_n > 0 and transit_n > 0:
                logger.info(f"r5py tables cached for {feed_id} (walk={walk_n:,}, transit={transit_n:,}) — skipping")
                results[f"walk_matrix_{safe_id}"] = walk_n
                results[f"transit_matrix_{safe_id}"] = transit_n
                continue

        gtfs_path = registry.resolve(feed_id)
        if gtfs_path is None:
            logger.info(f"No GTFS zip for {feed_id} — skipping r5py tables")
            continue

        try:
            network = build_transport_network(osm_path, [gtfs_path])
        except Exception as exc:
            logger.warning(f"r5py network build failed for {feed_id}: {exc}")
            continue

        # Load stop centroids as origins/destinations
        stops_table = db.table_name("stops", city_key)
        stops_gdf = db.query(
            f"""
            SELECT stop_id AS id, stop_lat, stop_lon
            FROM {stops_table}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": feed_id},
        )
        if stops_gdf.empty:
            continue

        import geopandas as _gpd
        from shapely.geometry import Point as _Point

        stops_gdf = _gpd.GeoDataFrame(
            stops_gdf,
            geometry=[_Point(lon, lat) for lon, lat in zip(stops_gdf["stop_lon"], stops_gdf["stop_lat"])],
            crs="EPSG:4326",
        )
        stops_gdf = stops_gdf[["id", "geometry"]]

        # Pick a Wednesday within this feed's calendar range
        cal_table = db.table_name("calendar", city_key)
        cal_df = db.query(
            f"SELECT MIN(start_date) AS sd, MAX(end_date) AS ed "
            f"FROM {cal_table} WHERE feed_id = :fid",
            {"fid": feed_id},
        )
        if cal_df.empty or pd.isna(cal_df["sd"].iloc[0]):
            analysis_date = DEFAULT_ANALYSIS_DATE
        else:
            import datetime as _dt
            cal_start = pd.to_datetime(str(cal_df["sd"].iloc[0]))
            cal_end = pd.to_datetime(str(cal_df["ed"].iloc[0]))
            mid = cal_start + (cal_end - cal_start) / 2
            # Shift to nearest Wednesday (weekday=2)
            days_to_wed = (2 - mid.weekday()) % 7
            wed = mid + _dt.timedelta(days=days_to_wed)
            if wed > cal_end:
                wed = wed - _dt.timedelta(weeks=1)
            analysis_date = wed.strftime("%Y-%m-%d")
        logger.info(f"r5py analysis date for {feed_id}: {analysis_date}")

        # Walk-only matrix
        try:
            walk_matrix = build_travel_time_matrix(
                network, stops_gdf, stops_gdf,
                modes=["WALK"],
                departure_date=analysis_date,
                max_minutes=JOBS_ACCESS_MAX_TRAVEL_MINUTES,
                percentiles=R5_PERCENTILES,
            )
            walk_table = db.table_name(f"walk_matrix_{safe_id}", city_key)
            db.load_table(walk_table, walk_matrix, mode="replace")
            results[f"walk_matrix_{safe_id}"] = len(walk_matrix)
        except Exception as exc:
            logger.warning(f"Walk matrix failed for {feed_id}: {exc}")

        # Transit+walk matrix
        try:
            transit_matrix = build_travel_time_matrix(
                network, stops_gdf, stops_gdf,
                modes=["TRANSIT", "WALK"],
                departure_date=analysis_date,
                max_minutes=JOBS_ACCESS_MAX_TRAVEL_MINUTES,
                percentiles=R5_PERCENTILES,
            )
            transit_table = db.table_name(f"transit_matrix_{safe_id}", city_key)
            db.load_table(transit_table, transit_matrix, mode="replace")
            results[f"transit_matrix_{safe_id}"] = len(transit_matrix)
        except Exception as exc:
            logger.warning(f"Transit matrix failed for {feed_id}: {exc}")

        # Isochrones for hub stops (top 20 by connection count)
        try:
            counts_table = db.table_name("stop_connection_counts", city_key)
            if db.relation_exists(counts_table):
                hub_ids = db.query(
                    f"""
                    SELECT from_stop_id AS id
                    FROM {counts_table}
                    WHERE feed_id = :feed_id
                    GROUP BY from_stop_id
                    ORDER BY SUM(frequency) DESC
                    LIMIT 20
                    """,
                    {"feed_id": feed_id},
                )["id"].tolist()

                hub_gdf = stops_gdf[stops_gdf["id"].isin(hub_ids)]
                if not hub_gdf.empty:
                    isochrones = build_isochrones(
                        network, hub_gdf,
                        modes=["TRANSIT", "WALK"],
                        departure_date=analysis_date,
                        cutoffs=R5_ISOCHRONE_MINUTES,
                        bounds=WPG_BOUNDS,
                    )
                    iso_table = db.table_name(f"isochrones_{safe_id}", city_key)
                    db.load_table(iso_table, isochrones, mode="replace")
                    results[f"isochrones_{safe_id}"] = len(isochrones)
        except Exception as exc:
            logger.warning(f"Isochrone build failed for {feed_id}: {exc}")

    return results



# ── Era Aggregates ──────────────────────────────────────

def build_era_aggregates(db: TransitDB, city_key: str) -> None:
    """Insert era-averaged synthetic feeds into metric tables.

    Creates ``avg_pre_ptn`` and ``avg_post_ptn`` feed_ids by averaging
    route-level and stop-level metrics across all feeds in each era.
    Also inserts averaged rows into neighbourhood density and jobs
    access tables. This lets existing comparison methods work with
    era-level baselines via ``baseline_feed_id='avg_pre_ptn'``.
    """
    registry_table = db.table_name("feed_regime_registry", city_key)
    if not db.relation_exists(registry_table):
        return

    for era, synthetic_id in [("pre_ptn", "avg_pre_ptn"), ("post_ptn", "avg_post_ptn")]:
        era_feeds = db.query(
            f"SELECT feed_id FROM {registry_table} "
            f"WHERE era_label = :era AND feed_id != 'current'",
            {"era": era},
        )["feed_id"].tolist()
        if len(era_feeds) < 1:
            continue

        # Validate feed_id values before interpolation
        for f in era_feeds:
            if not _SAFE_SQL_VALUE_RE.match(f):
                raise ValueError(f"Unsafe feed_id in era aggregates: {f!r}")

        feeds_sql = ", ".join(f"'{f}'" for f in era_feeds)

        # Route stats: average numeric columns by route_id + direction_id
        route_table = db.table_name("gtfs_route_stats", city_key)
        if db.relation_exists(route_table):
            db.execute(
                f"DELETE FROM {route_table} WHERE feed_id = :fid",
                {"fid": synthetic_id},
            )
            db.execute_native(
                f"""
                INSERT INTO {route_table}
                (feed_id, date, route_id, route_short_name, route_type,
                 num_trips, num_trip_starts, num_trip_ends, num_stop_patterns,
                 is_loop, start_time, end_time,
                 max_headway, min_headway, mean_headway,
                 peak_num_trips, peak_start_time, peak_end_time,
                 service_distance, service_duration, service_speed,
                 mean_trip_distance, mean_trip_duration, direction_id)
                SELECT
                    '{synthetic_id}',
                    MAX(date),
                    route_id, MAX(route_short_name), MAX(route_type),
                    AVG(num_trips), AVG(num_trip_starts), AVG(num_trip_ends),
                    AVG(num_stop_patterns),
                    MAX(is_loop), MIN(start_time), MAX(end_time),
                    AVG(max_headway), AVG(min_headway), AVG(mean_headway),
                    AVG(peak_num_trips), MIN(peak_start_time), MAX(peak_end_time),
                    AVG(service_distance), AVG(service_duration), AVG(service_speed),
                    AVG(mean_trip_distance), AVG(mean_trip_duration), direction_id
                FROM {route_table}
                WHERE feed_id IN ({feeds_sql})
                GROUP BY route_id, direction_id
                """
            )

        # Stop stats: average numeric columns by stop_id + direction_id
        stop_table = db.table_name("gtfs_stop_stats", city_key)
        if db.relation_exists(stop_table):
            db.execute(
                f"DELETE FROM {stop_table} WHERE feed_id = :fid",
                {"fid": synthetic_id},
            )
            db.execute_native(
                f"""
                INSERT INTO {stop_table}
                (feed_id, date, stop_id, num_trips, num_routes,
                 max_headway, min_headway, mean_headway,
                 start_time, end_time, direction_id)
                SELECT
                    '{synthetic_id}',
                    MAX(date),
                    stop_id,
                    AVG(num_trips), AVG(num_routes),
                    AVG(max_headway), AVG(min_headway), AVG(mean_headway),
                    MIN(start_time), MAX(end_time), direction_id
                FROM {stop_table}
                WHERE feed_id IN ({feeds_sql})
                GROUP BY stop_id, direction_id
                """
            )

        # Neighbourhood density: average by neighbourhood
        density_table = db.table_name("neighbourhood_stop_count_density", city_key)
        if db.relation_exists(density_table):
            db.execute(
                f"DELETE FROM {density_table} WHERE feed_id = :fid",
                {"fid": synthetic_id},
            )
            db.execute_native(
                f"""
                INSERT INTO {density_table}
                SELECT
                    '{synthetic_id}' AS feed_id,
                    neighbourhood_id, neighbourhood, area_km2,
                    AVG(stop_count) AS stop_count,
                    AVG(stop_density_per_km2) AS stop_density_per_km2
                FROM {density_table}
                WHERE feed_id IN ({feeds_sql})
                GROUP BY neighbourhood_id, neighbourhood, area_km2
                """
            )

        # Jobs access: average by neighbourhood
        jobs_table = db.table_name("neighbourhood_jobs_access_metrics", city_key)
        if db.relation_exists(jobs_table):
            db.execute(
                f"DELETE FROM {jobs_table} WHERE feed_id = :fid",
                {"fid": synthetic_id},
            )
            db.execute_native(
                f"""
                INSERT INTO {jobs_table}
                (feed_id, neighbourhood_id, neighbourhood, area_km2,
                 stop_count, stop_density_per_km2,
                 jobs_proxy_score, establishment_count, large_employer_count,
                 jobs_proxy_log, jobs_access_score)
                SELECT
                    '{synthetic_id}',
                    neighbourhood_id, neighbourhood, MAX(area_km2),
                    AVG(stop_count), AVG(stop_density_per_km2),
                    AVG(jobs_proxy_score), AVG(establishment_count),
                    AVG(large_employer_count),
                    AVG(jobs_proxy_log), AVG(jobs_access_score)
                FROM {jobs_table}
                WHERE feed_id IN ({feeds_sql})
                GROUP BY neighbourhood_id, neighbourhood
                """
            )

    # Register the synthetic feeds (delete first for rerun safety)
    db.execute(
        f"DELETE FROM {registry_table} WHERE feed_id LIKE 'avg_%'"
    )
    db.execute_native(
        f"""
        INSERT INTO {registry_table} (feed_id, feed_label, era_label, sort_order, is_current)
        VALUES
            ('avg_pre_ptn', 'Pre-PTN average', 'pre_ptn', 0, false),
            ('avg_post_ptn', 'Post-PTN average', 'post_ptn', 0, false)
        """
    )
    logger.info("Built era-aggregate feeds: avg_pre_ptn, avg_post_ptn")



# ── Network Connections ─────────────────────────────────

def build_connections(db: TransitDB, city_key: str) -> None:
    """Build stop-to-stop connection tables using city2graph.

    Uses city2graph's calendar-aware GTFS graph builder for each loaded
    feed. Output table ``ywg_stop_connection_counts`` keeps the same
    schema so downstream consumers are unaffected.
    """
    import city2graph as c2g

    ck = city_key
    counts_table = db.table_name("stop_connection_counts", ck)
    connections_table = db.table_name("stop_connections", ck)

    db.execute(f"DROP TABLE IF EXISTS {connections_table}")

    feed_ids_df = db.query(
        f"SELECT DISTINCT feed_id FROM {db.table_name('trips', ck)} ORDER BY feed_id"
    )
    if feed_ids_df.empty:
        logger.warning("No feeds loaded — skipping connection build")
        return

    from ptn_analysis.data.sources.routing import FeedAssetRegistry

    registry = FeedAssetRegistry(
        db=db,
        city_key=ck,
        gtfs_archive_dir=GTFS_ARCHIVE_DIR,
        current_gtfs_path=GTFS_ZIP_PATH,
    )

    all_frames = []
    for feed_id in feed_ids_df["feed_id"].tolist():
        gtfs_path = registry.resolve(feed_id)
        if gtfs_path is None:
            raise FileNotFoundError(
                f"No GTFS zip for feed_id={feed_id}. "
                f"Run `make data` to download all feeds."
            )

        logger.info(f"city2graph: building edges for feed_id={feed_id}")
        gtfs = c2g.load_gtfs(str(gtfs_path))

        cal_df = db.query(
            f"SELECT MIN(start_date) AS sd, MAX(end_date) AS ed "
            f"FROM {db.table_name('calendar', ck)} WHERE feed_id = :fid",
            {"fid": feed_id},
        )
        if cal_df.empty or pd.isna(cal_df["sd"].iloc[0]) or pd.isna(cal_df["ed"].iloc[0]):
            raise ValueError(
                f"No calendar dates for feed_id={feed_id}. GTFS may be corrupt."
            )

        cal_start = str(cal_df["sd"].iloc[0]).replace("-", "")
        cal_end = str(cal_df["ed"].iloc[0]).replace("-", "")

        nodes_gdf, edges_gdf = c2g.travel_summary_graph(
            gtfs, calendar_start=cal_start, calendar_end=cal_end,
        )

        if edges_gdf.empty:
            raise RuntimeError(
                f"city2graph returned 0 edges for feed_id={feed_id}. "
                f"Check GTFS calendar range [{cal_start}..{cal_end}]."
            )

        # Use city2graph native schema: from_stop_id, to_stop_id,
        # travel_time_sec, frequency (+ geometry dropped for DuckDB)
        edges_gdf = edges_gdf.reset_index()
        edge_df = pd.DataFrame({
            "feed_id": feed_id,
            "from_stop_id": edges_gdf["from_stop_id"].astype(str),
            "to_stop_id": edges_gdf["to_stop_id"].astype(str),
            "travel_time_sec": edges_gdf["travel_time_sec"],
            "frequency": edges_gdf["frequency"],
        })
        all_frames.append(edge_df)
        logger.info(f"city2graph: {len(edge_df)} edges for feed_id={feed_id}")

    combined = pd.concat(all_frames, ignore_index=True)
    db.execute(f"DROP TABLE IF EXISTS {counts_table}")
    db.load_table(counts_table, combined)
    logger.info(f"Loaded {len(combined)} edges into {counts_table}")

