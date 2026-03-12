"""Live transit refresh, sampling, and derived-table helpers."""

from __future__ import annotations

from datetime import date

from loguru import logger
import pandas as pd

from ptn_analysis.context.config import FEED_ID_CURRENT, PTN_LAUNCH_DATE
from ptn_analysis.data.sources import transit_api

LIVE_TABLE_COLUMNS: dict[str, list[str]] = {
    "effective_routes": [
        "requested_effective_on",
        "route_key",
        "route_number",
        "route_name",
        "effective_from",
        "effective_to",
        "customer_type",
        "coverage_type",
        "badge_label",
        "badge_style",
        "variant_count",
        "raw_payload",
    ],
    "effective_variants": [
        "requested_effective_on",
        "route_key",
        "route_number",
        "route_name",
        "variant_key",
        "variant_href",
        "variant_order",
    ],
    "effective_stops": [
        "requested_route_number",
        "requested_effective_on",
        "stop_order",
        "stop_key",
        "stop_number",
        "stop_name",
        "effective_from",
        "effective_to",
        "direction",
        "side",
        "stop_lat",
        "stop_lon",
        "distance_direct_metres",
        "distance_walking_metres",
    ],
    "route_stops": [
        "requested_route_number",
        "requested_effective_on",
        "stop_order",
        "stop_key",
        "stop_number",
        "stop_name",
        "effective_from",
        "effective_to",
        "direction",
        "side",
        "stop_lat",
        "stop_lon",
        "distance_direct_metres",
        "distance_walking_metres",
    ],
    "variant_destinations": [
        "variant_key",
        "destination_order",
        "destination_key",
        "destination_name",
        "destination_type",
        "raw_payload",
    ],
    "stop_features": [
        "stop_key",
        "feature_name",
        "feature_count",
    ],
    "stop_schedules": [
        "stop_key",
        "query_time",
        "route_key",
        "route_number",
        "route_name",
        "scheduled_stop_key",
        "trip_key",
        "cancelled",
        "variant_key",
        "variant_name",
        "bus_key",
        "bike_rack",
        "wifi",
        "scheduled_arrival_time",
        "estimated_arrival_time",
        "scheduled_departure_time",
        "estimated_departure_time",
    ],
    "trip_plans": [
        "origin",
        "destination",
        "plan_number",
        "segment_index",
        "segment_type",
        "plan_start_time",
        "plan_end_time",
        "plan_total_minutes",
        "plan_walking_minutes",
        "plan_waiting_minutes",
        "plan_riding_minutes",
        "segment_start_time",
        "segment_end_time",
        "segment_total_minutes",
        "route_key",
        "route_number",
        "route_name",
        "variant_key",
        "variant_name",
        "from_stop_key",
        "to_stop_key",
        "bus_key",
        "bike_rack",
        "wifi",
    ],
    "trip_schedules": [
        "trip_key",
        "previous_trip_key",
        "next_trip_key",
        "schedule_type",
        "variant_key",
        "effective_from",
        "effective_to",
        "bus_key",
        "bike_rack",
        "wifi",
        "query_time",
        "scheduled_stop_order",
        "scheduled_stop_key",
        "stop_key",
        "stop_number",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "cancelled",
        "scheduled_arrival_time",
        "estimated_arrival_time",
        "scheduled_departure_time",
        "estimated_departure_time",
    ],
    "trip_stop_delay_snapshot": [
        "trip_key",
        "previous_trip_key",
        "next_trip_key",
        "schedule_type",
        "variant_key",
        "effective_from",
        "effective_to",
        "bus_key",
        "bike_rack",
        "wifi",
        "query_time",
        "scheduled_stop_order",
        "scheduled_stop_key",
        "stop_key",
        "stop_number",
        "stop_name",
        "stop_lat",
        "stop_lon",
        "cancelled",
        "scheduled_arrival_time",
        "estimated_arrival_time",
        "scheduled_departure_time",
        "estimated_departure_time",
        "arrival_delay_seconds",
        "departure_delay_seconds",
    ],
    "trip_delay_summary": [
        "trip_key",
        "bus_key",
        "variant_key",
        "schedule_type",
        "previous_trip_key",
        "next_trip_key",
        "stop_count",
        "cancelled_stop_count",
        "mean_arrival_delay_seconds",
        "max_arrival_delay_seconds",
        "mean_departure_delay_seconds",
        "max_departure_delay_seconds",
    ],
    "bus_trip_chains": [
        "trip_key",
        "bus_key",
        "previous_trip_key",
        "next_trip_key",
        "variant_key",
        "schedule_type",
        "effective_from",
        "effective_to",
    ],
}


def build_trip_stop_delay_snapshot(trip_schedules: pd.DataFrame) -> pd.DataFrame:
    """Build one row per scheduled stop with computed delays.

    Args:
        trip_schedules: Raw trip-schedule snapshot rows.

    Returns:
        Stop-level delay table.
    """
    if trip_schedules.empty:
        return pd.DataFrame()
    delay_table = trip_schedules.copy()
    for time_column in [
        "scheduled_arrival_time",
        "estimated_arrival_time",
        "scheduled_departure_time",
        "estimated_departure_time",
    ]:
        delay_table[time_column] = pd.to_timedelta(delay_table[time_column], errors="coerce")
    delay_table["arrival_delay_seconds"] = (
        delay_table["estimated_arrival_time"] - delay_table["scheduled_arrival_time"]
    ).dt.total_seconds()
    delay_table["departure_delay_seconds"] = (
        delay_table["estimated_departure_time"] - delay_table["scheduled_departure_time"]
    ).dt.total_seconds()
    return delay_table


def build_trip_delay_summary(delay_snapshot: pd.DataFrame) -> pd.DataFrame:
    """Aggregate stop-level delays to one row per trip.

    Args:
        delay_snapshot: Stop-level delay table.

    Returns:
        Trip-level delay summary.
    """
    if delay_snapshot.empty:
        return pd.DataFrame()
    return (
        delay_snapshot.groupby("trip_key")
        .agg(
            bus_key=("bus_key", "first"),
            variant_key=("variant_key", "first"),
            schedule_type=("schedule_type", "first"),
            previous_trip_key=("previous_trip_key", "first"),
            next_trip_key=("next_trip_key", "first"),
            stop_count=("scheduled_stop_key", "count"),
            cancelled_stop_count=("cancelled", "sum"),
            mean_arrival_delay_seconds=("arrival_delay_seconds", "mean"),
            max_arrival_delay_seconds=("arrival_delay_seconds", "max"),
            mean_departure_delay_seconds=("departure_delay_seconds", "mean"),
            max_departure_delay_seconds=("departure_delay_seconds", "max"),
        )
        .reset_index()
    )


def build_bus_trip_chains(trip_schedules: pd.DataFrame) -> pd.DataFrame:
    """Build the current bus-to-trip chain table.

    Args:
        trip_schedules: Raw trip-schedule snapshot rows.

    Returns:
        Bus-to-trip chain rows.
    """
    if trip_schedules.empty:
        return pd.DataFrame()
    chain_columns = [
        "trip_key",
        "bus_key",
        "previous_trip_key",
        "next_trip_key",
        "variant_key",
        "schedule_type",
        "effective_from",
        "effective_to",
    ]
    return trip_schedules[chain_columns].drop_duplicates().reset_index(drop=True)


def concat_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Concatenate non-empty frames.

    Args:
        frames: Candidate DataFrames.

    Returns:
        Concatenated frame or an empty frame.
    """
    non_empty_frames = []
    for frame in frames:
        if frame is not None and not frame.empty:
            non_empty_frames.append(frame)
    if not non_empty_frames:
        return pd.DataFrame()
    return pd.concat(non_empty_frames, ignore_index=True)


def ensure_frame_columns(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Return a frame with a stable schema.

    Args:
        frame: Input DataFrame.
        columns: Required output columns.

    Returns:
        DataFrame with the requested columns in order.
    """
    if frame.empty and not list(frame.columns):
        return pd.DataFrame(columns=columns)

    shaped_frame = frame.copy()
    for column_name in columns:
        if column_name not in shaped_frame.columns:
            shaped_frame[column_name] = pd.Series(dtype="object")
    return shaped_frame.loc[:, columns]


def bootstrap_missing(db_instance, city_key: str) -> bool:
    """Return whether live-transit bootstrap tables are missing.

    Args:
        db_instance: Database handle.
        city_key: City namespace.

    Returns:
        True when bootstrap tables must be refreshed.
    """
    required_tables = [
        db_instance.transit_table_name("service_status", city_key),
        db_instance.transit_table_name("effective_routes", city_key),
        db_instance.transit_table_name("route_stops", city_key),
        db_instance.transit_table_name("variant_destinations", city_key),
    ]
    for table_name in required_tables:
        if not db_instance.relation_exists(table_name):
            return True
    return False


def sample_stop_keys(
    db_instance,
    city_key: str,
    feed_id: str,
    limit_per_tier: int = 3,
    include_hubs: bool = True,
    include_communities: bool = True,
) -> list[str]:
    """Build an auto-sampled stop set for live API snapshots.

    Args:
        db_instance: Database handle.
        city_key: City namespace.
        feed_id: Feed identifier.
        limit_per_tier: Number of sampled stops per PTN tier.
        include_hubs: Whether to include hub-based samples.
        include_communities: Whether to include one stop per community area.

    Returns:
        Sorted sampled stop keys.
    """
    sampled_keys: set[str] = set()
    route_stops_table = db_instance.transit_table_name("route_stops", city_key)
    route_tiers_table = db_instance.table_name("route_ptn_tiers", city_key)
    effective_on = date.today().isoformat()

    if db_instance.relation_exists(route_stops_table) and db_instance.relation_exists(route_tiers_table):
        tier_sample = db_instance.query(
            f"""
            WITH ordered_tier_stops AS (
                SELECT route_stops.stop_key,
                       route_tiers.ptn_tier,
                       ROW_NUMBER() OVER (
                           PARTITION BY route_tiers.ptn_tier
                           ORDER BY route_stops.requested_route_number, route_stops.stop_order
                       ) AS stop_rank
                FROM {route_stops_table} route_stops
                JOIN {route_tiers_table} route_tiers
                    ON route_tiers.feed_id = :feed_id
                   AND route_tiers.route_short_name = route_stops.requested_route_number
                WHERE route_stops.requested_effective_on = :effective_on
            )
            SELECT stop_key
            FROM ordered_tier_stops
            WHERE stop_rank <= :limit_per_tier
            """,
            {
                "feed_id": feed_id,
                "effective_on": effective_on,
                "limit_per_tier": limit_per_tier,
            },
        )
        sampled_keys.update(tier_sample["stop_key"].dropna().astype(str).tolist())

    if include_hubs:
        top_hubs_table = db_instance.table_name("top_hubs", city_key)
        if db_instance.relation_exists(top_hubs_table):
            hub_sample = db_instance.query(
                f"SELECT stop_id FROM {top_hubs_table} WHERE feed_id = :feed_id "
                f"ORDER BY total_degree DESC LIMIT 12",
                {"feed_id": feed_id},
            )
            sampled_keys.update(hub_sample["stop_id"].dropna().astype(str).tolist())

    if include_communities:
        stops_table = db_instance.table_name("stops", city_key)
        communities_table = db_instance.table_name("community_areas", city_key)
        if db_instance.relation_exists(stops_table) and db_instance.relation_exists(communities_table):
            community_sample = db_instance.query(
                f"""
                WITH community_stops AS (
                    SELECT stops.stop_id,
                           communities.name AS community_area,
                           ROW_NUMBER() OVER (
                               PARTITION BY communities.name
                               ORDER BY stops.stop_id
                           ) AS stop_rank
                    FROM {stops_table} stops
                    JOIN {communities_table} communities
                        ON ST_Contains(communities.geometry, ST_Point(stops.stop_lon, stops.stop_lat))
                    WHERE stops.feed_id = :feed_id
                )
                SELECT stop_id
                FROM community_stops
                WHERE stop_rank = 1
                """,
                {"feed_id": feed_id},
            )
            sampled_keys.update(community_sample["stop_id"].dropna().astype(str).tolist())

    return sorted(sampled_keys)


def sample_trip_plan_pairs(db_instance, city_key: str, feed_id: str) -> list[dict[str, str]]:
    """Build a bounded set of trip-planner origin and destination pairs.

    Args:
        db_instance: Database handle.
        city_key: City namespace.
        feed_id: Feed identifier.

    Returns:
        Trip-planner sample definitions.
    """
    corridor_table = db_instance.table_name("corridor_sample_pairs", city_key)
    if db_instance.relation_exists(corridor_table) and (db_instance.count(corridor_table) or 0) > 0:
        corridor_pairs = db_instance.query(
            f"""
            SELECT corridor_name,
                   origin_stop_id,
                   origin_lat,
                   origin_lon,
                   destination_stop_id,
                   destination_lat,
                   destination_lon
            FROM {corridor_table}
            """
        )
        rows: list[dict[str, str]] = []
        for _, pair in corridor_pairs.iterrows():
            corridor_name = str(pair.get("corridor_name") or "corridor")
            origin_stop_id = pair.get("origin_stop_id")
            destination_stop_id = pair.get("destination_stop_id")
            if pd.notna(origin_stop_id) and pd.notna(destination_stop_id):
                rows.append(
                    {
                        "corridor_name": corridor_name,
                        "origin": f"stops/{int(origin_stop_id)}",
                        "destination": f"stops/{int(destination_stop_id)}",
                    }
                )
                continue

            origin_lat = pair.get("origin_lat")
            origin_lon = pair.get("origin_lon")
            destination_lat = pair.get("destination_lat")
            destination_lon = pair.get("destination_lon")
            if all(pd.notna(value) for value in [origin_lat, origin_lon, destination_lat, destination_lon]):
                rows.append(
                    {
                        "corridor_name": corridor_name,
                        "origin": f"geo/{origin_lat},{origin_lon}",
                        "destination": f"geo/{destination_lat},{destination_lon}",
                    }
                )
        if rows:
            return rows

    route_stops_table = db_instance.transit_table_name("route_stops", city_key)
    route_tiers_table = db_instance.table_name("route_ptn_tiers", city_key)
    if not db_instance.relation_exists(route_stops_table) or not db_instance.relation_exists(route_tiers_table):
        return []

    pair_table = db_instance.query(
        f"""
        WITH ranked_routes AS (
            SELECT route_tiers.ptn_tier,
                   route_tiers.route_short_name,
                   ROW_NUMBER() OVER (
                       PARTITION BY route_tiers.ptn_tier
                       ORDER BY route_tiers.route_short_name
                   ) AS route_rank
            FROM {route_tiers_table} route_tiers
            WHERE route_tiers.feed_id = :feed_id
        ),
        route_endpoints AS (
            SELECT route_stops.requested_route_number AS route_short_name,
                   MIN_BY(route_stops.stop_key, route_stops.stop_order) AS origin_stop_key,
                   MAX_BY(route_stops.stop_key, route_stops.stop_order) AS destination_stop_key
            FROM {route_stops_table} route_stops
            WHERE route_stops.requested_effective_on = :effective_on
            GROUP BY route_stops.requested_route_number
        )
        SELECT ranked_routes.ptn_tier,
               ranked_routes.route_short_name,
               route_endpoints.origin_stop_key,
               route_endpoints.destination_stop_key
        FROM ranked_routes
        JOIN route_endpoints
            ON ranked_routes.route_short_name = route_endpoints.route_short_name
        WHERE ranked_routes.route_rank = 1
          AND route_endpoints.origin_stop_key IS NOT NULL
          AND route_endpoints.destination_stop_key IS NOT NULL
        ORDER BY ranked_routes.ptn_tier
        """,
        {"feed_id": feed_id, "effective_on": date.today().isoformat()},
    )
    rows = []
    for _, pair in pair_table.iterrows():
        rows.append(
            {
                "corridor_name": str(pair["ptn_tier"]),
                "origin": f"stops/{pair['origin_stop_key']}",
                "destination": f"stops/{pair['destination_stop_key']}",
            }
        )
    return rows


def log_sampled_skips(label: str, skipped_items: list[str], preview: int = 5) -> None:
    """Log a concise summary of skipped live-transit refresh items.

    Args:
        label: Human-readable operation label.
        skipped_items: Skipped item descriptions.
        preview: Number of examples to log.
    """
    if not skipped_items:
        return
    preview_rows = "; ".join(skipped_items[:preview])
    logger.warning(
        f"Skipped {len(skipped_items)} {label} items. Examples: {preview_rows}"
    )


def refresh_live_transit_bootstrap(
    db_instance,
    transit_source,
    city_key: str,
    force_refresh: bool = False,
) -> dict[str, int]:
    """Build the wide cached live-transit metadata layer.

    Args:
        db_instance: Database handle.
        transit_source: Winnipeg Transit source service.
        city_key: City namespace.
        force_refresh: Whether to bypass raw cache files.

    Returns:
        Row counts keyed by logical table name.
    """
    results: dict[str, int] = {}
    logger.info("Refreshing live-transit bootstrap tables")

    logger.info("Bootstrapping service status and advisories")
    try:
        status_frame = transit_api.refresh_service_status(transit_source, force_refresh=force_refresh)
    except Exception as exc:
        logger.warning(f"Skipping transit service status refresh: {exc}")
        status_frame = pd.DataFrame()
    db_instance.load_table(
        db_instance.transit_table_name("service_status", city_key),
        status_frame,
        mode="replace",
    )
    results["transit_service_status"] = len(status_frame)

    try:
        advisory_frame = transit_api.refresh_service_advisories(transit_source, force_refresh=force_refresh)
    except Exception as exc:
        logger.warning(f"Skipping transit service advisories refresh: {exc}")
        advisory_frame = pd.DataFrame()
    db_instance.load_table(
        db_instance.transit_table_name("service_advisories", city_key),
        advisory_frame,
        mode="replace",
    )
    results["transit_service_advisories"] = len(advisory_frame)

    effective_dates = ["2025-04-13", PTN_LAUNCH_DATE, "2025-08-31", date.today().isoformat()]
    route_frames: list[pd.DataFrame] = []
    variant_frames: list[pd.DataFrame] = []
    logger.info(f"Bootstrapping effective routes and variants for {len(effective_dates)} service dates")
    for i, effective_on in enumerate(effective_dates, 1):
        logger.info(f"  routes+variants {i}/{len(effective_dates)}: {effective_on}")
        try:
            route_frame = transit_api.refresh_effective_routes(transit_source, 
                effective_on=effective_on,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            logger.warning(f"Skipping effective route refresh for {effective_on}: {exc}")
            route_frame = pd.DataFrame()
        if not route_frame.empty:
            route_frames.append(route_frame)

        try:
            variant_frame = transit_api.refresh_effective_variants(transit_source, 
                effective_on=effective_on,
                force_refresh=force_refresh,
            )
        except Exception as exc:
            logger.warning(f"Skipping effective variant refresh for {effective_on}: {exc}")
            variant_frame = pd.DataFrame()
        if not variant_frame.empty:
            variant_frames.append(variant_frame)

    effective_routes = ensure_frame_columns(
        concat_frames(route_frames),
        LIVE_TABLE_COLUMNS["effective_routes"],
    )
    effective_variants = ensure_frame_columns(
        concat_frames(variant_frames),
        LIVE_TABLE_COLUMNS["effective_variants"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("effective_routes", city_key),
        effective_routes,
        mode="replace",
    )
    db_instance.load_table(
        db_instance.transit_table_name("effective_variants", city_key),
        effective_variants,
        mode="replace",
    )
    results["transit_effective_routes"] = len(effective_routes)
    results["transit_effective_variants"] = len(effective_variants)

    logger.info("Bootstrapping effective stops by service date")
    effective_stop_frames: list[pd.DataFrame] = []
    for i, effective_on in enumerate(effective_dates, 1):
        logger.info(f"  effective stops {i}/{len(effective_dates)}: {effective_on}")
        route_numbers_for_date = sorted(
            effective_routes.loc[
                effective_routes["requested_effective_on"] == effective_on,
                "route_number",
            ].dropna().astype(str).unique().tolist()
        )
        if not route_numbers_for_date:
            continue
        try:
            effective_stop_frames.append(
                transit_api.refresh_effective_stops(transit_source, 
                    effective_on=effective_on,
                    route_numbers=route_numbers_for_date,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            logger.warning(f"Skipping effective stop refresh for {effective_on}: {exc}")
    effective_stops = ensure_frame_columns(
        concat_frames(effective_stop_frames),
        LIVE_TABLE_COLUMNS["effective_stops"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("effective_stops", city_key),
        effective_stops,
        mode="replace",
    )
    results["transit_effective_stops"] = len(effective_stops)

    current_route_numbers = sorted(
        effective_routes.loc[
            effective_routes["requested_effective_on"] == date.today().isoformat(),
            "route_number",
        ].dropna().astype(str).unique().tolist()
    )
    if not current_route_numbers:
        current_route_numbers = sorted(
            effective_routes["route_number"].dropna().astype(str).unique().tolist()
        )

    logger.info(f"Bootstrapping route stops for {len(current_route_numbers)} routes")
    route_stop_frames: list[pd.DataFrame] = []
    skipped_route_numbers: list[str] = []
    for i, route_number in enumerate(current_route_numbers, 1):
        if i % 10 == 1 or i == len(current_route_numbers):
            logger.info(f"  route stops {i}/{len(current_route_numbers)}: route {route_number}")
        try:
            route_stop_frames.append(
                transit_api.refresh_route_stops(transit_source, 
                    route_number=route_number,
                    effective_on=date.today().isoformat(),
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_route_numbers.append(f"{route_number}: {exc}")
    log_sampled_skips("route-stop refresh", skipped_route_numbers)
    route_stops = ensure_frame_columns(
        concat_frames(route_stop_frames),
        LIVE_TABLE_COLUMNS["route_stops"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("route_stops", city_key),
        route_stops,
        mode="replace",
    )
    results["transit_route_stops"] = len(route_stops)

    variant_keys = sorted(
        effective_variants["variant_key"].dropna().astype(str).unique().tolist()
    )
    logger.info(f"Bootstrapping variant destinations for {len(variant_keys)} variants")
    destination_frames: list[pd.DataFrame] = []
    skipped_variant_keys: list[str] = []
    for variant_key in variant_keys:
        try:
            destination_frames.append(
                transit_api.refresh_variant_destinations(transit_source, 
                    variant_key=variant_key,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_variant_keys.append(f"{variant_key}: {exc}")
    log_sampled_skips("variant destination refresh", skipped_variant_keys)
    destinations = ensure_frame_columns(
        concat_frames(destination_frames),
        LIVE_TABLE_COLUMNS["variant_destinations"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("variant_destinations", city_key),
        destinations,
        mode="replace",
    )
    results["transit_variant_destinations"] = len(destinations)

    feature_stop_keys = sample_stop_keys(
        db_instance,
        city_key=city_key,
        feed_id=FEED_ID_CURRENT,
        limit_per_tier=4,
        include_hubs=True,
        include_communities=True,
    )
    logger.info(f"Bootstrapping stop features for {len(feature_stop_keys)} sampled stops")
    feature_frames: list[pd.DataFrame] = []
    skipped_feature_stops: list[str] = []
    for stop_key in feature_stop_keys:
        try:
            feature_frames.append(
                transit_api.refresh_stop_features(transit_source, 
                    stop_key=stop_key,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_feature_stops.append(f"{stop_key}: {exc}")
    log_sampled_skips("stop-features refresh", skipped_feature_stops)
    stop_features = ensure_frame_columns(
        concat_frames(feature_frames),
        LIVE_TABLE_COLUMNS["stop_features"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("stop_features", city_key),
        stop_features,
        mode="replace",
    )
    results["transit_stop_features"] = len(stop_features)
    return results


def refresh_live_transit_snapshots(
    db_instance,
    transit_source,
    city_key: str,
    force_refresh: bool = False,
) -> dict[str, int]:
    """Refresh bounded current-state live-transit snapshots.

    Args:
        db_instance: Database handle.
        transit_source: Winnipeg Transit source service.
        city_key: City namespace.
        force_refresh: Whether to bypass raw cache files.

    Returns:
        Row counts keyed by logical table name.
    """
    results: dict[str, int] = {}
    logger.info("Refreshing live-transit snapshot tables")
    status_table_name = db_instance.transit_table_name("service_status", city_key)
    if db_instance.relation_exists(status_table_name):
        current_status = db_instance.first(
            f"SELECT status_key FROM {status_table_name} ORDER BY query_time DESC LIMIT 1"
        )
        if current_status in {"esp-1", "esp-2", "esp-3"}:
            logger.warning("Skipping live snapshots because an emergency service plan is active.")
            return results

    stop_schedule_frames: list[pd.DataFrame] = []
    sampled_stop_keys = sample_stop_keys(db_instance, city_key=city_key, feed_id=FEED_ID_CURRENT)
    logger.info(f"Refreshing stop schedules for {len(sampled_stop_keys)} sampled stops")
    skipped_stop_schedules: list[str] = []
    for stop_key in sampled_stop_keys:
        try:
            stop_schedule_frames.append(
                transit_api.refresh_stop_schedule(transit_source, 
                    stop_key=stop_key,
                    max_results_per_route=2,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_stop_schedules.append(f"{stop_key}: {exc}")
    log_sampled_skips("stop schedule refresh", skipped_stop_schedules)
    stop_schedules = ensure_frame_columns(
        concat_frames(stop_schedule_frames),
        LIVE_TABLE_COLUMNS["stop_schedules"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("stop_schedules", city_key),
        stop_schedules,
        mode="replace",
    )
    results["transit_stop_schedules"] = len(stop_schedules)

    trip_plan_frames: list[pd.DataFrame] = []
    sampled_pairs = sample_trip_plan_pairs(db_instance, city_key=city_key, feed_id=FEED_ID_CURRENT)
    logger.info(f"Refreshing trip planner snapshots for {len(sampled_pairs)} sampled pairs")
    skipped_trip_plans: list[str] = []
    for pair in sampled_pairs:
        try:
            trip_plan_frames.append(
                transit_api.refresh_trip_plan(transit_source, 
                    origin=pair["origin"],
                    destination=pair["destination"],
                    mode="depart-after",
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_trip_plans.append(f"{pair['origin']}->{pair['destination']}: {exc}")
    log_sampled_skips("trip planner refresh", skipped_trip_plans)
    trip_plans = ensure_frame_columns(
        concat_frames(trip_plan_frames),
        LIVE_TABLE_COLUMNS["trip_plans"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("trip_plans", city_key),
        trip_plans,
        mode="replace",
    )
    results["transit_trip_plans"] = len(trip_plans)

    trip_keys = sorted(stop_schedules["trip_key"].dropna().astype(str).unique().tolist())
    logger.info(f"Refreshing trip schedules for {len(trip_keys)} sampled trips")
    trip_schedule_frames: list[pd.DataFrame] = []
    skipped_trip_keys: list[str] = []
    for trip_key in trip_keys:
        try:
            trip_schedule_frames.append(
                transit_api.refresh_trip_schedule(transit_source, 
                    trip_key=trip_key,
                    force_refresh=force_refresh,
                )
            )
        except Exception as exc:
            skipped_trip_keys.append(f"{trip_key}: {exc}")
    log_sampled_skips("trip schedule refresh", skipped_trip_keys)
    trip_schedules = ensure_frame_columns(
        concat_frames(trip_schedule_frames),
        LIVE_TABLE_COLUMNS["trip_schedules"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("trip_schedules", city_key),
        trip_schedules,
        mode="replace",
    )
    results["transit_trip_schedules"] = len(trip_schedules)

    trip_stop_delay_snapshot = ensure_frame_columns(
        build_trip_stop_delay_snapshot(trip_schedules),
        LIVE_TABLE_COLUMNS["trip_stop_delay_snapshot"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("trip_stop_delay_snapshot", city_key),
        trip_stop_delay_snapshot,
        mode="replace",
    )
    results["transit_trip_stop_delay_snapshot"] = len(trip_stop_delay_snapshot)

    trip_delay_summary = ensure_frame_columns(
        build_trip_delay_summary(trip_stop_delay_snapshot),
        LIVE_TABLE_COLUMNS["trip_delay_summary"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("trip_delay_summary", city_key),
        trip_delay_summary,
        mode="replace",
    )
    results["transit_trip_delay_summary"] = len(trip_delay_summary)

    bus_trip_chains = ensure_frame_columns(
        build_bus_trip_chains(trip_schedules),
        LIVE_TABLE_COLUMNS["bus_trip_chains"],
    )
    db_instance.load_table(
        db_instance.transit_table_name("bus_trip_chains", city_key),
        bus_trip_chains,
        mode="replace",
    )
    results["transit_bus_trip_chains"] = len(bus_trip_chains)
    return results
