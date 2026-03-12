"""Winnipeg Transit API v4 source service.

All public ``refresh_*`` functions take a ``_SourceContext`` as their first
argument.  Call ``create_source()`` once and pass the result everywhere.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import md5
import json
from pathlib import Path
import time
from typing import Any

import httpx
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    DEFAULT_CITY_KEY,
    TRANSIT_API_CACHE_DIR,
    WINNIPEG_TRANSIT_API_KEY,
)
from ptn_analysis.context.http import ApiClient

API_BASE_URL = "https://api.winnipegtransit.com/v4"
API_CACHE_DIR = TRANSIT_API_CACHE_DIR
EMERGENCY_STATUS_KEYS = {"esp-1", "esp-2", "esp-3"}
REQUESTS_PER_MINUTE = 60
MIN_REQUEST_INTERVAL_SECONDS = 60.0 / REQUESTS_PER_MINUTE
TRANSIT_API_TIMEOUT_SECONDS = 15.0


@dataclass
class _SourceContext:
    """Lightweight holder for a configured Winnipeg Transit API client."""

    __slots__ = ("city_key", "api_key", "client")
    city_key: str
    api_key: str
    client: ApiClient


def create_source(
    city_key: str = DEFAULT_CITY_KEY,
    api_key: str | None = None,
) -> _SourceContext:
    """Create a reusable Winnipeg Transit API source context.

    Args:
        city_key: City namespace prefix.
        api_key: Optional API key override.

    Returns:
        Configured source context.
    """
    resolved_key = api_key or WINNIPEG_TRANSIT_API_KEY
    client = ApiClient(
        api_key=resolved_key,
        base_url=API_BASE_URL,
        cache_dir=API_CACHE_DIR,
        throttle_rpm=REQUESTS_PER_MINUTE,
    )
    return _SourceContext(city_key=city_key, api_key=resolved_key, client=client)


def as_bool(value: Any) -> bool | None:
    """Normalize Winnipeg Transit boolean-like values."""
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def as_list(value: Any) -> list[Any]:
    """Normalize a maybe-list payload to a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def nested(payload: Any, keys: list[str]) -> Any:
    """Read a nested value from a payload."""
    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        if key not in current:
            return None
        current = current[key]
    return current


def stop_key_from_segment(segment: dict[str, Any], edge_name: str) -> Any:
    """Read a stop key from a trip-planner segment edge."""
    edge_payload = segment.get(edge_name, {})
    stop_payload = edge_payload.get("stop")
    if stop_payload is None:
        origin_payload = edge_payload.get("origin") or edge_payload.get("destination")
        if isinstance(origin_payload, dict):
            stop_payload = origin_payload.get("stop")
    if isinstance(stop_payload, dict):
        return stop_payload.get("key")
    return None


def coerce_status_key(raw_status: Any) -> str | None:
    """Normalize the service-status key from Winnipeg Transit payloads."""
    if raw_status is None:
        return None
    if isinstance(raw_status, dict):
        for key_name in ["value", "key", "status", "name"]:
            if raw_status.get(key_name):
                return str(raw_status[key_name])
        return json.dumps(raw_status)
    return str(raw_status)


def coerce_status_name(raw_status: Any) -> str | None:
    """Normalize the service-status name from Winnipeg Transit payloads."""
    if isinstance(raw_status, dict) and raw_status.get("name"):
        return str(raw_status["name"])
    return None


def coerce_status_message(raw_status: Any) -> str | None:
    """Normalize the service-status message from Winnipeg Transit payloads."""
    if isinstance(raw_status, dict) and raw_status.get("message"):
        return str(raw_status["message"])
    return None


def flatten_service_status(payload: dict[str, Any]) -> pd.DataFrame:
    """Build the one-row service-status frame."""
    schedule_payload = payload.get("scheduleStatus", payload)
    raw_status = schedule_payload.get("key") or schedule_payload.get("status")
    status_key = coerce_status_key(raw_status)
    return pd.DataFrame(
        [
            {
                "status_key": status_key,
                "status": status_key,
                "status_name": coerce_status_name(raw_status),
                "status_message": coerce_status_message(raw_status),
                "query_time": payload.get("queryTime") or schedule_payload.get("queryTime"),
                "is_emergency_service_plan": status_key in EMERGENCY_STATUS_KEYS,
                "raw_payload": json.dumps(payload),
            }
        ]
    )


def flatten_service_advisories(payload: dict[str, Any]) -> pd.DataFrame:
    """Build the service-advisories frame."""
    rows: list[dict[str, Any]] = []
    for advisory in as_list(payload.get("serviceAdvisories")):
        rows.append(
            {
                "advisory_key": advisory.get("key"),
                "priority": advisory.get("priority"),
                "title": advisory.get("title"),
                "body": advisory.get("body"),
                "category": advisory.get("category"),
                "updated_at": advisory.get("updatedAt"),
            }
        )
    return pd.DataFrame(rows)


def flatten_effective_routes(payload: dict[str, Any], effective_on: str | None) -> pd.DataFrame:
    """Build effective-route metadata rows."""
    rows: list[dict[str, Any]] = []
    for route in as_list(payload.get("routes")):
        rows.append(
            {
                "requested_effective_on": effective_on,
                "route_key": route.get("key"),
                "route_number": route.get("number"),
                "route_name": route.get("name"),
                "effective_from": route.get("effectiveFrom"),
                "effective_to": route.get("effectiveTo"),
                "customer_type": route.get("customerType"),
                "coverage_type": route.get("coverage"),
                "badge_label": route.get("badgeLabel"),
                "badge_style": json.dumps(route.get("badgeStyle", {})),
                "variant_count": len(as_list(route.get("variants"))),
                "raw_payload": json.dumps(route),
            }
        )
    return pd.DataFrame(rows)


def flatten_effective_variants(payload: dict[str, Any], effective_on: str | None) -> pd.DataFrame:
    """Build variant metadata rows derived from route payloads."""
    rows: list[dict[str, Any]] = []
    for route in as_list(payload.get("routes")):
        variants = as_list(route.get("variants"))
        for variant_index, variant in enumerate(variants):
            if isinstance(variant, dict):
                variant_key = variant.get("key")
                href = variant.get("href")
            else:
                variant_key = variant
                href = None
            rows.append(
                {
                    "requested_effective_on": effective_on,
                    "route_key": route.get("key"),
                    "route_number": route.get("number"),
                    "route_name": route.get("name"),
                    "variant_key": variant_key,
                    "variant_href": href,
                    "variant_order": variant_index,
                }
            )
    return pd.DataFrame(rows)


def flatten_route_stops(
    payload: dict[str, Any],
    route_number: str,
    effective_on: str | None,
) -> pd.DataFrame:
    """Build route-stop metadata rows."""
    rows: list[dict[str, Any]] = []
    for stop_order, stop in enumerate(as_list(payload.get("stops")), start=1):
        rows.append(
            {
                "requested_route_number": route_number,
                "requested_effective_on": effective_on,
                "stop_order": stop_order,
                "stop_key": stop.get("key"),
                "stop_number": stop.get("number"),
                "stop_name": stop.get("name"),
                "effective_from": stop.get("effectiveFrom"),
                "effective_to": stop.get("effectiveTo"),
                "direction": stop.get("direction"),
                "side": stop.get("side"),
                "stop_lat": nested(stop, ["centre", "geographic", "latitude"]),
                "stop_lon": nested(stop, ["centre", "geographic", "longitude"]),
                "distance_direct_metres": nested(stop, ["distances", "direct"]),
                "distance_walking_metres": nested(stop, ["distances", "walking"]),
            }
        )
    return pd.DataFrame(rows)


def deduplicate_effective_stops(stop_frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Deduplicate effective stops reconstructed from route-stop lists."""
    non_empty_frames: list[pd.DataFrame] = []
    for stop_frame in stop_frames:
        if stop_frame is not None and not stop_frame.empty:
            non_empty_frames.append(stop_frame)
    if not non_empty_frames:
        return pd.DataFrame()
    combined_stops = pd.concat(non_empty_frames, ignore_index=True)
    return combined_stops.sort_values(["stop_key", "requested_route_number", "stop_order"]).drop_duplicates(
        subset=["requested_effective_on", "stop_key"]
    ).reset_index(drop=True)


def flatten_variant_destinations(payload: dict[str, Any], variant_key: str) -> pd.DataFrame:
    """Build variant-destination rows."""
    rows: list[dict[str, Any]] = []
    for destination_order, destination in enumerate(as_list(payload.get("destinations")), start=1):
        rows.append(
            {
                "variant_key": variant_key,
                "destination_order": destination_order,
                "destination_key": destination.get("key"),
                "destination_name": destination.get("name"),
                "destination_type": destination.get("type"),
                "raw_payload": json.dumps(destination),
            }
        )
    return pd.DataFrame(rows)


def flatten_stop_features(payload: dict[str, Any], stop_key: str | int) -> pd.DataFrame:
    """Build stop-feature rows."""
    rows: list[dict[str, Any]] = []
    for feature in as_list(payload.get("stopFeatures")):
        rows.append(
            {
                "stop_key": str(stop_key),
                "feature_name": feature.get("name"),
                "feature_count": feature.get("count"),
            }
        )
    return pd.DataFrame(rows)


def flatten_stop_schedule(payload: dict[str, Any], stop_key: str | int) -> pd.DataFrame:
    """Build stop-schedule rows."""
    stop_schedule = payload.get("stopSchedule", payload)
    query_time = payload.get("queryTime") or stop_schedule.get("queryTime")
    rows: list[dict[str, Any]] = []
    for route_schedule in as_list(stop_schedule.get("routeSchedules")):
        route_payload = route_schedule.get("route", {})
        for scheduled_stop in as_list(route_schedule.get("scheduledStops")):
            bus_payload = scheduled_stop.get("bus", {})
            variant_payload = scheduled_stop.get("variant", {})
            arrival_payload = nested(scheduled_stop, ["times", "arrival"]) or {}
            departure_payload = nested(scheduled_stop, ["times", "departure"]) or {}
            rows.append(
                {
                    "stop_key": str(stop_key),
                    "query_time": query_time,
                    "route_key": route_payload.get("key"),
                    "route_number": route_payload.get("number"),
                    "route_name": route_payload.get("name"),
                    "scheduled_stop_key": scheduled_stop.get("key"),
                    "trip_key": scheduled_stop.get("tripKey"),
                    "cancelled": as_bool(scheduled_stop.get("cancelled")),
                    "variant_key": variant_payload.get("key"),
                    "variant_name": variant_payload.get("name"),
                    "bus_key": bus_payload.get("key"),
                    "bike_rack": as_bool(bus_payload.get("bikeRack")),
                    "wifi": as_bool(bus_payload.get("wifi")),
                    "scheduled_arrival_time": arrival_payload.get("scheduled"),
                    "estimated_arrival_time": arrival_payload.get("estimated"),
                    "scheduled_departure_time": departure_payload.get("scheduled"),
                    "estimated_departure_time": departure_payload.get("estimated"),
                }
            )
    return pd.DataFrame(rows)


def flatten_trip_plan(payload: dict[str, Any], origin: str, destination: str) -> pd.DataFrame:
    """Build trip-planner rows."""
    rows: list[dict[str, Any]] = []
    for plan in as_list(payload.get("plans")):
        plan_number = plan.get("number")
        plan_times = plan.get("times", {})
        plan_durations = plan_times.get("durations", {})
        for segment_index, segment in enumerate(as_list(plan.get("segments")), start=1):
            segment_times = segment.get("times", {})
            segment_durations = segment_times.get("durations", {})
            route_payload = segment.get("route", {})
            variant_payload = segment.get("variant", {})
            bus_payload = segment.get("bus", {})
            rows.append(
                {
                    "origin": origin,
                    "destination": destination,
                    "plan_number": plan_number,
                    "segment_index": segment_index,
                    "segment_type": segment.get("type"),
                    "plan_start_time": plan_times.get("start"),
                    "plan_end_time": plan_times.get("end"),
                    "plan_total_minutes": plan_durations.get("total"),
                    "plan_walking_minutes": plan_durations.get("walking"),
                    "plan_waiting_minutes": plan_durations.get("waiting"),
                    "plan_riding_minutes": plan_durations.get("riding"),
                    "segment_start_time": segment_times.get("start"),
                    "segment_end_time": segment_times.get("end"),
                    "segment_total_minutes": segment_durations.get("total"),
                    "route_key": route_payload.get("key"),
                    "route_number": route_payload.get("number"),
                    "route_name": route_payload.get("name"),
                    "variant_key": variant_payload.get("key"),
                    "variant_name": variant_payload.get("name"),
                    "from_stop_key": stop_key_from_segment(segment, "from"),
                    "to_stop_key": stop_key_from_segment(segment, "to"),
                    "bus_key": bus_payload.get("key"),
                    "bike_rack": as_bool(bus_payload.get("bikeRack")),
                    "wifi": as_bool(bus_payload.get("wifi")),
                }
            )
    return pd.DataFrame(rows)


def flatten_trip_schedule(payload: dict[str, Any], trip_key: str | int) -> pd.DataFrame:
    """Build trip-schedule rows."""
    trip_payload = payload.get("trip", payload)
    query_time = payload.get("queryTime")
    bus_payload = trip_payload.get("bus", {})
    variant_payload = trip_payload.get("variant", {})
    rows: list[dict[str, Any]] = []
    for stop_order, scheduled_stop in enumerate(as_list(trip_payload.get("scheduledStops")), start=1):
        stop_payload = scheduled_stop.get("stop", {})
        arrival_payload = nested(scheduled_stop, ["times", "arrival"]) or {}
        departure_payload = nested(scheduled_stop, ["times", "departure"]) or {}
        rows.append(
            {
                "trip_key": trip_payload.get("key") or trip_key,
                "previous_trip_key": trip_payload.get("previousTripKey"),
                "next_trip_key": trip_payload.get("nextTripKey"),
                "schedule_type": trip_payload.get("scheduleType"),
                "variant_key": variant_payload.get("key"),
                "effective_from": trip_payload.get("effectiveFrom"),
                "effective_to": trip_payload.get("effectiveTo"),
                "bus_key": bus_payload.get("key"),
                "bike_rack": as_bool(bus_payload.get("bikeRack")),
                "wifi": as_bool(bus_payload.get("wifi")),
                "query_time": query_time,
                "scheduled_stop_order": stop_order,
                "scheduled_stop_key": scheduled_stop.get("key"),
                "stop_key": stop_payload.get("key"),
                "stop_number": stop_payload.get("number"),
                "stop_name": stop_payload.get("name"),
                "stop_lat": nested(stop_payload, ["centre", "geographic", "latitude"]),
                "stop_lon": nested(stop_payload, ["centre", "geographic", "longitude"]),
                "cancelled": as_bool(scheduled_stop.get("cancelled")),
                "scheduled_arrival_time": arrival_payload.get("scheduled"),
                "estimated_arrival_time": arrival_payload.get("estimated"),
                "scheduled_departure_time": departure_payload.get("scheduled"),
                "estimated_departure_time": departure_payload.get("estimated"),
            }
        )
    return pd.DataFrame(rows)



# ---------------------------------------------------------------------------
# Module-level refresh functions (ctx: _SourceContext as first arg)
# ---------------------------------------------------------------------------


def _fetch_json(
    ctx: _SourceContext,
    path: str,
    params: dict[str, Any] | None = None,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Fetch a JSON endpoint with JSONL cache and throttle."""
    if not ctx.api_key:
        raise ValueError("No Winnipeg Transit API key. Set WINNIPEG_TRANSIT_API_KEY in .env.")
    request_params: dict[str, Any] = {
        "api-key": ctx.api_key,
        "json-camel-case": "true",
    }
    if params:
        request_params.update(params)

    family = _cache_family(path)
    cache_key = _cache_key(path, request_params)

    if not force_refresh:
        cached = _jsonl_read(family, cache_key)
        if cached is not None:
            return cached

    ctx.client._throttle()
    payload = ctx.client.request(
        f"{API_BASE_URL}/{path}",
        params=request_params,
        response_format="json",
        timeout=TRANSIT_API_TIMEOUT_SECONDS,
    )
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected Winnipeg Transit payload for {path}: {type(payload)!r}")
    _jsonl_write(family, cache_key, payload)
    return payload


def _cache_family(path: str) -> str:
    base = path.replace(".json", "").split("/")[0]
    return base.replace("-", "_")


def _cache_key(path: str, params: dict[str, Any]) -> str:
    filtered = []
    for key, value in sorted(params.items()):
        if key == "api-key":
            continue
        filtered.append(f"{key}={value}")
    raw = path + "|" + "|".join(filtered)
    return md5(raw.encode("utf-8")).hexdigest()[:16]


def _jsonl_path(family: str) -> Path:
    API_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return API_CACHE_DIR / f"{family}.jsonl"


def _jsonl_read(family: str, cache_key: str) -> dict | None:
    jsonl_path = _jsonl_path(family)
    if not jsonl_path.exists():
        return None
    for line in jsonl_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        entry = json.loads(line)
        if entry.get("key") == cache_key:
            return entry.get("payload")
    return None


def _jsonl_write(family: str, cache_key: str, payload: dict) -> None:
    jsonl_path = _jsonl_path(family)
    entry = {"key": cache_key, "payload": payload}
    with open(jsonl_path, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry) + "\n")


def refresh_service_status(ctx: _SourceContext, force_refresh: bool = False) -> pd.DataFrame:
    """Fetch current service status."""
    payload = _fetch_json(ctx, "statuses/schedule.json", force_refresh=force_refresh)
    return flatten_service_status(payload)


def refresh_service_advisories(ctx: _SourceContext, force_refresh: bool = False) -> pd.DataFrame:
    """Fetch active service advisories."""
    payload = _fetch_json(ctx, "service-advisories.json", force_refresh=force_refresh)
    return flatten_service_advisories(payload)


def refresh_effective_routes(
    ctx: _SourceContext,
    effective_on: str | None = None,
    stop: str | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch routes effective on a given date."""
    params: dict[str, Any] = {}
    if effective_on is not None:
        params["effective-on"] = effective_on
    if stop is not None:
        params["stop"] = stop
    payload = _fetch_json(ctx, "routes.json", params=params, force_refresh=force_refresh)
    return flatten_effective_routes(payload, effective_on)


def refresh_effective_variants(
    ctx: _SourceContext,
    effective_on: str | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch route variants effective on a given date."""
    params: dict[str, Any] = {}
    if effective_on is not None:
        params["effective-on"] = effective_on
    payload = _fetch_json(ctx, "routes.json", params=params, force_refresh=force_refresh)
    return flatten_effective_variants(payload, effective_on)


def refresh_route_stops(
    ctx: _SourceContext,
    route_number: str,
    effective_on: str | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch stops served by a specific route."""
    params: dict[str, Any] = {"route": route_number}
    if effective_on is not None:
        params["effective-on"] = effective_on
    payload = _fetch_json(ctx, "stops.json", params=params, force_refresh=force_refresh)
    return flatten_route_stops(payload, route_number, effective_on)


def refresh_effective_stops(
    ctx: _SourceContext,
    effective_on: str | None = None,
    route_numbers: list[str] | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch all effective stops, de-duplicated across routes."""
    requested_route_numbers = route_numbers
    if requested_route_numbers is None:
        route_table = refresh_effective_routes(ctx, effective_on=effective_on, force_refresh=force_refresh)
        requested_route_numbers = sorted(
            route_table["route_number"].dropna().astype(str).unique().tolist()
        )
    stop_frames: list[pd.DataFrame] = []
    for route_number in requested_route_numbers:
        stop_frames.append(
            refresh_route_stops(ctx, route_number=route_number, effective_on=effective_on, force_refresh=force_refresh)
        )
    return deduplicate_effective_stops(stop_frames)


def refresh_variant_destinations(
    ctx: _SourceContext,
    variant_key: str,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch destination stop information for a route variant."""
    payload = _fetch_json(ctx, f"variants/{variant_key}/destinations.json", force_refresh=force_refresh)
    return flatten_variant_destinations(payload, variant_key)


def refresh_stop_features(
    ctx: _SourceContext,
    stop_key: str | int,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch accessibility and amenity features for a stop."""
    payload = _fetch_json(ctx, f"stops/{stop_key}/features.json", force_refresh=force_refresh)
    return flatten_stop_features(payload, stop_key)


def refresh_stop_schedule(
    ctx: _SourceContext,
    stop_key: str | int,
    max_results_per_route: int = 2,
    start: str | None = None,
    end: str | None = None,
    route: str | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch upcoming departures from a stop."""
    params: dict[str, Any] = {"max-results-per-route": max_results_per_route}
    if start is not None:
        params["start"] = start
    if end is not None:
        params["end"] = end
    if route is not None:
        params["route"] = route
    payload = _fetch_json(ctx, f"stops/{stop_key}/schedule.json", params=params, force_refresh=force_refresh)
    return flatten_stop_schedule(payload, stop_key)


def refresh_trip_plan(
    ctx: _SourceContext,
    origin: str,
    destination: str,
    date: str | None = None,
    time_value: str | None = None,
    mode: str = "depart-after",
    walk_speed: float = 5.0,
    max_walk_time: int = 10,
    min_transfer_wait: int | None = None,
    max_transfer_wait: int | None = None,
    max_transfers: int = 3,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Plan a transit trip between two stop keys."""
    params: dict[str, Any] = {
        "origin": origin,
        "destination": destination,
        "mode": mode,
        "walk-speed": walk_speed,
        "max-walk-time": max_walk_time,
        "max-transfers": max_transfers,
    }
    if date is not None:
        params["date"] = date
    if time_value is not None:
        params["time"] = time_value
    if min_transfer_wait is not None:
        params["min-transfer-wait"] = min_transfer_wait
    if max_transfer_wait is not None:
        params["max-transfer-wait"] = max_transfer_wait
    try:
        payload = _fetch_json(ctx, "trip-planner.json", params=params, force_refresh=force_refresh)
    except httpx.HTTPStatusError as exc:
        response_text = exc.response.text.strip()
        if exc.response.status_code == 404 and response_text in {
            "NO_ORIGIN_SERVICE",
            "NO_DESTINATION_SERVICE",
            "NO_PLAN_FOUND",
        }:
            logger.warning(
                "Skipping trip-planner pair with no valid service: "
                f"origin={origin}, destination={destination}, reason={response_text}"
            )
            return pd.DataFrame()
        raise
    return flatten_trip_plan(payload, origin, destination)


def refresh_trip_schedule(
    ctx: _SourceContext,
    trip_key: str | int,
    effective_on: str | None = None,
    force_refresh: bool = False,
) -> pd.DataFrame:
    """Fetch the scheduled stop sequence for a specific trip."""
    params: dict[str, Any] = {}
    if effective_on is not None:
        params["effective-on"] = effective_on
    payload = _fetch_json(ctx, f"trips/{trip_key}.json", params=params, force_refresh=force_refresh)
    return flatten_trip_schedule(payload, trip_key)
