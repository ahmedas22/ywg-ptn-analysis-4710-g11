-- PTN Analysis schema reset
-- Drop repo-managed relations so the pipeline can rebuild from a clean state.

-- Views (cheap joins, alias views, CASE-based lookups, era-split helpers)
DROP VIEW IF EXISTS ywg_v_passups;
DROP VIEW IF EXISTS ywg_v_ontime_performance;
DROP VIEW IF EXISTS ywg_v_passenger_counts;
DROP VIEW IF EXISTS ywg_route_ptn_tiers;
DROP VIEW IF EXISTS ywg_route_schedule_speed_metrics;
DROP VIEW IF EXISTS ywg_route_schedule_facts;
DROP VIEW IF EXISTS ywg_route_departure_summary;
DROP VIEW IF EXISTS ywg_route_classification_features;
DROP VIEW IF EXISTS ywg_route_capacity_priority;
DROP VIEW IF EXISTS ywg_route_ontime;
DROP VIEW IF EXISTS ywg_route_performance;
DROP VIEW IF EXISTS ywg_neighbourhood_transit_access_metrics;
DROP VIEW IF EXISTS ywg_neighbourhood_priority_metrics;
DROP VIEW IF EXISTS ywg_neighbourhood_stop_count_density_comparison;
DROP VIEW IF EXISTS ywg_census_by_neighbourhood;
-- ywg_stop_schedule_metrics: ORPHANED — never created or used, intentionally omitted

-- Materialised tables (formerly views, now CTASed for performance)
DROP TABLE IF EXISTS ywg_route_schedule_metrics;
DROP TABLE IF EXISTS ywg_route_passups;
DROP TABLE IF EXISTS ywg_route_hourly_departures;
DROP TABLE IF EXISTS ywg_route_reliability_metrics;

DROP TABLE IF EXISTS ywg_daily_service;
DROP TABLE IF EXISTS ywg_stop_connections;
DROP TABLE IF EXISTS ywg_stop_connection_counts;
DROP TABLE IF EXISTS ywg_gtfs_route_stats;
DROP TABLE IF EXISTS ywg_gtfs_stop_stats;
DROP TABLE IF EXISTS ywg_neighbourhood_stop_count_density;
DROP TABLE IF EXISTS ywg_community_area_stop_count_density;
DROP TABLE IF EXISTS ywg_da_jobs_proxy_raw;
DROP TABLE IF EXISTS ywg_census_place_of_work_raw;
DROP TABLE IF EXISTS ywg_da_jobs_proxy;
DROP TABLE IF EXISTS ywg_neighbourhood_jobs_proxy;
DROP TABLE IF EXISTS ywg_neighbourhood_jobs_access_metrics;
DROP TABLE IF EXISTS ywg_community_area_jobs_access_metrics;
DROP TABLE IF EXISTS ywg_feed_regime_registry;
DROP TABLE IF EXISTS ywg_corridor_sample_pairs;

DROP TABLE IF EXISTS ywg_neighbourhood_jobs_access_comparison_metrics;
DROP TABLE IF EXISTS ywg_network_communities;
DROP TABLE IF EXISTS ywg_network_comparison_metrics;
DROP TABLE IF EXISTS ywg_network_metrics;
DROP TABLE IF EXISTS ywg_top_hubs;
DROP TABLE IF EXISTS ywg_transfer_burden_matrix;

DROP TABLE IF EXISTS ywg_transit_service_status;
DROP TABLE IF EXISTS ywg_transit_service_advisories;
DROP TABLE IF EXISTS ywg_transit_effective_routes;
DROP TABLE IF EXISTS ywg_transit_effective_stops;
DROP TABLE IF EXISTS ywg_transit_effective_variants;
DROP TABLE IF EXISTS ywg_transit_route_stops;
DROP TABLE IF EXISTS ywg_transit_variant_destinations;
DROP TABLE IF EXISTS ywg_transit_stop_features;
DROP TABLE IF EXISTS ywg_transit_stop_schedules;
DROP TABLE IF EXISTS ywg_transit_trip_plans;
DROP TABLE IF EXISTS ywg_transit_trip_schedules;
DROP TABLE IF EXISTS ywg_transit_trip_stop_delay_snapshot;
DROP TABLE IF EXISTS ywg_transit_trip_delay_summary;
DROP TABLE IF EXISTS ywg_transit_bus_trip_chains;

DROP TABLE IF EXISTS ywg_fare_rules;
DROP TABLE IF EXISTS ywg_fare_attributes;
DROP TABLE IF EXISTS ywg_feed_info;
DROP TABLE IF EXISTS ywg_stop_times;
DROP TABLE IF EXISTS ywg_shapes;
DROP TABLE IF EXISTS ywg_calendar_dates;
DROP TABLE IF EXISTS ywg_calendar;
DROP TABLE IF EXISTS ywg_trips;
DROP TABLE IF EXISTS ywg_stops;
DROP TABLE IF EXISTS ywg_routes;
DROP TABLE IF EXISTS ywg_agency;
DROP TABLE IF EXISTS ywg_neighbourhoods;
DROP TABLE IF EXISTS ywg_community_areas;
DROP TABLE IF EXISTS ywg_passups;
DROP TABLE IF EXISTS ywg_ontime_performance;
DROP TABLE IF EXISTS ywg_passenger_counts;
DROP TABLE IF EXISTS ywg_cycling_paths;
DROP TABLE IF EXISTS ywg_walkways;
DROP TABLE IF EXISTS ywg_census_poverty_2021;
DROP TABLE IF EXISTS ywg_census_da;

-- Remove stale legacy names from the pre-refactor schema.
DROP VIEW IF EXISTS stop_connections;
DROP VIEW IF EXISTS stop_connection_counts;
DROP VIEW IF EXISTS stop_connections_weighted;
DROP VIEW IF EXISTS neighbourhood_coverage;
DROP VIEW IF EXISTS community_coverage;
DROP VIEW IF EXISTS ptn_tier_routes;
DROP VIEW IF EXISTS route_speed_comparison;
DROP VIEW IF EXISTS hourly_departures_by_route;
DROP VIEW IF EXISTS ptn_comparison;
DROP VIEW IF EXISTS route_passups;
DROP VIEW IF EXISTS route_ontime;
DROP VIEW IF EXISTS route_performance;

DROP TABLE IF EXISTS stop_connections;
DROP TABLE IF EXISTS stop_connection_counts;
DROP TABLE IF EXISTS stop_connections_weighted;
DROP TABLE IF EXISTS neighbourhood_coverage;
DROP TABLE IF EXISTS community_coverage;
DROP TABLE IF EXISTS ptn_tier_routes;
DROP TABLE IF EXISTS route_speed_comparison;
DROP TABLE IF EXISTS hourly_departures_by_route;
DROP TABLE IF EXISTS ptn_comparison;
DROP TABLE IF EXISTS route_passups;
DROP TABLE IF EXISTS route_ontime;
DROP TABLE IF EXISTS route_performance;
DROP TABLE IF EXISTS daily_service;
DROP TABLE IF EXISTS agency;
DROP TABLE IF EXISTS stops;
DROP TABLE IF EXISTS routes;
DROP TABLE IF EXISTS trips;
DROP TABLE IF EXISTS stop_times;
DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS calendar_dates;
DROP TABLE IF EXISTS shapes;
DROP TABLE IF EXISTS feed_info;
DROP TABLE IF EXISTS fare_attributes;
DROP TABLE IF EXISTS fare_rules;
DROP TABLE IF EXISTS gtfs_route_stats;
DROP TABLE IF EXISTS gtfs_stop_stats;
DROP TABLE IF EXISTS neighbourhoods;
DROP TABLE IF EXISTS community_areas;
DROP TABLE IF EXISTS passups;
DROP TABLE IF EXISTS ontime_performance;
DROP TABLE IF EXISTS passenger_counts;
DROP TABLE IF EXISTS cycling_paths;
DROP TABLE IF EXISTS walkways;
DROP TABLE IF EXISTS census_poverty_2021;
DROP TABLE IF EXISTS census_da;
