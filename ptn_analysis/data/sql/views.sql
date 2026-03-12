-- Winnipeg analysis views and materialized coverage tables
--
-- ORDER MATTERS: ywg_v_* era-split views must appear before any
-- TABLE or VIEW that JOINs them (ywg_route_passups, reliability_metrics).

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. Neighbourhood / community stop-density tables (no upstream dependencies)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE TABLE ywg_neighbourhood_stop_count_density AS
WITH available_feeds AS (
    SELECT DISTINCT feed_id FROM ywg_stops
)
SELECT
    available_feeds.feed_id,
    neighbourhoods.id AS neighbourhood_id,
    neighbourhoods.name AS neighbourhood,
    neighbourhoods.area_km2,
    COUNT(DISTINCT stops.stop_id) AS stop_count,
    COUNT(DISTINCT stops.stop_id) / NULLIF(neighbourhoods.area_km2, 0) AS stop_density_per_km2
FROM available_feeds
CROSS JOIN ywg_neighbourhoods neighbourhoods
LEFT JOIN ywg_stops stops
    ON stops.feed_id = available_feeds.feed_id
   AND ST_Contains(neighbourhoods.geometry, ST_Point(stops.stop_lon, stops.stop_lat))
GROUP BY available_feeds.feed_id, neighbourhoods.id, neighbourhoods.name, neighbourhoods.area_km2;

CREATE OR REPLACE TABLE ywg_community_area_stop_count_density AS
WITH available_feeds AS (
    SELECT DISTINCT feed_id FROM ywg_stops
)
SELECT
    available_feeds.feed_id,
    community_areas.id AS community_area_id,
    community_areas.name AS community_area,
    community_areas.area_km2,
    COUNT(DISTINCT stops.stop_id) AS stop_count,
    COUNT(DISTINCT stops.stop_id) / NULLIF(community_areas.area_km2, 0) AS stop_density_per_km2
FROM available_feeds
CROSS JOIN ywg_community_areas community_areas
LEFT JOIN ywg_stops stops
    ON stops.feed_id = available_feeds.feed_id
   AND ST_Contains(community_areas.geometry, ST_Point(stops.stop_lon, stops.stop_lat))
GROUP BY available_feeds.feed_id, community_areas.id, community_areas.name, community_areas.area_km2;

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. Era-split views  ← MUST come before any table that LEFT JOINs them
--    ptn_era = 'pre_ptn'  for records before PTN launch date
--    ptn_era = 'post_ptn' for records on or after PTN launch date
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW ywg_v_passups AS
SELECT *,
    CASE
        WHEN TRY_CAST(time AS DATE) < '{{ptn_launch_date}}' THEN 'pre_ptn'
        ELSE 'post_ptn'
    END AS ptn_era
FROM ywg_passups;

CREATE OR REPLACE VIEW ywg_v_ontime_performance AS
SELECT *,
    CASE
        WHEN TRY_CAST(scheduled_time AS DATE) < '{{ptn_launch_date}}' THEN 'pre_ptn'
        ELSE 'post_ptn'
    END AS ptn_era
FROM ywg_ontime_performance;

CREATE OR REPLACE VIEW ywg_v_passenger_counts AS
SELECT *,
    CASE
        WHEN TRY_CAST(schedule_period_end_date AS DATE) < '{{ptn_launch_date}}' THEN 'pre_ptn'
        ELSE 'post_ptn'
    END AS ptn_era
FROM ywg_passenger_counts;

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. PTN tier lookup (cheap CASE on 71 routes — keep as view)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW ywg_route_ptn_tiers AS
SELECT
    feed_id,
    route_id,
    route_short_name,
    route_long_name,
    route_color,
    route_text_color,
    CASE
        WHEN route_short_name = 'BLUE' THEN 'Rapid Transit'
        WHEN route_short_name LIKE 'FX%' THEN 'Frequent Express'
        WHEN route_short_name LIKE 'F%' AND route_short_name NOT LIKE 'FX%' THEN 'Frequent'
        WHEN route_short_name LIKE 'D%' THEN 'Direct'
        WHEN route_short_name IN ('22','28','31','37','38','39','43','48','70','74') THEN 'Connector'
        WHEN route_short_name IN ('690','691','694','833','881','883','884','885','886','887','888','889','895') THEN 'Limited Span'
        WHEN LENGTH(route_short_name) <= 2 AND regexp_full_match(route_short_name, '^\d+$') THEN 'Connector'
        WHEN LENGTH(route_short_name) = 3 AND regexp_full_match(route_short_name, '^\d+$') THEN 'Community'
        ELSE 'Community'
    END AS ptn_tier
FROM ywg_routes;

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. Materialized tables (expensive scans — computed once, joined cheaply)
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. Route schedule metrics — root dependency for 5+ downstream relations.
--     Aggregates gtfs_route_stats, no era dependency.
CREATE OR REPLACE TABLE ywg_route_schedule_metrics AS
SELECT
    stats.feed_id,
    stats.route_id,
    ANY_VALUE(stats.route_short_name) AS route_short_name,
    ANY_VALUE(routes.route_long_name) AS route_long_name,
    SUM(stats.num_trips) AS scheduled_trip_count,
    AVG(stats.mean_headway) AS mean_headway_minutes,
    MIN(stats.min_headway) AS min_headway_minutes,
    MAX(stats.max_headway) AS max_headway_minutes,
    AVG(stats.service_speed) AS scheduled_speed_kmh,
    AVG(stats.mean_trip_distance) AS mean_trip_distance,
    AVG(stats.mean_trip_duration) AS mean_trip_duration
FROM ywg_gtfs_route_stats stats
LEFT JOIN ywg_routes routes
    ON stats.feed_id = routes.feed_id
   AND stats.route_id = routes.route_id
WHERE stats.mean_headway IS NOT NULL
GROUP BY stats.feed_id, stats.route_id;

-- 4b. Route passups — era-corrected join (pre_ptn routes only see pre_ptn passups).
--     ERA FIX: previously joined ywg_passups directly, mixing eras.
CREATE OR REPLACE TABLE ywg_route_passups AS
SELECT
    routes.feed_id,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    COUNT(passups.route_number) AS passup_count,
    COUNT(DISTINCT DATE_TRUNC('day', TRY_CAST(passups.time AS TIMESTAMP))) AS days_with_passups
FROM ywg_routes routes
LEFT JOIN ywg_v_passups passups
    ON routes.route_short_name = passups.route_number
   AND passups.ptn_era = CASE routes.feed_id
        WHEN 'pre_ptn' THEN 'pre_ptn'
        ELSE 'post_ptn'
       END
GROUP BY routes.feed_id, routes.route_id, routes.route_short_name, routes.route_long_name;

-- 4c. Hourly departures — scans stop_times once, cheap aggregation downstream.
CREATE OR REPLACE TABLE ywg_route_hourly_departures AS
SELECT
    stop_times.feed_id,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    CAST(SPLIT_PART(stop_times.departure_time, ':', 1) AS INTEGER) AS hour,
    COUNT(DISTINCT stop_times.trip_id) AS departures
FROM ywg_stop_times stop_times
JOIN ywg_trips trips
    ON stop_times.feed_id = trips.feed_id
   AND stop_times.trip_id = trips.trip_id
JOIN ywg_routes routes
    ON trips.feed_id = routes.feed_id
   AND trips.route_id = routes.route_id
WHERE stop_times.stop_sequence = 1
GROUP BY stop_times.feed_id, routes.route_id, routes.route_short_name, routes.route_long_name, hour;

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. Cheap join views (all upstream relations are now materialized tables)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW ywg_route_schedule_speed_metrics AS
SELECT
    metrics.feed_id,
    metrics.route_id,
    metrics.route_short_name,
    metrics.route_long_name,
    tiers.ptn_tier,
    metrics.scheduled_speed_kmh,
    metrics.mean_trip_distance,
    metrics.mean_trip_duration,
    metrics.mean_headway_minutes,
    metrics.scheduled_trip_count
FROM ywg_route_schedule_metrics metrics
LEFT JOIN ywg_route_ptn_tiers tiers
    ON metrics.feed_id = tiers.feed_id
   AND metrics.route_id = tiers.route_id;

CREATE OR REPLACE VIEW ywg_neighbourhood_transit_access_metrics AS
SELECT
    density.feed_id,
    density.neighbourhood_id,
    density.neighbourhood,
    density.area_km2,
    density.stop_count,
    density.stop_density_per_km2,
    CASE
        WHEN density.stop_density_per_km2 >= 5 THEN 'High'
        WHEN density.stop_density_per_km2 >= 1 THEN 'Medium'
        ELSE 'Low'
    END AS density_category
FROM ywg_neighbourhood_stop_count_density density;

CREATE OR REPLACE VIEW ywg_neighbourhood_priority_metrics AS
SELECT
    access.feed_id,
    access.neighbourhood_id,
    access.neighbourhood,
    access.stop_count,
    access.stop_density_per_km2,
    access.density_category,
    RANK() OVER (
        PARTITION BY access.feed_id
        ORDER BY access.stop_density_per_km2 ASC, access.stop_count ASC
    ) AS priority_rank
FROM ywg_neighbourhood_transit_access_metrics access;

CREATE OR REPLACE VIEW ywg_neighbourhood_stop_count_density_comparison AS
WITH feed_pairs AS (
    SELECT DISTINCT
        baseline.feed_id AS baseline_feed_id,
        comparison.feed_id AS comparison_feed_id
    FROM ywg_neighbourhood_stop_count_density baseline
    CROSS JOIN ywg_neighbourhood_stop_count_density comparison
    WHERE baseline.feed_id <> comparison.feed_id
)
SELECT
    feed_pairs.baseline_feed_id,
    feed_pairs.comparison_feed_id,
    comparison.neighbourhood_id,
    comparison.neighbourhood,
    comparison.area_km2,
    baseline.stop_count AS baseline_stop_count,
    comparison.stop_count AS comparison_stop_count,
    comparison.stop_count - baseline.stop_count AS stop_count_change,
    baseline.stop_density_per_km2 AS baseline_stop_density_per_km2,
    comparison.stop_density_per_km2 AS comparison_stop_density_per_km2,
    comparison.stop_density_per_km2 - baseline.stop_density_per_km2 AS stop_density_change
FROM feed_pairs
JOIN ywg_neighbourhood_stop_count_density baseline
    ON baseline.feed_id = feed_pairs.baseline_feed_id
JOIN ywg_neighbourhood_stop_count_density comparison
    ON comparison.feed_id = feed_pairs.comparison_feed_id
   AND comparison.neighbourhood_id = baseline.neighbourhood_id;

-- ═══════════════════════════════════════════════════════════════════════════
-- 6. Census-to-neighbourhood spatial allocation
--    Allocates DA-level Census 2021 data to neighbourhoods via overlap ratio.
--    Source: CHASS Census Profile 2021 DA (University of Toronto).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE VIEW ywg_census_by_neighbourhood AS
WITH da_overlap AS (
    SELECT neighbourhoods.id AS neighbourhood_id,
           neighbourhoods.name AS neighbourhood,
           neighbourhoods.area_km2,
           census_da.geo_uid,
           ST_Area(ST_Intersection(neighbourhoods.geometry, census_da.geometry))
               / NULLIF(ST_Area(census_da.geometry), 0) AS overlap_ratio,
           COALESCE(TRY_CAST(census_da.population_2021 AS DOUBLE), 0.0) AS population_total,
           COALESCE(TRY_CAST(census_da.commute_mode_total AS DOUBLE), 0.0) AS commute_total,
           COALESCE(TRY_CAST(census_da.commute_transit AS DOUBLE), 0.0) AS commute_public_transit,
           COALESCE(TRY_CAST(census_da.commute_car_total AS DOUBLE), 0.0) AS commute_car_truck_van,
           COALESCE(TRY_CAST(census_da.commute_walked AS DOUBLE), 0.0) AS commute_walked,
           COALESCE(TRY_CAST(census_da.commute_bicycle AS DOUBLE), 0.0) AS commute_bicycle,
           COALESCE(TRY_CAST(census_da.commute_other AS DOUBLE), 0.0) AS commute_other,
           COALESCE(TRY_CAST(census_da.commute_dest_total AS DOUBLE), 0.0) AS workplace_total,
           COALESCE(TRY_CAST(census_da.age_65_plus AS DOUBLE), 0.0) AS age_65_plus,
           COALESCE(TRY_CAST(census_da.immigrant_total AS DOUBLE), 0.0) AS immigration_total,
           COALESCE(TRY_CAST(census_da.recent_immigrant_2016_2021 AS DOUBLE), 0.0) AS recent_immigrants,
           COALESCE(TRY_CAST(census_da.median_total_income AS DOUBLE), 0.0) AS median_income,
           -- Journey to Work: commute duration
           COALESCE(TRY_CAST(census_da.commute_dur_total AS DOUBLE), 0.0) AS commute_dur_total,
           COALESCE(TRY_CAST(census_da.commute_dur_lt15 AS DOUBLE), 0.0) AS commute_dur_lt15,
           COALESCE(TRY_CAST(census_da.commute_dur_15_29 AS DOUBLE), 0.0) AS commute_dur_15_29,
           COALESCE(TRY_CAST(census_da.commute_dur_30_44 AS DOUBLE), 0.0) AS commute_dur_30_44,
           COALESCE(TRY_CAST(census_da.commute_dur_45_59 AS DOUBLE), 0.0) AS commute_dur_45_59,
           COALESCE(TRY_CAST(census_da.commute_dur_60_plus AS DOUBLE), 0.0) AS commute_dur_60_plus,
           -- Journey to Work: departure time
           COALESCE(TRY_CAST(census_da.depart_total AS DOUBLE), 0.0) AS depart_total,
           COALESCE(TRY_CAST(census_da.depart_5am AS DOUBLE), 0.0) AS depart_5am,
           COALESCE(TRY_CAST(census_da.depart_6am AS DOUBLE), 0.0) AS depart_6am,
           COALESCE(TRY_CAST(census_da.depart_7am AS DOUBLE), 0.0) AS depart_7am,
           COALESCE(TRY_CAST(census_da.depart_8am AS DOUBLE), 0.0) AS depart_8am,
           COALESCE(TRY_CAST(census_da.depart_9_11am AS DOUBLE), 0.0) AS depart_9_11am,
           COALESCE(TRY_CAST(census_da.depart_12_4am AS DOUBLE), 0.0) AS depart_12_4am,
           -- Mobility (1-year)
           COALESCE(TRY_CAST(census_da.mobility_1yr_total AS DOUBLE), 0.0) AS mobility_1yr_total,
           COALESCE(TRY_CAST(census_da.mobility_1yr_movers AS DOUBLE), 0.0) AS mobility_1yr_movers,
           COALESCE(TRY_CAST(census_da.mobility_1yr_external AS DOUBLE), 0.0) AS mobility_1yr_external
    FROM ywg_neighbourhoods neighbourhoods
    JOIN ywg_census_da census_da
        ON ST_Intersects(neighbourhoods.geometry, census_da.geometry)
),
allocated AS (
    SELECT neighbourhood_id,
           neighbourhood,
           area_km2,
           population_total * overlap_ratio AS pop_alloc,
           commute_total * overlap_ratio AS commute_total_alloc,
           commute_public_transit * overlap_ratio AS transit_alloc,
           commute_car_truck_van * overlap_ratio AS car_alloc,
           commute_walked * overlap_ratio AS walk_alloc,
           commute_bicycle * overlap_ratio AS bike_alloc,
           commute_other * overlap_ratio AS other_alloc,
           workplace_total * overlap_ratio AS workplace_total_alloc,
           age_65_plus * overlap_ratio AS seniors_alloc,
           immigration_total * overlap_ratio AS imm_total_alloc,
           recent_immigrants * overlap_ratio AS recent_imm_alloc,
           median_income * overlap_ratio AS income_alloc,
           -- Duration allocations
           commute_dur_total * overlap_ratio AS dur_total_alloc,
           commute_dur_lt15 * overlap_ratio AS dur_lt15_alloc,
           commute_dur_15_29 * overlap_ratio AS dur_15_29_alloc,
           commute_dur_30_44 * overlap_ratio AS dur_30_44_alloc,
           commute_dur_45_59 * overlap_ratio AS dur_45_59_alloc,
           commute_dur_60_plus * overlap_ratio AS dur_60_plus_alloc,
           -- Departure time allocations
           depart_total * overlap_ratio AS depart_total_alloc,
           depart_5am * overlap_ratio AS depart_5am_alloc,
           depart_6am * overlap_ratio AS depart_6am_alloc,
           depart_7am * overlap_ratio AS depart_7am_alloc,
           depart_8am * overlap_ratio AS depart_8am_alloc,
           depart_9_11am * overlap_ratio AS depart_9_11am_alloc,
           depart_12_4am * overlap_ratio AS depart_12_4am_alloc,
           -- Mobility allocations
           mobility_1yr_total * overlap_ratio AS mobility_total_alloc,
           mobility_1yr_movers * overlap_ratio AS mobility_movers_alloc,
           mobility_1yr_external * overlap_ratio AS mobility_external_alloc
    FROM da_overlap
)
SELECT neighbourhood_id,
       neighbourhood,
       area_km2,
       ROUND(SUM(pop_alloc), 0) AS population_total,
       ROUND(SUM(pop_alloc) / NULLIF(area_km2, 0), 2) AS population_density_per_km2,
       ROUND(100.0 * SUM(transit_alloc) / NULLIF(SUM(commute_total_alloc), 0), 2)
           AS pct_commute_public_transit,
       ROUND(100.0 * SUM(car_alloc) / NULLIF(SUM(commute_total_alloc), 0), 2)
           AS pct_commute_car,
       ROUND(100.0 * SUM(walk_alloc) / NULLIF(SUM(commute_total_alloc), 0), 2)
           AS pct_commute_walk,
       ROUND(100.0 * SUM(bike_alloc) / NULLIF(SUM(commute_total_alloc), 0), 2)
           AS pct_commute_cycle,
       ROUND(100.0 * SUM(other_alloc) / NULLIF(SUM(workplace_total_alloc), 0), 2)
           AS pct_commute_other,
       ROUND(100.0 * SUM(seniors_alloc) / NULLIF(SUM(pop_alloc), 0), 2)
           AS pct_seniors_65_plus,
       ROUND(100.0 * SUM(recent_imm_alloc) / NULLIF(SUM(imm_total_alloc), 0), 2)
           AS pct_recent_immigrants,
       ROUND(SUM(income_alloc) / NULLIF(SUM(pop_alloc), 0), 0)
           AS median_household_income_2020,
       -- Commute duration distribution
       ROUND(100.0 * SUM(dur_lt15_alloc) / NULLIF(SUM(dur_total_alloc), 0), 2)
           AS pct_commute_lt15min,
       ROUND(100.0 * SUM(dur_15_29_alloc) / NULLIF(SUM(dur_total_alloc), 0), 2)
           AS pct_commute_15_29min,
       ROUND(100.0 * SUM(dur_30_44_alloc) / NULLIF(SUM(dur_total_alloc), 0), 2)
           AS pct_commute_30_44min,
       ROUND(100.0 * SUM(dur_45_59_alloc) / NULLIF(SUM(dur_total_alloc), 0), 2)
           AS pct_commute_45_59min,
       ROUND(100.0 * SUM(dur_60_plus_alloc) / NULLIF(SUM(dur_total_alloc), 0), 2)
           AS pct_commute_60_plus_min,
       -- Departure time distribution
       ROUND(100.0 * SUM(depart_5am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_5am,
       ROUND(100.0 * SUM(depart_6am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_6am,
       ROUND(100.0 * SUM(depart_7am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_7am,
       ROUND(100.0 * SUM(depart_8am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_8am,
       ROUND(100.0 * SUM(depart_9_11am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_9_11am,
       ROUND(100.0 * SUM(depart_12_4am_alloc) / NULLIF(SUM(depart_total_alloc), 0), 2)
           AS pct_depart_12_4am,
       -- Mobility
       ROUND(100.0 * SUM(mobility_movers_alloc) / NULLIF(SUM(mobility_total_alloc), 0), 2)
           AS pct_1yr_movers,
       ROUND(100.0 * SUM(mobility_external_alloc) / NULLIF(SUM(mobility_total_alloc), 0), 2)
           AS pct_1yr_external_migrants
FROM allocated
GROUP BY neighbourhood_id, neighbourhood, area_km2
ORDER BY neighbourhood;

-- ═══════════════════════════════════════════════════════════════════════════
-- 7. Frequency and reliability analysis views
--    Depends on materialized tables from sections 3-4 above.
--    ywg_route_reliability_metrics is materialised as a TABLE (single 6.4M row scan).
-- ═══════════════════════════════════════════════════════════════════════════

-- 7a. Materialised reliability table (single scan of 6.4M OTP rows)

CREATE OR REPLACE TABLE ywg_route_reliability_metrics AS
SELECT
    routes.feed_id,
    routes.route_id,
    routes.route_short_name,
    route_tiers.ptn_tier,
    AVG(TRY_CAST(ontime.deviation AS DOUBLE)) AS mean_deviation_sec,
    STDDEV(TRY_CAST(ontime.deviation AS DOUBLE)) AS std_deviation_sec,
    SUM(
        CASE
            WHEN ontime.deviation IS NOT NULL
             AND ABS(TRY_CAST(ontime.deviation AS DOUBLE)) <= 60
                THEN 1
            ELSE 0
        END
    ) * 100.0 / NULLIF(COUNT(ontime.deviation), 0) AS pct_on_time,
    COUNT(ontime.deviation) AS measurement_count
FROM ywg_routes routes
LEFT JOIN ywg_route_ptn_tiers route_tiers
    ON routes.feed_id = route_tiers.feed_id
   AND routes.route_id = route_tiers.route_id
LEFT JOIN ywg_v_ontime_performance ontime
    ON routes.route_short_name = ontime.route_number
   AND ontime.deviation IS NOT NULL
   AND ontime.ptn_era = CASE routes.feed_id
        WHEN 'pre_ptn' THEN 'pre_ptn'
        ELSE 'post_ptn'
       END
GROUP BY routes.feed_id, routes.route_id, routes.route_short_name, route_tiers.ptn_tier;

-- 7b. Reliability-dependent helper views

CREATE OR REPLACE VIEW ywg_route_ontime AS
SELECT
    feed_id,
    route_id,
    route_short_name,
    mean_deviation_sec AS avg_deviation_seconds,
    measurement_count
FROM ywg_route_reliability_metrics;

CREATE OR REPLACE VIEW ywg_route_performance AS
SELECT
    routes.feed_id,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    COALESCE(passups.passup_count, 0) AS passup_count,
    COALESCE(passups.days_with_passups, 0) AS days_with_passups,
    COALESCE(ontime.avg_deviation_seconds, 0.0) AS avg_deviation_seconds,
    COALESCE(ontime.measurement_count, 0) AS ontime_measurements
FROM ywg_routes routes
LEFT JOIN ywg_route_passups passups
    ON routes.feed_id = passups.feed_id
   AND routes.route_id = passups.route_id
LEFT JOIN ywg_route_ontime ontime
    ON routes.feed_id = ontime.feed_id
   AND routes.route_id = ontime.route_id;

-- 7c. Cheap join views (all upstream is materialised)

CREATE OR REPLACE VIEW ywg_route_schedule_facts AS
SELECT
    metrics.feed_id,
    metrics.route_id,
    metrics.route_short_name,
    tiers.ptn_tier,
    metrics.scheduled_trip_count,
    metrics.mean_headway_minutes,
    metrics.min_headway_minutes,
    metrics.max_headway_minutes,
    metrics.scheduled_speed_kmh,
    metrics.mean_trip_distance,
    metrics.mean_trip_duration,
    COALESCE(performance.passup_count, 0) AS passup_count,
    COALESCE(performance.avg_deviation_seconds, 0.0) AS avg_deviation_seconds,
    COALESCE(performance.ontime_measurements, 0) AS ontime_measurements
FROM ywg_route_schedule_metrics metrics
LEFT JOIN ywg_route_ptn_tiers tiers
    ON metrics.feed_id = tiers.feed_id
   AND metrics.route_id = tiers.route_id
LEFT JOIN ywg_route_performance performance
    ON metrics.feed_id = performance.feed_id
   AND metrics.route_id = performance.route_id;

CREATE OR REPLACE VIEW ywg_route_departure_summary AS
SELECT
    feed_id,
    route_id,
    MAX(departures) AS peak_hour_departures,
    SUM(departures) AS daily_departures
FROM ywg_route_hourly_departures
GROUP BY feed_id, route_id;

CREATE OR REPLACE VIEW ywg_route_classification_features AS
SELECT
    facts.feed_id,
    facts.route_id,
    facts.route_short_name,
    facts.ptn_tier,
    facts.scheduled_trip_count,
    facts.mean_headway_minutes,
    facts.min_headway_minutes,
    facts.max_headway_minutes,
    facts.scheduled_speed_kmh,
    facts.mean_trip_distance,
    facts.mean_trip_duration,
    facts.passup_count,
    facts.avg_deviation_seconds,
    facts.ontime_measurements,
    COALESCE(departures.peak_hour_departures, 0) AS peak_hour_departures,
    COALESCE(departures.daily_departures, 0) AS daily_departures
FROM ywg_route_schedule_facts facts
LEFT JOIN ywg_route_departure_summary departures
    ON facts.feed_id = departures.feed_id
   AND facts.route_id = departures.route_id;

-- 7d. Capacity priority view — pure data, no composite scores.
--     upgrade_priority_score and recommendation are computed in Python
--     (FrequencyAnalyzer.build_capacity_priority_table) using a Negative
--     Binomial GLM, replacing the former arbitrary 0.4/0.6 weighted sum.

CREATE OR REPLACE VIEW ywg_route_capacity_priority AS
WITH weekday_boardings AS (
    SELECT
        CAST(route_number AS VARCHAR) AS route_key,
        AVG(TRY_CAST(average_boardings AS DOUBLE)) AS average_boardings
    FROM ywg_passenger_counts
    WHERE day_type = 'Weekday'
      AND average_boardings IS NOT NULL
    GROUP BY route_number
),
route_headways AS (
    SELECT
        feed_id,
        route_id,
        route_short_name AS route_key,
        mean_headway_minutes
    FROM ywg_route_schedule_metrics
    WHERE mean_headway_minutes IS NOT NULL
),
passup_rates AS (
    SELECT
        route_headways.feed_id,
        route_headways.route_id,
        route_headways.route_key,
        route_headways.mean_headway_minutes,
        COUNT(passups.route_number) AS total_passups,
        COALESCE(MAX(weekday_boardings.average_boardings), 0) AS weekday_boardings,
        ROUND(
            COUNT(passups.route_number) * 100000.0 /
            NULLIF(MAX(weekday_boardings.average_boardings), 0),
            1
        ) AS passups_per_100k_boardings
    FROM route_headways
    LEFT JOIN ywg_v_passups passups
        ON route_headways.route_key = passups.route_number
       AND passups.ptn_era = CASE route_headways.feed_id
            WHEN 'pre_ptn' THEN 'pre_ptn'
            ELSE 'post_ptn'
           END
    LEFT JOIN weekday_boardings
        ON route_headways.route_key = weekday_boardings.route_key
    GROUP BY route_headways.feed_id, route_headways.route_id, route_headways.route_key, route_headways.mean_headway_minutes
)
SELECT
    passup_rates.feed_id,
    passup_rates.route_id,
    passup_rates.route_key AS route_short_name,
    route_tiers.ptn_tier,
    passup_rates.total_passups,
    passup_rates.passups_per_100k_boardings,
    passup_rates.weekday_boardings,
    passup_rates.mean_headway_minutes,
    ROUND(60.0 / NULLIF(passup_rates.mean_headway_minutes, 0), 1) AS buses_per_hour,
    ROUND(60.0 / NULLIF(passup_rates.mean_headway_minutes, 0) * 40, 0) AS bus_capacity_low_pphpd,
    ROUND(60.0 / NULLIF(passup_rates.mean_headway_minutes, 0) * 60, 0) AS bus_capacity_high_pphpd,
    ROUND(
        passup_rates.weekday_boardings /
        NULLIF(60.0 / NULLIF(passup_rates.mean_headway_minutes, 0) * 16, 0),
        0
    ) AS demand_pphpd_est,
    reliability.pct_on_time,
    reliability.mean_deviation_sec
FROM passup_rates
LEFT JOIN ywg_route_ptn_tiers route_tiers
    ON passup_rates.feed_id = route_tiers.feed_id
   AND passup_rates.route_id = route_tiers.route_id
LEFT JOIN ywg_route_reliability_metrics reliability
    ON passup_rates.feed_id = reliability.feed_id
   AND passup_rates.route_id = reliability.route_id;

