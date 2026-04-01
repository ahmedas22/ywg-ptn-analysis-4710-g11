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

-- 4b. Route passups — era-corrected join via feed_regime_registry.
--     Uses registry to determine era for dated and synthetic feeds.
CREATE OR REPLACE TABLE ywg_route_passups AS
SELECT
    routes.feed_id,
    routes.route_id,
    routes.route_short_name,
    routes.route_long_name,
    COUNT(passups.route_number) AS passup_count,
    COUNT(DISTINCT DATE_TRUNC('day', TRY_CAST(passups.time AS TIMESTAMP))) AS days_with_passups
FROM ywg_routes routes
LEFT JOIN ywg_feed_regime_registry frr
    ON routes.feed_id = frr.feed_id
LEFT JOIN ywg_v_passups passups
    ON routes.route_short_name = passups.route_number
   AND passups.ptn_era = CASE WHEN frr.era_label = 'current' THEN 'post_ptn' ELSE COALESCE(frr.era_label, 'post_ptn') END
GROUP BY routes.feed_id, routes.route_id, routes.route_short_name, routes.route_long_name;

-- 4c. Hourly departures — scans stop_times once, cheap aggregation downstream.
-- Mobility views: frequency, reliability, era-corrected passup/OTP joins
-- Section 7 from original views.sql
-- Requires: ywg_passups, ywg_ontime_performance, ywg_passenger_counts

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
LEFT JOIN ywg_feed_regime_registry frr
    ON routes.feed_id = frr.feed_id
LEFT JOIN ywg_v_ontime_performance ontime
    ON routes.route_short_name = ontime.route_number
   AND ontime.deviation IS NOT NULL
   AND ontime.ptn_era = CASE WHEN frr.era_label = 'current' THEN 'post_ptn' ELSE COALESCE(frr.era_label, 'post_ptn') END
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
    LEFT JOIN ywg_feed_regime_registry frr
        ON route_headways.feed_id = frr.feed_id
    LEFT JOIN ywg_v_passups passups
        ON route_headways.route_key = passups.route_number
       AND passups.ptn_era = CASE WHEN frr.era_label = 'current' THEN 'post_ptn' ELSE COALESCE(frr.era_label, 'post_ptn') END
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

