-- PTN Analysis Views
-- Computed aggregations as views instead of materialized tables

-- =============================================================================
-- COVERAGE VIEWS
-- =============================================================================

CREATE OR REPLACE VIEW neighbourhood_coverage AS
SELECT
    n.id AS neighbourhood_id,
    n.name AS neighbourhood,
    n.area_km2,
    COUNT(DISTINCT s.stop_id) AS stop_count,
    COUNT(DISTINCT s.stop_id) / NULLIF(n.area_km2, 0) AS stops_per_km2
FROM neighbourhoods n
LEFT JOIN stops s
    ON ST_Contains(n.geometry, ST_Point(s.stop_lon, s.stop_lat))
GROUP BY n.id, n.name, n.area_km2;

CREATE OR REPLACE VIEW community_coverage AS
SELECT
    c.id AS community_id,
    c.name AS community,
    c.area_km2,
    COUNT(DISTINCT s.stop_id) AS stop_count,
    COUNT(DISTINCT s.stop_id) / NULLIF(c.area_km2, 0) AS stops_per_km2
FROM community_areas c
LEFT JOIN stops s
    ON ST_Contains(c.geometry, ST_Point(s.stop_lon, s.stop_lat))
GROUP BY c.id, c.name, c.area_km2;

-- =============================================================================
-- PERFORMANCE VIEWS
-- =============================================================================

-- Pass-ups aggregated by route
CREATE OR REPLACE VIEW route_passups AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    COUNT(p.pass_up_id) AS passup_count,
    COUNT(DISTINCT DATE_TRUNC('day', TRY_CAST(p.time AS TIMESTAMP))) AS days_with_passups
FROM routes r
LEFT JOIN passups p
    ON CAST(r.route_short_name AS VARCHAR) = CAST(p.route_number AS VARCHAR)
GROUP BY r.route_id, r.route_short_name, r.route_long_name;

-- On-time performance aggregated by route
CREATE OR REPLACE VIEW route_ontime AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    AVG(TRY_CAST(o.deviation AS DOUBLE)) AS avg_deviation_seconds,
    COUNT(*) AS measurement_count
FROM routes r
LEFT JOIN ontime_performance o
    ON CAST(r.route_short_name AS VARCHAR) = CAST(o.route_number AS VARCHAR)
WHERE o.deviation IS NOT NULL
GROUP BY r.route_id, r.route_short_name, r.route_long_name;

-- Combined route performance view
CREATE OR REPLACE VIEW route_performance AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COALESCE(p.passup_count, 0) AS passup_count,
    COALESCE(p.days_with_passups, 0) AS days_with_passups,
    o.avg_deviation_seconds,
    COALESCE(o.measurement_count, 0) AS ontime_measurements
FROM routes r
LEFT JOIN route_passups p ON r.route_id = p.route_id
LEFT JOIN route_ontime o ON r.route_id = o.route_id;

-- =============================================================================
-- FREQUENCY VIEWS
-- =============================================================================

-- Hourly departures by route (for frequency analysis)
CREATE OR REPLACE VIEW hourly_departures_by_route AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) % 24 AS hour,
    COUNT(DISTINCT st.trip_id) AS departures
FROM stop_times st
JOIN trips t ON st.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
WHERE st.stop_sequence = 1
GROUP BY r.route_id, r.route_short_name, r.route_long_name, hour;
