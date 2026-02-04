CREATE OR REPLACE TABLE ref_route_mapping AS
SELECT DISTINCT
    route_id,
    route_short_name,
    UPPER(TRIM(route_short_name)) AS route_number_norm,
    route_long_name,
    route_type
FROM raw_gtfs_routes
WHERE route_short_name IS NOT NULL;

CREATE OR REPLACE TABLE ref_stop_mapping AS
SELECT DISTINCT
    stop_id,
    stop_code,
    UPPER(TRIM(stop_code)) AS stop_number_norm,
    stop_name,
    stop_lat,
    stop_lon
FROM raw_gtfs_stops
WHERE stop_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_route_map_norm ON ref_route_mapping(route_number_norm);
CREATE INDEX IF NOT EXISTS idx_stop_map_norm ON ref_stop_mapping(stop_number_norm);
