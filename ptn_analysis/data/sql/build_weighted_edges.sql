-- Build weighted connections (aggregated by stop pair)

CREATE OR REPLACE TABLE stop_connections_weighted AS
SELECT
    from_stop_id,
    to_stop_id,
    COUNT(*) AS trip_count,
    COUNT(DISTINCT route_id) AS route_count,
    LIST(DISTINCT route_id) AS routes
FROM stop_connections
GROUP BY from_stop_id, to_stop_id;
