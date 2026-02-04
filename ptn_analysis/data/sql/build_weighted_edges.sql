CREATE OR REPLACE TABLE raw_gtfs_edges_weighted AS
SELECT
    from_stop_id,
    to_stop_id,
    COUNT(*) AS trip_count,
    COUNT(DISTINCT route_id) AS route_count,
    LIST(DISTINCT route_id) AS routes
FROM raw_gtfs_edges
GROUP BY from_stop_id, to_stop_id;

CREATE INDEX IF NOT EXISTS idx_weighted_from ON raw_gtfs_edges_weighted(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_weighted_to ON raw_gtfs_edges_weighted(to_stop_id);
