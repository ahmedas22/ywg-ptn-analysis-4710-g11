CREATE OR REPLACE TABLE raw_gtfs_edges AS
WITH ordered_stops AS (
    SELECT
        st.trip_id,
        st.stop_id,
        st.stop_sequence,
        st.departure_time,
        st.arrival_time,
        t.route_id,
        LEAD(st.stop_id) OVER (
            PARTITION BY st.trip_id
            ORDER BY st.stop_sequence
        ) AS next_stop_id,
        LEAD(st.arrival_time) OVER (
            PARTITION BY st.trip_id
            ORDER BY st.stop_sequence
        ) AS next_arrival_time
    FROM raw_gtfs_stop_times st
    JOIN raw_gtfs_trips t ON st.trip_id = t.trip_id
)
SELECT
    stop_id AS from_stop_id,
    next_stop_id AS to_stop_id,
    trip_id,
    route_id,
    departure_time,
    next_arrival_time AS arrival_time
FROM ordered_stops
WHERE next_stop_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_edges_from_stop ON raw_gtfs_edges(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_edges_to_stop ON raw_gtfs_edges(to_stop_id);
CREATE INDEX IF NOT EXISTS idx_edges_route ON raw_gtfs_edges(route_id);
