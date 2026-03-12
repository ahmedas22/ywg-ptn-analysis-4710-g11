-- Database indexes for join-heavy analysis

-- GTFS core tables
CREATE INDEX IF NOT EXISTS idx_trips_route ON trips(route_id);
CREATE INDEX IF NOT EXISTS idx_trips_service ON trips(service_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_trip ON stop_times(trip_id);
CREATE INDEX IF NOT EXISTS idx_stop_times_stop ON stop_times(stop_id);
CREATE INDEX IF NOT EXISTS idx_shapes_shape ON shapes(shape_id);

-- Network tables
CREATE INDEX IF NOT EXISTS idx_connections_from ON stop_connections(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_connections_to ON stop_connections(to_stop_id);
CREATE INDEX IF NOT EXISTS idx_connections_route ON stop_connections(route_id);
CREATE INDEX IF NOT EXISTS idx_weighted_from ON stop_connections_weighted(from_stop_id);
CREATE INDEX IF NOT EXISTS idx_weighted_to ON stop_connections_weighted(to_stop_id);
