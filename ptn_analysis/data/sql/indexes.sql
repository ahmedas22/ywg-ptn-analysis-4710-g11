CREATE INDEX IF NOT EXISTS idx_ywg_trips_feed_route ON ywg_trips(feed_id, route_id);
CREATE INDEX IF NOT EXISTS idx_ywg_trips_feed_service ON ywg_trips(feed_id, service_id);
CREATE INDEX IF NOT EXISTS idx_ywg_stop_times_feed_trip ON ywg_stop_times(feed_id, trip_id);
CREATE INDEX IF NOT EXISTS idx_ywg_stop_times_feed_stop ON ywg_stop_times(feed_id, stop_id);
CREATE INDEX IF NOT EXISTS idx_ywg_shapes_feed_shape ON ywg_shapes(feed_id, shape_id);
CREATE INDEX IF NOT EXISTS idx_ywg_stops_feed ON ywg_stops(feed_id);
CREATE INDEX IF NOT EXISTS idx_ywg_routes_feed ON ywg_routes(feed_id);
CREATE INDEX IF NOT EXISTS idx_ywg_connection_counts_feed_from ON ywg_stop_connection_counts(feed_id, from_stop_id);
CREATE INDEX IF NOT EXISTS idx_ywg_connection_counts_feed_to ON ywg_stop_connection_counts(feed_id, to_stop_id);
CREATE INDEX IF NOT EXISTS idx_ywg_route_stats_feed_route ON ywg_gtfs_route_stats(feed_id, route_id);
CREATE INDEX IF NOT EXISTS idx_ywg_route_stats_feed_date ON ywg_gtfs_route_stats(feed_id, date);
CREATE INDEX IF NOT EXISTS idx_ywg_stop_stats_feed_stop ON ywg_gtfs_stop_stats(feed_id, stop_id);
CREATE INDEX IF NOT EXISTS idx_ywg_neighbourhood_geom ON ywg_neighbourhoods USING RTREE (geometry);
CREATE INDEX IF NOT EXISTS idx_ywg_community_geom ON ywg_community_areas USING RTREE (geometry);
CREATE INDEX IF NOT EXISTS idx_ywg_passups_route_number ON ywg_passups(route_number);
CREATE INDEX IF NOT EXISTS idx_ywg_ontime_performance_route_number ON ywg_ontime_performance(route_number);
CREATE INDEX IF NOT EXISTS idx_ywg_passenger_counts_route_number ON ywg_passenger_counts(route_number);

-- Stop spatial index: add geometry column and RTREE for fast spatial queries
ALTER TABLE ywg_stops ADD COLUMN IF NOT EXISTS geom GEOMETRY;
UPDATE ywg_stops SET geom = ST_Point(stop_lon, stop_lat) WHERE geom IS NULL;
CREATE INDEX IF NOT EXISTS idx_ywg_stops_geom ON ywg_stops USING RTREE (geom);
