-- PTN Analysis Database Schema
-- Clean table names without prefixes

-- =============================================================================
-- GTFS CORE TABLES
-- =============================================================================

DROP TABLE IF EXISTS stops;
CREATE TABLE stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT,
    stop_lat DOUBLE,
    stop_lon DOUBLE
);

DROP TABLE IF EXISTS routes;
CREATE TABLE routes (
    route_id TEXT PRIMARY KEY,
    route_short_name TEXT,
    route_long_name TEXT,
    route_type INTEGER
);

DROP TABLE IF EXISTS trips;
CREATE TABLE trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT,
    service_id TEXT,
    trip_headsign TEXT,
    direction_id INTEGER
);

DROP TABLE IF EXISTS stop_times;
CREATE TABLE stop_times (
    trip_id TEXT,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT,
    stop_sequence INTEGER,
    PRIMARY KEY (trip_id, stop_sequence)
);

DROP TABLE IF EXISTS calendar;
CREATE TABLE calendar (
    service_id TEXT PRIMARY KEY,
    sunday INTEGER NOT NULL,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE NOT NULL
);

DROP TABLE IF EXISTS calendar_dates;
CREATE TABLE calendar_dates (
    service_id TEXT NOT NULL,
    date DATE NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (service_id, date)
);

DROP TABLE IF EXISTS shapes;
CREATE TABLE shapes (
    shape_id TEXT,
    shape_pt_lat DOUBLE,
    shape_pt_lon DOUBLE,
    shape_pt_sequence INTEGER,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

DROP TABLE IF EXISTS feed_info;
CREATE TABLE feed_info (
    feed_publisher_name TEXT,
    feed_publisher_url TEXT,
    feed_lang TEXT,
    feed_contact_email TEXT,
    feed_start_date TEXT,
    feed_end_date TEXT
);

-- =============================================================================
-- DERIVED NETWORK TABLE
-- =============================================================================

-- stop_connections is built by build_edges.sql

-- =============================================================================
-- SPATIAL TABLES (created dynamically from GeoJSON)
-- =============================================================================

-- neighbourhoods and community_areas are created by ingest.py load_boundaries()

-- =============================================================================
-- OPERATIONAL DATA TABLES (created dynamically from GeoJSON)
-- =============================================================================

-- passups, ontime_performance, and passenger_counts are created by ingest.py load_all_open_data()
