CREATE OR REPLACE TABLE agg_active_trips AS
WITH regular_service AS (
    SELECT t.trip_id, t.route_id, t.service_id, t.trip_headsign, t.direction_id
    FROM raw_gtfs_trips t
    JOIN raw_gtfs_calendar c ON t.service_id = c.service_id
WHERE c.{{day_column}} = 1
      AND DATE '{{date_gtfs}}' >= c.start_date
      AND DATE '{{date_gtfs}}' <= c.end_date
),
service_removed AS (
    SELECT service_id
    FROM raw_gtfs_calendar_dates
WHERE date = DATE '{{date_gtfs}}'
      AND exception_type = 2
),
service_added AS (
    SELECT t.trip_id, t.route_id, t.service_id, t.trip_headsign, t.direction_id
    FROM raw_gtfs_trips t
    JOIN raw_gtfs_calendar_dates cd ON t.service_id = cd.service_id
WHERE cd.date = DATE '{{date_gtfs}}'
      AND cd.exception_type = 1
)
SELECT DISTINCT trip_id, route_id, service_id, trip_headsign, direction_id,
       '{{target_date}}' AS service_date
FROM (
    SELECT * FROM regular_service
    WHERE service_id NOT IN (SELECT service_id FROM service_removed)
    UNION
    SELECT * FROM service_added
) combined;

CREATE INDEX IF NOT EXISTS idx_active_route ON agg_active_trips(route_id);
CREATE INDEX IF NOT EXISTS idx_active_service ON agg_active_trips(service_id);
