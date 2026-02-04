CREATE OR REPLACE TABLE agg_route_passups_summary AS
SELECT
    route_number AS route_id,
    COUNT(*) AS passup_count,
    COUNT(DISTINCT DATE(time)) AS days_with_passups
FROM raw_open_data_pass_ups
WHERE route_number IS NOT NULL
GROUP BY route_number
ORDER BY passup_count DESC;

CREATE OR REPLACE TABLE agg_route_ontime_summary AS
SELECT
    route_number AS route_id,
    AVG(CAST(deviation AS DOUBLE)) AS avg_deviation_seconds,
    COUNT(*) AS measurement_count
FROM raw_open_data_on_time
WHERE route_number IS NOT NULL AND deviation IS NOT NULL
GROUP BY route_number
ORDER BY avg_deviation_seconds;

CREATE OR REPLACE TABLE agg_stop_ontime_summary AS
SELECT
    stop_number,
    AVG(CAST(deviation AS DOUBLE)) AS avg_deviation_seconds,
    COUNT(*) AS measurement_count
FROM raw_open_data_on_time
WHERE stop_number IS NOT NULL AND deviation IS NOT NULL
GROUP BY stop_number
ORDER BY measurement_count DESC;

CREATE INDEX IF NOT EXISTS idx_passups_route ON agg_route_passups_summary(route_id);
CREATE INDEX IF NOT EXISTS idx_ontime_route ON agg_route_ontime_summary(route_id);
CREATE INDEX IF NOT EXISTS idx_ontime_stop ON agg_stop_ontime_summary(stop_number);
