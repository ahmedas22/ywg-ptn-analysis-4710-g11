DROP VIEW IF EXISTS v_route_performance;
CREATE VIEW v_route_performance AS
SELECT
    r.route_id,
    r.route_short_name,
    r.route_long_name,
    r.route_type,
    COALESCE(p.passup_count, 0) AS passup_count,
    COALESCE(p.days_with_passups, 0) AS days_with_passups,
    o.avg_deviation_seconds,
    COALESCE(o.measurement_count, 0) AS ontime_measurements
FROM ref_route_mapping r
LEFT JOIN agg_route_passups_summary p
    ON r.route_number_norm = UPPER(TRIM(CAST(p.route_id AS TEXT)))
LEFT JOIN agg_route_ontime_summary o
    ON r.route_number_norm = UPPER(TRIM(CAST(o.route_id AS TEXT)));

DROP VIEW IF EXISTS v_stop_performance;
CREATE VIEW v_stop_performance AS
SELECT
    s.stop_id,
    s.stop_code,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    pc.average_boardings,
    pc.average_alightings,
    pc.time_period,
    pc.day_type,
    os.avg_deviation_seconds,
    COALESCE(os.measurement_count, 0) AS ontime_measurements
FROM ref_stop_mapping s
LEFT JOIN raw_open_data_passenger_counts pc
    ON s.stop_number_norm = UPPER(TRIM(CAST(pc.stop_number AS TEXT)))
LEFT JOIN agg_stop_ontime_summary os
    ON s.stop_number_norm = UPPER(TRIM(CAST(os.stop_number AS TEXT)));
