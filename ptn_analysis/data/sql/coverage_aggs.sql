CREATE OR REPLACE TABLE agg_stops_per_neighbourhood AS
SELECT
    n.name AS neighbourhood,
    n.area_km2,
    COUNT(s.stop_id) AS stop_count,
    CASE WHEN n.area_km2 > 0 THEN COUNT(s.stop_id) / n.area_km2 ELSE 0 END AS stops_per_km2
FROM raw_neighbourhoods n
LEFT JOIN raw_gtfs_stops s ON ST_Contains(
    n.geometry,
    ST_Point(s.stop_lon, s.stop_lat)
)
GROUP BY n.name, n.area_km2
ORDER BY stop_count DESC;

CREATE OR REPLACE TABLE agg_stops_per_community AS
SELECT
    c.name AS community,
    c.area_km2,
    COUNT(s.stop_id) AS stop_count,
    CASE WHEN c.area_km2 > 0 THEN COUNT(s.stop_id) / c.area_km2 ELSE 0 END AS stops_per_km2
FROM raw_community_areas c
LEFT JOIN raw_gtfs_stops s ON ST_Contains(
    c.geometry,
    ST_Point(s.stop_lon, s.stop_lat)
)
GROUP BY c.name, c.area_km2
ORDER BY stop_count DESC;
