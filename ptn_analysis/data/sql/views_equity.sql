-- Equity views: poverty overlay, policy alignment, housing growth
-- Section 8 from original views.sql
-- Requires: ywg_census_poverty_2021, ywg_poverty_mbm, ywg_ourwpg_*, ywg_development_permits

-- ═══════════════════════════════════════════════════════════════════════════
-- 8. Poverty and equity derived tables (Final Report)
-- ═══════════════════════════════════════════════════════════════════════════

-- 8a. Neighbourhood poverty overlay (LIM-AT census poverty zones)
CREATE OR REPLACE VIEW ywg_neighbourhood_poverty_overlay AS
SELECT
    c.neighbourhood_id,
    c.neighbourhood,
    c.population_total,
    c.median_household_income_2020,
    c.pct_commute_public_transit,
    c.pct_seniors_65_plus,
    c.pct_recent_immigrants,
    CASE WHEN p.poverty_zone_count > 0 THEN TRUE ELSE FALSE END AS has_limat_poverty,
    COALESCE(p.poverty_zone_count, 0) AS limat_zone_count,
    CASE WHEN m.mbm_zone_count > 0 THEN TRUE ELSE FALSE END AS has_mbm_poverty,
    COALESCE(m.mbm_zone_count, 0) AS mbm_zone_count
FROM ywg_census_by_neighbourhood c
LEFT JOIN (
    SELECT n.id AS neighbourhood_id, COUNT(pov.geometry) AS poverty_zone_count
    FROM ywg_neighbourhoods n
    LEFT JOIN ywg_census_poverty_2021 pov ON ST_Intersects(n.geometry, pov.geometry)
    GROUP BY n.id
) p ON c.neighbourhood_id = p.neighbourhood_id
LEFT JOIN (
    SELECT n.id AS neighbourhood_id, COUNT(mbm.geometry) AS mbm_zone_count
    FROM ywg_neighbourhoods n
    LEFT JOIN ywg_poverty_mbm mbm ON ST_Intersects(n.geometry, mbm.geometry)
    GROUP BY n.id
) m ON c.neighbourhood_id = m.neighbourhood_id;

-- 8b. Stop-policy alignment: stops within 400m of OurWPG planning zones
CREATE OR REPLACE TABLE ywg_stop_policy_alignment AS
SELECT
    s.feed_id,
    s.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    COALESCE(c.corridor_count, 0) AS ourwpg_corridor_count,
    COALESCE(r.redev_count, 0) AS ourwpg_redev_count,
    COALESCE(mc.mature_count, 0) AS ourwpg_mature_count,
    COALESCE(rc.centre_count, 0) AS ourwpg_centre_count,
    CASE WHEN COALESCE(c.corridor_count, 0) + COALESCE(r.redev_count, 0)
              + COALESCE(mc.mature_count, 0) + COALESCE(rc.centre_count, 0) > 0
         THEN TRUE ELSE FALSE END AS in_ourwpg_zone
FROM ywg_stops s
LEFT JOIN (
    SELECT s2.feed_id, s2.stop_id, COUNT(*) AS corridor_count
    FROM ywg_stops s2
    JOIN ywg_ourwpg_mixed_use_corridors z
        ON ST_DWithin(
            ST_Transform(ST_Point(s2.stop_lon, s2.stop_lat), 'EPSG:4326', 'EPSG:32614'),
            ST_Transform(z.geometry, 'EPSG:4326', 'EPSG:32614'),
            400
        )
    GROUP BY s2.feed_id, s2.stop_id
) c ON s.feed_id = c.feed_id AND s.stop_id = c.stop_id
LEFT JOIN (
    SELECT s2.feed_id, s2.stop_id, COUNT(*) AS redev_count
    FROM ywg_stops s2
    JOIN ywg_ourwpg_major_redev_sites z
        ON ST_DWithin(
            ST_Transform(ST_Point(s2.stop_lon, s2.stop_lat), 'EPSG:4326', 'EPSG:32614'),
            ST_Transform(z.geometry, 'EPSG:4326', 'EPSG:32614'),
            400
        )
    GROUP BY s2.feed_id, s2.stop_id
) r ON s.feed_id = r.feed_id AND s.stop_id = r.stop_id
LEFT JOIN (
    SELECT s2.feed_id, s2.stop_id, COUNT(*) AS mature_count
    FROM ywg_stops s2
    JOIN ywg_ourwpg_mature_communities z
        ON ST_DWithin(
            ST_Transform(ST_Point(s2.stop_lon, s2.stop_lat), 'EPSG:4326', 'EPSG:32614'),
            ST_Transform(z.geometry, 'EPSG:4326', 'EPSG:32614'),
            400
        )
    GROUP BY s2.feed_id, s2.stop_id
) mc ON s.feed_id = mc.feed_id AND s.stop_id = mc.stop_id
LEFT JOIN (
    SELECT s2.feed_id, s2.stop_id, COUNT(*) AS centre_count
    FROM ywg_stops s2
    JOIN ywg_ourwpg_regional_centres z
        ON ST_DWithin(
            ST_Transform(ST_Point(s2.stop_lon, s2.stop_lat), 'EPSG:4326', 'EPSG:32614'),
            ST_Transform(z.geometry, 'EPSG:4326', 'EPSG:32614'),
            400
        )
    GROUP BY s2.feed_id, s2.stop_id
) rc ON s.feed_id = rc.feed_id AND s.stop_id = rc.stop_id
WHERE s.feed_id = 'current';

-- 8c. Stop-housing growth: development permits near transit stops
CREATE OR REPLACE TABLE ywg_stop_housing_growth AS
SELECT
    s.feed_id,
    s.stop_id,
    s.stop_name,
    COUNT(dp.geometry) AS permit_count,
    SUM(TRY_CAST(dp.dwelling_units_created AS INTEGER)) AS total_units_created
FROM ywg_stops s
LEFT JOIN ywg_development_permits dp
    ON ST_DWithin(
        ST_Transform(ST_Point(s.stop_lon, s.stop_lat), 'EPSG:4326', 'EPSG:32614'),
        ST_Transform(dp.geometry, 'EPSG:4326', 'EPSG:32614'),
        400
    )
    AND dp.status = 'Issued'
    AND TRY_CAST(dp.dwelling_units_created AS INTEGER) > 0
WHERE s.feed_id = 'current'
GROUP BY s.feed_id, s.stop_id, s.stop_name;

