"""Census 2021 dissemination-area data and boundary geometry.

Attribute data comes from CHASS Census Analyser (University of Toronto) —
a tracked CSV at ``data/external/chass_census_profile_2021_da.csv`` with 67
census variables including Journey to Work (commute duration, departure time,
destination geography).

Boundary polygons come from Statistics Canada's Digital Boundary Files
WFS (ArcGIS MapServer layer 12 — DA boundaries, 2021 Census).
Digital boundaries follow precise legal/administrative lines (vs Cartographic
which are simplified for display), giving accurate spatial intersects.
"""

from __future__ import annotations

import json
import ssl
import urllib.parse
import urllib.request
from pathlib import Path

import geopandas as gpd
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import CACHE_DATA_DIR, DATA_DIR
from ptn_analysis.context.db import TransitDB

CHASS_CSV: str = str(DATA_DIR / "external" / "chass_census_profile_2021_da.csv")
WINNIPEG_CD_PREFIX: str = "4611"

# StatCan Digital Boundary Files — DA layer (2021 Census)
# Digital = full extent including coastal water; Cartographic = major land mass
# only. For Winnipeg (inland) they're identical. Layer 12 = Dissemination Areas.
STATCAN_DA_WFS_URL: str = (
    "https://geo.statcan.gc.ca/geo_wa/rest/services/2021/"
    "Digital_boundary_files/MapServer/12/query"
)
DA_BOUNDARY_CACHE: Path = CACHE_DATA_DIR / "census" / "da_boundaries_4611.geojson"

# COL → semantic name mapping for all 78 columns in the CHASS CSV.
CHASS_COLUMN_MAP: dict[str, str] = {
    "COL0": "geo_uid",
    "COL1": "prov_code",
    "COL2": "da_name",
    # Population
    "COL3": "population_2021",
    "COL4": "dwellings_occupied",
    "COL5": "pop_density_sqkm",
    "COL6": "land_area_sqkm",
    # Age
    "COL7": "age_total",
    "COL8": "age_0_14",
    "COL9": "age_0_4",
    "COL10": "age_5_9",
    "COL11": "age_10_14",
    "COL12": "age_15_64",
    "COL13": "age_65_plus",
    "COL14": "age_distribution_pct",
    "COL15": "median_age",
    "COL16": "average_age",
    # Income
    "COL17": "median_total_income",
    "COL18": "median_after_tax_income",
    # Low income
    "COL19": "lim_total",
    "COL20": "lim_at_count",
    "COL21": "lim_at_pct",
    "COL22": "lico_total",
    "COL23": "lico_at_count",
    "COL24": "lico_at_pct",
    # Indigenous
    "COL25": "indigenous_total",
    "COL26": "indigenous_count",
    # Visible minority
    "COL27": "vismin_total",
    "COL28": "vismin_count",
    # Immigration
    "COL29": "immigrant_total",
    "COL30": "immigrant_count",
    "COL31": "recent_immigrant_2016_2021",
    "COL32": "non_permanent_residents",
    # Housing tenure
    "COL33": "tenure_total",
    "COL34": "tenure_owner",
    "COL35": "tenure_renter",
    # Housing cost
    "COL36": "shelter_cost_ratio_total",
    "COL37": "shelter_cost_30pct_plus",
    # Rent
    "COL38": "median_rent",
    "COL39": "average_rent",
    # Labour
    "COL40": "participation_rate",
    "COL41": "employment_rate",
    "COL42": "unemployment_rate",
    # Commute destination
    "COL43": "commute_dest_total",
    "COL44": "commute_within_csd",
    "COL45": "commute_diff_csd_same_cd",
    "COL46": "commute_diff_cd",
    "COL47": "commute_diff_province",
    # Commute mode
    "COL48": "commute_mode_total",
    "COL49": "commute_car_total",
    "COL50": "commute_car_driver",
    "COL51": "commute_car_passenger",
    "COL52": "commute_transit",
    "COL53": "commute_walked",
    "COL54": "commute_bicycle",
    "COL55": "commute_other",
    # Commute duration
    "COL56": "commute_dur_total",
    "COL57": "commute_dur_lt15",
    "COL58": "commute_dur_15_29",
    "COL59": "commute_dur_30_44",
    "COL60": "commute_dur_45_59",
    "COL61": "commute_dur_60_plus",
    # Departure time
    "COL62": "depart_total",
    "COL63": "depart_5am",
    "COL64": "depart_6am",
    "COL65": "depart_7am",
    "COL66": "depart_8am",
    "COL67": "depart_9_11am",
    "COL68": "depart_12_4am",
    # Mobility (1-year)
    "COL69": "mobility_1yr_total",
    "COL70": "mobility_1yr_nonmovers",
    "COL71": "mobility_1yr_movers",
    "COL72": "mobility_1yr_nonmigrants",
    "COL73": "mobility_1yr_migrants",
    "COL74": "mobility_1yr_internal",
    "COL75": "mobility_1yr_intraprov",
    "COL76": "mobility_1yr_interprov",
    "COL77": "mobility_1yr_external",
}

# Derived alias columns expected by downstream SQL and analysis modules.
# Maps alias_name → source CHASS column name.
_COLUMN_ALIASES: dict[str, str] = {
    "population_total": "population_2021",
    "commute_total": "commute_mode_total",
    "commute_public_transit": "commute_transit",
    "commute_car_truck_van": "commute_car_total",
    "workplace_total": "commute_dest_total",
    "worked_at_home": "commute_other",
    "immigration_total_households": "immigrant_total",
    "recent_immigrants_2016_2021": "recent_immigrant_2016_2021",
    "median_household_income_2020": "median_total_income",
    "da_uid": "geo_uid",
}


# ---------------------------------------------------------------------------
# DA boundary geometry from StatCan WFS
# ---------------------------------------------------------------------------


def _fetch_da_boundaries(cache_path: Path = DA_BOUNDARY_CACHE) -> gpd.GeoDataFrame:
    """Fetch Winnipeg DA boundary polygons from StatCan WFS (cached to disk).

    Uses the 2021 Cartographic Boundary Files MapServer, layer 12 (DA).
    Filters by ``DAUID LIKE '4611%'`` to get only Winnipeg Census Division DAs.
    """
    if cache_path.exists():
        logger.info(f"Loading cached DA boundaries from {cache_path.name}")
        return gpd.read_file(cache_path)

    logger.info("Fetching DA boundary polygons from Statistics Canada WFS...")
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    params = urllib.parse.urlencode({
        "where": f"DAUID LIKE '{WINNIPEG_CD_PREFIX}%'",
        "outFields": "DAUID",
        "f": "geojson",
        "returnGeometry": "true",
    })
    url = f"{STATCAN_DA_WFS_URL}?{params}"
    req = urllib.request.Request(url)
    req.add_header("User-Agent", "ptn-analysis/1.0 (University of Manitoba COMP 4710)")

    with urllib.request.urlopen(req, timeout=120, context=ctx) as resp:
        data = json.loads(resp.read())

    features = data.get("features", [])
    if not features:
        raise RuntimeError("StatCan WFS returned zero DA boundary features for Winnipeg.")
    logger.info(f"Fetched {len(features)} DA boundary polygons from StatCan WFS")

    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w") as fh:
        json.dump(data, fh)

    return gpd.read_file(cache_path)


def _backfill_da_geometry(city_key: str, db_instance: TransitDB) -> int:
    """Add boundary geometry to the census_da table from StatCan WFS.

    Downloads DA boundary polygons (if not cached), loads them into a temp
    table, then adds a ``geometry`` column to ``{city_key}_census_da`` via
    a JOIN on ``geo_uid = DAUID``.

    Returns the number of DAs that received geometry.
    """
    gdf = _fetch_da_boundaries()
    table_name = db_instance.table_name("census_da", city_key)

    # Load boundaries into a temporary DuckDB table
    db_instance.execute("INSTALL spatial; LOAD spatial;")
    db_instance.execute("DROP TABLE IF EXISTS _tmp_da_boundaries")
    db_instance.load_table("_tmp_da_boundaries", gdf, mode="replace")

    # Add geometry column and backfill from boundaries
    db_instance.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS geometry GEOMETRY")
    db_instance.execute(
        f"""
        UPDATE {table_name} AS census
        SET geometry = boundaries.geometry
        FROM _tmp_da_boundaries AS boundaries
        WHERE census.geo_uid = boundaries.DAUID
        """
    )
    db_instance.execute("DROP TABLE IF EXISTS _tmp_da_boundaries")

    matched = db_instance.query(
        f"SELECT COUNT(*) AS n FROM {table_name} WHERE geometry IS NOT NULL"
    )
    count = int(matched.iloc[0]["n"])
    logger.info(f"Backfilled geometry for {count} DAs in {table_name}")
    return count


# ---------------------------------------------------------------------------
# Main census loading function
# ---------------------------------------------------------------------------


def load_dissemination_areas(city_key: str, db_instance: TransitDB) -> dict[str, int]:
    """Load census DA attributes from CHASS CSV and geometry from StatCan WFS.

    1. Reads ``data/external/chass_census_profile_2021_da.csv``
    2. Filters for Winnipeg DAs (GeoUID starting with ``4611``)
    3. Computes derived percentage columns
    4. Loads into ``{city_key}_census_da``
    5. Backfills DA boundary geometry from StatCan Cartographic Boundary WFS

    Args:
        city_key: City namespace prefix (e.g. ``"ywg"``).
        db_instance: TransitDB to write into.

    Returns:
        Dictionary with loaded row counts.
    """
    csv_path = Path(CHASS_CSV)
    if not csv_path.exists():
        raise FileNotFoundError(
            f"CHASS Census CSV not found at {csv_path}. "
            f"Place the file in data/external/chass_census_profile_2021_da.csv."
        )

    logger.info(f"Loading census DAs from CHASS CSV: {csv_path.name}")
    raw_df = pd.read_csv(csv_path, dtype={"COL0": str, "COL1": str, "COL2": str})
    raw_df = raw_df.rename(columns=CHASS_COLUMN_MAP)

    # Filter for Winnipeg DAs only (8-digit GeoUIDs starting with 4611).
    winnipeg_mask = (
        raw_df["geo_uid"].str.startswith(WINNIPEG_CD_PREFIX)
        & (raw_df["geo_uid"].str.len() == 8)
    )
    census_df = raw_df[winnipeg_mask].copy()
    logger.info(f"Filtered {len(census_df)} Winnipeg DAs from {len(raw_df)} total rows")

    # Derived percentage columns.
    census_df["pct_transit_commute"] = (
        census_df["commute_transit"]
        .div(census_df["commute_mode_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_car_commute"] = (
        census_df["commute_car_total"]
        .div(census_df["commute_mode_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_renter"] = (
        census_df["tenure_renter"]
        .div(census_df["tenure_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_indigenous"] = (
        census_df["indigenous_count"]
        .div(census_df["indigenous_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_visible_minority"] = (
        census_df["vismin_count"]
        .div(census_df["vismin_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_immigrant"] = (
        census_df["immigrant_count"]
        .div(census_df["immigrant_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_walk_commute"] = (
        census_df["commute_walked"]
        .div(census_df["commute_mode_total"])
        .mul(100)
        .round(1)
    )
    census_df["pct_bicycle_commute"] = (
        census_df["commute_bicycle"]
        .div(census_df["commute_mode_total"])
        .mul(100)
        .round(1)
    )

    # Derived alias columns used by downstream SQL and analysis modules.
    for old_name, new_name in _COLUMN_ALIASES.items():
        if new_name in census_df.columns and old_name not in census_df.columns:
            census_df[old_name] = census_df[new_name]

    table_name = db_instance.table_name("census_da", city_key)
    db_instance.load_table(table_name, census_df, mode="replace")
    logger.info(f"Loaded {len(census_df)} census DAs into {table_name}")

    # Backfill DA boundary geometry from StatCan WFS.
    geom_count = _backfill_da_geometry(city_key, db_instance)

    return {"census_da": len(census_df), "census_da_with_geometry": geom_count}
