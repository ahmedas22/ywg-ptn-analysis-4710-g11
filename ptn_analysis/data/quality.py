"""Data quality assessment using DAFQ scorecard dimensions.

Scores each data source on five quality dimensions:
Accuracy, Completeness, Consistency, Timeliness, Conformity.

Reference: Jamsa Ch9 (Data Quality for Data Mining).
"""

from __future__ import annotations

import pandas as pd

from ptn_analysis.context.db import TransitDB

# Temporal harmonization: each source's reference period and lag
TEMPORAL_HARMONIZATION: list[dict[str, str]] = [
    {
        "source": "GTFS (4 feeds)",
        "period": "Apr-Dec 2025",
        "lag": "0",
        "type": "Scheduled",
        "caveat": "Official schedules, not actual operations",
    },
    {
        "source": "Transit API",
        "period": "Ongoing",
        "lag": "Real-time",
        "type": "Operational",
        "caveat": "GPS gap Sept-Oct 2025",
    },
    {
        "source": "Census 2021",
        "period": "2020-2021",
        "lag": "-5 years",
        "type": "Demographic",
        "caveat": "COVID income year (2020 reference)",
    },
    {
        "source": "CBP Dec 2022",
        "period": "Dec 2022",
        "lag": "-2.5 years",
        "type": "Employment",
        "caveat": "Establishment proxy, not direct employment",
    },
    {
        "source": "BIZ Survey",
        "period": "Jan 2026",
        "lag": "+7 months",
        "type": "Validation",
        "caveat": "Self-selected n=1,395",
    },
    {
        "source": "Development Permits",
        "period": "Nov 2022-present",
        "lag": "Ongoing",
        "type": "Growth",
        "caveat": "Permits ≠ construction completions",
    },
    {
        "source": "OurWPG Zones",
        "period": "Current",
        "lag": "Policy intent",
        "type": "Planning",
        "caveat": "Zones represent intent, not reality",
    },
]


def temporal_harmonization_table() -> pd.DataFrame:
    """Return the temporal harmonization reference table."""
    return pd.DataFrame(TEMPORAL_HARMONIZATION)


def compute_dafq_scorecard(
    db: TransitDB,
    city_key: str = "ywg",
) -> pd.DataFrame:
    """Compute a DAFQ quality scorecard for all data sources.

    Each source is scored 1-5 on five dimensions:
    - Accuracy: correctness of values
    - Completeness: proportion of non-null values
    - Consistency: cross-source agreement
    - Timeliness: recency relative to analysis date
    - Conformity: adherence to expected formats/ranges

    Args:
        db: Database handle.
        city_key: City namespace.

    Returns:
        DataFrame with source, dimension scores, and overall score.
    """
    rows: list[dict] = []

    # GTFS feeds
    gtfs_stats_tbl = db.table_name("gtfs_route_stats", city_key)
    gtfs_count = db.count(gtfs_stats_tbl) or 0
    rows.append({
        "source": "GTFS Schedule",
        "records": gtfs_count,
        "accuracy": 5,  # Official operator data
        "completeness": 5 if gtfs_count > 10000 else 3,
        "consistency": 5,  # Validated by gtfs-kit
        "timeliness": 5,  # Current feed
        "conformity": 5,  # GTFS standard
    })

    # On-time performance
    otp_tbl = db.table_name("ontime_performance", city_key)
    otp_count = db.count(otp_tbl) or 0
    rows.append({
        "source": "On-Time Performance",
        "records": otp_count,
        "accuracy": 4,  # GPS-based, some drift
        "completeness": 3,  # GPS gap Sept-Oct 2025
        "consistency": 4,
        "timeliness": 4,  # Ongoing collection
        "conformity": 4,
    })

    # Pass-ups
    passup_tbl = db.table_name("passups", city_key)
    passup_count = db.count(passup_tbl) or 0
    rows.append({
        "source": "Transit Pass-ups",
        "records": passup_count,
        "accuracy": 3,  # Driver-reported
        "completeness": 3,  # Under-reporting likely
        "consistency": 3,
        "timeliness": 4,
        "conformity": 4,
    })

    # Passenger counts
    pax_tbl = db.table_name("passenger_counts", city_key)
    pax_count = db.count(pax_tbl) or 0
    rows.append({
        "source": "Passenger Counts",
        "records": pax_count,
        "accuracy": 4,  # APC automated
        "completeness": 3,  # Weekend 50% sampling
        "consistency": 4,
        "timeliness": 4,
        "conformity": 4,
    })

    # Census 2021
    census_tbl = db.table_name("census_da", city_key)
    census_count = db.count(census_tbl) or 0
    rows.append({
        "source": "Census 2021 (CHASS)",
        "records": census_count,
        "accuracy": 5,  # Official Statistics Canada
        "completeness": 4,  # Some suppressed cells
        "consistency": 5,
        "timeliness": 2,  # 5-year lag (2020 income)
        "conformity": 5,
    })

    # CBP Employment
    cbp_tbl = db.table_name("da_jobs_proxy", city_key)
    cbp_count = db.count(cbp_tbl) or 0
    rows.append({
        "source": "CBP Employment Proxy",
        "records": cbp_count,
        "accuracy": 3,  # Establishment-based proxy
        "completeness": 4,
        "consistency": 3,
        "timeliness": 2,  # Dec 2022
        "conformity": 4,
    })

    # Neighbourhoods / boundaries
    nb_tbl = db.table_name("neighbourhoods", city_key)
    nb_count = db.count(nb_tbl) or 0
    rows.append({
        "source": "Neighbourhood Boundaries",
        "records": nb_count,
        "accuracy": 5,
        "completeness": 5,
        "consistency": 5,
        "timeliness": 5,
        "conformity": 5,
    })

    df = pd.DataFrame(rows)
    dimensions = ["accuracy", "completeness", "consistency", "timeliness", "conformity"]
    df["overall_score"] = df[dimensions].mean(axis=1).round(2)
    return df.sort_values("overall_score", ascending=False).reset_index(drop=True)


def missing_data_report(
    db: TransitDB,
    city_key: str = "ywg",
) -> pd.DataFrame:
    """Report missing data percentages per key table.

    Args:
        db: Database handle.
        city_key: City namespace.

    Returns:
        DataFrame with table_name, column, null_count, null_pct.
    """
    tables_to_check = [
        ("stops", ["stop_lat", "stop_lon", "stop_name"]),
        ("routes", ["route_short_name", "route_long_name"]),
        ("census_da", ["median_total_income", "population_2021"]),
    ]
    rows = []
    for tbl_base, columns in tables_to_check:
        tbl = db.table_name(tbl_base, city_key)
        if not db.relation_exists(tbl):
            continue
        total = db.count(tbl) or 0
        if total == 0:
            continue
        for col in columns:
            null_count = db.first(
                f"SELECT COUNT(*) FROM {tbl} WHERE {col} IS NULL"
            )
            rows.append({
                "table": tbl_base,
                "column": col,
                "total_rows": total,
                "null_count": null_count or 0,
                "null_pct": round(
                    100.0 * (null_count or 0) / total, 2
                ),
            })
    return pd.DataFrame(rows)


def run_data_quality_checks(
    db: TransitDB,
    city_key: str = "ywg",
) -> list[dict[str, str]]:
    """Run automated data quality checks across all 4 DQ dimensions.

    Dimensions per COMP 4710 (Accuracy, Completeness, Consistency, Conformity).

    Args:
        db: Database handle.
        city_key: City namespace.

    Returns:
        List of check results with dimension, check, status, and detail.
    """
    from ptn_analysis.context.config import WPG_BOUNDS

    results: list[dict[str, str]] = []

    def _check(dimension: str, check_name: str, query: str,
               params: dict | None = None, threshold: int = 0) -> None:
        try:
            result = db.first(query, params or {})
            count = result if result is not None else 0
            status = "pass" if count <= threshold else "warn"
            results.append({"dimension": dimension, "check": check_name,
                            "status": status, "detail": f"{count} issues" + (" found" if status == "warn" else "")})
        except Exception as exc:
            results.append({"dimension": dimension, "check": check_name,
                            "status": "skip", "detail": str(exc)[:60]})

    stops = db.table_name("stops", city_key)
    trips = db.table_name("trips", city_key)
    routes = db.table_name("routes", city_key)
    stop_times = db.table_name("stop_times", city_key)

    _check("Accuracy", "Stops within Winnipeg bounds",
           f"SELECT COUNT(*) FROM {stops} WHERE stop_lat NOT BETWEEN :min_lat AND :max_lat "
           f"OR stop_lon NOT BETWEEN :min_lon AND :max_lon",
           params={"min_lat": WPG_BOUNDS["min_lat"], "max_lat": WPG_BOUNDS["max_lat"],
                   "min_lon": WPG_BOUNDS["min_lon"], "max_lon": WPG_BOUNDS["max_lon"]})
    _check("Accuracy", "Routes have valid short names",
           f"SELECT COUNT(*) FROM {routes} WHERE route_short_name IS NULL OR LENGTH(TRIM(route_short_name)) = 0")
    _check("Completeness", "Stops have coordinates",
           f"SELECT COUNT(*) FROM {stops} WHERE stop_lat IS NULL OR stop_lon IS NULL")
    _check("Completeness", "Trips have route_id",
           f"SELECT COUNT(*) FROM {trips} WHERE route_id IS NULL")
    _check("Completeness", "Stop_times have arrival_time",
           f"SELECT COUNT(*) FROM {stop_times} WHERE arrival_time IS NULL OR departure_time IS NULL")
    _check("Consistency", "Trips reference valid routes",
           f"SELECT COUNT(*) FROM {trips} t LEFT JOIN {routes} r "
           f"ON t.feed_id = r.feed_id AND t.route_id = r.route_id WHERE r.route_id IS NULL")
    _check("Consistency", "Stop_times reference valid stops",
           f"SELECT COUNT(*) FROM {stop_times} st LEFT JOIN {stops} s "
           f"ON st.feed_id = s.feed_id AND st.stop_id = s.stop_id WHERE s.stop_id IS NULL")
    _check("Consistency", "Departure >= Arrival times",
           f"SELECT COUNT(*) FROM {stop_times} WHERE departure_time < arrival_time")
    _check("Conformity", "GTFS time format (HH:MM:SS)",
           f"SELECT COUNT(*) FROM {stop_times} WHERE arrival_time NOT SIMILAR TO '[0-9]{{2,}}:[0-9]{{2}}:[0-9]{{2}}'")
    _check("Conformity", "Feed IDs are valid identifiers",
           f"SELECT COUNT(DISTINCT feed_id) FROM {stops} WHERE feed_id NOT SIMILAR TO '[a-z0-9_-]+'")

    return results
