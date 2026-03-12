"""Streamlit dashboard for the Winnipeg PTN analysis."""

from __future__ import annotations

import matplotlib.pyplot as plt
import pandas as pd
import pydeck as pdk
import streamlit as st

from ptn_analysis.analysis.visualization import (
    PTN_TIER_COLORS,
    PTN_TIER_ORDER,
    create_employment_access_change_chart,
)
from ptn_analysis.context.reporting import collect_summary_stats
from ptn_analysis.context.config import DEFAULT_CITY_KEY, FEED_ID_CURRENT, SERVING_DUCKDB_PATH, WPG_BOUNDS
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.serving import Dashboard, MapDataLoader

# ---------------------------------------------------------------------------
# Cached DB access — @st.cache_resource prevents re-opening the DB engine on
# every Streamlit rerun (which includes every user interaction).
# ---------------------------------------------------------------------------


@st.cache_resource
def _get_db() -> Dashboard:
    return Dashboard(TransitDB(SERVING_DUCKDB_PATH))


@st.cache_resource
def _get_map_loader() -> MapDataLoader:
    return MapDataLoader(DEFAULT_CITY_KEY, FEED_ID_CURRENT, TransitDB(SERVING_DUCKDB_PATH))


@st.cache_data(show_spinner=False)
def _get_missing() -> list[str]:
    return _get_db().missing_relations()


@st.cache_data(show_spinner=False)
def _load_payload() -> dict:
    return _get_db().load_all(
        map_loader=_get_map_loader(),
        summary_stats_fn=collect_summary_stats,
    )


# ---------------------------------------------------------------------------
# Streamlit render helpers (pure UI, no DB access)
# ---------------------------------------------------------------------------


def _render_pydeck_map(stops_df: pd.DataFrame, connections_df: pd.DataFrame) -> None:
    """Render stop network using PyDeck."""
    stop_layer = pdk.Layer(
        "ScatterplotLayer",
        data=stops_df[["stop_lon", "stop_lat", "stop_name"]].dropna(),
        get_position=["stop_lon", "stop_lat"],
        get_radius=80,
        get_fill_color=[0, 100, 200, 180],
        pickable=True,
    )
    layers = [stop_layer]
    if not connections_df.empty and all(
        c in connections_df.columns
        for c in ["from_lon", "from_lat", "to_lon", "to_lat"]
    ):
        arc_layer = pdk.Layer(
            "ArcLayer",
            data=connections_df.dropna(),
            get_source_position=["from_lon", "from_lat"],
            get_target_position=["to_lon", "to_lat"],
            get_width=1,
            get_source_color=[200, 30, 0, 100],
            get_target_color=[0, 30, 200, 100],
        )
        layers.append(arc_layer)
    view_state = pdk.ViewState(
        latitude=WPG_BOUNDS["center_lat"],
        longitude=WPG_BOUNDS["center_lon"],
        zoom=11,
        pitch=0,
    )
    deck = pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style="https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
        tooltip={"text": "{stop_name}"},
    )
    st.pydeck_chart(deck)


def render_density_chart(coverage: pd.DataFrame) -> None:
    if coverage.empty:
        st.warning("No neighbourhood density data is available.")
        return
    top = coverage.nlargest(20, "stop_count").sort_values("stop_count")
    color_map = {"High": "#1a9850", "Medium": "#fee08b", "Low": "#d73027"}
    colors = [color_map.get(c, "#6B7280") for c in top["coverage_category"]]
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.barh(top["neighbourhood"], top["stop_count"], color=colors)
    ax.set_title("Top 20 Neighbourhoods by Stop Count")
    ax.set_xlabel("Stop count")
    ax.grid(axis="x", linestyle="--", alpha=0.35)
    ax.set_axisbelow(True)
    st.pyplot(fig)
    plt.close(fig)


def render_ptn_summary_chart(ptn_summary: pd.DataFrame) -> None:
    if ptn_summary.empty:
        st.warning("PTN summary data is not available.")
        return
    tbl = ptn_summary.copy()
    tbl["ptn_tier"] = pd.Categorical(tbl["ptn_tier"], categories=PTN_TIER_ORDER, ordered=True)
    tbl = tbl.sort_values("ptn_tier")
    colors = [PTN_TIER_COLORS.get(t, "#6B7280") for t in tbl["ptn_tier"]]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(tbl["ptn_tier"], tbl["avg_headway_minutes"], color=colors)
    ax.set_title("Average Scheduled Headway by PTN Tier")
    ax.set_ylabel("Average headway (minutes)")
    ax.tick_params(axis="x", rotation=45)
    st.pyplot(fig)
    plt.close(fig)


def render_live_status(service_status: pd.DataFrame, advisories: pd.DataFrame) -> None:
    if service_status.empty:
        st.info("No cached Winnipeg Transit API status is available yet.")
    else:
        row = service_status.iloc[0]
        status = row.get("status") or row.get("status_key") or row.get("key") or "unknown"
        st.metric("Current service status", str(status))
        st.caption(f"Last refreshed: {row.get('query_time', 'unknown')}")
        if row.get("status_message"):
            st.caption(str(row.get("status_message")))
        if str(status).lower() in {"esp-1", "esp-2", "esp-3"}:
            st.error("Emergency service plan is active. Live arrival and trip-planning outputs may be unreliable.")

    st.markdown("**Current Service Advisories**")
    if advisories.empty:
        st.info("No cached service advisories are available.")
        return
    cols = [c for c in ["priority", "title", "category", "updated_at"] if c in advisories.columns]
    st.dataframe(advisories[cols], width="stretch")


def render_jobs_access_chart(jobs_access_comparison: pd.DataFrame) -> None:
    if jobs_access_comparison.empty:
        st.info("Jobs-access comparison data is not available yet.")
        return
    fig = create_employment_access_change_chart(jobs_access_comparison, top_n=15)
    if fig is None:
        st.info("Jobs-access comparison data is not available yet.")
        return
    st.pyplot(fig)
    plt.close(fig)


def render_live_validation(trip_delay_summary: pd.DataFrame, stop_features: pd.DataFrame) -> None:
    left, right = st.columns(2)
    with left:
        st.markdown("**Current trip delay summary**")
        if trip_delay_summary.empty:
            st.info("No cached trip-delay snapshots are available yet.")
        else:
            delay_cols = [c for c in ["trip_key", "bus_key", "variant_key",
                "mean_arrival_delay_seconds", "max_arrival_delay_seconds", "cancelled_stop_count"]
                if c in trip_delay_summary.columns]
            st.dataframe(trip_delay_summary[delay_cols].head(20), width="stretch")
    with right:
        st.markdown("**Selected stop features**")
        if stop_features.empty:
            st.info("No cached stop-feature enrichment is available yet.")
        else:
            st.dataframe(stop_features.head(20), width="stretch")


# ---------------------------------------------------------------------------
# Streamlit entry point
# ---------------------------------------------------------------------------


def main() -> None:
    st.set_page_config(page_title="Winnipeg PTN Dashboard", page_icon="🚌", layout="wide")
    st.title("Winnipeg Primary Transit Network")
    st.caption("COMP 4710 Group 11 · PR2 dashboard")

    try:
        missing = _get_missing()
    except Exception as exc:
        st.error("The dashboard could not open the DuckDB database.")
        st.code(str(exc))
        st.stop()

    if missing:
        st.error("The dashboard database is incomplete. Run `make data` before opening the app.")
        st.code("\n".join(missing))
        st.stop()

    try:
        payload = _load_payload()
    except Exception as exc:
        st.error("The dashboard payload could not be loaded.")
        st.code(str(exc))
        st.stop()

    stats = payload["summary_stats"]
    sb = st.sidebar
    sb.header("Summary")
    sb.metric("Stops", f"{stats['num_stops']:,}")
    sb.metric("Connections", f"{stats['num_edges']:,}")
    sb.metric("Routes", f"{stats['route_count']:,}")
    sb.metric("Neighbourhoods", f"{stats['neighbourhood_count']:,}")
    sb.metric("Jobs access neighbourhoods", f"{stats['jobs_access_neighbourhood_count']:,}")

    overview_tab, map_tab, coverage_tab, network_tab, frequency_tab, live_tab = st.tabs(
        ["Overview", "Map", "Coverage", "Network", "Frequency", "Live"]
    )

    with overview_tab:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total neighbourhood stops", f"{stats['total_neighbourhood_stops']:,}")
        c2.metric("Median stop density", f"{stats['median_stop_density_per_km2']:.2f}")
        c3.metric("Max stop density", f"{stats['max_stop_density_per_km2']:.2f}")
        c4.metric("Mean jobs access score", f"{stats['mean_jobs_access_score']:.2f}")

        st.markdown("**Route schedule comparison**")
        rc = payload["route_comparison"]
        if rc.empty:
            st.info("Pre/post comparison data is not available yet. Load the pre-PTN feed first.")
        else:
            wanted = ["baseline_route_short_name", "comparison_route_short_name", "ptn_tier",
                      "headway_pre", "headway_post", "headway_improvement",
                      "speed_pre", "speed_post", "speed_improvement", "trip_count_change"]
            st.dataframe(rc[[c for c in wanted if c in rc.columns]], width="stretch")

    with map_tab:
        st.subheader("Interactive network map")
        _render_pydeck_map(payload["stops"], payload["connections"])

    with coverage_tab:
        st.subheader("Neighbourhood stop density and jobs access")
        st.caption(
            "Jobs access is a destination-side proxy derived from CBP establishment counts "
            "and size bands, not an exact employment count."
        )
        left, right = st.columns([3, 2])
        with left:
            render_density_chart(payload["coverage"])
        with right:
            st.markdown("**Underserved neighbourhoods**")
            u = payload["underserved"]
            if u.empty:
                st.info("No underserved-neighbourhood table is available.")
            else:
                st.dataframe(u[["neighbourhood", "stop_count", "stop_density_per_km2"]], width="stretch")

        st.markdown("**Jobs access comparison**")
        render_jobs_access_chart(payload["jobs_access_comparison"])

        st.markdown("**Current jobs access leaders**")
        ja = payload["jobs_access"]
        if ja.empty:
            st.info("Current jobs-access metrics are not available yet.")
        else:
            st.dataframe(ja.sort_values("jobs_access_score", ascending=False).head(15), width="stretch")

        st.markdown("**Priority matrix**")
        pm = payload["priority_matrix"]
        if pm.empty:
            st.info("Priority matrix data is not available yet.")
        else:
            sort_col = "priority_score" if "priority_score" in pm.columns else "priority_rank"
            pm_cols = [c for c in ["neighbourhood", "jobs_access_score", "walkability_score",
                "bikeability_score", "median_household_income_2020",
                "commute_public_transit", "priority_score"] if c in pm.columns]
            st.dataframe(pm.sort_values(sort_col, ascending=False).head(20)[pm_cols], width="stretch")

    with network_tab:
        st.subheader("Network metrics")
        left, right = st.columns(2)
        with left:
            nm = payload["network_metrics"]
            st.dataframe(nm, width="stretch") if not nm.empty else st.info("Network metrics are not available.")
        with right:
            st.markdown("**Top hubs**")
            th = payload["top_hubs"]
            st.dataframe(th, width="stretch") if not th.empty else st.info("Top hub data is not available.")

    with frequency_tab:
        st.subheader("Scheduled service by PTN tier")
        rf = payload["route_frequency"]
        if rf.empty:
            st.warning("Route schedule metrics are not available.")
        else:
            hw = rf["mean_headway_minutes"]
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Routes", len(rf))
            c2.metric("Trips", f"{int(rf['scheduled_trip_count'].sum()):,}")
            c3.metric("Mean headway", f"{hw.mean():.1f} min")
            c4.metric("Routes under 15 min", int(hw.lt(15).sum()))
            render_ptn_summary_chart(payload["ptn_summary"])

    with live_tab:
        st.subheader("Winnipeg Transit API v4")
        render_live_status(payload["service_status"], payload["service_advisories"])
        render_live_validation(payload["trip_delay_summary"], payload["stop_features"])


if __name__ == "__main__":
    main()
