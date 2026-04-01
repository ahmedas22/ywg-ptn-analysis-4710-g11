"""Equity, priority, and poverty analysis for transit service evaluation."""

from __future__ import annotations

import pandas as pd

from ptn_analysis.analysis.base import AnalyzerBase
from ptn_analysis.context.db import TransitDB


def _classify_quadrant(need: float, gap: float) -> str:
    """Classify a neighbourhood into a priority quadrant."""
    if pd.isna(need) or pd.isna(gap):
        return "Insufficient Data"
    if need > 0 and gap > 0:
        return "High Need / High Gap"
    if need > 0 and gap <= 0:
        return "High Need / Low Gap"
    if need <= 0 and gap > 0:
        return "Low Need / High Gap"
    return "Low Need / Low Gap"


class EquityAnalyzer(AnalyzerBase):
    """Equity and priority analysis using coverage and census data.

    Uses composition: delegates coverage/accessibility queries to a
    CoverageAnalyzer rather than reimplementing them.

    Args:
        city_key: City namespace.
        feed_id: Feed identifier.
        db_instance: Database handle.
    """

    def __init__(
        self,
        city_key: str,
        feed_id: str,
        db_instance: TransitDB,
    ) -> None:
        super().__init__(city_key, feed_id, db_instance)
        # Lazy import to avoid circular dependency
        from ptn_analysis.analysis.coverage import CoverageAnalyzer

        self._coverage = CoverageAnalyzer(city_key, feed_id, db_instance)

    @staticmethod
    def gini_coefficient(values: pd.Series) -> float:
        """Compute Gini coefficient (0=equality, 1=inequality)."""
        import numpy as np

        arr = np.sort(values.dropna().values).astype(float)
        n = len(arr)
        if n == 0:
            return float("nan")
        if arr.sum() == 0:
            return 0.0
        if (arr < 0).any():
            arr = arr - arr.min()
            if arr.sum() == 0:
                return 0.0
        index = np.arange(1, n + 1)
        return float(((2 * index - n - 1) * arr).sum() / (n * arr.sum()))


    def priority_matrix(self) -> pd.DataFrame:
        """Build a z-score-based neighbourhood priority matrix.

        * **need_index** = z(transit_dependency) + z(-income)
        * **gap_index**  = z(multimodal_gap) + z(-jobs_access)
        * **quadrant**   = classify(need > 0, gap > 0)
        """
        jobs_access_table = self._coverage.jobs_access()
        if jobs_access_table.empty:
            return jobs_access_table

        priority_table = jobs_access_table.copy()
        multimodal_table = self._coverage.multimodal_equity()
        try:
            equity_table = self._coverage.equity_profile()
        except NotImplementedError:
            equity_table = pd.DataFrame()

        if not multimodal_table.empty:
            priority_table = priority_table.merge(
                multimodal_table[
                    [c for c in ["neighbourhood_id", "transit_access_score",
                     "bikeability_score", "multimodal_gap_score"]
                     if c in multimodal_table.columns]
                ],
                on="neighbourhood_id",
                how="left",
            )
        else:
            priority_table["multimodal_gap_score"] = pd.NA

        if not equity_table.empty:
            priority_table = priority_table.merge(
                equity_table[["neighbourhood_id", "population_total",
                              "median_household_income_2020", "commute_public_transit"]],
                on="neighbourhood_id",
                how="left",
            )
        else:
            priority_table["population_total"] = pd.NA
            priority_table["median_household_income_2020"] = pd.NA
            priority_table["commute_public_transit"] = pd.NA

        z_transit = self._zscore(priority_table["commute_public_transit"])
        z_neg_income = self._zscore(priority_table["median_household_income_2020"].mul(-1))
        priority_table["need_index"] = (z_transit + z_neg_income).round(4)

        z_gap = self._zscore(priority_table["multimodal_gap_score"].fillna(0))
        z_neg_access = self._zscore(priority_table["jobs_access_score"].fillna(0).mul(-1))
        priority_table["gap_index"] = (z_gap + z_neg_access).round(4)

        priority_table["quadrant"] = priority_table.apply(
            lambda row: _classify_quadrant(row["need_index"], row["gap_index"]),
            axis=1,
        )
        priority_table["priority_score"] = (
            priority_table["need_index"] + priority_table["gap_index"]
        ).round(4)

        return priority_table.sort_values("priority_score", ascending=False)

    def build_priority_metrics_table(self) -> pd.DataFrame:
        """Load precomputed priority table, falling back to priority_matrix()."""
        table_name = self._table("neighbourhood_priority_metrics")
        if self._db.relation_exists(table_name):
            pt = self._db.query(
                f"SELECT * FROM {table_name} WHERE feed_id = :feed_id",
                {"feed_id": self._feed_id},
            )
            sort_col = "priority_score" if "priority_score" in pt.columns else "priority_rank"
            if sort_col in pt.columns:
                pt = pt.sort_values(sort_col, ascending=(sort_col == "priority_rank"))
            return pt
        return self.priority_matrix()

    def travel_time_equity_report(self) -> pd.DataFrame:
        """Compare transit accessibility across income quintiles."""
        census_view = self._table("census_by_neighbourhood")
        density_tbl = self._table("neighbourhood_stop_count_density")
        if not (self._db.relation_exists(census_view) and self._db.relation_exists(density_tbl)):
            return pd.DataFrame()

        nb_data = self._db.query(
            f"""
            SELECT c.neighbourhood_id, c.neighbourhood,
                   c.median_household_income_2020, c.population_total,
                   d.stop_density_per_km2
            FROM {census_view} c
            LEFT JOIN {density_tbl} d
                ON c.neighbourhood_id = d.neighbourhood_id AND d.feed_id = :feed_id
            WHERE c.median_household_income_2020 > 0
            """,
            {"feed_id": self._feed_id},
        )
        if nb_data.empty or len(nb_data) < 5:
            return pd.DataFrame()

        access = self._coverage.transit_accessibility_score()
        if access.empty:
            return pd.DataFrame()

        merged = nb_data.merge(
            access[["neighbourhood_id", "transit_access_score"]],
            on="neighbourhood_id", how="left",
        )
        quintile_labels = ["Q1 (lowest)", "Q2", "Q3", "Q4", "Q5 (highest)"]
        try:
            merged["income_quintile"] = pd.qcut(
                merged["median_household_income_2020"], 5,
                labels=quintile_labels,
                duplicates="drop",
            )
        except ValueError:
            merged["income_quintile"] = pd.cut(
                merged["median_household_income_2020"], 5,
                labels=quintile_labels[:5],
            )
        result = (
            merged.groupby("income_quintile", observed=True)
            .agg(
                median_access_score=("transit_access_score", "median"),
                mean_access_score=("transit_access_score", "mean"),
                median_stop_density=("stop_density_per_km2", "median"),
                mean_income=("median_household_income_2020", "mean"),
                neighbourhood_count=("neighbourhood_id", "count"),
            )
            .reset_index()
        )
        for col in ["median_access_score", "mean_access_score", "median_stop_density", "mean_income"]:
            result[col] = result[col].round(2)
        return result

    def poverty_transit_correlation(self) -> pd.DataFrame:
        """Correlate poverty indicators with transit access per neighbourhood."""
        census_view = self._table("census_by_neighbourhood")
        density_tbl = self._table("neighbourhood_stop_count_density")
        if not (self._db.relation_exists(census_view) and self._db.relation_exists(density_tbl)):
            return pd.DataFrame()

        base = self._db.query(
            f"""
            SELECT c.neighbourhood_id, c.neighbourhood,
                   c.median_household_income_2020, c.population_total,
                   c.pct_commute_public_transit, c.pct_seniors_65_plus,
                   c.pct_recent_immigrants, d.stop_count, d.stop_density_per_km2
            FROM {census_view} c
            LEFT JOIN {density_tbl} d
                ON c.neighbourhood_id = d.neighbourhood_id AND d.feed_id = :feed_id
            WHERE c.population_total > 0
            ORDER BY c.median_household_income_2020
            """,
            {"feed_id": self._feed_id},
        )
        if base.empty:
            return base
        access = self._coverage.transit_accessibility_score()
        if not access.empty:
            base = base.merge(
                access[["neighbourhood_id", "transit_access_score"]],
                on="neighbourhood_id", how="left",
            )
        return base

    def equity_weighted_accessibility(self) -> pd.DataFrame:
        """Prescriptive counterfactual: equity-weighted accessibility ranking."""
        access = self._coverage.transit_accessibility_score()
        if access.empty:
            return pd.DataFrame()
        census_view = self._table("census_by_neighbourhood")
        if not self._db.relation_exists(census_view):
            return pd.DataFrame()
        census = self._db.query(
            f"""
            SELECT neighbourhood_id, median_household_income_2020,
                   pct_commute_public_transit, pct_seniors_65_plus
            FROM {census_view}
            """
        )
        if census.empty:
            return pd.DataFrame()

        merged = access.merge(census, on="neighbourhood_id", how="left")
        z_neg_income = self._zscore(merged["median_household_income_2020"].mul(-1))
        z_transit_dep = self._zscore(merged["pct_commute_public_transit"])
        z_seniors = self._zscore(merged["pct_seniors_65_plus"])
        merged["vulnerability_index"] = (z_neg_income + z_transit_dep + z_seniors).round(4)
        merged["current_rank"] = merged["transit_access_score"].rank(ascending=False).astype(int)
        vuln_scaled = self._scale_metric(merged["vulnerability_index"])
        merged["equity_weighted_score"] = (
            merged["transit_access_score"] * (1 + vuln_scaled)
        ).round(4)
        merged["equity_rank"] = merged["equity_weighted_score"].rank(ascending=False).astype(int)
        merged["rank_change"] = merged["current_rank"] - merged["equity_rank"]
        return merged[
            ["neighbourhood_id", "neighbourhood", "current_rank", "equity_rank",
             "rank_change", "transit_access_score", "equity_weighted_score",
             "vulnerability_index", "median_household_income_2020",
             "pct_commute_public_transit", "pct_seniors_65_plus"]
        ].sort_values("equity_rank")

    def poverty_overlay(self) -> pd.DataFrame:
        """Join poverty indicators (LIM-AT + MBM) to neighbourhoods."""
        census_view = self._table("census_by_neighbourhood")
        poverty_tbl = self._table("census_poverty_2021")
        nb_tbl = self._table("neighbourhoods")
        if not (self._db.relation_exists(census_view) and self._db.relation_exists(nb_tbl)):
            return pd.DataFrame()
        base = self._db.query(
            f"SELECT neighbourhood_id, neighbourhood, population_total, "
            f"median_household_income_2020 FROM {census_view}"
        )
        if base.empty:
            return base
        if self._db.relation_exists(poverty_tbl):
            poverty = self._db.query(
                f"SELECT n.id AS neighbourhood_id, COUNT(p.geometry) AS poverty_zone_count "
                f"FROM {nb_tbl} n LEFT JOIN {poverty_tbl} p ON ST_Intersects(n.geometry, p.geometry) "
                f"GROUP BY n.id"
            )
            if not poverty.empty:
                base = base.merge(poverty, on="neighbourhood_id", how="left")
                base["has_poverty_zone"] = base["poverty_zone_count"].fillna(0) > 0
        else:
            base["poverty_zone_count"] = 0
            base["has_poverty_zone"] = False
        mbm_tbl = self._table("poverty_mbm")
        if self._db.relation_exists(mbm_tbl):
            mbm = self._db.query(
                f"SELECT n.id AS neighbourhood_id, COUNT(m.geometry) AS mbm_zone_count "
                f"FROM {nb_tbl} n LEFT JOIN {mbm_tbl} m ON ST_Intersects(n.geometry, m.geometry) "
                f"GROUP BY n.id"
            )
            if not mbm.empty:
                base = base.merge(mbm, on="neighbourhood_id", how="left")
                base["has_mbm_overlap"] = base["mbm_zone_count"].fillna(0) > 0
        else:
            base["has_mbm_overlap"] = False
        return base.sort_values("median_household_income_2020")

    def demographic_equity_profile(self) -> pd.DataFrame:
        """Census equity demographics per neighbourhood."""
        census_view = self._table("census_by_neighbourhood")
        if not self._db.relation_exists(census_view):
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT neighbourhood_id, neighbourhood, population_total,
                   population_density_per_km2, median_household_income_2020,
                   pct_commute_public_transit, pct_commute_car,
                   pct_commute_walk, pct_commute_cycle,
                   pct_seniors_65_plus, pct_recent_immigrants
            FROM {census_view}
            WHERE population_total > 0
            ORDER BY neighbourhood
            """
        )

    def commute_duration_vs_r5py(self) -> pd.DataFrame:
        """Compare census commute durations against r5py travel times."""
        census_table = self._table("census_da")
        transit_matrix_table = self._table(f"transit_matrix_{self._feed_suffix()}")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()
        census_df = self._db.query(
            f"SELECT geo_uid, commute_dur_total, commute_dur_lt15, commute_dur_15_29, "
            f"commute_dur_30_44, commute_dur_45_59, commute_dur_60_plus "
            f"FROM {census_table} WHERE commute_dur_total > 0"
        )
        if census_df.empty:
            return census_df
        bin_midpoints = {"commute_dur_lt15": 7.5, "commute_dur_15_29": 22.0,
                         "commute_dur_30_44": 37.0, "commute_dur_45_59": 52.0,
                         "commute_dur_60_plus": 75.0}
        weighted_sum = pd.Series(0.0, index=census_df.index)
        for col, mid in bin_midpoints.items():
            weighted_sum += census_df[col].fillna(0) * mid
        census_df["census_mean_commute_min"] = (weighted_sum / census_df["commute_dur_total"]).round(1)
        if self._db.relation_exists(transit_matrix_table):
            r5py_df = self._db.query(
                f"SELECT from_id AS geo_uid, ROUND(AVG(travel_time_p50), 1) AS r5py_p50_travel_time_min "
                f"FROM {transit_matrix_table} GROUP BY from_id"
            )
            census_df = census_df.merge(r5py_df, on="geo_uid", how="left")
        return census_df

    def departure_demand_vs_gtfs_supply(self) -> pd.DataFrame:
        """Overlay census departure demand on GTFS departure frequency."""
        census_table = self._table("census_da")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()
        dep = self._db.query(
            f"SELECT SUM(depart_total) AS total, SUM(depart_5am) AS h5, "
            f"SUM(depart_6am) AS h6, SUM(depart_7am) AS h7, SUM(depart_8am) AS h8, "
            f"SUM(depart_9_11am) AS h9_11, SUM(depart_12_4am) AS h12_4 "
            f"FROM {census_table} WHERE depart_total > 0"
        )
        if dep.empty or dep["total"].iloc[0] == 0:
            return pd.DataFrame()
        total = dep["total"].iloc[0]
        demand = pd.DataFrame([
            {"hour_label": "5:00-5:59", "hour": 5, "census_demand_pct": round(100 * dep["h5"].iloc[0] / total, 1)},
            {"hour_label": "6:00-6:59", "hour": 6, "census_demand_pct": round(100 * dep["h6"].iloc[0] / total, 1)},
            {"hour_label": "7:00-7:59", "hour": 7, "census_demand_pct": round(100 * dep["h7"].iloc[0] / total, 1)},
            {"hour_label": "8:00-8:59", "hour": 8, "census_demand_pct": round(100 * dep["h8"].iloc[0] / total, 1)},
            {"hour_label": "9:00-11:59", "hour": 10, "census_demand_pct": round(100 * dep["h9_11"].iloc[0] / total, 1)},
            {"hour_label": "12:00-4:59", "hour": 14, "census_demand_pct": round(100 * dep["h12_4"].iloc[0] / total, 1)},
        ])
        freq_view = self._table("route_hourly_departures")
        if self._db.relation_exists(freq_view):
            gtfs = self._db.query(
                f"SELECT hour, SUM(departures) AS gtfs_departures FROM {freq_view} "
                f"WHERE feed_id = :feed_id GROUP BY hour ORDER BY hour",
                {"feed_id": self._feed_id},
            )
            if not gtfs.empty:
                demand = demand.merge(gtfs, on="hour", how="left")
        return demand
