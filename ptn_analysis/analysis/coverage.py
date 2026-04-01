"""Coverage, access, and equity analysis for neighbourhood transit service."""

from __future__ import annotations

import pandas as pd

from ptn_analysis.analysis.base import AnalyzerBase
from ptn_analysis.context.config import (
    WGS84_CRS,
    WINNIPEG_PROJECTED_CRS,
)
from ptn_analysis.context.db import TransitDB

COVERAGE_HIGH = 5.0
COVERAGE_MEDIUM = 1.0


class CoverageAnalyzer(AnalyzerBase):
    """Analyze neighbourhood and community transit coverage.

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
        """Initialize the coverage analyzer.

        Args:
            city_key: City namespace.
            feed_id: Feed identifier.
            db_instance: Database handle.
        """
        super().__init__(city_key, feed_id, db_instance)
        self._result_cache: dict[str, pd.DataFrame] = {}

    def __repr__(self) -> str:
        return f"CoverageAnalyzer(city_key={self._city_key!r}, feed_id={self._feed_id!r})"

    def clear_cache(self) -> None:
        """Clear cached query results for this analyzer instance."""
        self._result_cache.clear()

    # _scale_metric and _zscore inherited from AnalyzerBase

    @staticmethod
    def _empty_neighbourhood_density_frame() -> pd.DataFrame:
        """Return an empty neighbourhood-density table with the expected schema."""
        return pd.DataFrame(
            {
                "neighbourhood_id": pd.Series(dtype="object"),
                "neighbourhood": pd.Series(dtype="object"),
                "area_km2": pd.Series(dtype="float64"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
            }
        )

    @staticmethod
    def _empty_community_density_frame() -> pd.DataFrame:
        """Return an empty community-density table with the expected schema."""
        return pd.DataFrame(
            {
                "community_area_id": pd.Series(dtype="object"),
                "community_area": pd.Series(dtype="object"),
                "area_km2": pd.Series(dtype="float64"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
            }
        )

    @staticmethod
    def _empty_equity_profile_frame() -> pd.DataFrame:
        """Return an empty neighbourhood equity profile with the expected schema."""
        return pd.DataFrame(
            {
                "feed_id": pd.Series(dtype="object"),
                "neighbourhood_id": pd.Series(dtype="object"),
                "neighbourhood": pd.Series(dtype="object"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
                "population_total": pd.Series(dtype="float64"),
                "median_household_income_2020": pd.Series(dtype="float64"),
                "commute_public_transit": pd.Series(dtype="float64"),
                "commute_car_truck_van": pd.Series(dtype="float64"),
                "commute_walked": pd.Series(dtype="float64"),
                "commute_bicycle": pd.Series(dtype="float64"),
            }
        )

    @staticmethod
    def _empty_jobs_access_frame() -> pd.DataFrame:
        """Return an empty jobs-access table with the expected schema."""
        return pd.DataFrame(
            {
                "feed_id": pd.Series(dtype="object"),
                "neighbourhood_id": pd.Series(dtype="object"),
                "neighbourhood": pd.Series(dtype="object"),
                "area_km2": pd.Series(dtype="float64"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
                "jobs_proxy_score": pd.Series(dtype="float64"),
                "establishment_count": pd.Series(dtype="float64"),
                "large_employer_count": pd.Series(dtype="float64"),
                "jobs_proxy_log": pd.Series(dtype="float64"),
                "jobs_access_score": pd.Series(dtype="float64"),
            }
        )

    def neighbourhood_density(self) -> pd.DataFrame:
        """Load neighbourhood stop density for one feed.

        Returns:
            Stop counts and densities by neighbourhood.
        """
        cache_key = "neighbourhood_density"
        if cache_key not in self._result_cache:
            self._result_cache[cache_key] = self._fetch_neighbourhood_density()
        return self._result_cache[cache_key].copy()

    def _fetch_neighbourhood_density(self) -> pd.DataFrame:
        """Query neighbourhood stop density for one feed."""
        table_name = self._table("neighbourhood_stop_count_density")
        if not self._db.relation_exists(table_name):
            return self._empty_neighbourhood_density_frame()
        return self._db.query(
            f"""
            SELECT neighbourhood_id,
                   neighbourhood,
                   area_km2,
                   stop_count,
                   stop_density_per_km2
            FROM {table_name}
            WHERE feed_id = :feed_id
            ORDER BY stop_count DESC
            """,
            {"feed_id": self._feed_id},
        )

    def community_density(self) -> pd.DataFrame:
        """Load community-area stop density for one feed.

        Returns:
            Stop counts and densities by community area.
        """
        table_name = self._table("community_area_stop_count_density")
        if not self._db.relation_exists(table_name):
            return self._empty_community_density_frame()
        return self._db.query(
            f"""
            SELECT community_area_id,
                   community_area,
                   area_km2,
                   stop_count,
                   stop_density_per_km2
            FROM {table_name}
            WHERE feed_id = :feed_id
            ORDER BY stop_count DESC
            """,
            {"feed_id": self._feed_id},
        )

    def summary(self) -> dict[str, float | int]:
        """Compute summary coverage statistics.

        Returns:
            Summary statistics for neighbourhood coverage.
        """
        coverage_table = self.neighbourhood_density()
        if coverage_table.empty:
            return {
                "total_neighbourhoods": 0,
                "total_stops": 0,
                "mean_stop_count": 0.0,
                "median_stop_count": 0.0,
                "zero_stop_areas": 0,
                "mean_density_per_km2": 0.0,
            }
        return {
            "total_neighbourhoods": int(len(coverage_table)),
            "total_stops": int(coverage_table["stop_count"].sum()),
            "mean_stop_count": float(coverage_table["stop_count"].mean()),
            "median_stop_count": float(coverage_table["stop_count"].median()),
            "zero_stop_areas": int((coverage_table["stop_count"] == 0).sum()),
            "mean_density_per_km2": float(coverage_table["stop_density_per_km2"].mean()),
        }

    def underserved_neighbourhoods(self, percentile: float = 25.0) -> pd.DataFrame:
        """Return neighbourhoods below a percentile density threshold.

        Args:
            percentile: Percentile cutoff.

        Returns:
            Underserved neighbourhood table.
        """
        coverage_table = self.neighbourhood_density()
        if coverage_table.empty:
            return coverage_table
        cutoff = coverage_table["stop_density_per_km2"].quantile(percentile / 100.0)
        return coverage_table[
            coverage_table["stop_density_per_km2"] <= cutoff
        ].sort_values("stop_density_per_km2")

    def density_categories(self) -> pd.DataFrame:
        """Categorize neighbourhoods by stop density.

        Returns:
            Neighbourhood table with density categories.
        """
        coverage_table = self.neighbourhood_density().copy()
        coverage_table["coverage_category"] = coverage_table["stop_density_per_km2"].apply(
            categorize_coverage
        )
        return coverage_table

    def ranked_neighbourhoods(self) -> pd.DataFrame:
        """Rank neighbourhoods by stop density.

        Returns:
            Neighbourhood table with density ranks.
        """
        coverage_table = self.neighbourhood_density().copy()
        coverage_table["density_rank"] = coverage_table["stop_density_per_km2"].rank(
            ascending=False,
            method="dense",
        ).astype("Int64")
        return coverage_table.sort_values("density_rank")

    def outliers(self, method: str = "iqr") -> pd.DataFrame:
        """Flag high and low outliers in neighbourhood stop counts.

        Args:
            method: Detection method. Supported values are ``"iqr"`` and ``"zscore"``.

        Returns:
            Outlier neighbourhood table.
        """
        coverage_table = self.neighbourhood_density()
        if coverage_table.empty:
            return coverage_table
        stop_counts = coverage_table["stop_count"]

        if method.lower() == "iqr":
            lower_quartile = stop_counts.quantile(0.25)
            upper_quartile = stop_counts.quantile(0.75)
            interquartile_range = upper_quartile - lower_quartile
            lower_bound = lower_quartile - 1.5 * interquartile_range
            upper_bound = upper_quartile + 1.5 * interquartile_range
            outlier_table = coverage_table[
                (stop_counts < lower_bound) | (stop_counts > upper_bound)
            ].copy()
            outlier_types = []
            for value in outlier_table["stop_count"]:
                if value < lower_bound:
                    outlier_types.append("Low")
                else:
                    outlier_types.append("High")
            outlier_table["outlier_type"] = outlier_types
        elif method.lower() == "zscore":
            mean_value = stop_counts.mean()
            std_value = stop_counts.std()
            if std_value == 0 or pd.isna(std_value):
                return pd.DataFrame(columns=["neighbourhood", "stop_count", "outlier_type"])
            z_scores = (stop_counts - mean_value) / std_value
            outlier_table = coverage_table[z_scores.abs() >= 3].copy()
            outlier_types = []
            for value in outlier_table["stop_count"]:
                if value < mean_value:
                    outlier_types.append("Low")
                else:
                    outlier_types.append("High")
            outlier_table["outlier_type"] = outlier_types
        else:
            raise ValueError("method must be 'iqr' or 'zscore'")

        return outlier_table[["neighbourhood", "stop_count", "outlier_type"]].sort_values(
            ["outlier_type", "stop_count"],
            ascending=[True, False],
        )

    def community_summary(self) -> pd.DataFrame:
        """Return community-level coverage with rank and category.

        Returns:
            Community coverage table.
        """
        coverage_table = self.community_density().copy()
        coverage_table["density_rank"] = coverage_table["stop_density_per_km2"].rank(
            ascending=False,
            method="dense",
        ).astype("Int64")
        categories = []
        for density in coverage_table["stop_density_per_km2"]:
            categories.append(categorize_coverage(density))
        coverage_table["coverage_category"] = categories
        return coverage_table.sort_values("density_rank")

    def equity_profile(self) -> pd.DataFrame:
        """Build a transit equity profile joined with census data.

        Returns:
            Neighbourhood equity profile table.
        """
        cache_key = "equity_profile"
        if cache_key not in self._result_cache:
            self._result_cache[cache_key] = self._fetch_equity_profile()
        return self._result_cache[cache_key].copy()

    def _fetch_equity_profile(self) -> pd.DataFrame:
        """Query the transit equity profile joined with census data."""
        density_table_name = self._table("neighbourhood_stop_count_density")
        census_table_name = self._table("census_by_neighbourhood")
        if not self._db.relation_exists(density_table_name):
            return self._empty_equity_profile_frame()
        if not self._db.relation_exists(census_table_name):
            return self._empty_equity_profile_frame()
        return self._db.query(
            f"""
            SELECT density.feed_id,
                   density.neighbourhood_id,
                   density.neighbourhood,
                   density.stop_count,
                   density.stop_density_per_km2,
                   census.population_total,
                   census.median_household_income_2020,
                   census.pct_commute_public_transit AS commute_public_transit,
                   census.pct_commute_car AS commute_car_truck_van,
                   census.pct_commute_walk AS commute_walked,
                   census.pct_commute_cycle AS commute_bicycle
            FROM {density_table_name} density
            LEFT JOIN {census_table_name} census
                ON density.neighbourhood = census.neighbourhood
            WHERE density.feed_id = :feed_id
            ORDER BY density.stop_density_per_km2 ASC
            """,
            {"feed_id": self._feed_id},
        )

    def _build_infrastructure_accessibility_base(
        self,
        infra_table: str,
        buffer_metres: float,
        output_segment_count_col: str,
    ) -> pd.DataFrame:
        """Shared SQL kernel for cycling infrastructure accessibility.

        Args:
            infra_table: Logical table name for infrastructure segments.
            buffer_metres: Stop-access buffer radius in metres.
            output_segment_count_col: Output column name for the segment count.

        Returns:
            Raw neighbourhood-level accessibility table before scoring.
        """
        density_tbl = self._table("neighbourhood_stop_count_density")
        nb_tbl = self._table("neighbourhoods")
        stops_tbl = self._table("stops")
        infra_tbl = self._table(infra_table)
        if not all(
            self._db.relation_exists(t)
            for t in (density_tbl, nb_tbl, stops_tbl, infra_tbl)
        ):
            return pd.DataFrame()

        result = self._db.query(
            f"""
            WITH centroid_stop_access AS (
                SELECT neighbourhoods.id AS neighbourhood_id,
                       COUNT(DISTINCT stops.stop_id) AS accessible_stop_count
                FROM {nb_tbl} neighbourhoods
                LEFT JOIN {stops_tbl} stops
                    ON stops.feed_id = :feed_id
                   AND ST_Distance(
                       ST_Transform(ST_Centroid(neighbourhoods.geometry), '{WGS84_CRS}', '{WINNIPEG_PROJECTED_CRS}'),
                       ST_Transform(ST_Point(stops.stop_lon, stops.stop_lat), '{WGS84_CRS}', '{WINNIPEG_PROJECTED_CRS}')
                   ) <= :buffer_metres
                GROUP BY neighbourhoods.id
            ),
            infra_segments AS (
                SELECT neighbourhoods.id AS neighbourhood_id,
                       COUNT(infra.geometry) AS segment_count
                FROM {nb_tbl} neighbourhoods
                LEFT JOIN {infra_tbl} infra
                    ON ST_Intersects(neighbourhoods.geometry, infra.geometry)
                GROUP BY neighbourhoods.id
            )
            SELECT density.feed_id,
                   density.neighbourhood_id,
                   density.neighbourhood,
                   density.area_km2,
                   density.stop_count,
                   density.stop_density_per_km2,
                   centroid_stop_access.accessible_stop_count,
                   infra_segments.segment_count AS {output_segment_count_col}
            FROM {density_tbl} density
            LEFT JOIN centroid_stop_access
                ON density.neighbourhood_id = centroid_stop_access.neighbourhood_id
            LEFT JOIN infra_segments
                ON density.neighbourhood_id = infra_segments.neighbourhood_id
            WHERE density.feed_id = :feed_id
            ORDER BY density.neighbourhood
            """,
            {"feed_id": self._feed_id, "buffer_metres": buffer_metres},
        )
        if not result.empty:
            result[["accessible_stop_count", output_segment_count_col]] = result[
                ["accessible_stop_count", output_segment_count_col]
            ].fillna(0)
        return result

    def cycling_infrastructure_index(self, buffer_metres: float = 500) -> pd.DataFrame:
        """Build a neighbourhood cycling infrastructure index.

        Uses real Cycling Network data (kjd9-dvf5) with segment counts
        per neighbourhood. Scores combine stop access, density and
        cycling infrastructure availability.

        Args:
            buffer_metres: Cycling buffer in metres.

        Returns:
            Neighbourhood cycling infrastructure table.
        """
        cache_key = f"bikeability_{buffer_metres}"
        if cache_key not in self._result_cache:
            self._result_cache[cache_key] = self._compute_bikeability(buffer_metres)
        return self._result_cache[cache_key].copy()

    def _compute_bikeability(self, buffer_metres: float) -> pd.DataFrame:
        """Compute cycling infrastructure scores for one buffer distance."""
        df = self._build_infrastructure_accessibility_base(
            "cycling_paths",
            buffer_metres,
            "cycling_segment_count",
        )
        if df.empty:
            return df
        df["stop_access_score"] = self._scale_metric(df["accessible_stop_count"])
        df["density_score"] = self._scale_metric(df["stop_density_per_km2"])
        df["cycling_score"] = self._scale_metric(df["cycling_segment_count"])
        df["bikeability_score"] = (
            0.40 * df["stop_access_score"]
            + 0.25 * df["density_score"]
            + 0.35 * df["cycling_score"]
        ).round(4)
        return df.sort_values("bikeability_score", ascending=False).reset_index(drop=True)

    def multimodal_equity(self) -> pd.DataFrame:
        """Combine transit accessibility, cycling infra and jobs access.

        Replaces walkability component with transit_accessibility_score
        (WalkScore-style decay model). Cycling uses real cycling network
        data from Winnipeg Open Data.

        Returns:
            Multimodal access table with gap scores.
        """
        jobs_access_table = self.jobs_access()
        bikeability_table = self.cycling_infrastructure_index()
        access_table = self.transit_accessibility_score()

        if jobs_access_table.empty and bikeability_table.empty and access_table.empty:
            return pd.DataFrame()

        if not jobs_access_table.empty:
            multimodal_table = jobs_access_table[
                ["neighbourhood_id", "neighbourhood", "jobs_access_score", "jobs_proxy_score"]
            ].copy()
        elif not access_table.empty:
            multimodal_table = access_table[["neighbourhood_id", "neighbourhood"]].copy()
        else:
            multimodal_table = bikeability_table[["neighbourhood_id", "neighbourhood"]].copy()

        if not access_table.empty:
            multimodal_table = multimodal_table.merge(
                access_table[["neighbourhood_id", "transit_access_score"]],
                on="neighbourhood_id",
                how="left",
            )
        if not bikeability_table.empty:
            multimodal_table = multimodal_table.merge(
                bikeability_table[["neighbourhood_id", "bikeability_score", "cycling_segment_count"]],
                on="neighbourhood_id",
                how="left",
            )

        for col in ["jobs_access_score", "transit_access_score", "bikeability_score"]:
            if col not in multimodal_table.columns:
                multimodal_table[col] = 0.0
            multimodal_table[col] = pd.to_numeric(
                multimodal_table[col], errors="coerce"
            ).fillna(0.0)

        multimodal_table["multimodal_access_score"] = (
            0.40 * self._scale_metric(multimodal_table["jobs_access_score"])
            + 0.35 * self._scale_metric(multimodal_table["transit_access_score"])
            + 0.25 * self._scale_metric(multimodal_table["bikeability_score"])
        ).round(4)
        multimodal_table["multimodal_gap_score"] = (
            1 - multimodal_table["multimodal_access_score"]
        ).round(4)
        return multimodal_table.sort_values(
            "multimodal_gap_score", ascending=False
        ).reset_index(drop=True)

    def transit_accessibility_score(self) -> pd.DataFrame:
        """Compute WalkScore-style transit accessibility per neighbourhood.

        Uses exponential distance decay from neighbourhood centroids to
        transit stops, weighted by PTN tier:
        ``score = SUM(tier_weight * exp(-dist * CIRCUITY / DECAY_M))``

        Stop tiers are derived through the proper join path:
        stops → stop_times → trips → routes → route_ptn_tiers view.

        Based on WalkScore (2011) Transit Score methodology.

        Returns:
            Neighbourhood transit accessibility table.
        """
        import numpy as np

        TIER_WEIGHTS = {
            "Rapid Transit": 2.0, "Frequent Express": 1.5, "Frequent": 1.25,
            "Direct": 1.0, "Connector": 0.5, "Limited Span": 0.25,
            "Community": 0.25,
        }
        CIRCUITY_FACTOR = 1.3
        WALKSCORE_DECAY_M = 1086.0  # WalkScore 2011 decay constant

        nb_tbl = self._table("neighbourhoods")
        stops_tbl = self._table("stops")
        stop_times_tbl = self._table("stop_times")
        trips_tbl = self._table("trips")
        tiers_view = self._table("route_ptn_tiers")
        if not all(
            self._db.relation_exists(t)
            for t in (nb_tbl, stops_tbl, stop_times_tbl, trips_tbl, tiers_view)
        ):
            return pd.DataFrame()

        # Derive stop tiers through trips → stop_times → routes → ptn_tiers
        stop_tiers = self._db.query(
            f"""
            SELECT DISTINCT s.stop_id, s.stop_lat, s.stop_lon,
                   COALESCE(t.ptn_tier, 'Community') AS ptn_tier
            FROM {stops_tbl} s
            JOIN {stop_times_tbl} st
                ON s.feed_id = st.feed_id AND s.stop_id = st.stop_id
            JOIN {trips_tbl} tr
                ON st.feed_id = tr.feed_id AND st.trip_id = tr.trip_id
            LEFT JOIN {tiers_view} t
                ON tr.feed_id = t.feed_id AND tr.route_id = t.route_id
            WHERE s.feed_id = :feed_id
            """,
            {"feed_id": self._feed_id},
        )
        if stop_tiers.empty:
            return pd.DataFrame()

        # Deduplicate: keep highest-tier per stop
        tier_rank = {v: i for i, v in enumerate(TIER_WEIGHTS)}
        stop_tiers["_rank"] = stop_tiers["ptn_tier"].map(tier_rank).fillna(len(tier_rank))
        stop_tiers = (
            stop_tiers.sort_values("_rank")
            .drop_duplicates(subset=["stop_id"], keep="first")
            .drop(columns=["_rank"])
        )

        # Get neighbourhood centroids
        nb_centroids = self._db.query(
            f"""
            SELECT id AS neighbourhood_id, name AS neighbourhood,
                   ST_X(ST_Centroid(geometry)) AS centroid_lon,
                   ST_Y(ST_Centroid(geometry)) AS centroid_lat
            FROM {nb_tbl}
            """
        )
        if nb_centroids.empty:
            return pd.DataFrame()

        # Vectorized decay computation
        stop_lat = stop_tiers["stop_lat"].values
        stop_lon = stop_tiers["stop_lon"].values
        weights = stop_tiers["ptn_tier"].map(TIER_WEIGHTS).fillna(0.25).values

        rows = []
        for _, nb in nb_centroids.iterrows():
            nb_lat, nb_lon = nb["centroid_lat"], nb["centroid_lon"]
            dlat = (stop_lat - nb_lat) * 111_320
            dlon = (stop_lon - nb_lon) * 111_320 * np.cos(np.radians(nb_lat))
            dist_m = np.sqrt(dlat**2 + dlon**2)
            decay = weights * np.exp(-dist_m * CIRCUITY_FACTOR / WALKSCORE_DECAY_M)
            rows.append({
                "neighbourhood_id": nb["neighbourhood_id"],
                "neighbourhood": nb["neighbourhood"],
                "transit_access_score": round(float(decay.sum()), 4),
            })
        result = pd.DataFrame(rows)
        result["transit_access_score_scaled"] = self._scale_metric(
            result["transit_access_score"]
        )
        return result.sort_values("transit_access_score", ascending=False).reset_index(drop=True)

    def jobs_access(self) -> pd.DataFrame:
        """Return neighbourhood-level jobs access metrics for one feed.

        Returns:
            Neighbourhood jobs access table.
        """
        cache_key = "jobs_access"
        if cache_key not in self._result_cache:
            self._result_cache[cache_key] = self._fetch_jobs_access()
        return self._result_cache[cache_key].copy()

    def _fetch_jobs_access(self) -> pd.DataFrame:
        """Query neighbourhood-level jobs access metrics for one feed."""
        table_name = self._table("neighbourhood_jobs_access_metrics")
        if not self._db.relation_exists(table_name):
            return self._empty_jobs_access_frame()
        return self._db.query(
            f"""
            SELECT feed_id,
                   neighbourhood_id,
                   neighbourhood,
                   area_km2,
                   stop_count,
                   stop_density_per_km2,
                   jobs_proxy_score,
                   establishment_count,
                   large_employer_count,
                   jobs_proxy_log,
                   jobs_access_score
            FROM {table_name}
            WHERE feed_id = :feed_id
            ORDER BY jobs_access_score DESC, jobs_proxy_score DESC
            """,
            {"feed_id": self._feed_id},
        )

    def jobs_access_comparison(self, baseline_feed_id: str = "avg_pre_ptn") -> pd.DataFrame:
        """Compare neighbourhood jobs access between two feeds.

        Args:
            baseline_feed_id: Feed identifier used as the baseline.

        Returns:
            Neighbourhood jobs access comparison table.
        """
        comparison_table_name = self._table("neighbourhood_jobs_access_comparison_metrics")
        if self._db.relation_exists(comparison_table_name):
            result = self._db.query(
                f"""
                SELECT *
                FROM {comparison_table_name}
                WHERE baseline_feed_id = :baseline_feed_id
                  AND comparison_feed_id = :comparison_feed_id
                ORDER BY jobs_access_change DESC NULLS LAST, jobs_proxy_score DESC
                """,
                {
                    "baseline_feed_id": baseline_feed_id,
                    "comparison_feed_id": self._feed_id,
                },
            )
            if not result.empty:
                return result

        table_name = self._table("neighbourhood_jobs_access_metrics")
        if not self._db.relation_exists(table_name):
            return pd.DataFrame()
        return self._db.query(
            f"""
            WITH baseline AS (
                SELECT neighbourhood_id,
                       neighbourhood,
                       jobs_access_score AS baseline_jobs_access_score,
                       stop_density_per_km2 AS baseline_stop_density_per_km2
                FROM {table_name}
                WHERE feed_id = :baseline_feed_id
            ),
            comparison AS (
                SELECT neighbourhood_id,
                       neighbourhood,
                       jobs_access_score AS comparison_jobs_access_score,
                       stop_density_per_km2 AS comparison_stop_density_per_km2,
                       jobs_proxy_score,
                       establishment_count,
                       large_employer_count
                FROM {table_name}
                WHERE feed_id = :comparison_feed_id
            )
            SELECT comparison.neighbourhood_id,
                   comparison.neighbourhood,
                   comparison.jobs_proxy_score,
                   comparison.establishment_count,
                   comparison.large_employer_count,
                   baseline.baseline_jobs_access_score,
                   comparison.comparison_jobs_access_score,
                   comparison.comparison_jobs_access_score - baseline.baseline_jobs_access_score AS jobs_access_change,
                   baseline.baseline_stop_density_per_km2,
                   comparison.comparison_stop_density_per_km2,
                   comparison.comparison_stop_density_per_km2 - baseline.baseline_stop_density_per_km2 AS stop_density_change
            FROM comparison
            LEFT JOIN baseline
                ON comparison.neighbourhood_id = baseline.neighbourhood_id
            ORDER BY jobs_access_change DESC NULLS LAST, comparison.jobs_proxy_score DESC
            """,
            {
                "baseline_feed_id": baseline_feed_id,
                "comparison_feed_id": self._feed_id,
            },
        )

    def build_density_comparison_table(self, baseline_feed_id: str) -> pd.DataFrame:
        """Build neighbourhood stop-density comparison rows.

        Args:
            baseline_feed_id: Feed identifier used as the baseline.

        Returns:
            Neighbourhood density comparison table.
        """
        comparison_table_name = self._table("neighbourhood_stop_count_density_comparison")
        if self._db.relation_exists(comparison_table_name):
            return self._db.query(
                f"""
                SELECT *
                FROM {comparison_table_name}
                WHERE baseline_feed_id = :baseline_feed_id
                  AND comparison_feed_id = :comparison_feed_id
                ORDER BY stop_density_change DESC NULLS LAST, comparison_stop_count DESC
                """,
                {
                    "baseline_feed_id": baseline_feed_id,
                    "comparison_feed_id": self._feed_id,
                },
            )

        density_table_name = self._table("neighbourhood_stop_count_density")
        if not self._db.relation_exists(density_table_name):
            return pd.DataFrame()
        return self._db.query(
            f"""
            WITH baseline AS (
                SELECT neighbourhood_id,
                       neighbourhood,
                       stop_count AS baseline_stop_count,
                       stop_density_per_km2 AS baseline_stop_density_per_km2
                FROM {density_table_name}
                WHERE feed_id = :baseline_feed_id
            ),
            comparison AS (
                SELECT neighbourhood_id,
                       neighbourhood,
                       area_km2,
                       stop_count AS comparison_stop_count,
                       stop_density_per_km2 AS comparison_stop_density_per_km2
                FROM {density_table_name}
                WHERE feed_id = :comparison_feed_id
            )
            SELECT comparison.neighbourhood_id,
                   comparison.neighbourhood,
                   comparison.area_km2,
                   baseline.baseline_stop_count,
                   comparison.comparison_stop_count,
                   comparison.comparison_stop_count - baseline.baseline_stop_count AS stop_count_change,
                   baseline.baseline_stop_density_per_km2,
                   comparison.comparison_stop_density_per_km2,
                   comparison.comparison_stop_density_per_km2 - baseline.baseline_stop_density_per_km2 AS stop_density_change
            FROM comparison
            LEFT JOIN baseline
                ON comparison.neighbourhood_id = baseline.neighbourhood_id
            ORDER BY stop_density_change DESC NULLS LAST, comparison.comparison_stop_count DESC
            """,
            {
                "baseline_feed_id": baseline_feed_id,
                "comparison_feed_id": self._feed_id,
            },
        )

    # Equity/priority/poverty methods are in ptn_analysis.analysis.equity.EquityAnalyzer

    def sidewalk_connectivity_proxy(self) -> pd.DataFrame:
        """Compute sidewalk infrastructure near each PTN stop.

        Stops with <200m of sidewalk within 100m are 'phantom coverage' -
        geometrically covered but physically inaccessible.

        Returns:
            DataFrame with stop_id, stop_name, sidewalk_m_100m, is_phantom_coverage.
        """
        stops_tbl = self._table("stops")
        walkways_tbl = self._table("walkways")
        if not all(
            self._db.relation_exists(t) for t in (stops_tbl, walkways_tbl)
        ):
            return pd.DataFrame()

        result = self._db.query(
            f"""
            SELECT s.stop_id, s.stop_name,
                   COALESCE(SUM(ST_Length(
                       ST_Intersection(
                           ST_Transform(w.geometry, 'EPSG:4326', 'EPSG:32614'),
                           ST_Buffer(
                               ST_Transform(ST_Point(s.stop_lon, s.stop_lat), 'EPSG:4326', 'EPSG:32614'),
                               100
                           )
                       )
                   )), 0)::DOUBLE AS sidewalk_m_100m
            FROM {stops_tbl} s
            LEFT JOIN {walkways_tbl} w
                ON ST_DWithin(
                    ST_Transform(ST_Point(s.stop_lon, s.stop_lat), 'EPSG:4326', 'EPSG:32614'),
                    ST_Transform(w.geometry, 'EPSG:4326', 'EPSG:32614'),
                    100
                )
            WHERE s.feed_id = :feed_id
            GROUP BY s.stop_id, s.stop_name
            """,
            {"feed_id": self._feed_id},
        )
        if result.empty:
            return result
        result["is_phantom_coverage"] = result["sidewalk_m_100m"] < 200
        return result

    def modal_share_by_neighbourhood(self) -> pd.DataFrame:
        """Census 2021 commute mode split aggregated by neighbourhood."""
        view_name = self._table("census_by_neighbourhood")
        if not self._db.relation_exists(view_name):
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT neighbourhood_id, neighbourhood, population_total,
                   pct_commute_public_transit, pct_commute_car,
                   pct_commute_walk, pct_commute_cycle, pct_commute_other,
                   median_household_income_2020
            FROM {view_name}
            ORDER BY pct_commute_public_transit DESC
            """
        )

    def build_neighbourhood_classification_feature_table(self) -> pd.DataFrame:
        """Build census-based neighbourhood feature table for coverage classification."""
        access_view = self._table("neighbourhood_transit_access_metrics")
        census_view = self._table("census_by_neighbourhood")
        if not (self._db.relation_exists(access_view) and self._db.relation_exists(census_view)):
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT a.neighbourhood_id, a.neighbourhood, a.density_category,
                   c.population_density_per_km2, c.pct_commute_public_transit,
                   c.median_household_income_2020, c.pct_seniors_65_plus,
                   c.pct_recent_immigrants, c.pct_commute_car,
                   c.pct_commute_walk, c.pct_commute_cycle
            FROM {access_view} a
            LEFT JOIN {census_view} c ON a.neighbourhood_id = c.neighbourhood_id
            WHERE a.feed_id = :feed_id
            ORDER BY a.neighbourhood
            """,
            {"feed_id": self._feed_id},
        )


def categorize_coverage(density: float) -> str:
    """Categorize stop density.

    Args:
        density: Stop density in stops per square kilometre.

    Returns:
        Coverage category label.
    """
    if density >= COVERAGE_HIGH:
        return "High"
    if density >= COVERAGE_MEDIUM:
        return "Medium"
    return "Low"


def _classify_quadrant(need: float, gap: float) -> str:
    """Classify a neighbourhood into a priority quadrant.

    Args:
        need: Need index z-score.
        gap: Gap index z-score.

    Returns:
        Quadrant label.
    """
    if need > 0 and gap > 0:
        return "High Need / High Gap"
    if need > 0 and gap <= 0:
        return "High Need / Low Gap"
    if need <= 0 and gap > 0:
        return "Low Need / High Gap"
    return "Low Need / Low Gap"
