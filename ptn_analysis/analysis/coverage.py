"""Coverage, access, and equity analysis for neighbourhood transit service."""

from __future__ import annotations


import pandas as pd

from ptn_analysis.context.config import (
    WGS84_CRS,
    WINNIPEG_PROJECTED_CRS,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.analysis.base import AnalyzerBase

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

    def _scale_metric(self, values: pd.Series) -> pd.Series:
        """Scale one numeric series to the ``0`` to ``1`` range.

        Args:
            values: Numeric series.

        Returns:
            Min-max scaled series.
        """
        numeric_values = pd.to_numeric(values, errors="coerce").fillna(0.0)
        min_value = float(numeric_values.min())
        max_value = float(numeric_values.max())
        if min_value == max_value:
            return pd.Series(0.0, index=numeric_values.index, dtype="float64")
        return (numeric_values - min_value) / (max_value - min_value)

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

    @staticmethod
    def _empty_accessibility_frame(output_segment_count_col: str) -> pd.DataFrame:
        """Return an empty walkability/bikeability base table with the expected schema."""
        return pd.DataFrame(
            {
                "feed_id": pd.Series(dtype="object"),
                "neighbourhood_id": pd.Series(dtype="object"),
                "neighbourhood": pd.Series(dtype="object"),
                "area_km2": pd.Series(dtype="float64"),
                "stop_count": pd.Series(dtype="float64"),
                "stop_density_per_km2": pd.Series(dtype="float64"),
                "accessible_stop_count": pd.Series(dtype="float64"),
                output_segment_count_col: pd.Series(dtype="float64"),
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
        """Shared SQL kernel for walkability and bikeability.

        Args:
            infra_table: Logical table name for infrastructure segments (e.g. ``"walkways"``).
            buffer_metres: Stop-access buffer radius in metres.
            output_segment_count_col: Output column name for the segment count
                (e.g. ``"walkway_segment_count"``).

        Returns:
            Raw neighbourhood-level accessibility table before scoring.
        """
        density_tbl = self._table("neighbourhood_stop_count_density")
        nb_tbl = self._table("neighbourhoods")
        stops_tbl = self._table("stops")
        infra_tbl = self._table(infra_table)
        if not all(
            self._db.relation_exists(table_name)
            for table_name in (density_tbl, nb_tbl, stops_tbl, infra_tbl)
        ):
            return self._empty_accessibility_frame(output_segment_count_col)

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
        """Build a neighbourhood cycling infrastructure index for one feed.

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
        """Compute bikeability scores for one buffer distance."""
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
        """Combine walkability, bikeability, and jobs access into one table.

        Returns:
            Multimodal access table.
        """
        jobs_access_table = self.jobs_access()
        walkability_table = self.walkability()
        bikeability_table = self.cycling_infrastructure_index()

        if jobs_access_table.empty and walkability_table.empty and bikeability_table.empty:
            return pd.DataFrame()

        multimodal_table = pd.DataFrame()
        if not jobs_access_table.empty:
            multimodal_table = jobs_access_table[
                [
                    "neighbourhood_id",
                    "neighbourhood",
                    "jobs_access_score",
                    "jobs_proxy_score",
                ]
            ].copy()
        elif not walkability_table.empty:
            multimodal_table = walkability_table[["neighbourhood_id", "neighbourhood"]].copy()
        else:
            multimodal_table = bikeability_table[["neighbourhood_id", "neighbourhood"]].copy()

        if not walkability_table.empty:
            multimodal_table = multimodal_table.merge(
                walkability_table[
                    [
                        "neighbourhood_id",
                        "walkability_score",
                        "accessible_stop_count",
                        "walkway_segment_count",
                    ]
                ],
                on="neighbourhood_id",
                how="left",
            )
        if not bikeability_table.empty:
            multimodal_table = multimodal_table.merge(
                bikeability_table[
                    [
                        "neighbourhood_id",
                        "bikeability_score",
                        "cycling_segment_count",
                    ]
                ],
                on="neighbourhood_id",
                how="left",
            )

        if "jobs_access_score" not in multimodal_table.columns:
            multimodal_table["jobs_access_score"] = 0.0
        if "walkability_score" not in multimodal_table.columns:
            multimodal_table["walkability_score"] = 0.0
        if "bikeability_score" not in multimodal_table.columns:
            multimodal_table["bikeability_score"] = 0.0

        multimodal_table["jobs_access_score"] = pd.to_numeric(
            multimodal_table["jobs_access_score"],
            errors="coerce",
        ).fillna(0.0)
        multimodal_table["walkability_score"] = pd.to_numeric(
            multimodal_table["walkability_score"],
            errors="coerce",
        ).fillna(0.0)
        multimodal_table["bikeability_score"] = pd.to_numeric(
            multimodal_table["bikeability_score"],
            errors="coerce",
        ).fillna(0.0)

        multimodal_table["multimodal_access_score"] = (
            0.40 * self._scale_metric(multimodal_table["jobs_access_score"])
            + 0.30 * self._scale_metric(multimodal_table["walkability_score"])
            + 0.30 * self._scale_metric(multimodal_table["bikeability_score"])
        ).round(4)
        multimodal_table["multimodal_gap_score"] = (
            1 - multimodal_table["multimodal_access_score"]
        ).round(4)
        return multimodal_table.sort_values(
            "multimodal_gap_score",
            ascending=False,
        ).reset_index(drop=True)

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

    @staticmethod
    def _zscore(series: pd.Series) -> pd.Series:
        """Compute z-scores for a numeric series, filling NaN with 0."""
        numeric = pd.to_numeric(series, errors="coerce")
        filled = numeric.fillna(numeric.median())
        std = filled.std()
        if std == 0 or pd.isna(std):
            return pd.Series(0.0, index=series.index)
        return (filled - filled.mean()) / std

    def priority_matrix(self) -> pd.DataFrame:
        """Build a z-score-based neighbourhood priority matrix.

        Uses standardised z-scores instead of ordinal ranks to produce
        a continuous need index and gap index with quadrant labels:

        * **need_index** = z(transit_dependency) + z(-income)
        * **gap_index**  = z(baseline_score) - z(current_score)
        * **quadrant**   = classify(need > 0, gap > 0)

        Returns:
            Priority table with z-score indices and quadrant labels.
        """
        jobs_access_table = self.jobs_access()
        if jobs_access_table.empty:
            return jobs_access_table

        priority_table = jobs_access_table.copy()
        multimodal_table = self.multimodal_equity()
        try:
            equity_table = self.equity_profile()
        except NotImplementedError:
            equity_table = pd.DataFrame()

        if not multimodal_table.empty:
            priority_table = priority_table.merge(
                multimodal_table[
                    [
                        "neighbourhood_id",
                        "walkability_score",
                        "bikeability_score",
                        "multimodal_gap_score",
                    ]
                ],
                on="neighbourhood_id",
                how="left",
            )
        else:
            priority_table["walkability_score"] = pd.NA
            priority_table["bikeability_score"] = pd.NA
            priority_table["multimodal_gap_score"] = pd.NA

        if not equity_table.empty:
            priority_table = priority_table.merge(
                equity_table[
                    [
                        "neighbourhood_id",
                        "population_total",
                        "median_household_income_2020",
                        "commute_public_transit",
                    ]
                ],
                on="neighbourhood_id",
                how="left",
            )
        else:
            priority_table["population_total"] = pd.NA
            priority_table["median_household_income_2020"] = pd.NA
            priority_table["commute_public_transit"] = pd.NA

        # Z-score-based need index: high transit dependency + low income = high need
        z_transit = self._zscore(priority_table["commute_public_transit"])
        z_neg_income = self._zscore(priority_table["median_household_income_2020"].mul(-1))
        priority_table["need_index"] = (z_transit + z_neg_income).round(4)

        # Gap index: high multimodal gap = underserved
        z_gap = self._zscore(priority_table["multimodal_gap_score"].fillna(0))
        z_neg_access = self._zscore(priority_table["jobs_access_score"].fillna(0).mul(-1))
        priority_table["gap_index"] = (z_gap + z_neg_access).round(4)

        # Quadrant classification
        priority_table["quadrant"] = priority_table.apply(
            lambda row: _classify_quadrant(row["need_index"], row["gap_index"]),
            axis=1,
        )

        # Composite priority score (sum of z-scores, higher = more urgent)
        priority_table["priority_score"] = (
            priority_table["need_index"] + priority_table["gap_index"]
        ).round(4)

        return priority_table.sort_values("priority_score", ascending=False)

    def build_priority_metrics_table(self) -> pd.DataFrame:
        """Build the canonical neighbourhood priority table.

        Returns:
            Priority metrics table.
        """
        table_name = self._table("neighbourhood_priority_metrics")
        if self._db.relation_exists(table_name):
            priority_table = self._db.query(
                f"""
                SELECT *
                FROM {table_name}
                WHERE feed_id = :feed_id
                """,
                {"feed_id": self._feed_id},
            )
            sort_column = "priority_score" if "priority_score" in priority_table.columns else "priority_rank"
            if sort_column in priority_table.columns:
                ascending = sort_column == "priority_rank"
                priority_table = priority_table.sort_values(sort_column, ascending=ascending)
            return priority_table
        return self.priority_matrix()

    # ------------------------------------------------------------------
    # CHASS Census Journey to Work analysis methods
    # ------------------------------------------------------------------

    def commute_duration_vs_r5py(self) -> pd.DataFrame:
        """Compare census self-reported commute durations against r5py travel times.

        Joins CHASS commute duration distribution per DA with r5py P50
        transit travel time. Useful for validating r5py against real-world
        commute behaviour.

        Returns:
            DataFrame with DA-level census duration bins and r5py P50 travel time.
            Empty DataFrame if required tables are missing.
        """
        census_table = self._table("census_da")
        transit_matrix_table = self._table(f"transit_matrix_{self._feed_id}")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()

        census_df = self._db.query(
            f"""
            SELECT geo_uid,
                   commute_dur_total,
                   commute_dur_lt15,
                   commute_dur_15_29,
                   commute_dur_30_44,
                   commute_dur_45_59,
                   commute_dur_60_plus
            FROM {census_table}
            WHERE commute_dur_total > 0
            """
        )
        if census_df.empty:
            return census_df

        # Compute weighted median commute duration from census bins.
        bin_midpoints = {
            "commute_dur_lt15": 7.5,
            "commute_dur_15_29": 22.0,
            "commute_dur_30_44": 37.0,
            "commute_dur_45_59": 52.0,
            "commute_dur_60_plus": 75.0,
        }
        weighted_sum = pd.Series(0.0, index=census_df.index)
        for col, midpoint in bin_midpoints.items():
            weighted_sum = weighted_sum + census_df[col].fillna(0) * midpoint
        census_df["census_mean_commute_min"] = (
            weighted_sum / census_df["commute_dur_total"]
        ).round(1)

        # Join r5py P50 transit travel time if available
        if self._db.relation_exists(transit_matrix_table):
            r5py_df = self._db.query(
                f"""
                SELECT from_id AS geo_uid,
                       ROUND(AVG(travel_time_p50), 1) AS r5py_p50_travel_time_min
                FROM {transit_matrix_table}
                GROUP BY from_id
                """
            )
            census_df = census_df.merge(r5py_df, on="geo_uid", how="left")

        return census_df

    def departure_demand_vs_gtfs_supply(self) -> pd.DataFrame:
        """Overlay census departure time distribution on GTFS departure frequency.

        Computes hourly departure demand from census Journey to Work departure
        time bins and compares with GTFS scheduled hourly departures.

        Returns:
            DataFrame with hour, census_demand_pct, gtfs_departures.
            Empty DataFrame if census data is missing.
        """
        census_table = self._table("census_da")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()

        # Aggregate departure time distribution across all Winnipeg DAs
        departure_df = self._db.query(
            f"""
            SELECT SUM(depart_total) AS total,
                   SUM(depart_5am) AS h5,
                   SUM(depart_6am) AS h6,
                   SUM(depart_7am) AS h7,
                   SUM(depart_8am) AS h8,
                   SUM(depart_9_11am) AS h9_11,
                   SUM(depart_12_4am) AS h12_4
            FROM {census_table}
            WHERE depart_total > 0
            """
        )
        if departure_df.empty or departure_df["total"].iloc[0] == 0:
            return pd.DataFrame()

        total = departure_df["total"].iloc[0]
        demand_rows = [
            {"hour_label": "5:00-5:59", "hour": 5, "census_demand_pct": round(100 * departure_df["h5"].iloc[0] / total, 1)},
            {"hour_label": "6:00-6:59", "hour": 6, "census_demand_pct": round(100 * departure_df["h6"].iloc[0] / total, 1)},
            {"hour_label": "7:00-7:59", "hour": 7, "census_demand_pct": round(100 * departure_df["h7"].iloc[0] / total, 1)},
            {"hour_label": "8:00-8:59", "hour": 8, "census_demand_pct": round(100 * departure_df["h8"].iloc[0] / total, 1)},
            {"hour_label": "9:00-11:59", "hour": 10, "census_demand_pct": round(100 * departure_df["h9_11"].iloc[0] / total, 1)},
            {"hour_label": "12:00-4:59", "hour": 14, "census_demand_pct": round(100 * departure_df["h12_4"].iloc[0] / total, 1)},
        ]
        demand = pd.DataFrame(demand_rows)

        # Join GTFS hourly departure counts if frequency view exists
        freq_view = self._table("route_hourly_departures")
        if self._db.relation_exists(freq_view):
            gtfs_hourly = self._db.query(
                f"""
                SELECT hour,
                       SUM(departures) AS gtfs_departures
                FROM {freq_view}
                WHERE feed_id = :feed_id
                GROUP BY hour
                ORDER BY hour
                """,
                {"feed_id": self._feed_id},
            )
            if not gtfs_hourly.empty:
                demand = demand.merge(gtfs_hourly, on="hour", how="left")

        return demand

    def commute_destination_analysis(self) -> pd.DataFrame:
        """Census commute destination geography per DA.

        Classifies commuters by destination: within CSD (intra-city),
        different CSD same CD (suburban), different CD, different province.

        Returns:
            DataFrame with DA-level commute destination breakdown.
        """
        census_table = self._table("census_da")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()

        return self._db.query(
            f"""
            SELECT geo_uid,
                   commute_dest_total,
                   commute_within_csd,
                   commute_diff_csd_same_cd,
                   commute_diff_cd,
                   commute_diff_province,
                   ROUND(100.0 * commute_within_csd / NULLIF(commute_dest_total, 0), 1)
                       AS pct_within_city,
                   ROUND(100.0 * commute_diff_csd_same_cd / NULLIF(commute_dest_total, 0), 1)
                       AS pct_suburban,
                   ROUND(100.0 * (commute_diff_cd + commute_diff_province)
                       / NULLIF(commute_dest_total, 0), 1)
                       AS pct_external
            FROM {census_table}
            WHERE commute_dest_total > 0
            """
        )

    def modal_share_by_neighbourhood(self) -> pd.DataFrame:
        """Census 2021 commute mode split aggregated by neighbourhood.

        Includes car_driver/car_passenger split from CHASS data. Requires
        the ``census_by_neighbourhood`` SQL view.

        Returns:
            DataFrame with neighbourhood-level modal share percentages.
        """
        view_name = self._table("census_by_neighbourhood")
        if not self._db.relation_exists(view_name):
            return pd.DataFrame()

        return self._db.query(
            f"""
            SELECT neighbourhood_id,
                   neighbourhood,
                   population_total,
                   pct_commute_public_transit,
                   pct_commute_car,
                   pct_commute_walk,
                   pct_commute_cycle,
                   pct_commute_other,
                   median_household_income_2020
            FROM {view_name}
            ORDER BY pct_commute_public_transit DESC
            """
        )

    def population_stability_map_data(self) -> pd.DataFrame:
        """One-year mobility status per DA for newcomer/stability analysis.

        Uses CHASS mobility variables to identify DAs with high population
        turnover (movers) and external migration (newcomers).

        Returns:
            DataFrame with DA-level mobility percentages.
            Empty DataFrame if census data is missing.
        """
        census_table = self._table("census_da")
        if not self._db.relation_exists(census_table):
            return pd.DataFrame()

        return self._db.query(
            f"""
            SELECT geo_uid,
                   population_2021,
                   mobility_1yr_total,
                   mobility_1yr_nonmovers,
                   mobility_1yr_movers,
                   mobility_1yr_external,
                   ROUND(100.0 * mobility_1yr_movers / NULLIF(mobility_1yr_total, 0), 1)
                       AS pct_movers,
                   ROUND(100.0 * mobility_1yr_external / NULLIF(mobility_1yr_total, 0), 1)
                       AS pct_external_migrants,
                   pct_immigrant,
                   pct_visible_minority
            FROM {census_table}
            WHERE mobility_1yr_total > 0
            """
        )

    # ------------------------------------------------------------------
    # r5py integration
    # ------------------------------------------------------------------

    def r5py_accessibility_summary(self) -> pd.DataFrame:
        """Summarize r5py transit travel times by neighbourhood.

        Returns empty frame when r5py tables don't exist (graceful degradation).
        """
        matrix = self.transit_matrix()
        if matrix.empty:
            return pd.DataFrame()
        p50_col = "travel_time_p50" if "travel_time_p50" in matrix.columns else matrix.columns[-1]
        bridge = self.stop_neighbourhood_bridge()
        if bridge.empty:
            return pd.DataFrame()
        merged = matrix.merge(
            bridge.rename(columns={"stop_id": "from_id", "neighbourhood": "origin_neighbourhood"}),
            on="from_id",
            how="left",
        )
        return (
            merged.groupby("origin_neighbourhood")[p50_col]
            .agg(["median", "mean", "count"])
            .reset_index()
            .rename(columns={
                "origin_neighbourhood": "neighbourhood",
                "median": "median_travel_time_p50",
                "mean": "mean_travel_time_p50",
                "count": "od_pair_count",
            })
        )

    def jobs_reachable_by_neighbourhood(self) -> pd.DataFrame:
        """Jobs reachable within 45 min by transit, per neighbourhood."""
        return self.jobs_reachable()

    def combined_jobs_access(self) -> pd.DataFrame:
        """neighbourhood_jobs_access_metrics enriched with r5py jobs_reachable when available."""
        base = self.neighbourhood_jobs_access()
        r5py = self.jobs_reachable_by_neighbourhood()
        if r5py.empty:
            return base
        join_col = "neighbourhood" if "neighbourhood" in r5py.columns else r5py.columns[0]
        return base.merge(r5py, on=join_col, how="left")

    # ------------------------------------------------------------------
    # PR2 analysis stubs — Sudipta
    # ------------------------------------------------------------------

    def travel_time_equity_report(self) -> pd.DataFrame:
        """Compare r5py travel times across income/demographic quintiles.

        Returns:
            DataFrame with quintile, median_travel_time, pct_over_45min.

        Hint:
            # 1. Load ywg_transit_matrix_{feed} (r5py P50 travel times)
            # 2. Load ywg_census_da (median_total_income, pct_visible_minority)
            # 3. pd.qcut(income, 5) → income_quintile
            # 4. Group travel times by quintile, compute median + pct > 45 min
        """
        return pd.DataFrame()

    def poverty_transit_correlation(self) -> pd.DataFrame:
        """Correlate LICO-AT poverty rate with transit access metrics.

        Returns:
            DataFrame with neighbourhood, lico_at_pct, stop_density, jobs_access_score.

        Hint:
            # 1. Load census_by_neighbourhood view (has lico_at_pct)
            # 2. Join neighbourhood_stop_count_density on neighbourhood_id
            # 3. Join neighbourhood_jobs_access_metrics on neighbourhood_id
            # 4. Return merged table for scatter plot
        """
        return pd.DataFrame()

    def build_equity_regression_table(self) -> pd.DataFrame:
        """OLS regression: transit access ~ income + demographics.

        Returns:
            DataFrame with coefficient, std_err, p_value, r_squared.

        Hint:
            # 1. Build X matrix: median_total_income, pct_indigenous,
            #    pct_visible_minority, pct_renter (from census_by_neighbourhood)
            # 2. Y = stop_density or jobs_access_score
            # 3. statsmodels.api.OLS(Y, sm.add_constant(X)).fit()
            # 4. Return summary_frame() + rsquared
        """
        return pd.DataFrame()

    def build_spatial_autocorrelation_table(self) -> pd.DataFrame:
        """Compute Moran's I and LISA clusters for stop density.

        Returns:
            DataFrame with neighbourhood, lisa_cluster, lisa_p_value, local_i.

        Hint:
            # 1. Load neighbourhood geometries + stop_density
            # 2. libpysal.weights.Queen.from_dataframe(gdf)
            # 3. esda.Moran(density, w) → global I
            # 4. esda.Moran_Local(density, w) → LISA clusters (HH/HL/LH/LL)
        """
        return pd.DataFrame()

    def cluster_neighbourhoods(self, k: int = 4) -> pd.DataFrame:
        """K-means clustering of neighbourhoods by transit service profile.

        Args:
            k: Number of clusters.

        Returns:
            DataFrame with neighbourhood, cluster_id, stop_density,
            mean_headway, jobs_access_score.

        Hint:
            # 1. Build feature matrix: stop_density, mean_headway, jobs_access
            # 2. StandardScaler().fit_transform(X)
            # 3. KMeans(n_clusters=k).fit(X_scaled)
            # 4. Attach cluster labels to neighbourhood table
        """
        return pd.DataFrame()


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
