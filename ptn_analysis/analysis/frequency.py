"""Route and stop schedule analysis.

This module keeps the public analysis surface small and explicit. Each method
returns one report-ready table instead of scattering related logic across many
small helpers.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from loguru import logger
import pandas as pd

from ptn_analysis.context.config import DEFAULT_ANALYSIS_DATE, PTN_HEADWAY_TARGETS
from ptn_analysis.context.db import TransitDB
from ptn_analysis.analysis.base import AnalyzerBase

MODELS_DIR: Final[Path] = Path(__file__).parents[2] / "models" / "production"

_GTFS_TIME_TO_SECONDS = """(
    CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) * 3600 +
    CAST(SPLIT_PART(st.departure_time, ':', 2) AS INTEGER) * 60 +
    CAST(SPLIT_PART(st.departure_time, ':', 3) AS INTEGER)
)"""

_ACTIVE_SERVICES_CTE = """
    active_services AS (
        SELECT service_id
        FROM {calendar_table}
        WHERE CAST(:service_date AS DATE) BETWEEN
              COALESCE(
                  TRY_STRPTIME(CAST(start_date AS VARCHAR), '%Y%m%d')::DATE,
                  TRY_CAST(start_date AS DATE)
              )
          AND COALESCE(
                  TRY_STRPTIME(CAST(end_date AS VARCHAR), '%Y%m%d')::DATE,
                  TRY_CAST(end_date AS DATE)
              )
          AND CASE EXTRACT(DOW FROM CAST(:service_date AS DATE))
                WHEN 0 THEN sunday
                WHEN 1 THEN monday
                WHEN 2 THEN tuesday
                WHEN 3 THEN wednesday
                WHEN 4 THEN thursday
                WHEN 5 THEN friday
                WHEN 6 THEN saturday
              END = 1
    )
"""


class FrequencyAnalyzer(AnalyzerBase):
    """Analyze scheduled service metrics for one GTFS feed.

    Args:
        city_key: City namespace used to resolve physical table names.
        feed_id: GTFS feed identifier.
        db_instance: Database connection wrapper.
        service_date: Optional analysis date. If not provided, the analyzer
            chooses a representative date from the route metrics table.
    """

    def __init__(
        self,
        city_key: str,
        feed_id: str,
        db_instance: TransitDB,
        service_date: str | None = None,
    ):
        """Initialize the frequency analyzer."""
        super().__init__(city_key, feed_id, db_instance)
        self._service_date = service_date

    def __repr__(self) -> str:
        return f"FrequencyAnalyzer(city_key={self._city_key!r}, feed_id={self._feed_id!r})"

    @staticmethod
    def _empty_hourly_departure_frame() -> pd.DataFrame:
        """Return an empty hourly-departure table with the expected schema."""
        return pd.DataFrame(
            {
                "route_id": pd.Series(dtype="object"),
                "route_short_name": pd.Series(dtype="object"),
                "route_long_name": pd.Series(dtype="object"),
                "hour": pd.Series(dtype="int64"),
                "departures": pd.Series(dtype="int64"),
            }
        )

    @property
    def service_date(self) -> str:
        """Return the active analysis date.

        Returns:
            Service date in ``YYYY-MM-DD`` format.
        """
        if self._service_date is None:
            self._service_date = self._detect_service_date()
        return self._service_date

    def _detect_service_date(self) -> str:
        """Pick a representative service date for the current feed.

        Returns:
            Service date in ``YYYY-MM-DD`` format.
        """
        route_metrics_table = self._table("gtfs_route_stats")
        feed_info_table = self._table("feed_info")

        try:
            detected_date = self._db.first(
                f"""
                SELECT date
                FROM {route_metrics_table}
                WHERE feed_id = :feed_id AND date IS NOT NULL
                GROUP BY date
                ORDER BY SUM(num_trips) DESC
                LIMIT 1
                """,
                {"feed_id": self._feed_id},
            )
            if detected_date:
                return str(detected_date)
        except Exception:
            logger.debug("Could not detect service date from route metrics.")

        try:
            raw_feed_start = self._db.first(
                f"SELECT feed_start_date FROM {feed_info_table} LIMIT 1"
            )
            if raw_feed_start:
                raw_text = str(raw_feed_start)
                if len(raw_text) == 8 and raw_text.isdigit():
                    year = raw_text[0:4]
                    month = raw_text[4:6]
                    day = raw_text[6:8]
                    return f"{year}-{month}-{day}"
                return raw_text
        except Exception:
            logger.debug("Could not detect service date from feed_info.")

        return DEFAULT_ANALYSIS_DATE

    def route_frequency(self, split_directions: bool = False) -> pd.DataFrame:
        """Return route-level scheduled frequency metrics.

        Args:
            split_directions: When True, keep one row per route direction.

        Returns:
            Route frequency metrics for the selected feed and service date.
        """
        route_metrics_table = self._table("gtfs_route_stats")
        if self._db.count(route_metrics_table) is None:
            logger.warning("Route metrics table is missing. Run the data pipeline first.")
            return pd.DataFrame()

        if split_directions:
            query = f"""
                SELECT route_id,
                       route_short_name,
                       direction_id,
                       num_trips AS scheduled_trip_count,
                       mean_headway AS mean_headway_minutes,
                       min_headway AS min_headway_minutes,
                       max_headway AS max_headway_minutes,
                       peak_num_trips,
                       service_duration,
                       service_speed AS scheduled_speed_kmh,
                       start_time,
                       end_time,
                       service_distance,
                       mean_trip_distance,
                       mean_trip_duration
                FROM {route_metrics_table}
                WHERE feed_id = :feed_id AND date = :service_date
                ORDER BY route_short_name, direction_id
            """
        else:
            query = f"""
                SELECT route_id,
                       ANY_VALUE(route_short_name) AS route_short_name,
                       SUM(num_trips) AS scheduled_trip_count,
                       SUM(mean_headway * num_trips) / NULLIF(SUM(num_trips), 0)
                           AS mean_headway_minutes,
                       MIN(min_headway) AS min_headway_minutes,
                       MAX(max_headway) AS max_headway_minutes,
                       SUM(peak_num_trips) AS peak_num_trips,
                       MAX(service_duration) AS service_duration,
                       AVG(service_speed) AS scheduled_speed_kmh,
                       MIN(start_time) AS start_time,
                       MAX(end_time) AS end_time,
                       SUM(service_distance) AS service_distance,
                       AVG(mean_trip_distance) AS mean_trip_distance,
                       AVG(mean_trip_duration) AS mean_trip_duration
                FROM {route_metrics_table}
                WHERE feed_id = :feed_id AND date = :service_date
                GROUP BY route_id
                ORDER BY route_short_name
            """

        params = {"feed_id": self._feed_id, "service_date": self.service_date}
        return self._db.query(query, params)

    def stop_headways(self, stop_id: str, split_directions: bool = True) -> pd.DataFrame:
        """Return stop-level headway metrics for one stop.

        Args:
            stop_id: Transit stop identifier.
            split_directions: When True, keep one row per direction.

        Returns:
            Stop headway metrics for the selected feed and service date.
        """
        stop_metrics_table = self._table("gtfs_stop_stats")
        if self._db.count(stop_metrics_table) is None:
            logger.warning("Stop metrics table is missing. Run the data pipeline first.")
            return pd.DataFrame()

        params = {
            "feed_id": self._feed_id,
            "service_date": self.service_date,
            "stop_id": stop_id,
        }

        if split_directions:
            return self._db.query(
                f"""
                SELECT stop_id,
                       direction_id,
                       num_routes,
                       num_trips AS scheduled_trip_count,
                       mean_headway AS mean_headway_minutes,
                       min_headway AS min_headway_minutes,
                       max_headway AS max_headway_minutes,
                       start_time,
                       end_time
                FROM {stop_metrics_table}
                WHERE feed_id = :feed_id
                  AND date = :service_date
                  AND stop_id = :stop_id
                ORDER BY direction_id
                """,
                params,
            )

        return self._db.query(
            f"""
            SELECT stop_id,
                   SUM(num_trips) AS scheduled_trip_count,
                   MAX(num_routes) AS num_routes,
                   AVG(mean_headway) AS mean_headway_minutes,
                   MIN(min_headway) AS min_headway_minutes,
                   MAX(max_headway) AS max_headway_minutes,
                   MIN(start_time) AS start_time,
                   MAX(end_time) AS end_time
            FROM {stop_metrics_table}
            WHERE feed_id = :feed_id
              AND date = :service_date
              AND stop_id = :stop_id
            GROUP BY stop_id
            """,
            params,
        )

    def frequency_summary(self) -> dict[str, float]:
        """Summarize scheduled service at the route level.

        Returns:
            Dictionary of route-count, trip-count, and headway summary values.
        """
        route_frequency_table = self.route_frequency(split_directions=False)
        if route_frequency_table.empty:
            return {
                "total_routes": 0,
                "total_trips": 0,
                "mean_headway_minutes": 0.0,
                "routes_under_15min": 0,
                "routes_under_30min": 0,
            }

        mean_headway_series = route_frequency_table["mean_headway_minutes"]
        routes_under_15 = mean_headway_series.lt(15).sum()
        routes_under_30 = mean_headway_series.lt(30).sum()

        return {
            "total_routes": int(len(route_frequency_table)),
            "total_trips": int(route_frequency_table["scheduled_trip_count"].sum()),
            "mean_headway_minutes": float(mean_headway_series.mean()),
            "routes_under_15min": int(routes_under_15),
            "routes_under_30min": int(routes_under_30),
        }

    def headway_statistics(self) -> pd.DataFrame:
        """Compute route-direction headway statistics from first-stop departures.

        Returns:
            DataFrame with mean, median, and quartile-based headway metrics.
        """
        calendar_table = self._table("calendar")
        stop_times_table = self._table("stop_times")
        trips_table = self._table("trips")
        routes_table = self._table("routes")
        active_services_cte = _ACTIVE_SERVICES_CTE.format(calendar_table=calendar_table)

        query = f"""
            WITH {active_services_cte},
            first_stop_departures AS (
                SELECT t.route_id,
                       r.route_short_name,
                       t.direction_id,
                       t.trip_id,
                       {_GTFS_TIME_TO_SECONDS} AS departure_seconds
                FROM {stop_times_table} st
                JOIN {trips_table} t
                    ON st.trip_id = t.trip_id AND st.feed_id = t.feed_id
                JOIN {routes_table} r
                    ON t.route_id = r.route_id AND t.feed_id = r.feed_id
                WHERE st.feed_id = :feed_id
                  AND st.stop_sequence = 1
                  AND t.service_id IN (SELECT service_id FROM active_services)
            ),
            ordered_departures AS (
                SELECT route_id,
                       route_short_name,
                       direction_id,
                       trip_id,
                       departure_seconds,
                       departure_seconds - LAG(departure_seconds) OVER (
                           PARTITION BY route_id, direction_id
                           ORDER BY departure_seconds
                       ) AS gap_seconds
                FROM first_stop_departures
            )
            SELECT route_id,
                   ANY_VALUE(route_short_name) AS route_short_name,
                   direction_id,
                   ROUND(AVG(gap_seconds) / 60.0, 1) AS mean_headway_minutes,
                   ROUND(MEDIAN(gap_seconds) / 60.0, 1) AS median_headway_minutes,
                   ROUND(
                       (
                           QUANTILE_CONT(gap_seconds, 0.75)
                           - QUANTILE_CONT(gap_seconds, 0.25)
                       ) / 60.0,
                       1
                   ) AS headway_iqr_minutes,
                   ROUND(QUANTILE_CONT(gap_seconds, 0.25) / 60.0, 1) AS headway_p25_minutes,
                   ROUND(QUANTILE_CONT(gap_seconds, 0.75) / 60.0, 1) AS headway_p75_minutes,
                   COUNT(*) + 1 AS scheduled_trip_count
            FROM ordered_departures
            WHERE gap_seconds IS NOT NULL AND gap_seconds > 0
            GROUP BY route_id, direction_id
            ORDER BY route_short_name, direction_id
        """
        params = {"feed_id": self._feed_id, "service_date": self.service_date}
        return self._db.query(query, params)

    def time_windowed_headway(self) -> pd.DataFrame:
        """Compute route headways for standard time-of-day windows.

        Returns:
            DataFrame with one row per route and time window.
        """
        calendar_table = self._table("calendar")
        stop_times_table = self._table("stop_times")
        trips_table = self._table("trips")
        routes_table = self._table("routes")
        active_services_cte = _ACTIVE_SERVICES_CTE.format(calendar_table=calendar_table)

        query = f"""
            WITH {active_services_cte},
            first_stop_departures AS (
                SELECT t.route_id,
                       r.route_short_name,
                       t.trip_id,
                       {_GTFS_TIME_TO_SECONDS} AS departure_seconds,
                       CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) AS departure_hour
                FROM {stop_times_table} st
                JOIN {trips_table} t
                    ON st.trip_id = t.trip_id AND st.feed_id = t.feed_id
                JOIN {routes_table} r
                    ON t.route_id = r.route_id AND t.feed_id = r.feed_id
                WHERE st.feed_id = :feed_id
                  AND st.stop_sequence = 1
                  AND t.service_id IN (SELECT service_id FROM active_services)
            ),
            departures_by_window AS (
                SELECT route_id,
                       route_short_name,
                       trip_id,
                       departure_seconds,
                       CASE
                           WHEN departure_hour BETWEEN 7 AND 8 THEN 'AM Peak'
                           WHEN departure_hour BETWEEN 10 AND 13 THEN 'Interpeak'
                           WHEN departure_hour BETWEEN 15 AND 17 THEN 'PM Peak'
                           WHEN departure_hour BETWEEN 18 AND 21 THEN 'Evening'
                           ELSE 'Off-Peak'
                       END AS time_window
                FROM first_stop_departures
            ),
            gap_table AS (
                SELECT route_id,
                       route_short_name,
                       trip_id,
                       time_window,
                       departure_seconds,
                       departure_seconds - LAG(departure_seconds) OVER (
                           PARTITION BY route_id, time_window
                           ORDER BY departure_seconds
                       ) AS gap_seconds
                FROM departures_by_window
            )
            SELECT route_id,
                   ANY_VALUE(route_short_name) AS route_short_name,
                   time_window,
                   ROUND(AVG(gap_seconds) / 60.0, 1) AS mean_headway_minutes,
                   ROUND(MEDIAN(gap_seconds) / 60.0, 1) AS median_headway_minutes,
                   COUNT(*) + 1 AS scheduled_trip_count
            FROM gap_table
            WHERE gap_seconds IS NOT NULL AND gap_seconds > 0
            GROUP BY route_id, time_window
            ORDER BY route_short_name, time_window
        """
        params = {"feed_id": self._feed_id, "service_date": self.service_date}
        return self._db.query(query, params)

    def route_speeds(self) -> pd.DataFrame:
        """Return route-level scheduled speed metrics.

        Returns:
            DataFrame of scheduled speed metrics for the selected feed.
        """
        speed_view = self._table("route_schedule_speed_metrics")
        if self._db.count(speed_view) is None:
            logger.warning("Route schedule speed view is missing. Run the data pipeline first.")
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT *
            FROM {speed_view}
            WHERE feed_id = :feed_id
            ORDER BY ptn_tier, route_short_name
            """,
            {"feed_id": self._feed_id},
        )

    def route_performance(self) -> pd.DataFrame:
        """Return route-level performance context from pass-ups and on-time data.

        Returns:
            Performance table sorted by pass-up count.
        """
        performance_view = self._table("route_performance")
        if self._db.count(performance_view) is None:
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT *
            FROM {performance_view}
            WHERE feed_id = :feed_id
            ORDER BY passup_count DESC
            """,
            {"feed_id": self._feed_id},
        )

    def departures_by_hour_by_route(self) -> pd.DataFrame:
        """Return hourly departures for each route.

        Returns:
            DataFrame with one row per route and service hour.
        """
        hourly_view = self._table("route_hourly_departures")
        if self._db.count(hourly_view) is not None:
            return self._db.query(
                f"""
                SELECT *
                FROM {hourly_view}
                WHERE feed_id = :feed_id
                ORDER BY route_short_name, hour
                """,
                {"feed_id": self._feed_id},
            )

        stop_times_table = self._table("stop_times")
        trips_table = self._table("trips")
        routes_table = self._table("routes")
        if not all(
            self._db.relation_exists(table_name)
            for table_name in (stop_times_table, trips_table, routes_table)
        ):
            return self._empty_hourly_departure_frame()
        return self._db.query(
            f"""
            SELECT r.route_id,
                   r.route_short_name,
                   r.route_long_name,
                   CAST(SPLIT_PART(st.departure_time, ':', 1) AS INTEGER) AS hour,
                   COUNT(DISTINCT st.trip_id) AS departures
            FROM {stop_times_table} st
            JOIN {trips_table} t
                ON st.trip_id = t.trip_id AND st.feed_id = t.feed_id
            JOIN {routes_table} r
                ON t.route_id = r.route_id AND t.feed_id = r.feed_id
            WHERE st.feed_id = :feed_id AND st.stop_sequence = 1
            GROUP BY r.route_id, r.route_short_name, r.route_long_name, hour
            ORDER BY r.route_short_name, hour
            """,
            {"feed_id": self._feed_id},
        )

    def build_hourly_departure_table(self) -> pd.DataFrame:
        """Build the canonical hourly departure table for notebook use.

        Returns:
            Hourly route departure table.
        """
        return self.departures_by_hour_by_route()

    def span_of_service(self) -> pd.DataFrame:
        """Return first trip, last trip, and service span per route.

        Returns:
            DataFrame with route-level span-of-service metrics.
        """
        calendar_table = self._table("calendar")
        stop_times_table = self._table("stop_times")
        trips_table = self._table("trips")
        routes_table = self._table("routes")

        query = f"""
            WITH service_days AS (
                SELECT service_id,
                       CASE
                           WHEN service_id LIKE '%sunday%' OR service_id LIKE '%sun%' THEN 'Sunday'
                           WHEN service_id LIKE '%saturday%' OR service_id LIKE '%sat%' THEN 'Saturday'
                           ELSE 'Weekday'
                       END AS service_type
                FROM {calendar_table}
                WHERE feed_id = :feed_id
            ),
            route_departures AS (
                SELECT t.route_id,
                       r.route_short_name,
                       service_days.service_type,
                       st.departure_time,
                       {_GTFS_TIME_TO_SECONDS} AS departure_seconds
                FROM {stop_times_table} st
                JOIN {trips_table} t
                    ON st.trip_id = t.trip_id AND st.feed_id = t.feed_id
                JOIN {routes_table} r
                    ON t.route_id = r.route_id AND t.feed_id = r.feed_id
                JOIN service_days
                    ON t.service_id = service_days.service_id
                WHERE st.feed_id = :feed_id AND st.stop_sequence = 1
            )
            SELECT route_id,
                   ANY_VALUE(route_short_name) AS route_short_name,
                   service_type,
                   MIN(departure_time) AS first_departure,
                   MAX(departure_time) AS last_departure,
                   ROUND((MAX(departure_seconds) - MIN(departure_seconds)) / 3600.0, 1)
                       AS span_hours
            FROM route_departures
            GROUP BY route_id, service_type
            ORDER BY route_short_name, service_type
        """
        return self._db.query(query, {"feed_id": self._feed_id})

    def ptn_summary(self) -> pd.DataFrame:
        """Summarize route service by PTN tier.

        Returns:
            PTN-tier summary table including target compliance.
        """
        route_fact_table = self.build_route_schedule_fact_table()
        if route_fact_table.empty:
            return pd.DataFrame()

        def meets_target(row: pd.Series) -> bool:
            targets = PTN_HEADWAY_TARGETS.get(row["ptn_tier"])
            if targets is None:
                return False
            if pd.isna(row["mean_headway_minutes"]):
                return False
            off_peak_target = targets[1]
            return bool(row["mean_headway_minutes"] <= off_peak_target)

        route_fact_table = route_fact_table.copy()
        route_fact_table["meets_target"] = route_fact_table.apply(meets_target, axis=1)
        summary = (
            route_fact_table.groupby("ptn_tier")
            .agg(
                route_count=("route_id", "count"),
                avg_headway_minutes=("mean_headway_minutes", "mean"),
                median_headway_minutes=("mean_headway_minutes", "median"),
                avg_speed_kmh=("scheduled_speed_kmh", "mean"),
                pct_meeting_target=("meets_target", "mean"),
            )
            .reset_index()
        )
        summary["avg_headway_minutes"] = summary["avg_headway_minutes"].round(1)
        summary["median_headway_minutes"] = summary["median_headway_minutes"].round(1)
        summary["avg_speed_kmh"] = summary["avg_speed_kmh"].round(1)
        summary["pct_meeting_target"] = (summary["pct_meeting_target"] * 100).round(0)
        return summary

    def calculate_capacity_stress(self, top_n: int = 20) -> pd.DataFrame:
        """Rank routes by crowding and service stress proxies.

        Args:
            top_n: Number of highest-priority routes to return.

        Returns:
            Route-level capacity stress table.
        """
        capacity_view = self._table("route_capacity_priority")
        if self._db.count(capacity_view) is None:
            logger.warning("Route capacity priority view is missing. Run the data pipeline first.")
            return pd.DataFrame()
        # upgrade_priority_score is computed in Python (build_capacity_priority_table),
        # not in the SQL view. Order by passup rate proxy instead.
        return self._db.query(
            f"""
            SELECT *
            FROM {capacity_view}
            WHERE feed_id = :feed_id
            ORDER BY passups_per_100k_boardings DESC NULLS LAST
            LIMIT :top_n
            """,
            {"feed_id": self._feed_id, "top_n": top_n},
        )

    def calculate_route_reliability(self) -> pd.DataFrame:
        """Summarize on-time performance by route.

        Returns:
            DataFrame sorted from worst to best on-time performance.
        """
        reliability_view = self._table("route_reliability_metrics")
        if self._db.count(reliability_view) is None:
            logger.warning("Route reliability view is missing. Run the data pipeline first.")
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT *
            FROM {reliability_view}
            WHERE feed_id = :feed_id
            ORDER BY pct_on_time ASC NULLS LAST
            """,
            {"feed_id": self._feed_id},
        )

    def build_route_reliability_table(self) -> pd.DataFrame:
        """Build the canonical route reliability table.

        Returns:
            Route reliability table.
        """
        return self.calculate_route_reliability()

    def build_route_schedule_fact_table(self) -> pd.DataFrame:
        """Build the canonical route-level PR2 fact table.

        Returns:
            Route-level fact table combining schedule, tier, and performance metrics.
        """
        facts_view = self._table("route_schedule_facts")
        if self._db.count(facts_view) is None:
            logger.warning("Route schedule facts view is missing. Run the data pipeline first.")
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT *
            FROM {facts_view}
            WHERE feed_id = :feed_id
            ORDER BY route_short_name
            """,
            {"feed_id": self._feed_id},
        )

    def build_capacity_priority_table(self, top_n: int = 20) -> pd.DataFrame:
        """Build route capacity and reliability priority outputs with NegBin risk scoring.

        Loads the capacity priority view and enriches it with a Negative Binomial
        GLM passup risk score if the trained model exists at
        ``models/production/passup_nb_v1.pkl``. Falls back to a heuristic
        ``upgrade_priority_score`` if the model is absent.

        Args:
            top_n: Number of rows to return, ordered by passup risk.

        Returns:
            Ranked capacity-priority table with ``upgrade_priority_score``,
            ``passup_risk_score``, and ``recommendation`` columns.
        """
        import numpy as np

        df = self.calculate_capacity_stress(top_n=top_n)
        if df.empty:
            return df

        models_dir = Path(__file__).parents[2] / "models" / "production"
        model_path = models_dir / "passup_nb_v1.pkl"

        if model_path.exists():
            try:
                import joblib
                import statsmodels.api as sm

                nb_model = joblib.load(model_path)
                log_headway = np.log1p(df["mean_headway_minutes"].fillna(30))
                log_boardings = np.log1p(df["weekday_boardings"].fillna(0))
                pct_on_time = df["pct_on_time"].fillna(0)
                exog = sm.add_constant(
                    pd.DataFrame(
                        {
                            "log_headway": log_headway,
                            "log_boardings": log_boardings,
                            "pct_on_time_filled": pct_on_time,
                        }
                    )
                )
                df["passup_risk_score"] = nb_model.predict(exog)
                df["upgrade_priority_score"] = df["passup_risk_score"]
            except Exception:
                logger.warning("NegBin model load failed; using heuristic scoring.")
                df = self._compute_fallback_priority_score(df)
        else:
            # Heuristic fallback: log-scaled passup rate weighted by headway penalty
            df = self._compute_fallback_priority_score(df)

        if "passup_risk_score" not in df.columns:
            df["passup_risk_score"] = df["upgrade_priority_score"]

        # Quantile-based recommendations
        q50 = df["upgrade_priority_score"].quantile(0.50)
        q85 = df["upgrade_priority_score"].quantile(0.85)
        df["recommendation"] = df["upgrade_priority_score"].apply(
            lambda v: "LRT Candidate" if v >= q85 else ("BRT Candidate" if v >= q50 else "Bus Ops Tuning")
        )
        return df.sort_values("upgrade_priority_score", ascending=False)

    def _compute_fallback_priority_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute heuristic upgrade_priority_score when NegBin model is absent.

        Uses unweighted mean of ordinal ranks (no arbitrary weights).

        Args:
            df: Capacity priority table from the SQL view.

        Returns:
            DataFrame with ``upgrade_priority_score`` column added.
        """
        logger.warning(
            "NegBin model absent; falling back to unweighted ordinal rank scoring."
        )
        passup_rank = df["passups_per_100k_boardings"].fillna(0).rank(pct=True)
        headway_rank = df["mean_headway_minutes"].fillna(60).rank(pct=True)
        df["upgrade_priority_score"] = (passup_rank + headway_rank) / 2.0
        return df

    def build_route_classification_feature_table(self) -> pd.DataFrame:
        """Build route features for clustering and classification notebooks.

        Returns:
            Route feature table with PTN tier labels.
        """
        classification_view = self._table("route_classification_features")
        if self._db.count(classification_view) is None:
            logger.warning("Route classification features view is missing. Run the data pipeline first.")
            return pd.DataFrame()
        return self._db.query(
            f"""
            SELECT *
            FROM {classification_view}
            WHERE feed_id = :feed_id
            ORDER BY route_short_name
            """,
            {"feed_id": self._feed_id},
        )

    def build_corridor_current_travel_time_comparison(self) -> pd.DataFrame:
        """Build current corridor travel-time summary from trip-planner snapshots.

        Reads ``data/reference/corridor_sample_pairs.csv`` and the
        ``ywg_transit_trip_plans`` live-transit table to produce per-corridor
        timing breakdowns.

        Returns:
            DataFrame with columns: corridor_name, plan_total_minutes,
            plan_walking_minutes, plan_waiting_minutes, plan_riding_minutes,
            snapshot_timestamp.

        Raises:
            NotImplementedError: Until live transit bootstrap has been run.
        """
        return pd.DataFrame()

    def build_neighbourhood_classification_feature_table(self) -> pd.DataFrame:
        """Build census-based neighbourhood feature table for coverage classification.

        Joins ``ywg_neighbourhood_transit_access_metrics`` to
        ``ywg_census_by_neighbourhood`` using exogenous demographic predictors
        only — no service-derived columns (to avoid leakage when predicting
        ``density_category``).

        Returns:
            DataFrame with columns: neighbourhood_id, neighbourhood,
            density_category, population_density_per_km2,
            pct_commute_public_transit, median_household_income_2020,
            pct_seniors_65_plus.

        Raises:
            NotImplementedError: Until implemented.
        """
        return pd.DataFrame()
