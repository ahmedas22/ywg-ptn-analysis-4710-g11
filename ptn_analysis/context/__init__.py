"""Composition root for the PTN analysis package.

Use TransitContext.from_defaults() as the single entry point for notebooks
and scripts. Inject custom TransitDB instances for testing.
"""

from __future__ import annotations

from ptn_analysis.context.config import (
    DEFAULT_CITY_KEY,
    DUCKDB_PATH,
    FEED_ID_CURRENT,
    SERVING_DUCKDB_PATH,
)
from ptn_analysis.context.db import TransitDB


class TransitContext:
    """Composition root: binds city_key + feed_id + both DB handles.

    Args:
        working_db: Working TransitDB (interim analytics database).
        serving_db: Serving TransitDB (curated dashboard database).
        city_key: City namespace prefix.
        feed_id: Primary GTFS feed identifier.
        baseline_feed_id: Baseline feed for pre/post comparisons.
    """

    def __init__(
        self,
        working_db: TransitDB,
        serving_db: TransitDB,
        city_key: str = DEFAULT_CITY_KEY,
        feed_id: str = FEED_ID_CURRENT,
        baseline_feed_id: str = "avg_pre_ptn",
    ) -> None:
        self.working_db = working_db
        self.serving_db = serving_db
        self.city_key = city_key
        self.feed_id = feed_id
        self.baseline_feed_id = baseline_feed_id

    @classmethod
    def from_defaults(
        cls,
        city_key: str = DEFAULT_CITY_KEY,
        feed_id: str = FEED_ID_CURRENT,
        baseline_feed_id: str = "avg_pre_ptn",
    ) -> "TransitContext":
        """Create context bound to the default local DuckDB files.

        Args:
            city_key: City namespace prefix.
            feed_id: Primary GTFS feed identifier.
            baseline_feed_id: Baseline feed for pre/post comparisons.

        Returns:
            TransitContext bound to the standard local database paths.
        """
        return cls(
            TransitDB(DUCKDB_PATH),
            TransitDB(SERVING_DUCKDB_PATH),
            city_key,
            feed_id,
            baseline_feed_id,
        )

    def for_feed(self, feed_id: str, baseline_feed_id: str | None = None) -> TransitContext:
        """Clone this context for a different feed, reusing DB handles."""
        return TransitContext(
            working_db=self.working_db,
            serving_db=self.serving_db,
            city_key=self.city_key,
            feed_id=feed_id,
            baseline_feed_id=baseline_feed_id or self.baseline_feed_id,
        )

    def frequency(self):
        """Return a FrequencyAnalyzer bound to this context.

        Returns:
            FrequencyAnalyzer instance.
        """
        from ptn_analysis.analysis.frequency import FrequencyAnalyzer

        return FrequencyAnalyzer(self.city_key, self.feed_id, self.working_db)

    def coverage(self):
        """Return a CoverageAnalyzer bound to this context.

        Returns:
            CoverageAnalyzer instance.
        """
        from ptn_analysis.analysis.coverage import CoverageAnalyzer

        return CoverageAnalyzer(self.city_key, self.feed_id, self.working_db)

    def network(self):
        """Return a NetworkAnalyzer bound to this context.

        Returns:
            NetworkAnalyzer instance.
        """
        from ptn_analysis.analysis.network import NetworkAnalyzer

        return NetworkAnalyzer(self.city_key, self.feed_id, self.working_db)

    def maps(self):
        """Return a MapDataLoader bound to this context.

        Returns:
            MapDataLoader instance.
        """
        from ptn_analysis.context.serving import MapDataLoader

        return MapDataLoader(self.city_key, self.feed_id, self.working_db)

    def pipeline(self):
        """Return a DatasetPipeline bound to this context.

        Returns:
            DatasetPipeline instance.
        """
        from ptn_analysis.data.pipeline import DatasetPipeline

        return DatasetPipeline(self.city_key, self.working_db, self.serving_db)

    def dashboard(self):
        """Return a Dashboard bound to the serving database.

        Returns:
            Dashboard instance.
        """
        from ptn_analysis.context.serving import Dashboard

        return Dashboard(self.serving_db, self.city_key, self.feed_id, self.baseline_feed_id)

    def close(self) -> None:
        """Dispose both DB engine handles.

        Call when done with the context in long-running sessions or when
        repeated notebook restarts would otherwise accumulate engine handles.
        """
        self.working_db.close()
        self.serving_db.close()

    def __enter__(self) -> "TransitContext":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def __repr__(self) -> str:
        return f"TransitContext(city_key={self.city_key!r}, feed_id={self.feed_id!r})"
