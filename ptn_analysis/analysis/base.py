"""Shared base class for analysis modules."""

from __future__ import annotations

import pandas as pd

from ptn_analysis.context.config import DEFAULT_CITY_KEY, FEED_ID_CURRENT
from ptn_analysis.context.db import TransitDB


class AnalyzerBase:
    """Shared infrastructure for Coverage, Network, and Frequency analyzers.

    Provides city-key-aware table name resolution, a per-instance result
    cache, and accessor methods for precomputed city2graph/r5py tables
    materialized by the data pipeline.

    Args:
        city_key: City namespace prefix (e.g. ``"ywg"``).
        feed_id: GTFS feed identifier (e.g. ``"current"``).
        db_instance: Working TransitDB handle (required).
    """

    def __init__(
        self,
        city_key: str = DEFAULT_CITY_KEY,
        feed_id: str = FEED_ID_CURRENT,
        db_instance: TransitDB = None,
    ) -> None:
        if db_instance is None:
            raise ValueError("db_instance is required")
        self._city_key = city_key
        self._feed_id = feed_id
        self._db = db_instance

    def _table(self, base: str) -> str:
        """Resolve a city-prefixed table name."""
        return self._db.table_name(base, self._city_key)

    def _has_table(self, base: str) -> bool:
        """Check if a city-prefixed table exists."""
        return self._db.relation_exists(self._table(base))

    @staticmethod
    def _empty_frame(schema: dict[str, str]) -> pd.DataFrame:
        """Create an empty DataFrame with typed columns.

        Args:
            schema: Mapping of column name to pandas dtype string.
        """
        return pd.DataFrame(
            {col: pd.Series(dtype=dtype) for col, dtype in schema.items()}
        )

    def _build_comparison(
        self,
        baseline_feed_id: str,
        metric_loader,
        key_col: str,
        metric_cols: list[str],
    ) -> pd.DataFrame:
        """Build a generic pre/post comparison table.

        Args:
            baseline_feed_id: Feed identifier for the baseline.
            metric_loader: Callable taking an analyzer instance, returning a DataFrame.
            key_col: Column to join on.
            metric_cols: Columns to compute deltas for.
        """
        baseline = self.__class__(self._city_key, baseline_feed_id, self._db)
        baseline_data = metric_loader(baseline)
        current_data = metric_loader(self)
        if baseline_data.empty or current_data.empty:
            return pd.DataFrame()
        merged = current_data.merge(
            baseline_data, on=key_col, suffixes=("_current", "_baseline"), how="outer"
        )
        for col in metric_cols:
            current_col = f"{col}_current"
            baseline_col = f"{col}_baseline"
            if current_col in merged.columns and baseline_col in merged.columns:
                merged[f"{col}_delta"] = merged[current_col] - merged[baseline_col]
        return merged

    # ------------------------------------------------------------------
    # Precomputed table accessors (city2graph + r5py)
    # ------------------------------------------------------------------

    def _cached_table(
        self,
        base: str,
        where: str | None = None,
        params: dict | None = None,
    ) -> pd.DataFrame:
        """Load a precomputed table via ``db.cached_query``.

        Args:
            base: Logical table base name (city-prefixed automatically).
            where: Optional WHERE clause (without the WHERE keyword).
            params: Optional query parameters.
        """
        table = self._table(base)
        if not self._db.relation_exists(table):
            return pd.DataFrame()
        sql = f"SELECT * FROM {table}"
        if where:
            sql += f" WHERE {where}"
        return self._db.cached_query(sql, params)

    def transit_edges(self) -> pd.DataFrame:
        """city2graph stop-to-stop edges for this feed."""
        return self._cached_table(
            "stop_connection_counts",
            where="feed_id = :f", params={"f": self._feed_id},
        )

    def neighbourhood_contiguity(self) -> pd.DataFrame:
        """city2graph Queen adjacency between neighbourhoods."""
        return self._cached_table("neighbourhood_contiguity")

    def stop_neighbourhood_bridge(self) -> pd.DataFrame:
        """city2graph stop-to-neighbourhood assignment."""
        return self._cached_table("stop_neighbourhood_bridge")

    def walk_matrix(self) -> pd.DataFrame:
        """r5py WALK-only travel times for this feed."""
        return self._cached_table(f"walk_matrix_{self._feed_id}")

    def transit_matrix(self) -> pd.DataFrame:
        """r5py TRANSIT+WALK travel times for this feed."""
        return self._cached_table(f"transit_matrix_{self._feed_id}")

    def jobs_reachable(self) -> pd.DataFrame:
        """Precomputed jobs reachable within cutoff for this feed."""
        return self._cached_table(f"jobs_reachable_{self._feed_id}")

    def isochrones(self):
        """Precomputed r5py isochrone polygons for this feed."""
        table = f"isochrones_{self._feed_id}"
        if not self._has_table(table):
            import geopandas as gpd

            return gpd.GeoDataFrame()
        return self._db.query(f"SELECT * FROM {self._table(table)}", geo=True)
