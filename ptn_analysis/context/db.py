"""DuckDB connection and city-prefixed table helpers."""

from __future__ import annotations

import duckdb_engine  # noqa: F401 — registers dialect with SQLAlchemy entry-points
from pathlib import Path
import re
from typing import Any

from loguru import logger
import pandas as pd
from sqlalchemy import create_engine

from ptn_analysis.context.config import DEFAULT_CITY_KEY, DUCKDB_PATH, WGS84_CRS

__all__ = ["TransitDB"]

_IDENTIFIER_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")
_CITY_KEY_RE = re.compile(r"^[a-z][a-z0-9_]*$")


class TransitDB:
    """Database interface for transit analysis queries.

    Args:
        path: Optional override for the DuckDB file path.
    """

    def __init__(self, path: Path | None = None) -> None:
        """Initialize the database wrapper.

        Args:
            path: Optional DuckDB file path.
        """
        self._path = path or DUCKDB_PATH
        self._engine = None
        self._query_cache: dict[tuple, pd.DataFrame] = {}

    @property
    def path(self) -> Path:
        """Return the DuckDB file path.

        Returns:
            DuckDB file path.
        """
        return self._path

    @property
    def engine(self):
        """Return a lazy SQLAlchemy engine with spatial support.

        Extensions are loaded via duckdb-engine's ``preload_extensions``
        connect_args. DuckDB 1.4+ auto-installs known extensions on first
        LOAD, so no raw bootstrap connection is needed.

        Returns:
            SQLAlchemy engine instance.
        """
        if self._engine is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._engine = self._make_engine(["spatial"])
            logger.info(f"SQLAlchemy engine created for {self._path}")
        return self._engine

    def _make_engine(self, extensions: list[str]):
        """Create a SQLAlchemy engine with connection-level settings."""
        from sqlalchemy import event

        eng = create_engine(
            f"duckdb:///{self._path}",
            connect_args={"preload_extensions": extensions},
        )

        @event.listens_for(eng, "connect")
        def _on_connect(dbapi_conn, _rec):
            try:
                dbapi_conn.execute("SET enable_progress_bar = false")
            except Exception:
                pass  # ipywidgets not available (e.g. papermill)

        return eng

    def supports_h3(self) -> bool:
        """Return whether H3 SQL functions are available in DuckDB.

        Installs the community H3 extension and rebuilds the engine with
        h3 in ``preload_extensions`` so every subsequent connection has it.

        Returns:
            True when the DuckDB H3 extension is available.
        """
        try:
            self.execute_native("INSTALL h3 FROM community; LOAD h3;")
            if self._engine is not None:
                self._engine.dispose()
                self._engine = self._make_engine(["spatial", "h3"])
            return self.first(
                "SELECT h3_latlng_to_cell(49.8951, -97.1384, 8)"
            ) is not None
        except Exception:
            logger.debug("DuckDB h3 extension unavailable")
            return False

    def table_name(self, base_name: str, city_key: str = DEFAULT_CITY_KEY) -> str:
        """Return a city-prefixed physical table or view name.

        Args:
            base_name: Logical table or view name.
            city_key: City namespace.

        Returns:
            City-prefixed physical relation name.
        """
        self._validate_city_key(city_key)
        self._validate_identifier(base_name)
        return f"{city_key}_{base_name}"

    def transit_table_name(self, base_name: str, city_key: str = DEFAULT_CITY_KEY) -> str:
        """Return a city-prefixed live transit relation name.

        Args:
            base_name: Logical transit table or view name.
            city_key: City namespace.

        Returns:
            Prefixed transit relation name.
        """
        self._validate_city_key(city_key)
        self._validate_identifier(base_name)
        return f"{city_key}_transit_{base_name}"

    def relation_exists(self, relation_name: str) -> bool:
        """Return whether a table or view exists.

        Args:
            relation_name: Physical relation name.

        Returns:
            True when the relation exists.
        """
        if not _IDENTIFIER_RE.match(relation_name):
            return False
        result = self.first(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = :name
            """,
            {"name": relation_name},
        )
        return bool(result)

    def relation_type(self, relation_name: str) -> str | None:
        """Return the information-schema relation type.

        Args:
            relation_name: Physical relation name.

        Returns:
            Relation type string such as ``"BASE TABLE"`` or ``"VIEW"``, or None.
        """
        if not _IDENTIFIER_RE.match(relation_name):
            return None
        return self.first(
            """
            SELECT table_type
            FROM information_schema.tables
            WHERE table_schema = 'main' AND table_name = :name
            """,
            {"name": relation_name},
        )

    def drop_relation_if_exists(self, relation_name: str) -> None:
        """Drop one table or view when it exists.

        Args:
            relation_name: Physical relation name.

        Returns:
            None.
        """
        relation_type = self.relation_type(relation_name)
        if relation_type is None:
            return
        if relation_type == "VIEW":
            self.execute(f"DROP VIEW IF EXISTS {relation_name}")
            return
        self.execute(f"DROP TABLE IF EXISTS {relation_name}")

    def _invalidate_cache(self) -> None:
        """Clear the query cache after any write operation."""
        self._query_cache.clear()

    def cached_query(self, sql: str, params: dict | None = None) -> pd.DataFrame:
        """Execute a read query with caching.

        Returns a copy so callers cannot mutate cached data.
        Cache is cleared on any ``execute`` or ``load_table`` call.

        Args:
            sql: SQL query text.
            params: Optional bound parameters.

        Returns:
            DataFrame result (copy of cached data).
        """
        frozen_params = tuple(sorted((params or {}).items()))
        cache_key = (sql, frozen_params)
        if cache_key not in self._query_cache:
            self._query_cache[cache_key] = self.query(sql, params)
        return self._query_cache[cache_key].copy()

    def query(
        self,
        sql: str,
        params: dict | None = None,
        geo: bool = False,
        geometry_col: str = "geometry",
        crs: str = WGS84_CRS,
    ) -> pd.DataFrame:
        """Execute SQL and return a DataFrame or GeoDataFrame.

        Args:
            sql: SQL query text.
            params: Optional bound parameters.
            geo: Whether to decode WKB geometry to a GeoDataFrame.
            geometry_col: Geometry column name.
            crs: Coordinate reference system for the geometry column.

        Returns:
            pandas or GeoPandas table.
        """
        from sqlalchemy import text

        with self.engine.connect() as connection:
            statement = text(sql).bindparams(**params) if params else text(sql)
            frame = pd.read_sql(statement, connection)

        if not geo:
            return frame

        import geopandas as gpd

        if frame.empty or geometry_col not in frame.columns:
            return gpd.GeoDataFrame(frame)

        geometry_values = frame[geometry_col].apply(
            lambda value: bytes(value) if isinstance(value, bytearray) else value
        )
        try:
            geometry_series = gpd.GeoSeries.from_wkb(geometry_values)
        except Exception:
            # DuckDB native GEOMETRY isn't plain WKB — re-query with ST_AsWKB
            wkb_sql = (
                f"SELECT * EXCLUDE ({geometry_col}), "
                f"ST_AsWKB({geometry_col}) AS {geometry_col} "
                f"FROM ({sql}) _geo_sub"
            )
            with self.engine.connect() as connection:
                statement = text(wkb_sql).bindparams(**params) if params else text(wkb_sql)
                frame = pd.read_sql(statement, connection)
            geometry_values = frame[geometry_col].apply(
                lambda value: bytes(value) if isinstance(value, bytearray) else value
            )
            geometry_series = gpd.GeoSeries.from_wkb(geometry_values)
        return gpd.GeoDataFrame(frame, geometry=geometry_series, crs=crs)

    def first(self, sql: str, params: dict | None = None) -> Any:
        """Execute SQL and return the first scalar value.

        Args:
            sql: SQL text.
            params: Optional bound parameters.

        Returns:
            First scalar result or None.
        """
        from sqlalchemy import text

        with self.engine.connect() as connection:
            statement = text(sql).bindparams(**params) if params else text(sql)
            row = connection.execute(statement).fetchone()
        if row is None:
            return None
        return row[0]

    def count(self, table_name: str) -> int | None:
        """Return the row count for one table or view.

        Args:
            table_name: Physical relation name.

        Returns:
            Row count, or None when the relation is missing.
        """
        if not _IDENTIFIER_RE.match(table_name):
            return None
        if not self.relation_exists(table_name):
            return None
        row_count = self.first(f"SELECT COUNT(*) FROM {table_name}")
        if row_count is None:
            return 0
        return int(row_count)

    def execute(self, sql: str, params: dict | None = None):
        """Execute one DDL or DML statement.

        Clears the query cache since table data may have changed.

        Args:
            sql: SQL text.
            params: Optional bound parameters.

        Returns:
            SQLAlchemy execution result.
        """
        from sqlalchemy import text

        self._invalidate_cache()
        with self.engine.begin() as connection:
            statement = text(sql).bindparams(**params) if params else text(sql)
            return connection.execute(statement)

    def execute_native(self, sql: str):
        """Execute SQL directly through the DuckDB DB-API connection.

        Use this when SQLAlchemy text parsing would misinterpret literal SQL,
        such as Statistics Canada column names containing ``:`` characters.

        Args:
            sql: SQL text.

        Returns:
            DB-API cursor result.
        """
        raw_connection = self.engine.raw_connection()
        try:
            cursor = raw_connection.cursor()
            try:
                result = cursor.execute(sql)
                raw_connection.commit()
                return result
            finally:
                cursor.close()
        finally:
            raw_connection.close()

    def load_table(self, table_name: str, data: Any, mode: str = "replace") -> None:
        """Load tabular data into DuckDB.

        Clears the query cache since table data may have changed.

        Args:
            table_name: Physical table name.
            data: pandas, GeoPandas, Arrow, or Polars table.
            mode: ``"replace"`` or ``"append"``.
        """
        self._invalidate_cache()
        self._validate_identifier(table_name)
        if mode not in {"replace", "append"}:
            raise ValueError("mode must be 'replace' or 'append'")

        table_like, select_sql = self._prepare_table_input(data)
        raw_connection = self.engine.raw_connection()
        try:
            raw_connection.register("__load_table__", table_like)
            if mode == "replace":
                raw_connection.execute(
                    f"CREATE OR REPLACE TABLE {table_name} AS {select_sql}"
                )
            else:
                if not self.relation_exists(table_name):
                    raw_connection.execute(
                        f"CREATE TABLE {table_name} AS {select_sql}"
                    )
                else:
                    raw_connection.execute(
                        f"INSERT INTO {table_name} {select_sql}"
                    )
        finally:
            try:
                raw_connection.unregister("__load_table__")
            except Exception:
                pass
            raw_connection.close()

    def close(self) -> None:
        """Dispose of the SQLAlchemy engine.

        Returns:
            None.
        """
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None

    def _prepare_table_input(self, data: Any) -> tuple[Any, str]:
        """Prepare supported input objects for DuckDB registration.

        Args:
            data: Table-like object.

        Returns:
            Tuple of registered table object and ``SELECT`` SQL.
        """
        try:
            import geopandas as gpd
        except ImportError:
            gpd = None

        if gpd is not None and isinstance(data, gpd.GeoDataFrame):
            geometry_column = data.geometry.name
            load_frame = pd.DataFrame(data.copy())
            load_frame[geometry_column] = data.geometry.to_wkb()
            select_columns = []
            for column_name in load_frame.columns:
                self._validate_identifier(column_name)
                if column_name == geometry_column:
                    select_columns.append(
                        f"ST_GeomFromWKB({geometry_column}) AS {geometry_column}"
                    )
                else:
                    select_columns.append(column_name)
            return load_frame, f"SELECT {', '.join(select_columns)} FROM __load_table__"

        if isinstance(data, pd.DataFrame):
            return data, "SELECT * FROM __load_table__"
        if hasattr(data, "to_arrow"):
            try:
                return data.to_arrow(), "SELECT * FROM __load_table__"
            except Exception:
                return data.to_pandas(), "SELECT * FROM __load_table__"
        if hasattr(data, "schema") and hasattr(data, "column_names"):
            return data, "SELECT * FROM __load_table__"
        if hasattr(data, "to_pandas"):
            return data.to_pandas(), "SELECT * FROM __load_table__"
        raise TypeError(f"Unsupported table input: {type(data)!r}")

    def neighbourhood_gdf(self, city_key: str = DEFAULT_CITY_KEY):
        """Load neighbourhood polygons as a GeoDataFrame.

        Args:
            city_key: City namespace.

        Returns:
            GeoDataFrame with neighbourhood name and geometry.
        """
        import geopandas as gpd

        tbl = self.table_name("neighbourhoods", city_key)
        if not self.relation_exists(tbl):
            return gpd.GeoDataFrame()
        df = self.query(
            f"SELECT name AS neighbourhood, ST_AsWKB(geometry) AS geometry FROM {tbl}"
        )
        return gpd.GeoDataFrame(
            df, geometry=gpd.GeoSeries.from_wkb(df["geometry"], crs="EPSG:4326")
        )

    def _validate_identifier(self, value: str) -> None:
        """Validate a SQL identifier.

        Args:
            value: Candidate identifier.
        """
        if not _IDENTIFIER_RE.match(value):
            raise ValueError(f"Invalid identifier: {value!r}")

    def _validate_city_key(self, city_key: str) -> None:
        """Validate a city key.

        Args:
            city_key: Candidate city key.
        """
        if not _CITY_KEY_RE.match(city_key):
            raise ValueError(f"Invalid city_key: {city_key!r}")

