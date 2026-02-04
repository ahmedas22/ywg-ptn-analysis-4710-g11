"""Data pipeline API surface."""

from ptn_analysis.data.db import get_duckdb
from ptn_analysis.data.ingest_gtfs import download_gtfs, extract_gtfs, load_gtfs_table
from ptn_analysis.data.ingest_open_data import (
    load_active_mobility_datasets,
    load_boundary_table,
    load_standard_open_data_tables,
)
from ptn_analysis.data.schemas import (
    BOUNDARY_TABLES,
    GTFS_TABLES,
)
from ptn_analysis.data.transform import (
    build_edges_table,
    create_aggregated_edges,
    materialize_active_trips,
)

__all__ = [
    "get_duckdb",
    "GTFS_TABLES",
    "BOUNDARY_TABLES",
    "download_gtfs",
    "extract_gtfs",
    "load_gtfs_table",
    "load_boundary_table",
    "load_standard_open_data_tables",
    "load_active_mobility_datasets",
    "build_edges_table",
    "create_aggregated_edges",
    "materialize_active_trips",
]
