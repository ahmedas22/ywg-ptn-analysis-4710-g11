"""Data pipeline API.

Lazy-loaded exports to avoid blocking CLI startup.
"""

__all__ = [
    "get_duckdb",
    "query_df",
    "load_gtfs_feed",
    "get_feed_date_range",
]


def __getattr__(name: str):
    """Lazy load modules on first access."""
    if name in ("get_duckdb", "query_df"):
        from ptn_analysis.data.db import get_duckdb, query_df

        globals()["get_duckdb"] = get_duckdb
        globals()["query_df"] = query_df
        return globals()[name]

    if name in ("load_gtfs_feed", "get_feed_date_range"):
        from ptn_analysis.data.loaders import get_feed_date_range, load_gtfs_feed

        globals()["load_gtfs_feed"] = load_gtfs_feed
        globals()["get_feed_date_range"] = get_feed_date_range
        return globals()[name]

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
