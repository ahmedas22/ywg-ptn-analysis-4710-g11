"""Coverage analysis for Sudipta."""

from duckdb import DuckDBPyConnection
import pandas as pd

from ptn_analysis.data.db import query_df

# Coverage thresholds (stops per km²)
COVERAGE_HIGH_THRESHOLD = 5.0
COVERAGE_MEDIUM_THRESHOLD = 1.0


def categorize_coverage(density: float) -> str:
    """Classify stop density into coverage category.

    Args:
        density: Stop density in stops per square kilometer.

    Returns:
        Category string: "High" (>=5), "Medium" (>=1), or "Low" (<1).
    """
    if density >= COVERAGE_HIGH_THRESHOLD:
        return "High"
    elif density >= COVERAGE_MEDIUM_THRESHOLD:
        return "Medium"
    return "Low"


def get_stops_per_neighbourhood(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop counts per neighbourhood.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with neighbourhood, area_km2, stop_count, stops_per_km2.
    """
    return query_df(
        """
        SELECT neighbourhood, area_km2, stop_count, stops_per_km2
        FROM agg_stops_per_neighbourhood
        ORDER BY stop_count DESC
        """,
        con,
    )


def get_stops_per_community(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop counts per community area.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with community, area_km2, stop_count, stops_per_km2.
    """
    return query_df(
        """
        SELECT community, area_km2, stop_count, stops_per_km2
        FROM agg_stops_per_community
        ORDER BY stop_count DESC
        """,
        con,
    )


def get_neighbourhoods_list(con: DuckDBPyConnection | None = None) -> list[str]:
    """Get sorted list of neighbourhood names.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        Alphabetically sorted list of neighbourhood names.
    """
    df = query_df(
        "SELECT DISTINCT neighbourhood FROM agg_stops_per_neighbourhood ORDER BY neighbourhood",
        con,
    )
    return df["neighbourhood"].tolist()


# -----------------------------------------------------------------------------
# Stubs for Sudipta - See GitHub issue #4 for implementation hints
# -----------------------------------------------------------------------------


def compute_coverage_statistics(con: DuckDBPyConnection | None = None) -> dict:
    """Compute summary coverage statistics across neighbourhoods.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        Dictionary with total_neighbourhoods, total_stops, mean_stops,
        median_stops, std_stops, min_stops, max_stops, zero_stop_areas,
        mean_density_per_km2.
    """
    raise NotImplementedError("Sudipta: Implement this function")


def identify_underserved_neighbourhoods(
    threshold_percentile: float = 25.0,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Return neighbourhoods below percentile density threshold.

    Args:
        threshold_percentile: Percentile cutoff for stops_per_km2 (0-100).
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame of underserved neighbourhoods sorted by density ascending.
    """
    raise NotImplementedError("Sudipta: Implement this function")


def compute_coverage_by_area(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Add density rank to coverage data.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with density_rank column (1 = highest density).
    """
    raise NotImplementedError("Sudipta: Implement this function")


def get_coverage_by_category(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Categorize neighbourhoods by coverage level.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with coverage_category column (High/Medium/Low).
    """
    raise NotImplementedError("Sudipta: Implement this function")


def detect_coverage_outliers(method: str = "iqr", con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Flag high/low outliers in neighbourhood stop counts.

    Args:
        method: Detection method ("iqr" or "zscore").
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with neighbourhood, stop_count, outlier_type columns.
    """
    raise NotImplementedError("Sudipta: Implement this function")


def compare_community_coverage(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Return community-level coverage comparison.

    Args:
        con: Optional DuckDB connection. Uses default if None.

    Returns:
        DataFrame with community stats, density_rank, coverage_category.
    """
    raise NotImplementedError("Sudipta: Implement this function")
