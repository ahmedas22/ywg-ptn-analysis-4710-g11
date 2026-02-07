"""Coverage analysis module for Sudipta.

Provides neighbourhood and community coverage statistics using DuckDB views.
"""

from duckdb import DuckDBPyConnection
import pandas as pd

from ptn_analysis.data.db import query_df

# Coverage thresholds (stops per km²)
COVERAGE_HIGH = 5.0
COVERAGE_MEDIUM = 1.0


def categorize_coverage(density: float) -> str:
    """Classify stop density into coverage category.

    Args:
        density: Stop density in stops per km².

    Returns:
        Category: "High" (>=5), "Medium" (>=1), or "Low" (<1).
    """
    if density >= COVERAGE_HIGH:
        return "High"
    if density >= COVERAGE_MEDIUM:
        return "Medium"
    return "Low"


def get_stops_per_neighbourhood(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop counts per neighbourhood with computed area and density.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with neighbourhood, area_km2, stop_count, stops_per_km2.
    """
    return query_df(
        """
        WITH areas AS (
          SELECT
            name AS neighbourhood,
            ST_Area(geometry) * 111.32 * (111.32 * COS(RADIANS(ST_Y(ST_Centroid(geometry))))) AS area_km2
          FROM neighbourhoods
        ),
        stops AS (
          SELECT neighbourhood, stop_count
          FROM neighbourhood_coverage
        )
        SELECT
          s.neighbourhood,
          a.area_km2,
          s.stop_count,
          s.stop_count / NULLIF(a.area_km2, 0) AS stops_per_km2
        FROM stops s
        JOIN areas a USING (neighbourhood)
        ORDER BY s.stop_count DESC
        """,
        con,
    )



def get_stops_per_community(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Load stop counts per community area.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with community, area_km2, stop_count, stops_per_km2.
    """

    """Load stop counts per community area with computed area and density."""
    return query_df(
        """
        WITH areas AS (
          SELECT
            name AS community,
            ST_Area(geometry) * 111.32 * (111.32 * COS(RADIANS(ST_Y(ST_Centroid(geometry))))) AS area_km2
          FROM community_areas
        ),
        stops AS (
          SELECT community, stop_count
          FROM community_coverage
        )
        SELECT
          s.community,
          a.area_km2,
          s.stop_count,
          s.stop_count / NULLIF(a.area_km2, 0) AS stops_per_km2
        FROM stops s
        JOIN areas a USING (community)
        ORDER BY s.stop_count DESC
        """,
        con,
    )


def get_neighbourhoods_list(con: DuckDBPyConnection | None = None) -> list[str]:
    """Get sorted list of neighbourhood names.

    Args:
        con: Optional DuckDB connection.

    Returns:
        Alphabetically sorted list of neighbourhood names.
    """
    df = query_df(
        "SELECT DISTINCT neighbourhood FROM neighbourhood_coverage ORDER BY neighbourhood",
        con,
    )
    return df["neighbourhood"].tolist()


# =============================================================================
# STUBS FOR SUDIPTA
# =============================================================================


def compute_coverage_statistics(con: DuckDBPyConnection | None = None) -> dict:
    """Compute summary coverage statistics across neighbourhoods.

    Args:
        con: Optional DuckDB connection.

    Returns:
        Dict with total_neighbourhoods, total_stops, mean_stops, median_stops,
        std_stops, min_stops, max_stops, zero_stop_areas, mean_density_per_km2.
    """
    # raise NotImplementedError("Sudipta: implement")

    df = get_stops_per_neighbourhood(con)

    #Ensure numeric (should already be numeric from DuckDB)
    df["stop_count"] = pd.to_numeric(df["stop_count"], errors="coerce")
    df["stops_per_km2"] = pd.to_numeric(df["stops_per_km2"], errors="coerce")

    return{
        "total_neighbourhoods": int (len(df)),
        "total_stops"         : int (df["stop_count"].sum()),
        "mean_stops"          : int (df["stop_count"].mean()),
        "median_stops"        : int (df["stop_count"].median()),
        "std_stops"           : int (df["stop_count"].std()),
        "min_stops"           : int (df["stop_count"].min()),
        "max_stops"           : int (df["stop_count"].max()),
        "zero_stop_areas"     : int ((df["stop_count"] == 0).sum()),
        "mean_density_per_km2": float (df["stops_per_km2"].mean()),
    }


def identify_underserved_neighbourhoods(
    threshold_percentile: float = 25.0,
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Return neighbourhoods below percentile density threshold.

    Args:
        threshold_percentile: Percentile cutoff for stops_per_km2 (0-100).
        con: Optional DuckDB connection.

    Returns:
        DataFrame of underserved neighbourhoods sorted by density ascending.
    """
    df = get_stops_per_neighbourhood(con).copy()

    # Defensive: ensure numeric
    df["stops_per_km2"] = pd.to_numeric(df["stops_per_km2"], errors="coerce")

    # Percentile cutoff (e.g., 25th percentile)
    cutoff = df["stops_per_km2"].quantile(threshold_percentile / 100.0)

    underserved = df[df["stops_per_km2"] <= cutoff].sort_values(
        "stops_per_km2", ascending=True
    )

    return underserved


def get_coverage_by_category(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Categorize neighbourhoods by coverage level (High/Medium/Low).

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with coverage_category column added.
    """
    raise NotImplementedError("Sudipta: implement using categorize_coverage()")


def compute_coverage_by_area(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Add density rank to coverage data.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with density_rank column (1 = highest density).
    """
    df = get_stops_per_neighbourhood(con).copy()

    df["stops_per_km2"] = pd.to_numeric(df["stops_per_km2"], errors="coerce")

    # 1 = highest density
    df["density_rank"] = (
        df["stops_per_km2"].rank(ascending=False, method="dense").astype("Int64")
    )

    return df.sort_values("density_rank", ascending=True)


def detect_coverage_outliers(
    method: str = "iqr",
    con: DuckDBPyConnection | None = None,
) -> pd.DataFrame:
    """Flag high/low outliers in neighbourhood stop counts.

    Args:
        method: Detection method ("iqr" or "zscore").
        con: Optional DuckDB connection.

    Returns:
        DataFrame with neighbourhood, stop_count, outlier_type columns.
    """

    df = get_stops_per_neighbourhood(con).copy()
    df["stop_count"] = pd.to_numeric(df["stop_count"], errors="coerce")

    x = df["stop_count"]

    if method.lower() == "iqr":
        q1 = x.quantile(0.25)
        q3 = x.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        out = df[(x < lower) | (x > upper)].copy()
        out["outlier_type"] = out["stop_count"].apply(
            lambda v: "Low" if v < lower else "High"
        )

    elif method.lower() == "zscore":
        mean = x.mean()
        std = x.std()
        if std == 0 or pd.isna(std):
            return pd.DataFrame(columns=["neighbourhood", "stop_count", "outlier_type"])

        z = (x - mean) / std
        out = df[(z.abs() >= 3)].copy()
        out["outlier_type"] = out["stop_count"].apply(
            lambda v: "Low" if v < mean else "High"
        )

    else:
        raise ValueError("method must be 'iqr' or 'zscore'")

    return out[["neighbourhood", "stop_count", "outlier_type"]].sort_values(
        ["outlier_type", "stop_count"], ascending=[True, False]
    )

def compare_community_coverage(con: DuckDBPyConnection | None = None) -> pd.DataFrame:
    """Return community-level coverage comparison.

    Args:
        con: Optional DuckDB connection.

    Returns:
        DataFrame with community stats, density_rank, coverage_category.
    """
    df = get_stops_per_community(con).copy()

    df["stops_per_km2"] = pd.to_numeric(df["stops_per_km2"], errors="coerce")

    # 1 = highest density among communities
    df["density_rank"] = (
        df["stops_per_km2"].rank(ascending=False, method="dense").astype("Int64")
    )

    # Coverage category using the same thresholds
    df["coverage_category"] = df["stops_per_km2"].apply(categorize_coverage)

    return df.sort_values("density_rank", ascending=True)
