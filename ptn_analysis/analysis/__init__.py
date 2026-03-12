"""Analysis modules for network, coverage, and visualization.

Lazy-loaded exports to avoid blocking CLI startup.
"""

__all__ = [
    # Network (Cathy)
    "get_edges_df",
    "get_stops_df",
    "get_routes_df",
    # Coverage (Sudipta)
    "get_stops_per_neighbourhood",
    "get_stops_per_community",
    "get_neighbourhoods_list",
    # Visualization (Stephenie)
    "get_stops_with_coords",
    "get_edges_with_routes",
    "get_neighbourhood_coverage",
    "get_neighbourhood_geodata",
]


def __getattr__(name: str):
    """Lazy load analysis functions on first access.

    Args:
        name: Attribute name to load.

    Returns:
        The requested function from the appropriate submodule.

    Raises:
        AttributeError: If name is not a valid export.
    """
    if name in ("get_edges_df", "get_stops_df", "get_routes_df"):
        from ptn_analysis.analysis import network

        return getattr(network, name)

    if name in (
        "get_stops_per_neighbourhood",
        "get_stops_per_community",
        "get_neighbourhoods_list",
    ):
        from ptn_analysis.analysis import coverage

        return getattr(coverage, name)

    if name in (
        "get_stops_with_coords",
        "get_edges_with_routes",
        "get_neighbourhood_coverage",
        "get_neighbourhood_geodata",
    ):
        from ptn_analysis.analysis import visualization

        return getattr(visualization, name)

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
