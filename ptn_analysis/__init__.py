"""
Winnipeg PTN Analysis - COMP 4710 Group 11

Built on the Cookiecutter Data Science template (https://drivendata.github.io/cookiecutter-data-science/)
for reproducible analysis workflows.

Quick Start:
    from ptn_analysis import get_edges_df, get_stops_df  # Cathy - Network
    from ptn_analysis import get_stops_per_neighbourhood  # Sudipta - Coverage
    from ptn_analysis import get_stops_with_coords        # Stephenie - Viz
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ptn_analysis.analysis import (
        get_edges_df,
        get_edges_with_routes,
        get_neighbourhood_coverage,
        get_neighbourhood_geodata,
        get_neighbourhoods_list,
        get_routes_df,
        get_stops_df,
        get_stops_per_community,
        get_stops_per_neighbourhood,
        get_stops_with_coords,
    )

__all__ = [
    "get_edges_df",
    "get_stops_df",
    "get_routes_df",
    "get_stops_per_neighbourhood",
    "get_stops_per_community",
    "get_neighbourhoods_list",
    "get_stops_with_coords",
    "get_edges_with_routes",
    "get_neighbourhood_coverage",
    "get_neighbourhood_geodata",
]


def __getattr__(name: str):
    """Lazy import public API functions from analysis submodule."""
    if name in __all__:
        from ptn_analysis import analysis

        return getattr(analysis, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
