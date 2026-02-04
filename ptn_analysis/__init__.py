"""
Winnipeg PTN Analysis - COMP 4710 Group 11

Built on the Cookiecutter Data Science template (https://drivendata.github.io/cookiecutter-data-science/)
for reproducible analysis workflows.

Quick Start:
    from ptn_analysis import get_edges_df, get_stops_df  # Cathy - Network
    from ptn_analysis import get_stops_per_neighbourhood  # Sudipta - Coverage
    from ptn_analysis import get_stops_with_coords        # Stephenie - Viz
"""

from typing import Any


def get_edges_df(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_edges_df``."""
    from ptn_analysis.analysis import get_edges_df as _impl

    return _impl(*args, **kwargs)


def get_edges_with_routes(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_edges_with_routes``."""
    from ptn_analysis.analysis import get_edges_with_routes as _impl

    return _impl(*args, **kwargs)


def get_neighbourhood_coverage(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_neighbourhood_coverage``."""
    from ptn_analysis.analysis import get_neighbourhood_coverage as _impl

    return _impl(*args, **kwargs)


def get_neighbourhood_geodata(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_neighbourhood_geodata``."""
    from ptn_analysis.analysis import get_neighbourhood_geodata as _impl

    return _impl(*args, **kwargs)


def get_neighbourhoods_list(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_neighbourhoods_list``."""
    from ptn_analysis.analysis import get_neighbourhoods_list as _impl

    return _impl(*args, **kwargs)


def get_routes_df(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_routes_df``."""
    from ptn_analysis.analysis import get_routes_df as _impl

    return _impl(*args, **kwargs)


def get_stops_df(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_stops_df``."""
    from ptn_analysis.analysis import get_stops_df as _impl

    return _impl(*args, **kwargs)


def get_stops_per_community(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_stops_per_community``."""
    from ptn_analysis.analysis import get_stops_per_community as _impl

    return _impl(*args, **kwargs)


def get_stops_per_neighbourhood(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_stops_per_neighbourhood``."""
    from ptn_analysis.analysis import get_stops_per_neighbourhood as _impl

    return _impl(*args, **kwargs)


def get_stops_with_coords(*args: Any, **kwargs: Any):
    """Proxy to ``ptn_analysis.analysis.get_stops_with_coords``."""
    from ptn_analysis.analysis import get_stops_with_coords as _impl

    return _impl(*args, **kwargs)


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
