"""Public analysis exports."""

from ptn_analysis.context.config import (
    HEADWAY_TIER_COLORS,
    HEADWAY_TIER_LIST,
    PTN_HEADWAY_TARGETS,
    PTN_TIER_COLORS,
    PTN_TIER_ORDER,
    classify_ptn_tier,
    get_route_display_color,
    headway_tier,
)

__all__ = [
    "CoverageAnalyzer",
    "EquityAnalyzer",
    "FrequencyAnalyzer",
    "NetworkAnalyzer",
    "AssociationRuleMiner",
    "MapDataLoader",
    "Plotter",
    "categorize_coverage",
    "save_report_figure",
    "classify_ptn_tier",
    "get_route_display_color",
    "headway_tier",
    "HEADWAY_TIER_COLORS",
    "HEADWAY_TIER_LIST",
    "PTN_HEADWAY_TARGETS",
    "PTN_TIER_COLORS",
    "PTN_TIER_ORDER",
    "WEB_MERCATOR",
    "NEIGHBOURHOOD_STYLE",
    "POINT_MARKER_STYLE",
    "LABEL_STYLE",
    "add_consistent_basemap",
    "plot_neighbourhood_base",
]


def __getattr__(name: str):
    if name == "CoverageAnalyzer":
        from ptn_analysis.analysis.coverage import CoverageAnalyzer
        return CoverageAnalyzer
    if name == "categorize_coverage":
        from ptn_analysis.analysis.coverage import categorize_coverage
        return categorize_coverage
    if name == "EquityAnalyzer":
        from ptn_analysis.analysis.equity import EquityAnalyzer
        return EquityAnalyzer
    if name == "FrequencyAnalyzer":
        from ptn_analysis.analysis.frequency import FrequencyAnalyzer
        return FrequencyAnalyzer
    if name == "NetworkAnalyzer":
        from ptn_analysis.analysis.network import NetworkAnalyzer
        return NetworkAnalyzer
    if name in ("Plotter", "save_report_figure", "add_consistent_basemap",
                 "plot_neighbourhood_base", "WEB_MERCATOR",
                 "NEIGHBOURHOOD_STYLE", "POINT_MARKER_STYLE", "LABEL_STYLE"):
        from ptn_analysis.analysis import visualization as _viz
        return getattr(_viz, name)
    if name == "AssociationRuleMiner":
        from ptn_analysis.analysis.mining import AssociationRuleMiner
        return AssociationRuleMiner
    if name == "MapDataLoader":
        from ptn_analysis.context.serving import MapDataLoader
        return MapDataLoader
    raise AttributeError(f"module 'ptn_analysis.analysis' has no attribute {name!r}")
