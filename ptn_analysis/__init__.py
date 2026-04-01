"""Winnipeg PTN Analysis."""

from ptn_analysis.context import TransitContext

__all__ = [
    "TransitContext",
    "CoverageAnalyzer",
    "EquityAnalyzer",
    "FrequencyAnalyzer",
    "NetworkAnalyzer",
    "AssociationRuleMiner",
]


def __getattr__(name: str):
    if name == "CoverageAnalyzer":
        from ptn_analysis.analysis.coverage import CoverageAnalyzer
        return CoverageAnalyzer
    if name == "EquityAnalyzer":
        from ptn_analysis.analysis.equity import EquityAnalyzer
        return EquityAnalyzer
    if name == "FrequencyAnalyzer":
        from ptn_analysis.analysis.frequency import FrequencyAnalyzer
        return FrequencyAnalyzer
    if name == "NetworkAnalyzer":
        from ptn_analysis.analysis.network import NetworkAnalyzer
        return NetworkAnalyzer
    if name == "AssociationRuleMiner":
        from ptn_analysis.analysis.mining import AssociationRuleMiner
        return AssociationRuleMiner
    raise AttributeError(f"module 'ptn_analysis' has no attribute {name!r}")
