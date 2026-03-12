"""Winnipeg PTN Analysis."""

from ptn_analysis.context import TransitContext

__all__ = ["TransitContext", "CoverageAnalyzer", "FrequencyAnalyzer", "NetworkAnalyzer"]


def __getattr__(name: str):
    if name == "CoverageAnalyzer":
        from ptn_analysis.analysis.coverage import CoverageAnalyzer
        return CoverageAnalyzer
    if name == "FrequencyAnalyzer":
        from ptn_analysis.analysis.frequency import FrequencyAnalyzer
        return FrequencyAnalyzer
    if name == "NetworkAnalyzer":
        from ptn_analysis.analysis.network import NetworkAnalyzer
        return NetworkAnalyzer
    raise AttributeError(f"module 'ptn_analysis' has no attribute {name!r}")
