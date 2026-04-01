"""Tests for the EquityAnalyzer module."""

import pytest

from ptn_analysis.analysis.equity import EquityAnalyzer, _classify_quadrant
from ptn_analysis.analysis.base import AnalyzerBase


def test_equity_analyzer_importable():
    """EquityAnalyzer can be imported from the analysis package."""
    from ptn_analysis.analysis import EquityAnalyzer as EA
    assert EA is EquityAnalyzer


def test_equity_analyzer_inherits_base():
    """EquityAnalyzer extends AnalyzerBase."""
    assert issubclass(EquityAnalyzer, AnalyzerBase)


def test_equity_analyzer_has_coverage_composition():
    """EquityAnalyzer uses composition (self._coverage) not inheritance from CoverageAnalyzer."""
    from ptn_analysis.analysis.coverage import CoverageAnalyzer
    assert not issubclass(EquityAnalyzer, CoverageAnalyzer)
    # Check _coverage is set in __init__ signature
    import inspect
    src = inspect.getsource(EquityAnalyzer.__init__)
    assert "_coverage" in src


def test_classify_quadrant():
    """Quadrant classification returns correct labels."""
    assert _classify_quadrant(1.0, 1.0) == "High Need / High Gap"
    assert _classify_quadrant(1.0, -0.5) == "High Need / Low Gap"
    assert _classify_quadrant(-0.5, 1.0) == "Low Need / High Gap"
    assert _classify_quadrant(-0.5, -0.5) == "Low Need / Low Gap"
    assert _classify_quadrant(0.0, 0.0) == "Low Need / Low Gap"


def test_transit_context_has_equity():
    """TransitContext exposes .equity() factory method."""
    from ptn_analysis.context import TransitContext
    assert hasattr(TransitContext, "equity")
