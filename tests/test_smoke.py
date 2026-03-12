"""Smoke tests: verify imports, config, and analyzer instantiation.

Run with: pytest tests/ -v -m "not integration"
For integration tests (requires populated DB): pytest tests/ -v -m integration
"""
import pytest
from ptn_analysis.context.config import (
    DUCKDB_PATH,
    PTN_LAUNCH_DATE,
    FEED_ID_CURRENT,
    FEED_ID_PRE_PTN,
)
from ptn_analysis.context.db import TransitDB


def test_config_feed_ids():
    """Config exports canonical feed IDs."""
    assert FEED_ID_CURRENT == "current"
    assert FEED_ID_PRE_PTN == "pre_ptn"


def test_table_name_prefix():
    """table_name() applies city prefix correctly."""
    db = TransitDB()
    assert db.table_name("stops", "ywg") == "ywg_stops"
    assert db.transit_table_name("service_status", "ywg") == "ywg_transit_service_status"


def test_transit_context_import():
    """TransitContext importable from ptn_analysis."""
    from ptn_analysis import TransitContext
    assert TransitContext is not None


def test_transit_context_repr():
    """TransitContext repr contains city_key."""
    from ptn_analysis import TransitContext
    ctx = TransitContext.from_defaults()
    assert "TransitContext" in repr(ctx)
    assert "ywg" in repr(ctx)


def test_dashboard_importable_without_streamlit():
    """Dashboard importable without requiring streamlit at module level."""
    from ptn_analysis.context.serving import Dashboard
    assert Dashboard is not None


def test_no_singleton_in_db():
    """db singleton must not exist in db module."""
    import ptn_analysis.context.db as db_module
    assert not hasattr(db_module, "db"), "db singleton must be removed"


def test_analyzer_imports():
    """All public analyzer classes importable from analysis subpackage."""
    from ptn_analysis.analysis.frequency import FrequencyAnalyzer
    from ptn_analysis.analysis.network import NetworkAnalyzer
    from ptn_analysis.analysis.coverage import CoverageAnalyzer
    assert FrequencyAnalyzer
    assert NetworkAnalyzer
    assert CoverageAnalyzer


def test_pipeline_import():
    """DatasetPipeline importable."""
    from ptn_analysis.data.pipeline import DatasetPipeline
    assert DatasetPipeline


def test_reporting_registry_complete():
    """REPORT_NOTEBOOKS has both pr1 and pr2 entries."""
    from ptn_analysis.context.reporting import REPORT_NOTEBOOKS, get_report_names
    assert "pr1" in REPORT_NOTEBOOKS
    assert "pr2" in REPORT_NOTEBOOKS
    pr2_stems = list(REPORT_NOTEBOOKS["pr2"].keys())
    expected = [
        "0.1-ahmed-pr2-comparison", "0.2-ahmed-pr2-clustering",
        "0.3-ahmed-pr2-classification", "0.4-ahmed-pr2-capacity",
        "1.2-cathy-pr2-network", "1.3-sudipta-pr2-coverage",
        "2.1-stephenie-pr2-viz",
    ]
    for stem in expected:
        assert stem in pr2_stems, f"Missing from registry: {stem}"


@pytest.mark.integration
def test_network_tables_exist():
    """Network tables materialized by pipeline (requires populated DB)."""
    from ptn_analysis import TransitContext
    ctx = TransitContext.from_defaults()
    assert ctx.working_db.relation_exists(ctx.working_db.table_name("network_metrics", "ywg"))
    assert ctx.working_db.relation_exists(ctx.working_db.table_name("top_hubs", "ywg"))
    assert ctx.working_db.relation_exists(ctx.working_db.table_name("transfer_burden_matrix", "ywg"))
