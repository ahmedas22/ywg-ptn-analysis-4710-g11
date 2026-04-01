"""Tests for DAFQ data quality scorecard module."""

from ptn_analysis.data.quality import (
    compute_dafq_scorecard,
    temporal_harmonization_table,
)


def test_temporal_harmonization_table_structure():
    """Temporal harmonization table has all expected columns."""
    df = temporal_harmonization_table()
    assert not df.empty
    assert set(df.columns) == {"source", "period", "lag", "type", "caveat"}
    assert len(df) >= 7


def test_dafq_scorecard_structure(tmp_path):
    """DAFQ scorecard returns expected columns and score range."""
    from ptn_analysis.context.db import TransitDB

    db = TransitDB(tmp_path / "test.duckdb")
    # Create minimal tables so scorecard can run
    db.execute("CREATE TABLE ywg_gtfs_route_stats (x INT)")
    db.execute("CREATE TABLE ywg_ontime_performance (x INT)")
    db.execute("CREATE TABLE ywg_passups (x INT)")
    db.execute("CREATE TABLE ywg_passenger_counts (x INT)")
    db.execute("CREATE TABLE ywg_census_da (x INT)")
    db.execute("CREATE TABLE ywg_da_jobs_proxy (x INT)")
    db.execute("CREATE TABLE ywg_neighbourhoods (x INT)")

    df = compute_dafq_scorecard(db, city_key="ywg")
    assert not df.empty
    assert "overall_score" in df.columns
    assert (df["overall_score"] >= 1).all()
    assert (df["overall_score"] <= 5).all()
    db.close()
