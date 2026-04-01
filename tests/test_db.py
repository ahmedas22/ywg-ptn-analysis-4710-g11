"""Tests for TransitDB cache invalidation and identifier validation."""

import pytest

from ptn_analysis.context.db import TransitDB


def test_cache_invalidated_after_execute(tmp_path):
    """Verify execute() clears the query cache."""
    db = TransitDB(tmp_path / "test.duckdb")
    db.execute("CREATE TABLE t1 (x INT)")
    db.execute("INSERT INTO t1 VALUES (1)")
    result_1 = db.cached_query("SELECT * FROM t1")
    assert len(result_1) == 1

    db.execute("INSERT INTO t1 VALUES (2)")
    result_2 = db.cached_query("SELECT * FROM t1")
    assert len(result_2) == 2
    db.close()


def test_cache_invalidated_after_execute_native(tmp_path):
    """Verify execute_native() clears the query cache."""
    db = TransitDB(tmp_path / "test.duckdb")
    db.execute("CREATE TABLE t2 (x INT)")
    db.execute("INSERT INTO t2 VALUES (1)")
    result_1 = db.cached_query("SELECT * FROM t2")
    assert len(result_1) == 1

    db.execute_native("INSERT INTO t2 VALUES (2)")
    result_2 = db.cached_query("SELECT * FROM t2")
    assert len(result_2) == 2
    db.close()


def test_identifier_validation():
    """TransitDB rejects invalid SQL identifiers."""
    db = TransitDB()
    with pytest.raises(ValueError, match="Invalid identifier"):
        db.table_name("DROP TABLE --", "ywg")


def test_city_key_validation():
    """TransitDB rejects invalid city keys."""
    db = TransitDB()
    with pytest.raises(ValueError, match="Invalid city_key"):
        db.table_name("stops", "'; DROP TABLE")
