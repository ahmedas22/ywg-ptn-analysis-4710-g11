"""Tests for GTFS manifest loading and feed resolution."""

import pytest

from ptn_analysis.context.config import load_gtfs_manifest, gtfs_zip_path
from ptn_analysis.data.sources.gtfs import manifest_feeds


def test_manifest_loads():
    """GTFS manifest YAML loads without error."""
    manifest = load_gtfs_manifest()
    assert "feeds" in manifest
    assert len(manifest["feeds"]) >= 9


def test_manifest_has_winnipeg_feeds():
    """Manifest includes Winnipeg feeds across eras."""
    feeds = manifest_feeds("ywg")
    assert len(feeds) >= 8
    eras = {f["era"] for f in feeds}
    assert "pre_ptn" in eras
    assert "post_ptn" in eras


def test_manifest_has_edmonton():
    """Manifest includes Edmonton comparison feed."""
    feeds = manifest_feeds("yeg")
    assert len(feeds) >= 1
    assert feeds[0]["era"] == "comparison"


def test_manifest_no_duplicate_snapshot_ids():
    """No duplicate (city_key, snapshot_id) pairs in manifest."""
    manifest = load_gtfs_manifest()
    seen = set()
    for f in manifest["feeds"]:
        key = (f["city_key"], f["snapshot_id"])
        assert key not in seen, f"Duplicate manifest entry: {key}"
        seen.add(key)


def test_manifest_providers_valid():
    """All provider types are recognized."""
    valid_types = {"mobility_data", "wtlivewpg", "direct"}
    manifest = load_gtfs_manifest()
    for f in manifest["feeds"]:
        for p in f.get("providers", []):
            assert p["type"] in valid_types, f"Unknown provider: {p['type']}"


def test_gtfs_zip_path_format():
    """gtfs_zip_path returns per-city/per-snapshot paths."""
    path = gtfs_zip_path("ywg", "2024-09-01")
    assert "ywg" in str(path)
    assert "2024-09-01" in str(path)
    assert str(path).endswith(".zip")


def test_manifest_feeds_era_filter():
    """manifest_feeds correctly filters by era."""
    pre = manifest_feeds("ywg", era="pre_ptn")
    post = manifest_feeds("ywg", era="post_ptn")
    assert all(f["era"] == "pre_ptn" for f in pre)
    assert all(f["era"] == "post_ptn" for f in post)
    assert len(pre) + len(post) <= len(manifest_feeds("ywg"))
