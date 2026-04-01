"""Tests for the unified DataClient."""

from ptn_analysis.context.http import DataClient


def test_dataclient_importable():
    """DataClient can be imported."""
    client = DataClient()
    assert client is not None


def test_dataclient_has_core_methods():
    """DataClient exposes all required methods."""
    methods = [m for m in dir(DataClient) if not m.startswith("_")]
    expected = [
        "api_fetch", "bearer_headers", "cached_get", "download",
        "get", "get_all_pages", "jsonl_read", "jsonl_write",
        "oauth2_token", "post", "request",
    ]
    for m in expected:
        assert m in methods, f"Missing method: {m}"


def test_no_downloader_or_apiclient():
    """Legacy Downloader and ApiClient classes are removed."""
    import ptn_analysis.context.http as http_mod
    assert not hasattr(http_mod, "Downloader")
    assert not hasattr(http_mod, "ApiClient")


def test_bearer_headers_format():
    """bearer_headers returns correct Authorization format."""
    client = DataClient()
    headers = client.bearer_headers("test_token_123")
    assert headers == {"Authorization": "Bearer test_token_123"}
