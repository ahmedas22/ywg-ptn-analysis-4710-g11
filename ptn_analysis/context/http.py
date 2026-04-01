"""Unified HTTP client for all PTN analysis data sources.

Consolidates file downloads, JSON API calls, OAuth2 authentication,
SODA-style pagination, response caching, and retry logic into one
consistent interface used by every source module.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any

import httpx
from loguru import logger

HTTP_TIMEOUT: float = 120.0
HTTP_MAX_RETRIES: int = 3
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


class DataClient:
    """Unified HTTP client with retry, caching, OAuth2, and pagination.

    Every source module (gtfs, open_data, census, transit_api, mobility_data)
    should use a shared DataClient instance rather than raw httpx calls.

    Args:
        cache_dir: Base directory for response caching.
        timeout: Default request timeout in seconds.
        max_retries: Maximum retry attempts for transient failures.
        throttle_rpm: Rate limit (requests per minute). 0 = unlimited.
    """

    def __init__(
        self,
        cache_dir: Path | None = None,
        timeout: float = HTTP_TIMEOUT,
        max_retries: int = HTTP_MAX_RETRIES,
        throttle_rpm: int = 0,
    ) -> None:
        from ptn_analysis.context.config import CACHE_DATA_DIR

        self._timeout = timeout
        self._max_retries = max_retries
        self._throttle_rpm = throttle_rpm
        self._last_request_mono: float | None = None
        self._cache_dir = cache_dir or CACHE_DATA_DIR
        self._oauth_tokens: dict[str, tuple[str, datetime]] = {}

    # ── Core request with retry ────────────────────────────────────────

    def _throttle(self) -> None:
        if self._throttle_rpm <= 0:
            return
        min_interval = 60.0 / self._throttle_rpm
        if self._last_request_mono is not None:
            elapsed = time.monotonic() - self._last_request_mono
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_mono = time.monotonic()

    def request(
        self,
        url: str,
        *,
        method: str = "GET",
        params: dict | None = None,
        headers: dict | None = None,
        json_body: dict | None = None,
        response_format: str = "json",
        cache_path: Path | None = None,
        force_refresh: bool = False,
        timeout: float | None = None,
    ) -> Any:
        """Execute an HTTP request with retry, throttle, and optional caching.

        Args:
            url: Absolute request URL.
            method: HTTP method (GET, POST, etc.).
            params: Query parameters.
            headers: HTTP headers.
            json_body: JSON request body (for POST).
            response_format: ``"json"``, ``"text"``, or ``"bytes"``.
            cache_path: File path for response caching.
            force_refresh: Bypass existing cache.
            timeout: Override default timeout.

        Returns:
            Parsed response in the requested format.
        """
        if cache_path and cache_path.exists() and not force_refresh:
            if cache_path.stat().st_size > 0:
                return self._read_cache(cache_path, response_format)

        self._throttle()
        last_error: Exception | None = None

        for attempt in range(self._max_retries):
            is_last = attempt == self._max_retries - 1
            try:
                resp = httpx.request(
                    method, url,
                    params=params,
                    headers=headers or {},
                    json=json_body,
                    timeout=timeout or self._timeout,
                    follow_redirects=True,
                )
                if resp.status_code in RETRYABLE_STATUS_CODES and not is_last:
                    retry_after = resp.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else 2 ** attempt
                    time.sleep(min(delay, 60.0))
                    continue
                resp.raise_for_status()
                payload = self._parse(resp, response_format)
                if cache_path:
                    self._write_cache(cache_path, payload, response_format)
                return payload
            except (
                httpx.ConnectError, httpx.ReadError,
                httpx.RemoteProtocolError, httpx.TimeoutException,
            ) as exc:
                last_error = exc
                if not is_last:
                    time.sleep(2 ** attempt)

        if last_error:
            raise last_error
        raise RuntimeError(f"Request failed after {self._max_retries} attempts: {url}")

    # ── Convenience methods ────────────────────────────────────────────

    def get(self, url: str, *, params: dict | None = None,
            headers: dict | None = None, **kwargs) -> Any:
        """GET JSON response."""
        return self.request(url, params=params, headers=headers, **kwargs)

    def post(self, url: str, *, json_body: dict | None = None,
             headers: dict | None = None, **kwargs) -> Any:
        """POST with JSON body."""
        return self.request(url, method="POST", json_body=json_body,
                            headers=headers, **kwargs)

    def download(self, url: str, dest: Path, *, force: bool = False,
                 headers: dict | None = None) -> Path:
        """Download a file. Skips if dest exists and force=False."""
        if dest.exists() and not force:
            return dest
        dest.parent.mkdir(parents=True, exist_ok=True)
        self.request(url, response_format="bytes", cache_path=dest,
                     force_refresh=force, headers=headers)
        return dest

    # ── OAuth2 ─────────────────────────────────────────────────────────

    def oauth2_token(self, provider: str, token_url: str,
                     refresh_token: str) -> str:
        """Get a valid OAuth2 access token, refreshing if expired.

        Args:
            provider: Identifier for token caching (e.g. ``"mobility_data"``).
            token_url: Token endpoint URL.
            refresh_token: Long-lived refresh token.

        Returns:
            Valid access token string.
        """
        cached = self._oauth_tokens.get(provider)
        if cached:
            token, expiry = cached
            if datetime.now(timezone.utc) < expiry:
                return token

        data = self.post(token_url, json_body={"refresh_token": refresh_token})
        access_token = data["access_token"]
        expiry_str = data.get("expiration_datetime_utc", "")
        if expiry_str:
            expiry = datetime.fromisoformat(expiry_str.rstrip("Z")).replace(
                tzinfo=timezone.utc
            )
        else:
            expiry = datetime.now(timezone.utc).replace(
                hour=datetime.now(timezone.utc).hour + 1
            )
        self._oauth_tokens[provider] = (access_token, expiry)
        return access_token

    def bearer_headers(self, token: str) -> dict[str, str]:
        """Build Authorization: Bearer header dict."""
        return {"Authorization": f"Bearer {token}"}

    # ── Paginated fetch (SODA API style) ───────────────────────────────

    def get_all_pages(
        self,
        url: str,
        *,
        params: dict | None = None,
        headers: dict | None = None,
        page_size: int = 50_000,
        offset_key: str = "$offset",
        limit_key: str = "$limit",
    ) -> list[dict]:
        """Fetch all pages from a paginated JSON API.

        Args:
            url: Base URL.
            params: Base query parameters (merged with pagination params).
            headers: HTTP headers.
            page_size: Records per page.
            offset_key: Pagination offset parameter name.
            limit_key: Pagination limit parameter name.

        Returns:
            Concatenated list of all records.
        """
        all_records: list[dict] = []
        offset = 0
        while True:
            page_params = {**(params or {}), limit_key: page_size, offset_key: offset}
            page = self.get(url, params=page_params, headers=headers)
            if not page:
                break
            all_records.extend(page)
            if len(page) < page_size:
                break
            offset += page_size
            logger.debug(f"Fetched {len(all_records)} records from {url}")
        return all_records

    # ── Cached JSON (with TTL) ─────────────────────────────────────────

    def cached_get(self, cache_key: str, url: str, *,
                   ttl_hours: float = 24, **kwargs) -> Any:
        """GET with file-based JSON cache and TTL.

        Args:
            cache_key: Relative cache file path (without extension).
            url: Request URL.
            ttl_hours: Cache time-to-live in hours.
            **kwargs: Additional keyword arguments passed to ``get()``.

        Returns:
            Cached or fresh JSON response.
        """
        cache_path = self._cache_dir / f"{cache_key}.json"
        if cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < ttl_hours:
                return json.loads(cache_path.read_text(encoding="utf-8"))
        return self.get(url, cache_path=cache_path, force_refresh=True, **kwargs)

    # ── API-key authenticated fetch ────────────────────────────────────

    def api_fetch(self, base_url: str, endpoint: str, *,
                  api_key: str = "", key_param: str = "api-key",
                  params: dict | None = None) -> Any:
        """Fetch from an API-key-authenticated endpoint.

        Args:
            base_url: API base URL.
            endpoint: Relative path.
            api_key: API key value.
            key_param: Query parameter name for the key.
            params: Additional query parameters.

        Returns:
            Parsed JSON response.
        """
        url = f"{base_url}/{endpoint.lstrip('/')}"
        request_params = dict(params or {})
        if api_key:
            request_params[key_param] = api_key
        return self.get(url, params=request_params)

    # ── JSONL family cache (for Transit API v4) ────────────────────────

    def jsonl_read(self, cache_dir: Path, family: str,
                   params: dict) -> dict | None:
        """Read a cached JSONL entry matching params."""
        jsonl_path = cache_dir / f"{family}.jsonl"
        if not jsonl_path.exists():
            return None
        frozen_key = json.dumps(params, sort_keys=True)
        for line in jsonl_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            entry = json.loads(line)
            if json.dumps(entry.get("params", {}), sort_keys=True) == frozen_key:
                return entry.get("payload")
        return None

    def jsonl_write(self, cache_dir: Path, family: str,
                    params: dict, payload: dict) -> None:
        """Append a cache entry to a JSONL file."""
        cache_dir.mkdir(parents=True, exist_ok=True)
        jsonl_path = cache_dir / f"{family}.jsonl"
        entry = {"params": params, "payload": payload}
        with open(jsonl_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry) + "\n")

    # ── Internal helpers ───────────────────────────────────────────────

    @staticmethod
    def _parse(resp: httpx.Response, fmt: str) -> Any:
        if fmt == "json":
            return resp.json()
        if fmt == "text":
            return resp.text
        if fmt == "bytes":
            return resp.content
        raise ValueError(f"Unsupported format: {fmt!r}")

    @staticmethod
    def _read_cache(path: Path, fmt: str) -> Any:
        if fmt == "json":
            return json.loads(path.read_text(encoding="utf-8"))
        if fmt == "text":
            return path.read_text(encoding="utf-8")
        if fmt == "bytes":
            return path.read_bytes()
        raise ValueError(f"Unsupported format: {fmt!r}")

    @staticmethod
    def _write_cache(path: Path, payload: Any, fmt: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        part = path.with_name(f"{path.name}.part")
        if fmt == "json":
            part.write_text(json.dumps(payload, default=str), encoding="utf-8")
        elif fmt == "text":
            part.write_text(str(payload), encoding="utf-8")
        elif fmt == "bytes":
            part.write_bytes(payload)
        else:
            raise ValueError(f"Unsupported format: {fmt!r}")
        part.replace(path)
