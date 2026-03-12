"""HTTP client with retry, caching, and authenticated API access."""

from __future__ import annotations

import json
from pathlib import Path
import time
from typing import Any

import httpx

HTTP_TIMEOUT: float = 120.0
HTTP_MAX_RETRIES: int = 3
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})


# ---------------------------------------------------------------------------
# Downloader — HTTP client with retry and file cache
# ---------------------------------------------------------------------------


class Downloader:
    """Download HTTP resources with retry and optional caching."""

    def request(
        self,
        url: str,
        params: dict | None = None,
        response_format: str = "json",
        headers: dict | None = None,
        cache_path: Path | None = None,
        force_refresh: bool = False,
        method: str = "GET",
        files: dict | None = None,
        timeout: float = HTTP_TIMEOUT,
    ) -> Any:
        """Fetch a resource and optionally cache it.

        Args:
            url: Absolute request URL.
            params: Optional query parameters.
            response_format: ``"json"``, ``"text"``, or ``"bytes"``.
            headers: Optional HTTP headers.
            cache_path: Optional cache file path.
            force_refresh: Whether to bypass an existing cache file.
            method: HTTP method.
            files: Optional multipart payload.
            timeout: Request timeout in seconds.

        Returns:
            Parsed JSON, decoded text, or raw bytes.
        """
        if cache_path is not None and cache_path.exists() and not force_refresh:
            if cache_path.stat().st_size > 0:
                return self._read_cache(cache_path, response_format)

        request_headers = headers or {}
        request_params = params
        last_error: Exception | None = None

        for attempt_index in range(HTTP_MAX_RETRIES):
            is_last_attempt = attempt_index == HTTP_MAX_RETRIES - 1
            try:
                response = httpx.request(
                    method,
                    url,
                    params=request_params,
                    headers=request_headers,
                    files=files,
                    timeout=timeout,
                    follow_redirects=True,
                )
                if response.status_code in RETRYABLE_STATUS_CODES and not is_last_attempt:
                    delay_seconds = 2 ** attempt_index
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay_seconds = min(float(retry_after), 60.0)
                        except ValueError:
                            delay_seconds = 2 ** attempt_index
                    time.sleep(delay_seconds)
                    continue
                response.raise_for_status()
                payload = self._parse_response(response, response_format)
                if cache_path is not None:
                    cache_path.parent.mkdir(parents=True, exist_ok=True)
                    self._write_cache(cache_path, payload, response_format)
                return payload
            except (
                httpx.ConnectError,
                httpx.ReadError,
                httpx.RemoteProtocolError,
                httpx.TimeoutException,
            ) as exc:
                last_error = exc
                if is_last_attempt:
                    break
                time.sleep(2 ** attempt_index)

        if last_error is not None:
            raise last_error
        raise RuntimeError(f"Request failed: {url}")

    def _parse_response(self, response: httpx.Response, response_format: str) -> Any:
        """Parse one HTTP response payload."""
        if response_format == "json":
            return response.json()
        if response_format == "text":
            return response.text
        if response_format == "bytes":
            return response.content
        raise ValueError(f"Unsupported response_format: {response_format!r}")

    def _read_cache(self, cache_path: Path, response_format: str) -> Any:
        """Read one cached payload."""
        if response_format == "json":
            return json.loads(cache_path.read_text(encoding="utf-8"))
        if response_format == "text":
            return cache_path.read_text(encoding="utf-8")
        if response_format == "bytes":
            return cache_path.read_bytes()
        raise ValueError(f"Unsupported response_format: {response_format!r}")

    def _write_cache(self, cache_path: Path, payload: Any, response_format: str) -> None:
        """Write one payload to cache."""
        part_path = cache_path.with_name(f"{cache_path.name}.part")
        if part_path.exists():
            part_path.unlink()
        if response_format == "json":
            part_path.write_text(json.dumps(payload), encoding="utf-8")
        elif response_format == "text":
            part_path.write_text(str(payload), encoding="utf-8")
        elif response_format == "bytes":
            part_path.write_bytes(payload)
        else:
            raise ValueError(f"Unsupported response_format: {response_format!r}")
        part_path.replace(cache_path)


# ---------------------------------------------------------------------------
# ApiClient — authenticated HTTP client with throttle and JSONL cache
# ---------------------------------------------------------------------------


class ApiClient(Downloader):
    """Authenticated API client with JSONL family cache and throttle.

    Args:
        api_key: API authentication key.
        base_url: API base URL.
        cache_dir: Directory for JSONL cache files.
        throttle_rpm: Maximum requests per minute.
    """

    def __init__(
        self,
        api_key: str,
        base_url: str,
        cache_dir: Path,
        throttle_rpm: int = 60,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.cache_dir = cache_dir
        self._throttle_rpm = throttle_rpm
        self._last_request_mono: float | None = None

    def _throttle(self) -> None:
        """Enforce request rate limit."""
        if self._throttle_rpm <= 0:
            return
        min_interval = 60.0 / self._throttle_rpm
        if self._last_request_mono is not None:
            elapsed = time.monotonic() - self._last_request_mono
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
        self._last_request_mono = time.monotonic()

    def fetch_json(self, endpoint: str, params: dict | None = None) -> dict:
        """Fetch a JSON endpoint with throttle and auth.

        Args:
            endpoint: Relative endpoint path.
            params: Optional query parameters.

        Returns:
            Parsed JSON response.
        """
        self._throttle()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        request_params = dict(params or {})
        if self.api_key:
            request_params["api-key"] = self.api_key
        return self.request(url, params=request_params, response_format="json")

    def _jsonl_path(self, family: str) -> Path:
        """Return the JSONL cache path for an endpoint family.

        Args:
            family: Endpoint family name.

        Returns:
            Path to the JSONL cache file.
        """
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        return self.cache_dir / f"{family}.jsonl"

    def _jsonl_read(self, family: str, params: dict) -> dict | None:
        """Read a cached JSONL entry matching params.

        Args:
            family: Endpoint family name.
            params: Request parameters to match.

        Returns:
            Cached payload or None if not found.
        """
        jsonl_path = self._jsonl_path(family)
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

    def _jsonl_write(self, family: str, params: dict, payload: dict) -> None:
        """Append a cache entry to the JSONL file.

        Args:
            family: Endpoint family name.
            params: Request parameters.
            payload: Response payload.
        """
        jsonl_path = self._jsonl_path(family)
        entry = {"params": params, "payload": payload}
        with open(jsonl_path, "a", encoding="utf-8") as file_handle:
            file_handle.write(json.dumps(entry) + "\n")
