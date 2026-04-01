"""MobilityData Mobility Database API client.

Primary GTFS feed source. Provides access to 2000+ transit agencies
globally with historical feed versions (after Feb 2024).

Auth: OAuth2 Bearer via refresh token (MOBILITY_DATA_REFRESH_TOKEN in .env).
Docs: https://mobilitydatabase.org / https://api.mobilitydatabase.org/v1
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger

from ptn_analysis.context.config import CACHE_DATA_DIR
from ptn_analysis.context.http import DataClient

MOBILITY_DATA_API_URL = "https://api.mobilitydatabase.org/v1"
MOBILITY_DATA_TOKEN_URL = f"{MOBILITY_DATA_API_URL}/tokens/access"
MOBILITY_DATA_CACHE_DIR = CACHE_DATA_DIR / "mobility_data"

# City search terms for feed discovery
CITY_SEARCH_TERMS: dict[str, str] = {
    "ywg": "Winnipeg Transit",
    "yeg": "Edmonton Transit",
    "yyc": "Calgary Transit",
    "yow": "OC Transpo",
    "yhm": "Hamilton Street Railway",
}


class MobilityDataClient:
    """Client for the MobilityData Mobility Database API.

    Uses DataClient for HTTP, OAuth2, and caching. Thin typed
    wrapper over the catalog API endpoints.

    Args:
        refresh_token: OAuth2 refresh token. If empty, client is unavailable.
        client: Shared DataClient instance.
    """

    def __init__(
        self,
        refresh_token: str = "",
        client: DataClient | None = None,
    ) -> None:
        import os

        self._refresh_token = refresh_token or os.getenv("MOBILITY_DATA_REFRESH_TOKEN", "")
        self._client = client or DataClient(cache_dir=MOBILITY_DATA_CACHE_DIR)

    @property
    def available(self) -> bool:
        """Whether the client has a valid refresh token."""
        return bool(self._refresh_token)

    def _auth_headers(self) -> dict[str, str]:
        """Get Bearer auth headers, refreshing token if needed."""
        token = self._client.oauth2_token(
            "mobility_data", MOBILITY_DATA_TOKEN_URL, self._refresh_token,
        )
        return self._client.bearer_headers(token)

    def search_feeds(self, provider: str, limit: int = 10) -> list[dict]:
        """Search GTFS feeds by provider name.

        Args:
            provider: Provider search string (e.g. "Winnipeg Transit").
            limit: Max results.

        Returns:
            List of feed metadata dicts.
        """
        return self._client.get(
            f"{MOBILITY_DATA_API_URL}/gtfs_feeds",
            params={"provider": provider, "limit": limit},
            headers=self._auth_headers(),
        )

    def get_feed(self, feed_id: str) -> dict:
        """Get metadata for one GTFS feed."""
        return self._client.get(
            f"{MOBILITY_DATA_API_URL}/gtfs_feeds/{feed_id}",
            headers=self._auth_headers(),
        )

    def list_datasets(self, feed_id: str, limit: int = 20) -> list[dict]:
        """List historical datasets (versions) for a feed.

        Returns newest first (by downloaded_at).
        """
        return self._client.get(
            f"{MOBILITY_DATA_API_URL}/gtfs_feeds/{feed_id}/datasets",
            params={"limit": limit},
            headers=self._auth_headers(),
        )

    def get_dataset(self, dataset_id: str) -> dict:
        """Get metadata for one dataset version."""
        return self._client.get(
            f"{MOBILITY_DATA_API_URL}/datasets/gtfs/{dataset_id}",
            headers=self._auth_headers(),
        )

    def discover_feed_id(self, city_key: str) -> str | None:
        """Discover the MobilityData feed_id for a city.

        Uses CITY_SEARCH_TERMS for the search query.

        Args:
            city_key: City namespace (e.g. "ywg", "yeg").

        Returns:
            Feed ID string or None if not found.
        """
        search_term = CITY_SEARCH_TERMS.get(city_key)
        if not search_term:
            logger.warning(f"No search term configured for city_key={city_key!r}")
            return None

        feeds = self.search_feeds(search_term)
        if not feeds:
            logger.warning(f"No MobilityData feeds found for '{search_term}'")
            return None

        # Take the first result (most relevant)
        feed_id = feeds[0].get("id")
        logger.info(f"Discovered MobilityData feed_id for {city_key}: {feed_id}")
        return feed_id

    def find_dataset_for_date(
        self, feed_id: str, target_date: str,
    ) -> dict | None:
        """Find the dataset whose service range covers target_date.

        Falls back to the most recently downloaded dataset.

        Args:
            feed_id: MobilityData feed ID.
            target_date: Target service date (YYYY-MM-DD) or "current"/"latest".

        Returns:
            Dataset metadata dict or None.
        """
        datasets = self.list_datasets(feed_id)
        if not datasets:
            return None

        if target_date in ("current", "latest"):
            return datasets[0]  # newest by downloaded_at

        # Find dataset covering the target date
        for ds in datasets:
            start = ds.get("service_date_range_start", "")
            end = ds.get("service_date_range_end", "")
            if start and end and start <= target_date <= end:
                return ds

        # Fallback: closest dataset by download date
        logger.warning(
            f"No dataset covers {target_date} for feed {feed_id}; "
            f"using most recent download"
        )
        return datasets[0]

    def download_dataset(
        self, dataset: dict, dest_path: Path, force: bool = False,
    ) -> Path:
        """Download a dataset ZIP file.

        Args:
            dataset: Dataset metadata dict (must have 'hosted_url').
            dest_path: Local destination path.
            force: Re-download even if cached.

        Returns:
            Path to downloaded ZIP.
        """
        url = dataset.get("hosted_url")
        if not url:
            raise ValueError(f"Dataset has no hosted_url: {dataset.get('id')}")

        return self._client.download(
            url, dest_path, force=force,
            headers=self._auth_headers(),
        )

    def get_latest_url(self, feed_id: str) -> str | None:
        """Get the download URL for the latest dataset of a feed."""
        feed = self.get_feed(feed_id)
        latest = feed.get("latest_dataset", {})
        return latest.get("hosted_url")
