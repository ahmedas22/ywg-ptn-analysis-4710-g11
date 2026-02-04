"""Shared helpers used by ingestion modules."""

from pathlib import Path

import httpx
import pandas as pd
from tqdm import tqdm

TQDM_MIN_INTERVAL = 0.5
TQDM_SMOOTHING = 0.1


def select_existing_columns(columns: list[str], available: list[str]) -> list[str]:
    """Return columns that exist in a source table/file.

    Args:
        columns: Desired columns in target order.
        available: Columns currently available in source data.

    Returns:
        Ordered subset that exists in source data.
    """
    available_set = set(available)
    return [column for column in columns if column in available_set]


def parse_yyyymmdd_columns(df, date_columns: list[str]):
    """Parse YYYYMMDD date columns in-place.

    Args:
        df: DataFrame-like object with column access.
        date_columns: Column names to parse.

    Returns:
        DataFrame with parsed date columns.
    """
    for column in date_columns:
        if column in df.columns:
            df[column] = df[column].astype(str).str.strip()
            df[column] = df[column].where(df[column] != "", None)
            df[column] = pd.to_datetime(df[column], format="%Y%m%d", errors="coerce")
    return df


def download_with_cache(
    url: str,
    output_path: Path,
    *,
    description: str,
    headers: dict[str, str] | None = None,
    timeout_seconds: float = 600.0,
    follow_redirects: bool = True,
    chunk_size: int = 4 * 1024 * 1024,
) -> Path:
    """Download URL to a local file if not already cached.

    Args:
        url: Source URL.
        output_path: Destination path.
        description: Progress bar label.
        headers: Optional HTTP headers.
        timeout_seconds: HTTP timeout in seconds.
        follow_redirects: Whether to follow redirects.
        chunk_size: Stream chunk size in bytes.

    Returns:
        Path to cached/downloaded file.
    """
    if output_path.exists():
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with httpx.stream(
        "GET",
        url,
        headers=headers,
        timeout=timeout_seconds,
        follow_redirects=follow_redirects,
    ) as response:
        response.raise_for_status()
        total_size = int(response.headers.get("content-length", 0))
        with open(output_path, "wb") as output_file:
            with tqdm(
                total=total_size,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                mininterval=TQDM_MIN_INTERVAL,
                smoothing=TQDM_SMOOTHING,
                desc=description,
            ) as progress:
                for chunk in response.iter_bytes(chunk_size=chunk_size):
                    output_file.write(chunk)
                    progress.update(len(chunk))
    return output_path
