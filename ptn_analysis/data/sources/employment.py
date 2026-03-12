"""Employment and workplace context source service."""

from __future__ import annotations

from pathlib import Path
import zipfile

import duckdb
from loguru import logger
import pandas as pd

from ptn_analysis.context.config import (
    CBP_DA_SOURCE_PATH,
    CBP_DA_SOURCE_URL,
    CENSUS_POW_SOURCE_PATHS,
    EMPLOYMENT_CACHE_DIR,
    STATCAN_POW_PRODUCT_IDS,
    STATCAN_WDS_URL,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.http import Downloader

_downloader = Downloader()

SIZE_RANGE_WEIGHTS = {
    "Without employees": 0.0,
    "Indeterminate": 1.0,
    "1 to 4 employees": 2.5,
    "5 to 9 employees": 7.0,
    "10 to 19 employees": 14.5,
    "20 to 49 employees": 34.5,
    "50 to 99 employees": 74.5,
    "100 to 199 employees": 149.5,
    "200 to 499 employees": 349.5,
    "500 employees and over": 500.0,
    "500 or more employees": 500.0,
    "500 +": 500.0,
}
INVALID_DA_CODES = {"00000000", "10000000", "99999999"}
WINNIPEG_POW_DGUIDS = {"2021A00054611040", "2021S0503602"}
LARGE_EMPLOYER_RANGES = {
    "100 to 199 employees",
    "200 to 499 employees",
    "500 employees and over",
    "500 or more employees",
    "500 +",
}


def ensure_cbp_source(source, force_refresh: bool) -> Path | None:
    """Ensure one raw CBP CSV is cached locally."""
    cache_path = source.raw_cache_dir / source.cbp_da_path.name
    if cache_path.exists() and not force_refresh:
        return cache_path
    if source.cbp_da_path.exists():
        logger.info(f"Using staged CBP source directly from {source.cbp_da_path}")
        return source.cbp_da_path
    if CBP_DA_SOURCE_URL:
        logger.info(f"Downloading CBP source from {CBP_DA_SOURCE_URL}")
        source.downloader.request(
            CBP_DA_SOURCE_URL,
            response_format="bytes",
            cache_path=cache_path,
            force_refresh=force_refresh,
        )
        return cache_path
    return None


def ensure_place_of_work_sources(source, force_refresh: bool) -> list[Path]:
    """Ensure place-of-work CSVs exist in the raw cache."""
    local_cached_paths: list[Path] = []
    for source_path in source.census_pow_paths:
        if not source_path.exists():
            continue
        logger.info(f"Using staged place-of-work source directly from {source_path}")
        local_cached_paths.append(source_path)
    if local_cached_paths:
        return local_cached_paths

    cached_paths: list[Path] = []
    for product_id in STATCAN_POW_PRODUCT_IDS:
        extracted_path = source.raw_cache_dir / f"{product_id}.csv"
        if extracted_path.exists() and not force_refresh:
            cached_paths.append(extracted_path)
            continue
        try:
            downloaded_path = download_statcan_table_csv(source, product_id, force_refresh)
            cached_paths.append(downloaded_path)
        except Exception as exc:
            logger.warning(
                "Failed to download Statistics Canada place-of-work table "
                f"{product_id}: {exc}"
            )
    return cached_paths


def read_csv_with_fallback_encoding(source_path: Path, **kwargs) -> pd.DataFrame:
    """Read a CSV with a small encoding fallback set."""
    for encoding in ["utf-8", "iso-8859-1"]:
        try:
            return pd.read_csv(source_path, encoding=encoding, **kwargs)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(source_path, encoding="latin-1", **kwargs)


def ensure_cbp_reduced_cache(source, source_path: Path, force_refresh: bool) -> Path:
    """Build the reduced CBP Parquet cache used by normal pipeline runs."""
    reduced_path = source.reduced_cache_dir / "cbp_da_jobs_proxy.parquet"
    if reduced_path.exists() and not force_refresh:
        return reduced_path
    reduced_frame = read_csv_with_fallback_encoding(
        source_path,
        usecols=[0, 1, 2, 4],
        header=None,
        names=[
            "da_uid",
            "employee_size_range",
            "establishment_count",
            "classified_establishment_count",
        ],
        skiprows=1,
        dtype="string",
        low_memory=False,
    )
    reduced_frame.to_parquet(reduced_path, index=False)
    return reduced_path


def ensure_place_of_work_reduced_cache(
    source,
    source_paths: list[Path],
    force_refresh: bool,
) -> Path:
    """Build the reduced place-of-work Parquet cache used by normal pipeline runs."""
    reduced_path = source.reduced_cache_dir / "place_of_work.parquet"
    if reduced_path.exists() and not force_refresh:
        return reduced_path
    union_queries = [place_of_work_select_sql(path) for path in source_paths]
    reduced_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            COPY (
                {" UNION ALL ".join(union_queries)}
            ) TO {sql_string(reduced_path.as_posix())} (FORMAT PARQUET)
            """
        )
    finally:
        connection.close()
    return reduced_path


def download_statcan_table_csv(source, product_id: str, force_refresh: bool) -> Path:
    """Download and extract one Statistics Canada CSV table."""
    metadata_path = source.raw_cache_dir / f"statcan_{product_id}_metadata.json"
    metadata = source.downloader.request(
        f"{STATCAN_WDS_URL}/getFullTableDownloadCSV/{product_id}/en",
        response_format="json",
        cache_path=metadata_path,
        force_refresh=force_refresh,
    )
    if not isinstance(metadata, dict) or "object" not in metadata:
        raise ValueError(f"Unexpected WDS response for product {product_id}: {metadata!r}")
    zip_path = source.raw_cache_dir / f"statcan_{product_id}.zip"
    source.downloader.request(
        metadata["object"],
        response_format="bytes",
        cache_path=zip_path,
        force_refresh=force_refresh,
    )
    if zip_path.stat().st_size == 0:
        raise ValueError(f"Downloaded Statistics Canada ZIP is empty for product {product_id}")
    return extract_single_csv(source, zip_path, force_refresh)


def extract_single_csv(source, zip_path: Path, force_refresh: bool) -> Path:
    """Extract the first real CSV file from a ZIP archive."""
    with zipfile.ZipFile(zip_path) as archive_file:
        csv_members = [name for name in archive_file.namelist() if name.lower().endswith(".csv")]
        if not csv_members:
            raise ValueError(f"No CSV file found in archive: {zip_path}")
        csv_member_name = max(
            csv_members,
            key=lambda member_name: archive_file.getinfo(member_name).file_size,
        )
        extracted_path = source.raw_cache_dir / Path(csv_member_name).name
        if extracted_path.exists() and not force_refresh:
            return extracted_path
        with archive_file.open(csv_member_name) as zipped_file:
            extracted_path.write_bytes(zipped_file.read())
        if extracted_path.stat().st_size == 0:
            raise ValueError(f"Extracted Statistics Canada CSV is empty: {extracted_path}")
        return extracted_path


def build_cbp_source_sql(source_path: Path) -> str:
    """Build a DuckDB relation SQL snippet for the CBP source."""
    file_sql = sql_string(source_path.as_posix())
    if source_path.suffix.lower() == ".parquet":
        return (
            "SELECT "
            "LPAD(TRIM(da_uid), 8, '0') AS column000, "
            "employee_size_range AS column001, "
            "establishment_count AS column002, "
            "classified_establishment_count AS column004 "
            f"FROM read_parquet({file_sql})"
        )
    return (
        "SELECT * "
        f"FROM read_csv_auto({file_sql}, header = false, skip = 1, all_varchar = true, ignore_errors = true)"
    )


def place_of_work_select_sql(source_path: Path) -> str:
    """Build one filtered place-of-work SELECT statement."""
    if source_path.suffix.lower() == ".parquet":
        return f"SELECT * FROM read_parquet({sql_string(source_path.as_posix())})"
    dguid_sql = ", ".join(sql_string(value) for value in sorted(WINNIPEG_POW_DGUIDS))
    source_sql = build_place_of_work_source_sql(source_path)
    return f"""
        SELECT
            CAST(REF_DATE AS VARCHAR) AS ref_date,
            CAST(GEO AS VARCHAR) AS geo,
            CAST(DGUID AS VARCHAR) AS dguid,
            CAST("Work activity during the reference year (4A)" AS VARCHAR) AS work_activity,
            CAST("Age (15A)" AS VARCHAR) AS age_group,
            CAST("Gender (3)" AS VARCHAR) AS gender,
            CAST("Industry - Sectors - North American Industry Classification System (NAICS) 2017 (21)" AS VARCHAR) AS industry_sector,
            CAST(Coordinate AS VARCHAR) AS coordinate,
            COALESCE(
                TRY_CAST(NULLIF("Place of work status (3):Total - Place of work status[1]", '') AS DOUBLE),
                0.0
            ) AS place_of_work_total,
            COALESCE(
                TRY_CAST(NULLIF("Place of work status (3):Worked at home[2]", '') AS DOUBLE),
                0.0
            ) AS worked_at_home,
            COALESCE(
                TRY_CAST(NULLIF("Place of work status (3):Usual place of work[3]", '') AS DOUBLE),
                0.0
            ) AS usual_place_of_work
        FROM ({source_sql})
        WHERE CAST(DGUID AS VARCHAR) IN ({dguid_sql})
           OR CAST(GEO AS VARCHAR) ILIKE '%Winnipeg%'
    """


def build_place_of_work_source_sql(source_path: Path) -> str:
    """Build a DuckDB relation SQL snippet for a place-of-work source."""
    file_sql = sql_string(source_path.as_posix())
    if source_path.suffix.lower() == ".parquet":
        return f"SELECT * FROM read_parquet({file_sql})"
    return (
        "SELECT * "
        f"FROM read_csv_auto({file_sql}, header = true, all_varchar = true, ignore_errors = true)"
    )


def employee_size_weight_case(column_name: str) -> str:
    """Return a SQL CASE expression for employee-size weights."""
    case_lines = ["CASE"]
    for label, weight in SIZE_RANGE_WEIGHTS.items():
        case_lines.append(f"    WHEN {column_name} = {sql_string(label)} THEN {weight}")
    case_lines.append("    ELSE 1.0")
    case_lines.append("END")
    return "\n".join(case_lines)


def large_employer_case(range_column: str, count_column: str) -> str:
    """Return a SQL CASE expression for large-employer proxy counts."""
    ranges_sql = ", ".join(sql_string(value) for value in sorted(LARGE_EMPLOYER_RANGES))
    return f"CASE WHEN {range_column} IN ({ranges_sql}) THEN {count_column} ELSE 0.0 END"


def sql_string(value: str) -> str:
    """Quote one string value for a DuckDB SQL literal."""
    escaped_value = value.replace("'", "''")
    return f"'{escaped_value}'"


class _SourceContext:
    """Internal context for helper function compatibility."""

    __slots__ = (
        "city_key",
        "cbp_da_path",
        "census_pow_paths",
        "downloader",
        "cache_dir",
        "raw_cache_dir",
        "reduced_cache_dir",
    )

    def __init__(self, city_key: str) -> None:
        self.city_key = city_key
        self.cbp_da_path = CBP_DA_SOURCE_PATH
        self.census_pow_paths = CENSUS_POW_SOURCE_PATHS
        self.downloader = _downloader
        self.cache_dir = EMPLOYMENT_CACHE_DIR / city_key
        self.raw_cache_dir = self.cache_dir / "raw"
        self.reduced_cache_dir = self.cache_dir / "reduced"
        self.raw_cache_dir.mkdir(parents=True, exist_ok=True)
        self.reduced_cache_dir.mkdir(parents=True, exist_ok=True)


def load(city_key: str, db_instance: TransitDB, force_refresh: bool = False) -> dict[str, int]:
    """Load normalized employment context tables."""
    ctx = _SourceContext(city_key)
    results: dict[str, int] = {}

    cbp_source_path = ensure_cbp_source(ctx, force_refresh=force_refresh)
    if cbp_source_path is None:
        logger.warning("Skipping employment load: no CBP source was available.")
        return results
    cbp_reduced_path = ensure_cbp_reduced_cache(
        ctx,
        cbp_source_path,
        force_refresh=force_refresh,
    )

    logger.info(f"Loading reduced CBP jobs-proxy cache from {cbp_reduced_path}")
    _load_cbp_raw_table(city_key, db_instance, cbp_reduced_path)
    logger.info("Loaded reduced CBP jobs-proxy cache into DuckDB")
    results["da_jobs_proxy_raw"] = db_instance.count(db_instance.table_name("da_jobs_proxy_raw", city_key)) or 0

    pow_source_paths = ensure_place_of_work_sources(ctx, force_refresh=force_refresh)
    if pow_source_paths:
        pow_reduced_path = ensure_place_of_work_reduced_cache(
            ctx,
            pow_source_paths,
            force_refresh=force_refresh,
        )
        logger.info(
            "Loading reduced Statistics Canada place-of-work cache from "
            f"{pow_reduced_path}"
        )
        _load_place_of_work_raw_table(city_key, db_instance, [pow_reduced_path])
        logger.info("Loaded reduced place-of-work cache into DuckDB")
        results["census_place_of_work_raw"] = (
            db_instance.count(db_instance.table_name("census_place_of_work_raw", city_key)) or 0
        )
    else:
        logger.warning("Skipping place-of-work context: no Statistics Canada CSV source was available.")

    if not db_instance.relation_exists(db_instance.table_name("census_da", city_key)):
        logger.warning("Skipping jobs proxy build: census dissemination-area geometry is missing.")
        return results

    logger.info("Building jobs-proxy tables from normalized raw employment sources")
    build_jobs_proxy_tables(city_key, db_instance)
    logger.info("Built jobs-proxy tables")
    results["da_jobs_proxy"] = db_instance.count(db_instance.table_name("da_jobs_proxy", city_key)) or 0
    results["neighbourhood_jobs_proxy"] = (
        db_instance.count(db_instance.table_name("neighbourhood_jobs_proxy", city_key)) or 0
    )
    return results


def build_jobs_proxy_tables(city_key: str, db_instance: TransitDB) -> None:
    """Build dissemination-area and neighbourhood jobs-proxy tables."""
    raw_table_name = db_instance.table_name("da_jobs_proxy_raw", city_key)
    census_table_name = db_instance.table_name("census_da", city_key)
    da_proxy_table_name = db_instance.table_name("da_jobs_proxy", city_key)
    neighbourhoods_table_name = db_instance.table_name("neighbourhoods", city_key)
    neighbourhood_proxy_table_name = db_instance.table_name("neighbourhood_jobs_proxy", city_key)

    logger.info("Building dissemination-area jobs proxy table")
    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {da_proxy_table_name} AS
        WITH da_jobs AS (
            SELECT da_uid,
                   SUM(establishment_count) AS establishment_count,
                   SUM(classified_establishment_count) AS classified_establishment_count,
                   SUM(jobs_proxy_score) AS jobs_proxy_score,
                   SUM(large_employer_count) AS large_employer_count
            FROM {raw_table_name}
            GROUP BY da_uid
        )
        SELECT census.da_uid,
               TRY_CAST(census.population_total AS DOUBLE) AS population_total,
               COALESCE(da_jobs.establishment_count, 0) AS establishment_count,
               COALESCE(da_jobs.classified_establishment_count, 0) AS classified_establishment_count,
               COALESCE(da_jobs.jobs_proxy_score, 0) AS jobs_proxy_score,
               COALESCE(da_jobs.large_employer_count, 0) AS large_employer_count,
               census.geometry AS geometry
        FROM {census_table_name} census
        LEFT JOIN da_jobs
            ON census.da_uid = da_jobs.da_uid
        """
    )

    logger.info("Building neighbourhood jobs proxy table")
    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {neighbourhood_proxy_table_name} AS
        SELECT neighbourhoods.id AS neighbourhood_id,
               neighbourhoods.name AS neighbourhood,
               neighbourhoods.area_km2,
               SUM(COALESCE(da_jobs.jobs_proxy_score, 0)) AS jobs_proxy_score,
               SUM(COALESCE(da_jobs.establishment_count, 0)) AS establishment_count,
               SUM(COALESCE(da_jobs.large_employer_count, 0)) AS large_employer_count
        FROM {neighbourhoods_table_name} neighbourhoods
        LEFT JOIN {da_proxy_table_name} da_jobs
            ON ST_Intersects(neighbourhoods.geometry, da_jobs.geometry)
        GROUP BY neighbourhoods.id, neighbourhoods.name, neighbourhoods.area_km2
        """
    )


def build_jobs_access_tables(city_key: str, db_instance: TransitDB) -> None:
    """Build canonical jobs-access tables and comparison outputs."""
    neighbourhood_density_table_name = db_instance.table_name(
        "neighbourhood_stop_count_density",
        city_key,
    )
    community_density_table_name = db_instance.table_name(
        "community_area_stop_count_density",
        city_key,
    )
    neighbourhood_proxy_table_name = db_instance.table_name("neighbourhood_jobs_proxy", city_key)
    community_areas_table_name = db_instance.table_name("community_areas", city_key)
    da_proxy_table_name = db_instance.table_name("da_jobs_proxy", city_key)
    neighbourhood_access_table_name = db_instance.table_name(
        "neighbourhood_jobs_access_metrics",
        city_key,
    )
    community_access_table_name = db_instance.table_name(
        "community_area_jobs_access_metrics",
        city_key,
    )
    neighbourhood_comparison_table_name = db_instance.table_name(
        "neighbourhood_jobs_access_comparison_metrics",
        city_key,
    )

    if not db_instance.relation_exists(neighbourhood_density_table_name):
        raise ValueError(
            "Cannot build jobs access metrics before neighbourhood stop density exists."
        )

    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {neighbourhood_access_table_name} AS
        SELECT density.feed_id,
               density.neighbourhood_id,
               density.neighbourhood,
               density.area_km2,
               density.stop_count,
               density.stop_density_per_km2,
               COALESCE(proxy.jobs_proxy_score, 0) AS jobs_proxy_score,
               COALESCE(proxy.establishment_count, 0) AS establishment_count,
               COALESCE(proxy.large_employer_count, 0) AS large_employer_count,
               ROUND(LN(COALESCE(proxy.jobs_proxy_score, 0) + 1), 4) AS jobs_proxy_log,
               ROUND(
                   COALESCE(density.stop_density_per_km2, 0)
                   * LN(COALESCE(proxy.jobs_proxy_score, 0) + 1),
                   4
               ) AS jobs_access_score
        FROM {neighbourhood_density_table_name} density
        LEFT JOIN {neighbourhood_proxy_table_name} proxy
            ON density.neighbourhood_id = proxy.neighbourhood_id
        """
    )

    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {community_access_table_name} AS
        WITH community_jobs AS (
            SELECT communities.id AS community_area_id,
                   communities.name AS community_area,
                   communities.area_km2,
                   SUM(COALESCE(da_jobs.jobs_proxy_score, 0)) AS jobs_proxy_score,
                   SUM(COALESCE(da_jobs.establishment_count, 0)) AS establishment_count,
                   SUM(COALESCE(da_jobs.large_employer_count, 0)) AS large_employer_count
            FROM {community_areas_table_name} communities
            LEFT JOIN {da_proxy_table_name} da_jobs
                ON ST_Intersects(communities.geometry, da_jobs.geometry)
            GROUP BY communities.id, communities.name, communities.area_km2
        )
        SELECT density.feed_id,
               density.community_area_id,
               density.community_area,
               density.area_km2,
               density.stop_count,
               density.stop_density_per_km2,
               COALESCE(community_jobs.jobs_proxy_score, 0) AS jobs_proxy_score,
               COALESCE(community_jobs.establishment_count, 0) AS establishment_count,
               COALESCE(community_jobs.large_employer_count, 0) AS large_employer_count,
               ROUND(
                   COALESCE(density.stop_density_per_km2, 0)
                   * LN(COALESCE(community_jobs.jobs_proxy_score, 0) + 1),
                   4
               ) AS jobs_access_score
        FROM {community_density_table_name} density
        LEFT JOIN community_jobs
            ON density.community_area_id = community_jobs.community_area_id
        """
    )

    db_instance.execute(
        f"""
        CREATE OR REPLACE TABLE {neighbourhood_comparison_table_name} AS
        WITH feed_pairs AS (
            SELECT DISTINCT
                baseline.feed_id AS baseline_feed_id,
                comparison.feed_id AS comparison_feed_id
            FROM {neighbourhood_access_table_name} baseline
            CROSS JOIN {neighbourhood_access_table_name} comparison
            WHERE baseline.feed_id <> comparison.feed_id
        )
        SELECT
            feed_pairs.baseline_feed_id,
            feed_pairs.comparison_feed_id,
            comparison.neighbourhood_id,
            comparison.neighbourhood,
            comparison.jobs_proxy_score,
            comparison.establishment_count,
            comparison.large_employer_count,
            baseline.jobs_access_score AS baseline_jobs_access_score,
            comparison.jobs_access_score AS comparison_jobs_access_score,
            comparison.jobs_access_score - baseline.jobs_access_score AS jobs_access_change,
            baseline.stop_density_per_km2 AS baseline_stop_density_per_km2,
            comparison.stop_density_per_km2 AS comparison_stop_density_per_km2,
            comparison.stop_density_per_km2 - baseline.stop_density_per_km2 AS stop_density_change
        FROM feed_pairs
        JOIN {neighbourhood_access_table_name} baseline
            ON baseline.feed_id = feed_pairs.baseline_feed_id
        JOIN {neighbourhood_access_table_name} comparison
            ON comparison.feed_id = feed_pairs.comparison_feed_id
           AND comparison.neighbourhood_id = baseline.neighbourhood_id
        """
    )


def _load_cbp_raw_table(city_key: str, db_instance: TransitDB, source_path: Path) -> None:
    raw_table_name = db_instance.table_name("da_jobs_proxy_raw", city_key)
    invalid_codes_sql = ", ".join(sql_string(code) for code in sorted(INVALID_DA_CODES))
    source_sql = build_cbp_source_sql(source_path)
    db_instance.execute_native(
        f"""
        CREATE OR REPLACE TABLE {raw_table_name} AS
        WITH source_rows AS (
            SELECT
                LPAD(TRIM(column000), 8, '0') AS da_uid,
                COALESCE(NULLIF(TRIM(column001), ''), 'Unknown') AS employee_size_range,
                COALESCE(TRY_CAST(NULLIF(TRIM(column002), '') AS DOUBLE), 0.0) AS establishment_count,
                COALESCE(TRY_CAST(NULLIF(TRIM(column004), '') AS DOUBLE), 0.0) AS classified_establishment_count
            FROM ({source_sql})
        ),
        weighted_rows AS (
            SELECT
                da_uid,
                employee_size_range,
                establishment_count,
                classified_establishment_count,
                {employee_size_weight_case('employee_size_range')} AS employee_size_weight
            FROM source_rows
            WHERE da_uid NOT IN ({invalid_codes_sql})
        )
        SELECT
            da_uid,
            employee_size_range,
            employee_size_weight,
            establishment_count,
            classified_establishment_count,
            establishment_count * employee_size_weight AS jobs_proxy_score,
            {large_employer_case('employee_size_range', 'establishment_count')} AS large_employer_count
        FROM weighted_rows
        """
    )


def _load_place_of_work_raw_table(city_key: str, db_instance: TransitDB, source_paths: list[Path]) -> None:
    raw_table_name = db_instance.table_name("census_place_of_work_raw", city_key)
    union_queries = [place_of_work_select_sql(source_path) for source_path in source_paths]
    if not union_queries:
        return
    db_instance.execute_native(
        f"""
        CREATE OR REPLACE TABLE {raw_table_name} AS
        {" UNION ALL ".join(union_queries)}
        """
    )
