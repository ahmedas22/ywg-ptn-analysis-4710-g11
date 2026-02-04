"""Data pipeline CLI."""

from typing import Callable

from loguru import logger
import typer

from ptn_analysis.config import GTFS_ZIP_PATH, TRANSITLAND_API_KEY
from ptn_analysis.data import BOUNDARY_TABLES, GTFS_TABLES
from ptn_analysis.data.db import get_duckdb
import ptn_analysis.data.ingest_gtfs as ingest_gtfs
import ptn_analysis.data.ingest_open_data as ingest_open_data
import ptn_analysis.data.transform as transform

app = typer.Typer(help="Winnipeg PTN Analysis data pipeline.", no_args_is_help=True)


def _safe_count(table_name: str) -> int | None:
    """Return table count or None if unavailable.

    Args:
        table_name: Fully-qualified physical table name.

    Returns:
        Table row count when available, otherwise None.
    """
    try:
        conn = get_duckdb()
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return row[0] if row else 0
    except Exception:
        return None


def _print_named_counts(header: str, rows: list[tuple[str, str]]) -> None:
    """Print table counts with user-friendly labels.

    Args:
        header: Section header label.
        rows: List of (table_name, display_label) pairs.
    """
    typer.echo(f"\n{header}:")
    for table_name, label in rows:
        count = _safe_count(table_name)
        if count is None:
            typer.echo(f"  {label}: Not loaded")
        else:
            typer.echo(f"  {label}: {count:,}")


def _print_pipeline_row_summary(prefixes: tuple[str, ...] = ("raw_", "agg_", "ref_")) -> None:
    """Print row counts for pipeline tables and overall totals.

    Args:
        prefixes: Table-name prefixes to include in the summary.
    """
    conn = get_duckdb()
    conditions = " OR ".join([f"table_name LIKE '{prefix}%'" for prefix in prefixes])
    tables = conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND ({conditions})
        ORDER BY table_name
        """
    ).fetchall()

    if not tables:
        typer.echo("\nNo pipeline tables found for row-count summary.")
        return

    typer.echo("\n=== TABLE ROW COUNTS ===")
    grand_total = 0
    for (table_name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        grand_total += count
        typer.echo(f"  {table_name}: {count:,}")

    typer.echo(f"\n  Total rows across {len(tables)} tables: {grand_total:,}")


def _run_historical_load(period: str, resolver, suffix: str, label: str) -> None:
    """Resolve versions and load one historical period.

    Args:
        period: Period key used in log messages.
        resolver: Callable returning feed versions for a period.
        suffix: Table suffix to use when loading historical GTFS.
        label: Human-friendly period label.
    """
    typer.echo(f"Loading {label} GTFS data...")
    try:
        versions = resolver()
    except ValueError as error:
        typer.echo(f"Error: {error}", err=True)
        raise typer.Exit(1)

    if not versions:
        typer.echo(f"No {period} feed versions found")
        raise typer.Exit(1)

    selected = versions[0]
    typer.echo(f"Selected: {selected.earliest_service_date} to {selected.latest_service_date}")

    results = ingest_gtfs.load_historical_gtfs_to_schema(selected, suffix)
    typer.echo(f"\n{label} GTFS loaded:")
    for table_name, row_count in results.items():
        typer.echo(f"  {table_name}: {row_count:,}")


@app.command()
def gtfs() -> None:
    """Download and load core GTFS tables."""
    ingest_gtfs.download_gtfs()
    ingest_gtfs.extract_gtfs()

    results: dict[str, int] = {}
    for config in GTFS_TABLES:
        try:
            results[config.log_name] = ingest_gtfs.load_gtfs_table(config)
        except FileNotFoundError as error:
            logger.warning(f"Skipping {config.filename}: {error}")
            results[config.log_name] = 0

    typer.echo("\nGTFS load complete!")
    for name, count in results.items():
        typer.echo(f"  {name.replace('_', ' ').title()}: {count:,}")


@app.command()
def boundaries() -> None:
    """Load boundary datasets and coverage aggregates."""
    results = {
        config.log_name: ingest_open_data.load_boundary_table(config) for config in BOUNDARY_TABLES
    }
    transform.create_coverage_aggs()

    typer.echo("\nBoundary loading complete!")
    for name, count in results.items():
        typer.echo(f"  {name.title()}: {count}")
    typer.echo("  Coverage aggregations: created")


@app.command("open-data")
def open_data(limit: int = typer.Option(None, help="Maximum records per dataset")) -> None:
    """Load tabular Open Data datasets and route summaries."""
    typer.echo("Loading Winnipeg Open Data datasets...")
    results = ingest_open_data.load_standard_open_data_tables(limit=limit)
    transform.create_route_summary_aggs()

    typer.echo("\nWinnipeg Open Data loading complete!")
    typer.echo(f"  Pass-ups: {results['pass_ups']:,}")
    typer.echo(f"  On-time: {results['on_time']:,}")
    typer.echo(f"  Passenger counts: {results['passenger_counts']:,}")
    typer.echo("  Route summaries: created")


@app.command("active-mobility")
def active_mobility() -> None:
    """Load cycling and walkway datasets."""
    typer.echo("Loading active mobility datasets...")
    results = ingest_open_data.load_active_mobility_datasets()

    typer.echo("\nActive mobility loading complete!")
    typer.echo(f"  Cycling network features: {results['cycling']:,}")
    typer.echo(f"  Walkway features: {results['walkways']:,}")


@app.command()
def graph() -> None:
    """Build graph edge tables."""
    edges = transform.build_edges_table()
    weighted = transform.create_aggregated_edges()

    typer.echo("\nGraph build complete!")
    typer.echo(f"  Raw edges: {edges:,}")
    typer.echo(f"  Weighted edges: {weighted:,}")


@app.command()
def service(date: str) -> None:
    """Materialize active trips for date (YYYY-MM-DD)."""
    count = transform.materialize_active_trips(date)
    typer.echo(f"Created agg_active_trips with {count:,} trips for {date}")


@app.command("historical")
def historical(
    period: str = typer.Argument(
        ..., help="Period: 'pre-ptn', 'post-ptn', 'transition', or 'list'"
    ),
) -> None:
    """Load historical GTFS data for PTN comparisons."""
    if period == "list":
        typer.echo("Fetching available GTFS feed versions from Transitland...\n")
        try:
            versions = ingest_gtfs.fetch_transitland_feed_versions(limit=20)
        except ValueError as error:
            typer.echo(f"Error: {error}", err=True)
            raise typer.Exit(1)

        typer.echo(f"{'Service Period':<30} {'Fetched At':<20} {'Type':<10}")
        typer.echo("-" * 60)
        for version in versions:
            period_str = f"{version.earliest_service_date} to {version.latest_service_date}"
            fetched_str = version.fetched_at[:10] if version.fetched_at else "unknown"
            version_type = "pre-PTN" if version.is_pre_ptn else "post-PTN"
            typer.echo(f"{period_str:<30} {fetched_str:<20} {version_type:<10}")
        return

    loaders = {
        "pre-ptn": (ingest_gtfs.get_pre_ptn_feed_versions, "pre_ptn", "Pre-PTN"),
        "post-ptn": (ingest_gtfs.get_post_ptn_feed_versions, "post_ptn", "Post-PTN"),
        "transition": (ingest_gtfs.get_ptn_transition_feed_versions, "transition", "Transition"),
    }
    entry = loaders.get(period)
    if entry is None:
        typer.echo(f"Unknown period: {period}. Use 'pre-ptn', 'post-ptn', 'transition', or 'list'")
        raise typer.Exit(1)

    resolver, suffix, label = entry
    _run_historical_load(period, resolver, suffix, label)


def _parse_filter_values(values: list[str] | None) -> set[str] | None:
    """Normalize repeated CLI filter values.

    Args:
        values: Optional list of repeated filter values.

    Returns:
        Normalized lowercase token set, or None when not provided.
    """
    if not values:
        return None
    return {value.strip().lower() for value in values if value.strip()}


def _load_open_data_with_filters(
    include: set[str] | None = None,
    exclude: set[str] | None = None,
    limit: int | None = None,
) -> dict[str, int]:
    """Load tabular Open Data with optional dataset filters.

    Args:
        include: Optional include tokens (dataset key/id).
        exclude: Optional exclude tokens (dataset key/id).
        limit: Optional row limit for tabular datasets.

    Returns:
        Loaded row counts by logical dataset key.
    """
    results = ingest_open_data.load_standard_open_data_tables(
        limit=limit, include=include, exclude=exclude
    )
    transform.create_route_summary_aggs()
    return results


def _load_active_mobility_with_filters(
    include: set[str] | None = None,
    exclude: set[str] | None = None,
) -> dict[str, int]:
    """Load active mobility datasets with optional filters.

    Args:
        include: Optional include tokens (dataset key/id).
        exclude: Optional exclude tokens (dataset key/id).

    Returns:
        Loaded row counts by logical dataset key.
    """
    return ingest_open_data.load_active_mobility_datasets(include=include, exclude=exclude)


@app.command("all")
def run_all(
    skip_section: list[str] = typer.Option(
        None,
        "--skip-section",
        help="Skip section(s): gtfs, boundaries, open-data, active-mobility, graph, historical, finalize",
    ),
    include_dataset: list[str] = typer.Option(
        None,
        "--include-dataset",
        help="Include only specific Open Data dataset key/id (repeatable).",
    ),
    exclude_dataset: list[str] = typer.Option(
        None,
        "--exclude-dataset",
        help="Exclude specific Open Data dataset key/id (repeatable).",
    ),
    include_historical: bool = typer.Option(
        True,
        "--include-historical/--no-historical",
        help="Include historical GTFS load in comprehensive pipeline run.",
    ),
    historical_period: str = typer.Option(
        "pre-ptn",
        help="Historical period to load during comprehensive run: pre-ptn, post-ptn, transition.",
    ),
) -> None:
    """Run full pipeline end-to-end with optional filtering.

    Args:
        skip_section: Section names to skip.
        include_dataset: Dataset key/id include filters for Open Data loaders.
        exclude_dataset: Dataset key/id exclude filters for Open Data loaders.
        include_historical: Whether to include historical GTFS load.
        historical_period: Historical period key used when historical load is enabled.
    """
    typer.echo("Running full data pipeline...")
    skipped = _parse_filter_values(skip_section) or set()
    include = _parse_filter_values(include_dataset)
    exclude = _parse_filter_values(exclude_dataset)

    def run_open_data_section() -> None:
        """Run filtered Open Data loading section."""
        _load_open_data_with_filters(include=include, exclude=exclude, limit=None)

    def run_active_mobility_section() -> None:
        """Run filtered active-mobility loading section."""
        _load_active_mobility_with_filters(include=include, exclude=exclude)

    sections: list[tuple[str, str, Callable[[], None]]] = [
        ("gtfs", "GTFS", gtfs),
        ("boundaries", "BOUNDARIES", boundaries),
        ("open-data", "OPEN DATA", run_open_data_section),
        ("active-mobility", "ACTIVE MOBILITY", run_active_mobility_section),
        ("graph", "GRAPH", graph),
    ]

    for key, section, fn in sections:
        if key in skipped:
            typer.echo(f"\n=== {section} (SKIPPED) ===")
            continue
        typer.echo(f"\n=== {section} ===")
        fn()

    if include_historical and "historical" not in skipped:
        typer.echo("\n=== HISTORICAL GTFS ===")
        if not TRANSITLAND_API_KEY:
            typer.echo("Historical GTFS skipped (TRANSITLAND_API_KEY is not configured).")
        else:
            try:
                historical(period=historical_period)
            except typer.Exit:
                typer.echo("Historical GTFS skipped (unavailable versions for selected period).")

    if "finalize" not in skipped:
        typer.echo("\n=== REFERENCES & INDEXES ===")
        transform.create_reference_tables()
        transform.create_database_indexes()
        typer.echo("\n=== PERFORMANCE VIEWS ===")
        transform.create_performance_views()

    _print_pipeline_row_summary()
    typer.echo("\nPipeline complete!")


@app.command()
def status() -> None:
    """Show data pipeline status summary."""
    typer.echo("=== Data Pipeline Status ===\n")

    typer.echo("Ingest Status:")
    if GTFS_ZIP_PATH.exists():
        size_mb = GTFS_ZIP_PATH.stat().st_size / (1024 * 1024)
        typer.echo(f"  GTFS ZIP: {GTFS_ZIP_PATH} ({size_mb:.1f} MB)")
    else:
        typer.echo("  GTFS ZIP: Not downloaded")

    _print_named_counts(
        "GTFS Tables",
        [
            ("raw_gtfs_stops", "Stops"),
            ("raw_gtfs_routes", "Routes"),
            ("raw_gtfs_trips", "Trips"),
            ("raw_gtfs_stop_times", "Stop times"),
            ("raw_gtfs_calendar", "Calendar"),
            ("raw_gtfs_shapes", "Shapes"),
        ],
    )

    _print_named_counts(
        "Boundary Tables",
        [
            ("raw_neighbourhoods", "Neighbourhoods"),
            ("raw_community_areas", "Community areas"),
        ],
    )

    _print_named_counts(
        "Transform Status",
        [
            ("raw_gtfs_edges", "Raw edges"),
            ("raw_gtfs_edges_weighted", "Weighted edges"),
            ("agg_active_trips", "Active trips"),
        ],
    )

    _print_named_counts(
        "Coverage Aggregations",
        [
            ("agg_stops_per_neighbourhood", "Stops per neighbourhood"),
            ("agg_stops_per_community", "Stops per community"),
        ],
    )

    _print_named_counts(
        "Open Data Tables",
        [
            ("raw_open_data_pass_ups", "Pass-ups"),
            ("raw_open_data_on_time", "On-time"),
            ("raw_open_data_passenger_counts", "Passenger counts"),
        ],
    )


if __name__ == "__main__":
    app()
