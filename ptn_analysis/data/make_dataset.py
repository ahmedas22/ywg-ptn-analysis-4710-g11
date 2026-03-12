"""Typer CLI entry point for the data pipeline."""

from __future__ import annotations

from rich.console import Console
import typer

from ptn_analysis.data.pipeline import DatasetPipeline

console = Console()
app = typer.Typer(help="Winnipeg PTN Analysis data pipeline.", no_args_is_help=True)
history_app = typer.Typer(help="Historical GTFS management.", no_args_is_help=True)
app.add_typer(history_app, name="historical")


def get_pipeline() -> DatasetPipeline:
    """Build the default dataset pipeline.

    Returns:
        Dataset pipeline instance.
    """
    from ptn_analysis.context import TransitContext

    return TransitContext.from_defaults().pipeline()


def _print_results(results: dict[str, int]) -> None:
    """Render one pipeline result mapping to the console.

    Args:
        results: Mapping of step names to row counts.
    """
    for name, count in results.items():
        console.print(f"[green]{name}[/]: {count:,}")


def _run_pipeline_step(method_name: str, *args, **kwargs):
    """Build the default pipeline and run one named method.

    Args:
        method_name: DatasetPipeline method name.
        *args: Positional arguments passed to the method.
        **kwargs: Keyword arguments passed to the method.

    Returns:
        The method result.
    """
    pipeline = get_pipeline()
    method = getattr(pipeline, method_name)
    return method(*args, **kwargs)


@app.command()
def gtfs() -> None:
    """Download and load current GTFS data."""
    _print_results(_run_pipeline_step("refresh_gtfs"))


@app.command()
def boundaries() -> None:
    """Load boundary layers."""
    _print_results(_run_pipeline_step("refresh_boundaries"))


@app.command("open-data")
def open_data_command() -> None:
    """Load configured open-data datasets."""
    _print_results(_run_pipeline_step("refresh_open_data"))


@app.command("live-transit")
def live_transit_command(
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cached raw JSON files."),
) -> None:
    """Refresh Winnipeg Transit API v4 tables."""
    _print_results(_run_pipeline_step("refresh_live_transit", force_refresh=refresh))


@app.command("live-bootstrap")
def live_bootstrap_command(
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cached raw JSON files."),
) -> None:
    """Build the wide cached Winnipeg Transit metadata layer."""
    _print_results(_run_pipeline_step("refresh_live_transit_bootstrap", force_refresh=refresh))


@app.command("live-snapshots")
def live_snapshots_command(
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cached raw JSON files."),
) -> None:
    """Refresh bounded current Winnipeg Transit validation snapshots."""
    _print_results(_run_pipeline_step("refresh_live_transit_snapshots", force_refresh=refresh))


@app.command()
def employment(
    refresh: bool = typer.Option(False, "--refresh", help="Re-download or re-copy raw employment files."),
) -> None:
    """Load jobs-proxy and place-of-work context tables."""
    _print_results(_run_pipeline_step("refresh_employment", force_refresh=refresh))


@app.command()
def graph() -> None:
    """Build connection tables and views."""
    _print_results(_run_pipeline_step("build_derived_tables"))


@app.command()
def service(target_date: str) -> None:
    """Materialize active trips for one service date.

    Args:
        target_date: Service date in ``YYYY-MM-DD`` format.
    """
    _run_pipeline_step("build_service_table", target_date)
    console.print(f"[green]Daily service materialized for {target_date}[/]")


@app.command()
def census() -> None:
    """Load CHASS Census Profile dissemination areas."""
    from ptn_analysis.data.sources.census import load_dissemination_areas

    pipeline = get_pipeline()
    _print_results(load_dissemination_areas(pipeline.city_key, pipeline.db))


@app.command()
def exports() -> None:
    """Export analysis-ready datasets."""
    _print_results(_run_pipeline_step("export_outputs"))


@app.command("all")
def run_all(
    refresh: bool = typer.Option(False, "--refresh", help="Bypass cached raw API files."),
) -> None:
    """Run the full data pipeline."""
    _run_pipeline_step("run_full_refresh", force_refresh=refresh)


@app.command()
def status() -> None:
    """Show pipeline status and key row counts."""
    console.print(_run_pipeline_step("render_status_table"))


@app.command()
def validate() -> None:
    """Run data quality checks independently."""
    pipeline = get_pipeline()
    dq_results = pipeline.run_data_quality_checks()
    pipeline._render_dq_table(dq_results)


@history_app.command("list")
def history_list() -> None:
    """List available historical GTFS archives."""
    pipeline = get_pipeline()
    archive_dates = pipeline.gtfs.available_archives()
    console.print(f"Found [bold]{len(archive_dates)}[/] archives:")
    for archive_date in archive_dates:
        console.print(f"  {archive_date}")


@history_app.command("pre-ptn")
def history_pre_ptn() -> None:
    """Load pre-PTN GTFS archives and metrics."""
    results = _run_pipeline_step("load_pre_ptn_archives")
    if results is None:
        console.print("[red]No pre-PTN archives found[/]")
        raise typer.Exit(1)
    _print_results(results)


@history_app.command("post-ptn")
def history_post_ptn() -> None:
    """Load post-PTN GTFS archives and metrics."""
    _print_results(_run_pipeline_step("load_post_ptn_archives"))


@history_app.command("load")
def history_load(archive_date: str) -> None:
    """Load one specific historical GTFS archive.

    Args:
        archive_date: Archive date in ``YYYY-MM-DD`` format.
    """
    pipeline = get_pipeline()
    feed = pipeline.gtfs.read_feed(pipeline.gtfs.download_archive(archive_date))
    raw_results = pipeline.gtfs.load_archive(archive_date, archive_date, feed)
    metric_results = pipeline.build_derived_tables()
    for name, count in {**raw_results, **metric_results}.items():
        console.print(f"[green]{name}[/]: {count:,}")


def main() -> None:
    """Run the Typer CLI.

    Returns:
        None.
    """
    app()


if __name__ == "__main__":
    main()
