"""Data pipeline CLI.

Commands:
    gtfs       - Download and load GTFS data
    boundaries - Load neighbourhood/community boundaries
    open-data  - Load Winnipeg Open Data tables
    graph      - Build stop connection edges
    all        - Run full pipeline
    status     - Show pipeline status
"""

from __future__ import annotations


def _safe_count(table_name: str) -> int | None:
    """Return table count or None if table doesn't exist.

    Args:
        table_name: Table name to count.

    Returns:
        Row count if table exists, None otherwise.
    """
    from ptn_analysis.data.db import get_duckdb

    try:
        conn = get_duckdb()
        row = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
        return row[0] if row else 0
    except Exception:
        return None


def main() -> None:
    """CLI entry point - imports typer here to avoid import-time overhead."""
    import typer

    app = typer.Typer(help="Winnipeg PTN Analysis data pipeline.", no_args_is_help=True)

    @app.command()
    def gtfs() -> None:
        """Download and load GTFS data."""
        from ptn_analysis.data.ingest import download_gtfs, extract_gtfs, load_gtfs

        download_gtfs()
        extract_gtfs()
        results = load_gtfs()

        typer.echo("\nGTFS load complete!")
        for name, count in results.items():
            typer.echo(f"  {name}: {count:,}")

    @app.command()
    def boundaries() -> None:
        """Load neighbourhood and community boundary data."""
        from ptn_analysis.data.ingest import load_boundaries

        results = load_boundaries()

        typer.echo("\nBoundary loading complete!")
        for name, count in results.items():
            typer.echo(f"  {name}: {count:,}")

    @app.command("open-data")
    def open_data() -> None:
        """Load Winnipeg Open Data tables."""
        from ptn_analysis.data.ingest import load_all_open_data

        results = load_all_open_data()

        typer.echo("\nOpen Data loading complete!")
        for name, count in results.items():
            typer.echo(f"  {name}: {count:,}")

    @app.command()
    def graph() -> None:
        """Build stop connection graph edges."""
        from ptn_analysis.data.transform import build_stop_connections, build_weighted_connections

        build_stop_connections()
        build_weighted_connections()
        typer.echo("Graph build complete!")

    @app.command("all")
    def run_all() -> None:
        """Run full data pipeline."""
        from ptn_analysis.data.transform import (
            create_indexes,
            create_views,
            materialize_gtfs_metrics,
        )

        typer.echo("Running full data pipeline...\n")

        typer.echo("=== GTFS ===")
        gtfs()

        typer.echo("\n=== GTFS METRICS ===")
        metrics = materialize_gtfs_metrics()
        for name, count in metrics.items():
            typer.echo(f"  {name}: {count:,}")

        typer.echo("\n=== BOUNDARIES ===")
        boundaries()

        typer.echo("\n=== OPEN DATA ===")
        open_data()

        typer.echo("\n=== GRAPH ===")
        graph()

        typer.echo("\n=== VIEWS & INDEXES ===")
        create_views()
        create_indexes()

        typer.echo("\nPipeline complete!")

    @app.command()
    def status() -> None:
        """Show data pipeline status."""
        from ptn_analysis.config import GTFS_ZIP_PATH

        typer.echo("=== Data Pipeline Status ===")

        if GTFS_ZIP_PATH.exists():
            size_mb = GTFS_ZIP_PATH.stat().st_size / (1024 * 1024)
            typer.echo(f"\nGTFS ZIP: {GTFS_ZIP_PATH.name} ({size_mb:.1f} MB)")
        else:
            typer.echo("\nGTFS ZIP: Not downloaded")

        tables = [
            ("GTFS", [("stops", "Stops"), ("routes", "Routes"), ("trips", "Trips")]),
            (
                "GTFS Metrics",
                [("gtfs_route_stats", "Route stats"), ("gtfs_stop_stats", "Stop stats")],
            ),
            (
                "Boundaries",
                [("neighbourhoods", "Neighbourhoods"), ("community_areas", "Communities")],
            ),
            (
                "Network",
                [("stop_connections", "Edges"), ("stop_connections_weighted", "Weighted")],
            ),
            (
                "Open Data",
                [
                    ("passups", "Pass-ups"),
                    ("ontime_performance", "On-time"),
                    ("passenger_counts", "Passenger counts"),
                    ("cycling_paths", "Cycling paths"),
                    ("walkways", "Walkways"),
                ],
            ),
        ]

        for header, table_list in tables:
            typer.echo(f"\n{header}:")
            for table_name, label in table_list:
                count = _safe_count(table_name)
                typer.echo(
                    f"  {label}: {count:,}" if count is not None else f"  {label}: Not loaded"
                )

    app()


if __name__ == "__main__":
    main()
