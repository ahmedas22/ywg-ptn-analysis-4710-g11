"""Dataset orchestration for the full data pipeline."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import re
import time
import warnings

from loguru import logger
import pandas as pd
from pandas.errors import PerformanceWarning
from rich import box
from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TaskProgressColumn,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from ptn_analysis.context.config import (
    DATA_DIR,
    FEED_ID_CURRENT,
    H3_RESOLUTION,
    PTN_LAUNCH_DATE,
    SERVICE_DAY_END,
    SERVICE_DAY_START,
    WINNIPEG_TRANSIT_API_KEY,
    normalize_gtfs_date,
)
from ptn_analysis.context.db import TransitDB
from ptn_analysis.context.exports import (
    export_flat_files,
    export_serving_duckdb,
    render_storage_rows,
    status_relation_names,
)
from ptn_analysis.data.live_transit import (
    bootstrap_missing,
    refresh_live_transit_bootstrap,
    refresh_live_transit_snapshots,
)
from ptn_analysis.data.sources import employment as employment_mod
from ptn_analysis.data.sources import gtfs as gtfs_mod
from ptn_analysis.data.sources import open_data as open_data_mod
from ptn_analysis.data.sources import transit_api
from ptn_analysis.data.sources.census import load_dissemination_areas

console = Console()
_SAFE_SQL_VALUE_RE = re.compile(r"^[a-zA-Z0-9_.:-]+$")


class DatasetPipeline:
    """Orchestrate data refresh, reference loading, exports, and status checks.

    Args:
        city_key: City namespace.
    """

    def __init__(self, city_key: str, working_db: TransitDB, serving_db: TransitDB) -> None:
        """Initialize the pipeline.

        Args:
            city_key: City namespace.
            working_db: Working TransitDB (interim analytics database).
            serving_db: Serving TransitDB (curated dashboard database).
        """
        self.city_key = city_key
        self.db = working_db
        self.serving_db = serving_db
        self.transit = transit_api.create_source(city_key=city_key)
        self._progress = None
        self._progress_task_id = None

    def refresh_gtfs(self) -> dict[str, int]:
        """Download and load current GTFS data.

        Returns:
            Row counts keyed by table name.
        """
        self._run_sql_script("schema.sql")
        gtfs_mod.resolve_and_download("current", self.city_key)
        return gtfs_mod.load_current(self.city_key, self.db)

    def refresh_boundaries(self) -> dict[str, int]:
        """Load boundary layers.

        Returns:
            Row counts keyed by table name.
        """
        return open_data_mod.load_boundaries(self.city_key, self.db)

    def refresh_open_data(self) -> dict[str, int]:
        """Load open-data datasets with parallel download and progress bars.

        Returns:
            Row counts keyed by table name.
        """
        from ptn_analysis.data.sources.open_data import (
            _load_prepared_dataset,
            _prepare_dataset_cache,
            _SourceContext,
            get_config,
        )

        ctx = _SourceContext(self.city_key)
        datasets = get_config(self.city_key)["datasets"]

        with Progress(
            SpinnerColumn("dots"),
            TextColumn("{task.description}", style="cyan"),
            BarColumn(bar_width=20, pulse_style="cyan"),
            TaskProgressColumn(),
            TextColumn("[dim]{task.fields[status]}"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            # Create a task row per dataset
            task_ids: dict[str, int] = {}
            for ds in datasets:
                name = ds["base_table_name"]
                task_ids[name] = progress.add_task(
                    f"    {name}", total=None, status="pending"
                )

            # Phase 1: download caches in parallel
            prepared_caches: dict[str, Path] = {}

            def _download(ds):
                name = ds["base_table_name"]
                progress.update(task_ids[name], status="downloading...")
                try:
                    cache_path = _prepare_dataset_cache(ctx, ds)
                except Exception as exc:
                    progress.update(task_ids[name], status=f"[red]error: {exc!s:.40}")
                    logger.error(f"Open data download '{name}' failed: {exc}")
                    return name, None
                progress.update(task_ids[name], status="cached")
                return name, cache_path

            try:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures = [pool.submit(_download, ds) for ds in datasets]
                    for future in as_completed(futures):
                        name, path = future.result()
                        if path is not None:
                            prepared_caches[name] = path
            except KeyboardInterrupt:
                console.print("\n    [red]Interrupted[/]")
                raise

            # Phase 2: load into DuckDB sequentially
            results: dict[str, int] = {}
            for ds in datasets:
                name = ds["base_table_name"]
                if name not in prepared_caches:
                    results[name] = 0
                    progress.update(task_ids[name], visible=False)
                    continue
                progress.update(task_ids[name], status="loading into DB...")
                cache_path = prepared_caches[name]
                count = _load_prepared_dataset(ctx, self.db, ds, cache_path)
                results[name] = count
                progress.update(
                    task_ids[name],
                    completed=1, total=1,
                    status=f"[green]{count:,} rows",
                )

            # Hide all completed tasks before exiting so transient cleanup is clean
            for tid in task_ids.values():
                progress.update(tid, visible=False)

        return results

    def refresh_employment(self, force_refresh: bool = False) -> dict[str, int]:
        """Load jobs-proxy and place-of-work context data.

        Args:
            force_refresh: Whether to bypass cached raw employment files.

        Returns:
            Row counts keyed by logical table name.
        """
        return employment_mod.load(self.city_key, self.db, force_refresh=force_refresh)

    def refresh_live_transit_bootstrap(self, force_refresh: bool = False) -> dict[str, int]:
        """Build the wide cached live-transit metadata layer.

        Args:
            force_refresh: Whether to bypass raw JSON cache files.

        Returns:
            Row counts keyed by logical table name, or empty dict if API key is unset.
        """
        if not WINNIPEG_TRANSIT_API_KEY:
            logger.warning(
                "WINNIPEG_TRANSIT_API_KEY not set — skipping live transit bootstrap. "
                "Set the key in .env to enable live transit features."
            )
            return {}
        return refresh_live_transit_bootstrap(
            db_instance=self.db,
            transit_source=self.transit,
            city_key=self.city_key,
            force_refresh=force_refresh,
        )

    def refresh_live_transit_snapshots(self, force_refresh: bool = False) -> dict[str, int]:
        """Refresh bounded current-state live-transit snapshots.

        Args:
            force_refresh: Whether to bypass raw JSON cache files.

        Returns:
            Row counts keyed by logical table name.
        """
        return refresh_live_transit_snapshots(
            db_instance=self.db,
            transit_source=self.transit,
            city_key=self.city_key,
            force_refresh=force_refresh,
        )

    def refresh_live_transit(self, force_refresh: bool = False) -> dict[str, int]:
        """Refresh cached live-transit metadata and sampled snapshots.

        Args:
            force_refresh: Whether to bypass raw JSON cache files.

        Returns:
            Row counts keyed by logical table name.
        """
        results: dict[str, int] = {}
        if force_refresh or bootstrap_missing(self.db, self.city_key):
            self.update_progress("bootstrap: fetching routes...")
            results.update(self.refresh_live_transit_bootstrap(force_refresh=force_refresh))
        self.update_progress("snapshots: fetching live data...")
        results.update(self.refresh_live_transit_snapshots(force_refresh=force_refresh))
        return results

    def _build_network_tables(self) -> None:
        """Materialize NetworkAnalyzer export tables into the working database.

        Called by build_derived_tables() after GTFS metrics are ready.
        Dashboard requires network_metrics and top_hubs in the serving DB.
        Degrades gracefully if networkx is unavailable or graph build fails.

        Args:
            None.

        Returns:
            None.
        """
        # NOTE: Intentional cross-layer lazy import. NetworkAnalyzer depends on
        # optional graph tooling, so this remains deferred until build time.
        # The split try/except blocks handle both missing imports and runtime
        # graph build failures without breaking the rest of the data pipeline.
        try:
            from ptn_analysis.analysis.network import NetworkAnalyzer
        except ImportError as exc:
            logger.warning(f"Skipping network tables: networkx not installed ({exc})")
            return

        logger.info("Building network analysis tables")
        try:
            na = NetworkAnalyzer(self.city_key, FEED_ID_CURRENT, self.db)

            current_metrics = na.build_network_metrics_table()
            if not current_metrics.empty:
                self.db.load_table(
                    self.db.table_name("network_metrics", self.city_key),
                    current_metrics,
                    mode="replace",
                )

            exports = na.build_network_export_tables(
                baseline_feed_id="avg_pre_ptn", top_n=20
            )
            key_to_table = {
                "network_metrics_prepost": "network_comparison_metrics",
                "top_hubs_current": "top_hubs",
                "hub_transfer_burden": "transfer_burden_matrix",
                "network_communities_current": "network_communities",
            }
            for export_key, table_base in key_to_table.items():
                frame = exports.get(export_key)
                if frame is not None and not frame.empty:
                    self.db.load_table(
                        self.db.table_name(table_base, self.city_key),
                        frame,
                        mode="replace",
                    )
        except Exception as exc:
            logger.warning(
                f"Network table build failed — dashboard network tab will be empty. "
                f"Cause: {type(exc).__name__}: {exc}"
            )

    def build_derived_tables(self) -> dict[str, int]:
        """Build metrics, graph tables, views, and reference-backed outputs.

        Returns:
            Row counts keyed by logical output name.
        """
        results: dict[str, int] = {}
        self.update_progress("route & stop metrics...")
        results.update(
            self._transform_route_and_stop_metrics(
                gtfs_mod.read_feed(),
                FEED_ID_CURRENT,
            )
        )
        self.update_progress("feed registry...")
        self.build_feed_registry()
        self.update_progress("connections...")
        self._transform_connections()
        self.update_progress("views...")
        self._transform_views()
        self.update_progress("jobs access...")
        da_jobs_proxy_table = self.db.table_name("da_jobs_proxy", self.city_key)
        if self.db.relation_exists(da_jobs_proxy_table):
            employment_mod.build_jobs_access_tables(self.city_key, self.db)
        self.update_progress("era aggregates...")
        self._build_era_aggregates()
        self.update_progress("H3 hexagons...")
        self.build_h3_metrics()
        self.update_progress("network graph...")
        self._build_network_tables()
        results["network_metrics"] = (
            self.db.count(self.db.table_name("network_metrics", self.city_key)) or 0
        )
        results["stop_connection_counts"] = (
            self.db.count(self.db.table_name("stop_connection_counts", self.city_key)) or 0
        )
        results["neighbourhood_jobs_access_metrics"] = (
            self.db.count(self.db.table_name("neighbourhood_jobs_access_metrics", self.city_key)) or 0
        )
        results["community_area_jobs_access_metrics"] = (
            self.db.count(self.db.table_name("community_area_jobs_access_metrics", self.city_key)) or 0
        )
        return results

    def load_historical_feeds(self, era: str | None = None) -> dict[str, int]:
        """Load historical GTFS archive feeds into raw and metrics tables.

        Args:
            era: ``"pre_ptn"``, ``"post_ptn"``, or None for both.

        Returns:
            Route-stat counts keyed by archive date.
        """
        results: dict[str, int] = {}

        # Use manifest entries instead of HTML-scraped archives
        manifest_entries = gtfs_mod.manifest_feeds(self.city_key, era=era)
        if not manifest_entries:
            # Fall back to legacy archive discovery
            logger.warning("No manifest entries found, using legacy archive discovery")
            from ptn_analysis.context.config import PRE_PTN_ARCHIVE_COUNT
            all_dates = gtfs_mod.available_archives()
            if era == "pre_ptn":
                dates = [d for d in all_dates if gtfs_mod.is_pre_ptn(d)][:PRE_PTN_ARCHIVE_COUNT]
            elif era == "post_ptn":
                dates = [d for d in all_dates if d >= PTN_LAUNCH_DATE]
            else:
                dates = all_dates
            manifest_entries = [{"snapshot_id": d} for d in dates]

        # Filter out "current" — that's handled by refresh_gtfs()
        manifest_entries = [e for e in manifest_entries if e["snapshot_id"] != "current"]

        for i, entry in enumerate(manifest_entries, 1):
            sid = entry["snapshot_id"]
            self.update_progress(f"feed {i}/{len(manifest_entries)}: {sid}")
            try:
                zip_path = gtfs_mod.resolve_and_download(sid, self.city_key)
                feed = gtfs_mod.read_feed(zip_path)
                gtfs_mod.load_archive(self.city_key, self.db, sid, sid, feed)
                metric_results = self._transform_route_and_stop_metrics(feed, sid)
                results[sid] = metric_results.get("gtfs_route_stats", 0)
            except Exception as exc:
                logger.warning(f"Failed to load feed {sid}: {exc}")
                results[sid] = 0

        if era in (None, "pre_ptn"):
            self.build_feed_registry()
        return results

    def load_pre_ptn_archives(self) -> dict[str, int]:
        """Load pre-PTN feeds from manifest."""
        return self.load_historical_feeds(era="pre_ptn")

    def load_post_ptn_archives(self) -> dict[str, int]:
        """Load post-PTN feeds from manifest."""
        return self.load_historical_feeds(era="post_ptn")

    def _current_feed_start_date(self) -> str | None:
        """Read the start date of the current GTFS feed from feed_info."""
        table = self.db.table_name("feed_info", self.city_key)
        if not self.db.relation_exists(table):
            return None
        row = self.db.first(
            f"SELECT feed_start_date FROM {table} WHERE feed_id = :fid",
            {"fid": FEED_ID_CURRENT},
        )
        if row is None:
            return None
        return normalize_gtfs_date(str(row))


    def build_service_table(self, target_date: str) -> None:
        """Materialize daily service rows for one date.

        Args:
            target_date: Service date in ``YYYY-MM-DD`` format.
        """
        self._load_daily_service(gtfs_mod.read_feed(), target_date)

    def export_outputs(self, export_dir: Path | None = None) -> dict[str, int]:
        """Export analysis-ready flat files and the serving DuckDB package.

        Args:
            export_dir: Optional export directory.

        Returns:
            Export counts keyed by logical dataset name.
        """
        if export_dir is None:
            export_dir = DATA_DIR / "exports"
        export_dir.mkdir(parents=True, exist_ok=True)
        results = {}
        results.update(export_flat_files(db_instance=self.db, export_dir=export_dir, city_key=self.city_key))
        results.update(
            export_serving_duckdb(
                db_instance=self.db,
                serving_db=self.serving_db,
                city_key=self.city_key,
            )
        )
        return results

    def status(self) -> dict[str, int | None]:
        """Return row counts for key pipeline tables.

        Returns:
            Row counts keyed by physical table name.
        """
        table_names = status_relation_names(self.db, self.city_key)
        results: dict[str, int | None] = {}
        for table_name in table_names:
            results[table_name] = self.db.count(table_name)
        return results

    def render_status_table(self) -> Table:
        """Render a Rich status table.

        Returns:
            Rich table showing key row counts.
        """
        table = Table(title="Data Pipeline Status", show_header=True, header_style="bold")
        table.add_column("Table", style="cyan")
        table.add_column("Rows", justify="right")
        for table_name, row_count in self.status().items():
            if row_count is None:
                table.add_row(table_name, "Not loaded")
            else:
                table.add_row(table_name, f"{row_count:,}")
        for row_index, (label, value) in enumerate(render_storage_rows(self.db, self.serving_db)):
            if row_index == 0:
                table.add_section()
            table.add_row(label, value)
        return table

    def run_full_refresh(self, force_refresh: bool = False) -> None:
        """Run the full PR2 data pipeline in three phases.

        Phase A: Ingest — download and load all data sources (threaded I/O).
        Phase B: Compute — build derived tables, accessibility, live transit, H3.
        Phase C: Export — flat files and serving DB.

        Args:
            force_refresh: Whether to bypass raw API cache files.

        Returns:
            None.
        """
        # Suppress loguru stderr during pipeline — Rich owns the terminal
        _log_id = logger.add(
            DATA_DIR / "pipeline.log", rotation="5 MB", retention=2, level="DEBUG"
        )
        _default_id = None
        try:
            _default_id = logger.remove(0)
        except ValueError:
            pass

        try:
            total_started = time.perf_counter()
            console.print()
            console.print("[bold]Winnipeg PTN Analysis — Data Pipeline[/]")

            def _census_step():
                return load_dissemination_areas(self.city_key, self.db)

            # ── Phase A: Ingest ──────────────────────────────────────────
            console.print()
            console.print("[bold blue]Phase A:[/] [bold]Data Sources[/]")

            self._run_step("GTFS (current)", self.refresh_gtfs)
            self._run_step("Post-PTN archives", self.load_post_ptn_archives)
            self._run_step("Pre-PTN archives", self.load_pre_ptn_archives)
            self._run_step("Boundaries", self.refresh_boundaries)
            self._run_step("Census (CHASS)", _census_step)

            # Open Data — per-dataset sub-task display (threaded downloads)
            console.print("  [dim]Open Data[/]")
            od_results = self.refresh_open_data()
            od_summary = self._format_result_summary(od_results)
            console.print(f"  [green]✔[/] Open Data{od_summary}")

            # Employment after Census + Boundaries (depends on both)
            self._run_step("Employment", lambda: self.refresh_employment(force_refresh=force_refresh))

            # ── Phase B: Compute ─────────────────────────────────────────
            console.print()
            console.print("[bold blue]Phase B:[/] [bold]Compute[/]")

            self._run_step("Derived tables", self.build_derived_tables)
            self._run_step("Accessibility (r5py)", self.build_accessibility_tables)
            self._run_step("Live transit", lambda: self.refresh_live_transit(force_refresh=force_refresh))

            # ── Phase C: Export ──────────────────────────────────────────
            console.print()
            console.print("[bold blue]Phase C:[/] [bold]Export[/]")
            self._run_step("Exports", self.export_outputs)

            # ── Data Quality ─────────────────────────────────────────────
            console.print()
            dq_results = self.run_data_quality_checks()
            self._render_dq_table(dq_results)

            total_elapsed = time.perf_counter() - total_started
            console.print()
            console.print(f"[bold green]✔ Pipeline complete in {total_elapsed:.1f}s[/]")
        finally:
            logger.remove(_log_id)
            if _default_id is not None:
                try:
                    logger.add(
                        __import__("sys").stderr, level="DEBUG",
                        format="<level>{level: <8}</level> | "
                               "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                               "<level>{message}</level>",
                    )
                except Exception:
                    pass

    def _run_phase_parallel(self, steps: list[tuple[str, object]]) -> None:
        """Run multiple steps with a shared multi-task progress display.

        Each step gets its own row in the progress panel. Steps run
        concurrently in threads (useful for I/O-bound downloads).

        DuckDB note: duckdb-engine serialises writes internally, so
        concurrent load_table calls are safe but won't overlap on disk I/O.
        The win here is overlapping *network* I/O with DB writes.

        Args:
            steps: List of (label, callable) pairs.
        """
        with Progress(
            SpinnerColumn("dots"),
            TextColumn("{task.description}", style="cyan"),
            BarColumn(bar_width=20, pulse_style="cyan"),
            TaskProgressColumn(),
            TextColumn("[dim]{task.fields[status]}"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            # Create all task rows upfront so they're all visible.
            task_ids: dict[str, int] = {}
            for label, _ in steps:
                task_ids[label] = progress.add_task(
                    f"  {label}", total=None, status="pending"
                )

            results: dict[str, tuple[dict | None, float]] = {}

            def _worker(label: str, step_fn):
                progress.update(task_ids[label], status="running...")
                started = time.perf_counter()
                try:
                    result = step_fn()
                except Exception as exc:
                    elapsed = time.perf_counter() - started
                    progress.update(
                        task_ids[label],
                        status=f"[red]error: {exc!s:.40}",
                        completed=0, total=1,
                    )
                    results[label] = (None, elapsed)
                    logger.error(f"Step '{label}' failed: {exc}")
                    return  # don't re-raise — let other threads finish
                elapsed = time.perf_counter() - started
                progress.update(
                    task_ids[label],
                    completed=1, total=1,
                    status=f"[green]done {elapsed:.1f}s",
                )
                results[label] = (result, elapsed)

            try:
                with ThreadPoolExecutor(max_workers=3) as pool:
                    futures = {
                        pool.submit(_worker, label, step_fn): label
                        for label, step_fn in steps
                    }
                    for future in as_completed(futures):
                        future.result()
            except KeyboardInterrupt:
                console.print("\n  [red]Interrupted[/]")
                raise

        # Print summary lines after the progress panel clears.
        errors = []
        for label, _ in steps:
            result, elapsed = results.get(label, (None, 0))
            summary = self._format_result_summary(result)
            if result is None and elapsed > 0:
                console.print(f"  [red]✗[/] {label} [dim]{elapsed:.1f}s[/] [red]failed[/]")
                errors.append(label)
            else:
                console.print(f"  [green]✔[/] {label} [dim]{elapsed:.1f}s[/]{summary}")
        if errors:
            logger.warning(f"Phase had {len(errors)} failed step(s): {', '.join(errors)}")

    def build_accessibility_tables(self) -> dict[str, int]:
        """Build r5py travel time matrices (delegated to builders module)."""
        from ptn_analysis.data.builders import build_accessibility_tables
        return build_accessibility_tables(self.db, self.city_key, self.update_progress)

    def build_feed_registry(self) -> dict[str, int]:
        """Build the feed regime registry from DB metadata.

        Scans ``gtfs_route_stats`` for distinct feed_ids and classifies
        each by era relative to ``PTN_LAUNCH_DATE``.

        Returns:
            Row counts keyed by logical table name.
        """
        route_stats_table = self.db.table_name("gtfs_route_stats", self.city_key)
        if not self.db.relation_exists(route_stats_table):
            logger.warning(f"Cannot build feed registry: {route_stats_table} not found")
            return {"feed_regime_registry": 0}

        feed_ids = self.db.query(
            f"SELECT DISTINCT feed_id FROM {route_stats_table} WHERE feed_id NOT LIKE 'avg_%' ORDER BY feed_id"
        )["feed_id"].tolist()

        rows = []
        for sort_order, feed_id in enumerate(feed_ids, 1):
            if feed_id == "current":
                era_label = "current"
                feed_label = "Current feed"
                is_current = True
            elif feed_id >= PTN_LAUNCH_DATE:
                era_label = "post_ptn"
                feed_label = f"Post-PTN {feed_id}"
                is_current = False
            else:
                era_label = "pre_ptn"
                feed_label = f"Pre-PTN {feed_id}"
                is_current = False
            rows.append({
                "feed_id": feed_id,
                "feed_label": feed_label,
                "era_label": era_label,
                "sort_order": sort_order,
                "is_current": is_current,
            })

        frame = pd.DataFrame(rows)
        table_name = self.db.table_name("feed_regime_registry", self.city_key)
        self.db.load_table(table_name, frame, mode="replace")
        return {"feed_regime_registry": len(frame)}

    def _build_era_aggregates(self) -> None:
        """Build synthetic era-average feeds (delegated to builders module)."""
        from ptn_analysis.data.builders import build_era_aggregates
        build_era_aggregates(self.db, self.city_key)

    def build_h3_metrics(self) -> dict[str, int]:
        """Build H3-based stop service and live delay metrics.

        Uses H3 resolution 8 (~0.74 km² average cell area) — appropriate for
        urban transit stop density mapping. Hexagonal bins have uniform
        adjacency (6 neighbours vs grid's 4/8 diagonal ambiguity), reducing
        edge-effect bias in spatial aggregations.

        Returns:
            H3 table row counts keyed by logical table name.
        """
        stop_stats_table = self.db.table_name("gtfs_stop_stats", self.city_key)
        stops_table_name = self.db.table_name("stops", self.city_key)
        h3_stop_table_name = self.db.table_name("h3_stop_service_metrics", self.city_key)
        h3_delay_table_name = self.db.table_name("h3_live_delay_metrics", self.city_key)
        results = {"h3_stop_service_metrics": 0, "h3_live_delay_metrics": 0}

        if not self.db.supports_h3():
            logger.warning("Skipping H3 metric tables — DuckDB H3 extension unavailable")
            return results

        if self.db.relation_exists(stop_stats_table) and self.db.relation_exists(stops_table_name):
            self.db.execute(
                f"""
                CREATE OR REPLACE TABLE {h3_stop_table_name} AS
                SELECT
                    ss.feed_id,
                    h3_latlng_to_cell(stops.stop_lat, stops.stop_lon, {H3_RESOLUTION}) AS h3_cell,
                    COUNT(DISTINCT ss.stop_id) AS stop_count,
                    SUM(COALESCE(ss.num_trips, 0)) AS scheduled_trip_count,
                    AVG(COALESCE(ss.mean_headway, NULL)) AS mean_headway_minutes,
                    AVG(stops.stop_lat) AS centroid_lat,
                    AVG(stops.stop_lon) AS centroid_lon
                FROM {stop_stats_table} ss
                JOIN {stops_table_name} stops
                    ON ss.feed_id = stops.feed_id
                   AND ss.stop_id = stops.stop_id
                GROUP BY ss.feed_id, h3_cell
                """
            )
            results["h3_stop_service_metrics"] = self.db.count(h3_stop_table_name) or 0

        delay_snapshot_table_name = self.db.transit_table_name("trip_stop_delay_snapshot", self.city_key)
        if self.db.relation_exists(delay_snapshot_table_name) and self.db.relation_exists(stops_table_name):
            self.db.execute(
                f"""
                CREATE OR REPLACE TABLE {h3_delay_table_name} AS
                SELECT
                    h3_latlng_to_cell(stops.stop_lat, stops.stop_lon, {H3_RESOLUTION}) AS h3_cell,
                    COUNT(*) AS stop_event_count,
                    AVG(snapshot.arrival_delay_seconds) AS mean_arrival_delay_seconds,
                    MAX(snapshot.arrival_delay_seconds) AS max_arrival_delay_seconds,
                    AVG(snapshot.departure_delay_seconds) AS mean_departure_delay_seconds,
                    SUM(CASE WHEN snapshot.cancelled THEN 1 ELSE 0 END) AS cancelled_stop_count,
                    AVG(stops.stop_lat) AS centroid_lat,
                    AVG(stops.stop_lon) AS centroid_lon
                FROM {delay_snapshot_table_name} snapshot
                JOIN {stops_table_name} stops
                    ON CAST(snapshot.stop_number AS VARCHAR) = CAST(stops.stop_id AS VARCHAR)
                   AND stops.feed_id = :feed_id
                GROUP BY h3_cell
                """,
                {"feed_id": FEED_ID_CURRENT},
            )
            results["h3_live_delay_metrics"] = self.db.count(h3_delay_table_name) or 0

        return results

    def _transform_connections(self) -> None:
        """Build city2graph edges (delegated to builders module)."""
        from ptn_analysis.data.builders import build_connections
        build_connections(self.db, self.city_key)

    def _transform_views(self) -> None:
        """Build shared analysis views and supporting indexes.

        Executes SQL bundles in order: core (always), mobility (if passup/OTP
        tables exist), equity (if poverty/OurWPG tables exist). Gracefully
        skips optional bundles when their required tables are missing.
        """
        logger.info("Building core views (GTFS + census)")
        self._run_sql_script("views_core.sql", ptn_launch_date=PTN_LAUNCH_DATE)

        # Mobility views need era-split operational tables
        mobility_deps = ["passups", "ontime_performance", "passenger_counts"]
        if all(self.db.relation_exists(self.db.table_name(t, self.city_key)) for t in mobility_deps):
            logger.info("Building mobility views (passups, OTP, reliability)")
            self._run_sql_script("views_mobility.sql", ptn_launch_date=PTN_LAUNCH_DATE)
        else:
            missing = [t for t in mobility_deps if not self.db.relation_exists(self.db.table_name(t, self.city_key))]
            logger.warning(f"Skipping mobility views: missing {missing}")

        # Equity views need poverty/OurWPG/permits tables
        equity_deps = [
            "census_poverty_2021", "poverty_mbm",
            "ourwpg_mixed_use_corridors", "ourwpg_major_redev_sites",
            "ourwpg_mature_communities", "ourwpg_regional_centres",
            "development_permits",
        ]
        if all(self.db.relation_exists(self.db.table_name(t, self.city_key)) for t in equity_deps):
            logger.info("Building equity views (poverty, policy alignment)")
            try:
                self._run_sql_script("views_equity.sql", ptn_launch_date=PTN_LAUNCH_DATE)
            except Exception as exc:
                logger.warning(f"Equity views failed (optional tables may be missing): {exc}")
        else:
            logger.warning("Skipping equity views: poverty tables not loaded")

        self._run_sql_script("indexes.sql")

    def _load_daily_service(self, feed, target_date: str) -> None:
        """Create a daily service table for one date.

        Args:
            feed: gtfs-kit feed object.
            target_date: Service date in YYYY-MM-DD format.
        """
        try:
            datetime.strptime(target_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError(f"Invalid date format: {target_date}. Use YYYY-MM-DD.") from exc

        restricted_feed = feed.restrict_to_dates([target_date.replace("-", "")])
        table_name = self.db.table_name("daily_service", self.city_key)
        self.db.execute(f"DROP TABLE IF EXISTS {table_name}")

        if restricted_feed.trips is None or restricted_feed.trips.empty:
            self.db.execute(
                f"""
                CREATE TABLE {table_name} (
                    trip_id VARCHAR,
                    route_id VARCHAR,
                    service_id VARCHAR,
                    trip_headsign VARCHAR,
                    direction_id INTEGER,
                    service_date VARCHAR
                )
                """
            )
            return

        trips_frame = restricted_feed.trips.copy()
        trips_frame["service_date"] = target_date
        load_frame = trips_frame[
            ["trip_id", "route_id", "service_id", "trip_headsign", "direction_id", "service_date"]
        ].drop_duplicates()
        self.db.load_table(table_name, load_frame, mode="replace")

    def _transform_route_and_stop_metrics(self, feed, feed_id: str) -> dict[str, int]:
        """Compute route and stop metrics from one GTFS feed.

        Args:
            feed: gtfs-kit feed object.
            feed_id: Feed identifier.

        Returns:
            Row counts for route and stop metrics.
        """
        logger.info(f"Computing GTFS metrics for feed_id={feed_id!r}")
        self.update_progress(f"computing metrics: {feed_id}")
        service_dates = feed.get_dates()
        if not service_dates:
            raise ValueError("No service dates found in GTFS feed")

        route_table_name = self.db.table_name("gtfs_route_stats", self.city_key)
        stop_table_name = self.db.table_name("gtfs_stop_stats", self.city_key)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=PerformanceWarning)
            route_stats = feed.compute_route_stats(
                dates=service_dates,
                headway_start_time=SERVICE_DAY_START,
                headway_end_time=SERVICE_DAY_END,
                split_directions=True,
            )
        route_stats["date"] = route_stats["date"].astype(str).map(normalize_gtfs_date)
        route_stats.insert(0, "feed_id", feed_id)
        self._upsert_metric_table(route_table_name, route_stats, feed_id)

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=PerformanceWarning)
            stop_stats = feed.compute_stop_stats(
                dates=service_dates,
                headway_start_time=SERVICE_DAY_START,
                headway_end_time=SERVICE_DAY_END,
                split_directions=True,
            )
        stop_stats["date"] = stop_stats["date"].astype(str).map(normalize_gtfs_date)
        stop_stats.insert(0, "feed_id", feed_id)
        self._upsert_metric_table(stop_table_name, stop_stats, feed_id)

        return {"gtfs_route_stats": len(route_stats), "gtfs_stop_stats": len(stop_stats)}

    def _run_sql_script(self, filename: str, **replacements: str) -> None:
        """Execute one SQL script from the data SQL directory.

        Args:
            filename: SQL file name.
            **replacements: Template replacements for {{key}} placeholders.
        """
        sql_path = Path(__file__).with_name("sql") / filename
        if not sql_path.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        sql_text = sql_path.read_text(encoding="utf-8")
        for key, value in replacements.items():
            if not self._is_safe_template_value(value):
                raise ValueError(f"Unsafe SQL replacement value: {value!r}")
            sql_text = sql_text.replace(f"{{{{{key}}}}}", value)

        for raw_statement in sql_text.split(";"):
            statement = raw_statement.strip()
            if statement:
                self.db.execute(statement)

    def _upsert_metric_table(self, table_name: str, frame: pd.DataFrame, feed_id: str) -> None:
        """Replace metric rows for one feed in a unified table.

        Args:
            table_name: Target table name.
            frame: Metrics DataFrame.
            feed_id: Feed identifier.
        """
        if not re.match(r"^[a-z_][a-z0-9_]*$", table_name):
            raise ValueError(f"Invalid table name: {table_name!r}")
        if not self.db.relation_exists(table_name):
            self.db.load_table(table_name, frame, mode="replace")
            return
        self.db.execute(f"DELETE FROM {table_name} WHERE feed_id = :feed_id", {"feed_id": feed_id})
        self.db.load_table(table_name, frame, mode="append")

    def _is_safe_template_value(self, value: str) -> bool:
        """Return whether a SQL replacement value is safe to interpolate."""
        return bool(_SAFE_SQL_VALUE_RE.match(value))

    def run_data_quality_checks(self) -> list[dict[str, str]]:
        """Run automated data quality checks (delegated to quality module)."""
        from ptn_analysis.data.quality import run_data_quality_checks
        results = run_data_quality_checks(self.db, self.city_key)
        for r in results:
            icon = "PASS" if r["status"] == "pass" else ("WARN" if r["status"] == "warn" else "SKIP")
            logger.info(f"DQ [{r['dimension']}] {icon}: {r['check']} — {r['detail']}")
        return results

    def _render_dq_table(self, dq_results: list[dict[str, str]]) -> None:
        """Render data quality check results as a Rich table.

        Args:
            dq_results: Check results from ``run_data_quality_checks()``.
        """
        table = Table(
            title="Data Quality Validation",
            box=box.SIMPLE,
            show_header=True,
            header_style="bold",
            pad_edge=False,
        )
        table.add_column("Dimension", style="cyan", width=14)
        table.add_column("Check", width=36)
        table.add_column("Result", width=8)
        table.add_column("Detail", style="dim")

        for r in dq_results:
            if r["status"] == "pass":
                icon = "[green]✔[/]"
            elif r["status"] == "warn":
                icon = "[yellow]⚠[/]"
            else:
                icon = "[dim]⊘[/]"
            table.add_row(r["dimension"], r["check"], icon, r["detail"])

        console.print()
        console.print(table)

        passed = sum(1 for r in dq_results if r["status"] == "pass")
        total = len(dq_results)
        console.print(f"  [dim]{passed}/{total} checks passed[/]")

    def _run_step(self, label: str, step_function) -> dict | None:
        """Run one pipeline step with a live progress bar showing elapsed time.

        Args:
            label: Step label for display.
            step_function: Callable to execute.

        Returns:
            Step result when present.
        """
        started = time.perf_counter()
        with Progress(
            SpinnerColumn("dots"),
            TextColumn("[bold cyan]{task.description}"),
            BarColumn(bar_width=20, pulse_style="cyan"),
            TaskProgressColumn(),
            TextColumn("[dim]{task.fields[status]}"),
            TimeElapsedColumn(),
            console=console,
            transient=True,
        ) as progress:
            task_id = progress.add_task(f"  {label}", total=None, status="running...")
            self._progress = progress
            self._progress_task_id = task_id
            result = step_function()
            self._progress = None
            self._progress_task_id = None
        elapsed = time.perf_counter() - started

        summary = self._format_result_summary(result)
        icon = "[green]✔[/]"
        if isinstance(result, dict):
            if any(v == 0 for v in result.values()):
                icon = "[yellow]⚠[/]"
        console.print(f"  {icon} {label} [dim]{elapsed:.1f}s[/]{summary}")
        return result

    @staticmethod
    def _format_result_summary(result) -> str:
        """Format step result dict into a compact summary string."""
        summary_parts: list[str] = []
        zero_parts: list[str] = []
        if isinstance(result, dict) and result:
            for name, count in result.items():
                if count and count > 0:
                    summary_parts.append(f"{name} {count:,}")
                elif count == 0:
                    zero_parts.append(name)
        summary = ""
        if summary_parts:
            shown = summary_parts[:3]
            if len(summary_parts) > 3:
                shown.append(f"+{len(summary_parts) - 3} more")
            summary = f"  [dim]({', '.join(shown)})[/]"
        if zero_parts:
            summary += f"  [yellow]0 rows: {', '.join(zero_parts)}[/]"
        return summary

    def update_progress(self, status: str, completed: int | None = None, total: int | None = None) -> None:
        """Update the live progress bar from within a step.

        Args:
            status: Short status string (e.g. "passups 50,000/194,686").
            completed: Current progress count (sets bar position).
            total: Total expected count (switches bar from pulse to %).
        """
        if self._progress is None:
            return
        kwargs: dict = {"status": status}
        if total is not None:
            kwargs["total"] = total
        if completed is not None:
            kwargs["completed"] = completed
        self._progress.update(self._progress_task_id, **kwargs)
