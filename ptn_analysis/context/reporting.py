"""Report artifact registry, export helpers, and notebook execution."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from ptn_analysis.context.config import (
    DEFAULT_CITY_KEY,
    FEED_ID_CURRENT,
    PROJ_ROOT,
)

NOTEBOOKS_DIR = PROJ_ROOT / "notebooks"

REPORT_NOTEBOOKS: dict[str, dict[str, dict[str, list[str]]]] = {
    "pr1": {
        "0.0-ahmed-methodology": {
            "figures": [
                "ptn_network_map.png",
                "headway_map.png",
                "frequency_4panel.png",
                "speed_comparison.png",
            ],
        },
        "1.0-cathy-network": {
            "figures": ["network_map.png", "network_graph.png"],
        },
        "1.1-sudipta-coverage": {
            "figures": [
                "coverage_choropleth.png",
                "neighbourhood_density_hist.png",
            ],
        },
        "2.0-stephenie-viz": {
            "figures": ["coverage_bar.png", "transfer_points.png"],
        },
    },
    "pr2": {
        "0.1-ahmed-pr2-comparison": {
            "figures": [
                "prepost_combined.png",
                "demand_validation.png",
            ],
        },
        "0.2-ahmed-pr2-clustering": {
            "figures": [
                "clustering_elbow.png",
                "clustering_combined.png",
            ],
        },
        "0.3-ahmed-pr2-classification": {
            "figures": [
                "classification_combined.png",
            ],
        },
        "0.4-ahmed-pr2-capacity": {
            "figures": [
                "equity_combined.png",
                "upgrade_priority.png",
                "reliability_ontime.png",
            ],
        },
        "1.2-cathy-pr2-network": {
            "figures": [
                "network_metrics_prepost.png",
                "transfer_heatmap.png",
                "weighted_centrality_comparison.png",
                "community_boundary_alignment.png",
            ],
        },
        "1.3-sudipta-pr2-coverage": {
            "figures": [
                "coverage_change_map.png",
                "underserved_neighbourhoods.png",
            ],
        },
        "2.1-stephenie-pr2-viz": {
            "figures": [
                "pr2_summary_panel.png",
                "coverage_cluster_map.png",
                "network_communities_pr2.png",
                "before_after_routes.png",
            ],
        },
    },
}


def get_notebook_artifacts(report_name: str, notebook_stem: str) -> dict[str, list[str]]:
    """Return the expected artifacts for one notebook.

    Args:
        report_name: Report key such as ``"pr2"``.
        notebook_stem: Notebook stem without the ``.ipynb`` suffix.

    Returns:
        Expected figure and table names.

    Raises:
        KeyError: If the report or notebook is unknown.
    """
    return REPORT_NOTEBOOKS[report_name][notebook_stem]


def get_report_names(report_name: str) -> list[str]:
    """Resolve one report selector to concrete report names.

    Args:
        report_name: Report key such as ``"pr2"`` or ``"all"``.

    Returns:
        Concrete report names in execution order.

    Raises:
        ValueError: If the report selector is unknown.
    """
    if report_name == "all":
        return list(REPORT_NOTEBOOKS)
    if report_name in REPORT_NOTEBOOKS:
        return [report_name]
    raise ValueError(f"Unknown report: {report_name}")


def get_report_notebooks(report_name: str) -> list[str]:
    """Return the configured notebook stems for one report.

    Args:
        report_name: Report key such as ``"pr2"``.

    Returns:
        Notebook stems in execution order.
    """
    return list(REPORT_NOTEBOOKS[report_name])


def get_notebook_paths(notebook_stem: str) -> tuple[Path, Path]:
    """Return the source and executed paths for one notebook.

    Args:
        notebook_stem: Notebook stem without the ``.ipynb`` suffix.

    Returns:
        Source and executed notebook paths.
    """
    notebook_path = NOTEBOOKS_DIR / f"{notebook_stem}.ipynb"
    executed_path = NOTEBOOKS_DIR / f"{notebook_stem}.executed.ipynb"
    return notebook_path, executed_path


def ensure_report_dirs(report_name: str) -> Path:
    """Create the report figures directory when needed.

    Args:
        report_name: Report key such as ``"pr2"``.

    Returns:
        Figures directory path.
    """
    report_dir = PROJ_ROOT / "reports" / report_name
    figures_dir = report_dir / "figures"
    figures_dir.mkdir(parents=True, exist_ok=True)
    return figures_dir


def build_notebook_parameters(report_name: str, dpi: int) -> dict[str, object]:
    """Build the shared papermill parameter payload.

    Args:
        report_name: Report key such as ``"pr2"``.
        dpi: Figure export DPI.

    Returns:
        Papermill parameter mapping.
    """
    figures_dir = ensure_report_dirs(report_name)
    return {
        "save_figures": True,
        "figures_dir": str(figures_dir),
        "dpi": dpi,
    }


def _resolve_report_output_path(output_path_or_name: str | Path, default_dir: Path) -> Path:
    """Resolve one report output path.

    Args:
        output_path_or_name: Relative file name or full output path.
        default_dir: Directory used for relative names.

    Returns:
        Resolved output path.
    """
    output_path = Path(output_path_or_name)
    if output_path.is_absolute() or output_path.parent != Path("."):
        return output_path
    return default_dir / output_path


def save_report_figure(
    fig,
    output_path_or_name: str | Path,
    report_name: str = "pr2",
    figures_dir: str | Path | None = None,
    dpi: int = 200,
    enabled: bool = True,
) -> Path:
    """Save one report figure when export is enabled.

    Args:
        fig: Matplotlib figure instance.
        output_path_or_name: Relative file name or full output path.
        report_name: Report key such as ``"pr2"``.
        figures_dir: Optional figure directory override.
        dpi: Saved figure resolution.
        enabled: Whether figure export is enabled.

    Returns:
        Path to the figure output.
    """
    report_figures_dir = ensure_report_dirs(report_name)
    target_dir = Path(figures_dir) if figures_dir is not None else report_figures_dir
    output_path = _resolve_report_output_path(output_path_or_name, target_dir)
    if enabled:
        from ptn_analysis.analysis.visualization import save_report_figure as _save_report_figure

        _save_report_figure(fig, str(output_path), dpi=dpi)
    return output_path


def save_placeholder_figure(
    file_name: str,
    message: str,
    report_name: str = "pr2",
    figures_dir: str | Path | None = None,
    dpi: int = 200,
    enabled: bool = True,
) -> Path:
    """Save a placeholder figure for a missing dataset.

    Args:
        file_name: Figure file name.
        message: Placeholder message.
        report_name: Report key such as ``"pr2"``.
        figures_dir: Optional figure directory override.
        dpi: Saved figure resolution.
        enabled: Whether figure export is enabled.

    Returns:
        Path to the saved placeholder image.
    """
    import matplotlib.pyplot as plt

    report_figures_dir = ensure_report_dirs(report_name)
    target_dir = Path(figures_dir) if figures_dir is not None else report_figures_dir
    output_path = target_dir / file_name
    figure, axis = plt.subplots(figsize=(8, 4))
    axis.text(0.5, 0.5, message, ha="center", va="center", wrap=True)
    axis.set_axis_off()
    if enabled:
        figure.savefig(
            output_path,
            dpi=dpi,
            bbox_inches="tight",
            pad_inches=0.04,
            facecolor="white",
        )
    plt.close(figure)
    return output_path


def verify_report_artifacts(report_name: str, notebook_stem: str) -> dict[str, list[Path]]:
    """Verify the declared artifacts for one executed notebook.

    Args:
        report_name: Report key such as ``"pr2"``.
        notebook_stem: Notebook stem without the ``.ipynb`` suffix.

    Returns:
        Mapping of artifact type to verified paths.

    Raises:
        FileNotFoundError: If any declared artifact is missing.
    """
    figures_dir = ensure_report_dirs(report_name)
    artifacts = get_notebook_artifacts(report_name, notebook_stem)
    figure_paths = [figures_dir / name for name in artifacts["figures"]]
    missing_paths = [path for path in figure_paths if not path.exists()]
    if missing_paths:
        missing_list = "\n".join(str(path) for path in missing_paths)
        raise FileNotFoundError(
            f"Missing report artifacts for {report_name}/{notebook_stem}:\n{missing_list}"
        )
    return {"figures": figure_paths}


def execute_report_notebook(
    report_name: str,
    notebook_stem: str,
    dpi: int,
) -> dict[str, object]:
    """Execute one report notebook and verify its outputs.

    Args:
        report_name: Report key such as ``"pr2"``.
        notebook_stem: Notebook stem without the ``.ipynb`` suffix.
        dpi: Saved figure resolution.

    Returns:
        Execution metadata and verified artifact paths.

    Raises:
        FileNotFoundError: If the source notebook is missing.
        RuntimeError: If papermill execution fails.
    """
    from loguru import logger

    ensure_report_dirs(report_name)
    notebook_path, executed_path = get_notebook_paths(notebook_stem)

    if not notebook_path.exists():
        raise FileNotFoundError(f"Notebook not found: {notebook_path}")

    import papermill as pm

    logger.info(f"Running {report_name}/{notebook_stem}...")
    try:
        pm.execute_notebook(
            str(notebook_path),
            str(executed_path),
            parameters=build_notebook_parameters(report_name, dpi),
            kernel_name="python3",
        )
    except Exception as exc:  # pragma: no cover - papermill wraps notebook errors
        raise RuntimeError(f"Notebook execution failed for {notebook_stem}") from exc

    verified_artifacts = verify_report_artifacts(report_name, notebook_stem)
    artifact_names = get_notebook_artifacts(report_name, notebook_stem)
    logger.info(
        "Verified %s figures for %s",
        len(artifact_names["figures"]),
        notebook_stem,
    )
    return {
        "notebook": notebook_stem,
        "executed_notebook": executed_path,
        "figures": verified_artifacts["figures"],
    }


def generate_figures(report_name: str = "pr1", dpi: int = 200) -> dict[str, list[dict[str, object]]]:
    """Generate one report or all configured reports.

    Args:
        report_name: Report key or ``"all"``.
        dpi: Saved figure resolution.

    Returns:
        Executed notebook metadata grouped by report name.

    Raises:
        ValueError: If the requested report is unknown.
    """
    from loguru import logger

    report_names = get_report_names(report_name)

    results: dict[str, list[dict[str, object]]] = {}
    for selected_report in report_names:
        ensure_report_dirs(selected_report)
        results[selected_report] = []
        for notebook_stem in get_report_notebooks(selected_report):
            results[selected_report].append(
                execute_report_notebook(
                    report_name=selected_report,
                    notebook_stem=notebook_stem,
                    dpi=dpi,
                )
            )

    completed_count = sum(len(notebooks) for notebooks in results.values())
    logger.info("Completed %s notebook runs", completed_count)
    return results


# ---------------------------------------------------------------------------
# DB-bound summary and figure helpers
# ---------------------------------------------------------------------------


def collect_summary_stats(
    db_instance,
    city_key: str = DEFAULT_CITY_KEY,
    feed_id: str = FEED_ID_CURRENT,
) -> dict[str, float | int]:
    """Collect summary metrics for notebooks and the dashboard.

    Args:
        db_instance: TransitDB handle.
        city_key: City namespace.
        feed_id: Feed identifier.

    Returns:
        Summary statistics dictionary.
    """
    database = db_instance
    stops_relation = database.table_name("stops", city_key)
    connection_relation = database.table_name("stop_connection_counts", city_key)
    route_metrics_relation = database.table_name("route_schedule_metrics", city_key)
    density_relation = database.table_name("neighbourhood_stop_count_density", city_key)
    jobs_access_relation = database.table_name("neighbourhood_jobs_access_metrics", city_key)

    coverage_table = pd.DataFrame()
    if database.relation_exists(density_relation):
        coverage_table = database.query(
            f"""
            SELECT neighbourhood_id,
                   neighbourhood,
                   stop_count,
                   stop_density_per_km2
            FROM {density_relation}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": feed_id},
        )

    jobs_access_table = pd.DataFrame()
    if database.relation_exists(jobs_access_relation):
        jobs_access_table = database.query(
            f"""
            SELECT neighbourhood_id,
                   neighbourhood,
                   jobs_access_score
            FROM {jobs_access_relation}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": feed_id},
        )

    route_table = pd.DataFrame()
    if database.relation_exists(route_metrics_relation):
        route_table = database.query(
            f"""
            SELECT route_id, route_short_name, scheduled_trip_count, mean_headway_minutes
            FROM {route_metrics_relation}
            WHERE feed_id = :feed_id
            """,
            {"feed_id": feed_id},
        )

    stop_count = 0
    if database.relation_exists(stops_relation):
        stop_count = int(
            database.first(
                f"SELECT COUNT(*) FROM {stops_relation} WHERE feed_id = :feed_id",
                {"feed_id": feed_id},
            )
            or 0
        )

    edge_count = 0
    if database.relation_exists(connection_relation):
        edge_count = int(
            database.first(
                f"""
                SELECT COUNT(*)
                FROM {connection_relation}
                WHERE feed_id = :feed_id
                """,
                {"feed_id": feed_id},
            )
            or 0
        )

    return {
        "num_stops": stop_count,
        "num_edges": edge_count,
        "neighbourhood_count": int(len(coverage_table)),
        "route_count": int(len(route_table)),
        "total_neighbourhood_stops": int(coverage_table["stop_count"].sum())
        if not coverage_table.empty
        else 0,
        "min_stop_density_per_km2": float(coverage_table["stop_density_per_km2"].min())
        if not coverage_table.empty
        else 0.0,
        "median_stop_density_per_km2": float(coverage_table["stop_density_per_km2"].median())
        if not coverage_table.empty
        else 0.0,
        "max_stop_density_per_km2": float(coverage_table["stop_density_per_km2"].max())
        if not coverage_table.empty
        else 0.0,
        "jobs_access_neighbourhood_count": int(len(jobs_access_table)),
        "mean_jobs_access_score": float(jobs_access_table["jobs_access_score"].mean())
        if not jobs_access_table.empty
        else 0.0,
        "max_jobs_access_score": float(jobs_access_table["jobs_access_score"].max())
        if not jobs_access_table.empty
        else 0.0,
    }


