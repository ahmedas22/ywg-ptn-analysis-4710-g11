"""Chart helpers and PTN presentation utilities.

Pure DataFrame/GeoDataFrame → Figure transforms. No DB access.
PTN domain constants live in context.config; DB-bound report helpers
live in context.reporting.
"""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ptn_analysis.context.config import (
    FX_ROUTE_COLORS,
    HEADWAY_TIER_COLORS,
    HEADWAY_TIER_LIST,
    PTN_HEADWAY_TARGETS,
    PTN_TIER_COLORS,
    PTN_TIER_ORDER,
    WEB_MERCATOR_CRS,
    classify_ptn_tier,
    get_route_display_color,
    headway_tier,
)

__all__ = [
    # re-exports from config (used by analysis.__init__)
    "HEADWAY_TIER_COLORS",
    "HEADWAY_TIER_LIST",
    "PTN_TIER_COLORS",
    "PTN_TIER_ORDER",
    "PTN_HEADWAY_TARGETS",
    "FX_ROUTE_COLORS",
    "classify_ptn_tier",
    "get_route_display_color",
    "headway_tier",
    # map helpers
    "WEB_MERCATOR",
    "add_consistent_basemap",
    # chart rendering
    "Plotter",
    "save_report_figure",
    "create_employment_access_change_chart",
    "plot_metric_comparison_bar",
    "plot_heatmap",
    "plot_choropleth_change",
]

WEB_MERCATOR = WEB_MERCATOR_CRS


# ---------------------------------------------------------------------------
# Basemap helper (matplotlib + contextily — no folium)
# ---------------------------------------------------------------------------


def add_consistent_basemap(ax, zoom: int = 11) -> None:
    """Apply the standard basemap style for matplotlib map figures.

    Args:
        ax: Matplotlib axes to add the basemap to.
        zoom: Zoom level for the contextily tile download.
    """
    import contextily as cx

    cx.add_basemap(ax, source=cx.providers.CartoDB.Positron, zoom=zoom)
    ax.set_axis_off()


# ---------------------------------------------------------------------------
# Chart helpers (pure DataFrame → Figure)
# ---------------------------------------------------------------------------


def save_report_figure(fig, output_path: str | Path, dpi: int = 200) -> Path:
    """Save a chart or panel with consistent report export settings."""
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(
        output_file,
        dpi=dpi,
        bbox_inches="tight",
        pad_inches=0.04,
        facecolor="white",
    )
    return output_file


def create_employment_access_change_chart(
    jobs_access_comparison_table: pd.DataFrame,
    top_n: int = 15,
):
    """Create a neighbourhood jobs-access change chart."""
    if jobs_access_comparison_table.empty:
        return None

    display_table = jobs_access_comparison_table.sort_values(
        "jobs_access_change", ascending=False,
    ).head(top_n)
    display_table = display_table.sort_values("jobs_access_change")

    colors = []
    for change_value in display_table["jobs_access_change"]:
        if pd.isna(change_value):
            colors.append("#bdbdbd")
        elif change_value >= 0:
            colors.append("#1a9850")
        else:
            colors.append("#d73027")

    figure, axis = plt.subplots(figsize=(10, 6))
    axis.barh(display_table["neighbourhood"], display_table["jobs_access_change"], color=colors)
    axis.axvline(0, color="black", linewidth=1)
    axis.set_title("Neighbourhood Jobs Access Change")
    axis.set_xlabel("Jobs access score change (positive is better)")
    axis.set_ylabel("Neighbourhood")
    axis.grid(axis="x", linestyle="--", alpha=0.35)
    axis.set_axisbelow(True)
    return figure


class Plotter:
    """Pure DataFrame→Figure builder. No DB access."""

    def __init__(self, figures_dir: Path | str, dpi: int = 200) -> None:
        self.figures_dir = Path(figures_dir)
        self.dpi = dpi

    def __repr__(self) -> str:
        return f"Plotter(figures_dir={self.figures_dir}, dpi={self.dpi})"

    def save(self, fig, filename: str, override_dir: Path | None = None) -> Path:
        out_dir = Path(override_dir) if override_dir else self.figures_dir
        return save_report_figure(fig, out_dir / filename, self.dpi)

    def employment_access_change(self, jobs_access_comparison_table: pd.DataFrame, top_n: int = 15):
        return create_employment_access_change_chart(jobs_access_comparison_table, top_n=top_n)


def plot_metric_comparison_bar(df: pd.DataFrame, metric: str, label: str, pre: str, post: str, title: str) -> tuple:
    """Side-by-side bar chart comparing a metric before and after PTN."""
    import numpy as np

    categories = df[label].tolist()
    x = np.arange(len(categories))
    width = 0.35

    fig, ax = plt.subplots(figsize=(12, 5))
    ax.bar(x - width / 2, df[pre], width, label=pre, color="#4C72B0", alpha=0.85)
    ax.bar(x + width / 2, df[post], width, label=post, color="#DD8452", alpha=0.85)
    ax.set_xticks(x)
    ax.set_xticklabels(categories, rotation=45, ha="right", fontsize=8)
    ax.set_title(title)
    ax.set_ylabel(metric)
    ax.legend()
    fig.tight_layout()
    return fig, ax


def plot_heatmap(df: pd.DataFrame, origin: str, dest: str, value: str, title: str, cmap: str = "YlOrRd") -> tuple:
    """Pivot heatmap for origin-destination matrices."""
    pivot = df.pivot(index=origin, columns=dest, values=value)
    fig, ax = plt.subplots(figsize=(10, 8))
    im = ax.imshow(pivot.values, aspect="auto", cmap=cmap)
    ax.set_xticks(range(len(pivot.columns)))
    ax.set_yticks(range(len(pivot.index)))
    ax.set_xticklabels(pivot.columns, rotation=45, ha="right", fontsize=7)
    ax.set_yticklabels(pivot.index, fontsize=7)
    plt.colorbar(im, ax=ax, label=value)
    ax.set_title(title)
    fig.tight_layout()
    return fig, ax


def plot_choropleth_change(gdf, value_col: str, title: str, cmap: str = "RdYlGn") -> tuple:
    """Choropleth map showing change in a metric across neighbourhoods."""
    fig, ax = plt.subplots(1, 1, figsize=(10, 8))
    gdf.plot(
        column=value_col, cmap=cmap, linewidth=0.5, edgecolor="white",
        legend=True, ax=ax, missing_kwds={"color": "lightgrey", "label": "No data"},
    )
    ax.set_title(title, fontsize=13)
    ax.set_axis_off()
    fig.tight_layout()
    return fig, ax
