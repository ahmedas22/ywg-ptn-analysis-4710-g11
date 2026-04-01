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
    "NEIGHBOURHOOD_STYLE",
    "POINT_MARKER_STYLE",
    "LABEL_STYLE",
    "add_consistent_basemap",
    "plot_neighbourhood_base",
    # chart rendering
    "Plotter",
    "save_report_figure",
    "create_employment_access_change_chart",
    "plot_association_rules_network",
]

WEB_MERCATOR = WEB_MERCATOR_CRS

# Standard neighbourhood overlay style (used as **kwargs in neigh_gdf.plot())
NEIGHBOURHOOD_STYLE = dict(
    facecolor="#f7f7f7",
    edgecolor="#999999",
    linewidth=0.4,
    alpha=0.6,
)

# Standard point-marker style
POINT_MARKER_STYLE = dict(
    edgecolor="gray",
    linewidth=0.2,
)

# Standard label-annotation style
LABEL_STYLE = dict(
    fontweight="bold",
    bbox=dict(boxstyle="round,pad=0.15", facecolor="white", edgecolor="gray", alpha=0.85),
)


# ---------------------------------------------------------------------------
# Basemap helper (matplotlib + contextily — no folium)
# ---------------------------------------------------------------------------


def plot_neighbourhood_base(ax, neigh_gdf, **overrides):
    """Plot the neighbourhood polygons as a background layer.

    Args:
        ax: Matplotlib axes.
        neigh_gdf: GeoDataFrame of neighbourhood polygons (should be in EPSG:3857).
        **overrides: Any NEIGHBOURHOOD_STYLE keys to override.
    """
    style = {**NEIGHBOURHOOD_STYLE, **overrides}
    neigh_gdf.plot(ax=ax, **style)


def add_consistent_basemap(ax, zoom: int = 12) -> None:
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
        """Save a figure to the configured directory."""
        out_dir = Path(override_dir) if override_dir else self.figures_dir
        return save_report_figure(fig, out_dir / filename, self.dpi)

    def employment_access_change(self, jobs_access_comparison_table: pd.DataFrame, top_n: int = 15):
        """Create an employment access change chart."""
        return create_employment_access_change_chart(jobs_access_comparison_table, top_n=top_n)


def plot_association_rules_network(
    rules_df: pd.DataFrame,
    min_lift: float = 1.0,
    top_n: int = 30,
) -> tuple:
    """Render association rules as a network graph.

    Nodes are itemset features, edges represent rules with width proportional
    to lift and colour mapped to confidence.

    Args:
        rules_df: DataFrame with ``antecedents``, ``consequents``,
            ``support``, ``confidence``, ``lift`` columns.
        min_lift: Minimum lift threshold for display.
        top_n: Maximum rules to display.

    Returns:
        (fig, ax) tuple.
    """
    import networkx as nx

    filtered = rules_df[rules_df["lift"] >= min_lift].nlargest(top_n, "lift")
    if filtered.empty:
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.text(0.5, 0.5, "No rules above lift threshold", ha="center", va="center")
        return fig, ax

    G = nx.DiGraph()
    for _, row in filtered.iterrows():
        ant = ", ".join(sorted(row["antecedents"]))
        cons = ", ".join(sorted(row["consequents"]))
        G.add_edge(ant, cons, lift=row["lift"], confidence=row["confidence"])

    fig, ax = plt.subplots(figsize=(14, 10))
    pos = nx.spring_layout(G, seed=42, k=2.0)
    edges = G.edges(data=True)
    widths = [e[2]["lift"] * 1.5 for e in edges]
    colors = [e[2]["confidence"] for e in edges]

    nx.draw_networkx_nodes(G, pos, ax=ax, node_size=800, node_color="#4C72B0", alpha=0.8)
    nx.draw_networkx_edges(
        G, pos, ax=ax, width=widths, edge_color=colors,
        edge_cmap=plt.cm.YlOrRd, edge_vmin=0, edge_vmax=1,
        arrowsize=15, alpha=0.7,
    )
    nx.draw_networkx_labels(G, pos, ax=ax, font_size=7, font_weight="bold")
    # Manual colorbar for confidence (nx edge collection is not ScalarMappable)
    sm = plt.cm.ScalarMappable(cmap=plt.cm.YlOrRd, norm=plt.Normalize(0, 1))
    sm.set_array([])
    plt.colorbar(sm, ax=ax, label="Confidence", shrink=0.6)
    ax.set_title("Association Rules Network (edge width = lift)", fontsize=13)
    ax.axis("off")
    fig.tight_layout()
    return fig, ax


