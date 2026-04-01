# Figure QA Checklist — Final Report

**Owner:** Stephenie Michael
**Deadline:** March 26 checkpoint, March 31 freeze

## Instructions

For each figure in `reports/final/figures/`, verify:

1. **Readable at print size** — text is legible when printed on letter paper
2. **Consistent palette** — uses PTN tier colours where applicable
3. **Axes labelled** — all axes have labels with units
4. **No clipped text** — legends and labels fully visible
5. **Caption matches content** — LaTeX caption accurately describes the figure
6. **DPI ≥ 200** — no pixelation at print resolution
7. **White background** — consistent with report style

## Figure Inventory

### From PR2 (verified — just confirm readability)

| Figure | Notebook | Status |
|---|---|---|
| `prepost_combined.png` | 0.1-ahmed | [ ] Verified |
| `demand_validation.png` | 0.1-ahmed | [ ] Verified |
| `clustering_elbow.png` | 0.2-ahmed | [ ] Verified |
| `clustering_combined.png` | 0.2-ahmed | [ ] Verified |
| `classification_combined.png` | 0.3-ahmed | [ ] Verified |
| `equity_combined.png` | 0.4-ahmed | [ ] Verified |
| `upgrade_priority.png` | 0.4-ahmed | [ ] Verified |
| `reliability_ontime.png` | 0.4-ahmed | [ ] Verified |
| `network_metrics_prepost.png` | 1.2-cathy | [ ] Verified |
| `transfer_heatmap.png` | 1.2-cathy | [ ] Verified |
| `weighted_centrality_comparison.png` | 1.2-cathy | [ ] Verified |
| `community_boundary_alignment.png` | 1.2-cathy | [ ] Verified |
| `coverage_change_map.png` | 1.3-sudipta | [ ] Verified |
| `underserved_neighbourhoods.png` | 1.3-sudipta | [ ] Verified |
| `pr2_summary_panel.png` | 2.1-stephenie | [ ] Verified |
| `coverage_cluster_map.png` | 2.1-stephenie | [ ] Verified |
| `network_communities_pr2.png` | 2.1-stephenie | [ ] Verified |
| `before_after_routes.png` | 2.1-stephenie | [ ] Verified |

### Final Report Figures (NEW — verify carefully)

| Figure | Notebook | Status |
|---|---|---|
| `association_rules_network.png` | 0.5-ahmed | [ ] Verified |
| `model_evaluation_combined.png` | 0.6-ahmed | [ ] Verified |
| `equity_quintile.png` | 0.7-ahmed / 1.4-sudipta | [ ] Verified |
| `poverty_correlation.png` | 0.7-ahmed / 1.4-sudipta | [ ] Verified |
| `counterfactual_rerank.png` | 0.7-ahmed / 1.4-sudipta | [ ] Verified |

### Copied from PR2 (for reuse in final)

| Figure | Status |
|---|---|
| `temporal_evolution.png` | [ ] Verified |
| `degree_distribution.png` | [ ] Verified |
| `negbin_diagnostics.png` | [ ] Verified |
| `income_coverage_correlation.png` | [ ] Verified |
| `neighbourhood_clustering.png` | [ ] Verified |
| `equity_triptych.png` | [ ] Verified |

## Common Issues to Watch For

- **Tiny text in multi-panel figures** — especially `*_combined.png` files
- **Legend overlap** — legends covering data points
- **Colour-blind accessibility** — avoid red/green only encoding
- **Axis tick density** — too many or too few tick marks
- **Figure title vs caption** — title should be in LaTeX caption, not in the figure itself
