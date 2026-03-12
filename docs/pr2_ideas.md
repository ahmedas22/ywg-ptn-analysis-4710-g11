# PR2 / Final Report Analysis Directions

Analysis techniques for the final report (15-20 pages, 50 marks). Course requires data mining techniques.

## Literature grounding

The PR2/final report should stay grounded in the actual source literature that already
matches this repo's work:

- Bao et al. (2020), *PubtraVis*, for multi-view GTFS operational visualization.
- Farber et al. (2024), *GTFS2STN*, for spatiotemporal GTFS network analysis and
  accessibility framing. This paper also uses Streamlit, which supports keeping
  Streamlit as a valid project delivery surface.
- Steve Chicken (2024), *The Narwhal*, for Winnipeg-specific context on who is affected
  by service changes.
- Stanley Ho (2024), Winnipeg transit unreliability thesis, for local equity and reliability
  framing.
- Lucas (2012), transport disadvantage / social exclusion framework, for the conceptual
  equity discussion in the methodology notebook and final paper.

## Policy and network-change context

The final paper should include a separate context section, not a methods section, covering:

- WTMP short-term and long-term roadmap items
- City of Winnipeg update dated February 25, 2026

Use that section to explain that:
- the PTN launch is not the end state of the network
- post-launch route and frequency changes continued into 2026
- results should be interpreted as part of an ongoing transition

Recommended details to mention:
- spring schedule improvements effective April 12, 2026
- added trips on F5, F8, 74, 557, and 676
- after-midnight service extensions on D10, D14, D16, D17, 28, 38, 43, 70, 74, and 680
- downtown route changes under consideration for summer 2026
- D19 access changes
- D16 reliability split and proposed Route 18

WTMP roadmap snapshot to keep in the paper's context section:

| Project | Type | Status | Timeline |
|---|---|---|---|
| Primary Transit Network design and implementation | Service | Launch approved by Council in 2024 | Launched June 2025 |
| Primary Transit Network Infrastructure | Infrastructure | Upcoming | Funding anticipated to begin in 2025 |
| Design of Downtown Rapid Transit Corridors | Infrastructure | Upcoming | Funding anticipated to begin design in 2025 |
| Transit Plus transition to Family of Services | Service | Upcoming | Timeline TBD |
| Construction of Downtown Rapid Transit Corridors | Infrastructure | Future | 5+ years |
| Ongoing service development | Service | Future, funding dependent | Ongoing |
| Ongoing rapid transit expansion | Infrastructure | Future, funding dependent | 10+ years |

## Clustering (Unsupervised)

1. **Neighbourhood Clustering** - K-means/DBSCAN on DAs by (population, employment, stop_count, headway, centrality) to identify service archetypes (transit-rich urban core, underserved suburban, etc.)

2. **Temporal Route Clustering** - Cluster routes by hourly departure profile to find peak-only vs all-day vs evening service patterns.

## Classification (Supervised)

3. **Route Classification** - Predict PTN tier from features (headway, speed, trip_count, direction_count) using decision tree / random forest. Validates whether PTN tier assignment aligns with operational reality.

4. **Coverage Category Prediction** - Predict High/Medium/Low coverage from DA demographics (population density, employment mix).

## Spatial Service Analysis

5. **Neighbourhood Service Ranking** - Rank neighbourhoods by stop density, jobs access, and priority score to identify the most underserved areas.

6. **Coverage Change Mapping** - Compare pre-PTN and current neighbourhood access metrics with straightforward change maps and ranked deltas.

## Before/After (Historical GTFS)

7. **Pre-PTN vs Post-PTN Comparison** - Headway, coverage, jobs access, and speed changes per route using historical GTFS feeds.

8. **Equity Impact** - Did PTN improvements benefit high-need DAs? Cross-reference census population + employment data with service changes.

## Accessibility

9. **Neighbourhood Transit Access** - Compare neighbourhood stop density, walkability, and jobs access using the shared `CoverageAnalyzer` outputs.

10. **Jobs Accessibility** - Employment destination proxy reachable by transit using DA-level CBP and workplace context data.

## Network Mining

11. **Transfer Penalty Analysis** - NetworkX shortest paths for transfer counts pre/post PTN. Map transfer hotspots and identify routes where transfers increased/decreased.

12. **Community Detection** - Louvain on transit graph to find service clusters (already implemented in `NetworkAnalyzer.detect_communities()`).

## Additional Data Sources

13. **Census DA Data** - Population, workplace context, and jobs-proxy data at DA level for equity and employment-access analysis.

14. **Current Service Validation** - Use Winnipeg Transit API v4 stop schedules, trip schedules, and trip planner snapshots to validate current reliability stories.

## GitHub Issues Status

Already implemented in codebase but issues still open:
- #6 Degree Centrality - `NetworkAnalyzer.degree_centrality()`
- #7 Betweenness Centrality - `NetworkAnalyzer.betweenness_centrality()`
- #8 Community Detection - `NetworkAnalyzer.detect_communities()` (Louvain)
- #9 Underserved Areas - `CoverageAnalyzer.underserved_neighbourhoods()`
- #10 Coverage Outlier Detection - `CoverageAnalyzer.outliers()` (IQR + z-score)
- #11 Enhanced Kepler.gl - `maps.build_kepler_config()`
- #12 Overture Maps - PR2/final project enhancement (not yet started)
