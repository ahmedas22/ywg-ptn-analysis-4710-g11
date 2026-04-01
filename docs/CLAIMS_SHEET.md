# Claims Sheet — All [NB~X] Placeholder Values

> Generated 2026-03-24 from working DB. Re-run Python snippets to verify.

## Association Rules (NB 0.5)

| Placeholder | Value | Source |
|---|---|---|
| Rules discovered | **45** (6 tautological, 39 genuine) | `AssociationRuleMiner.mine_rules()` |
| Top tautological | {underserved} => {low_income, low_stop_density} | lift=6.58, conf=1.00, supp=0.152 |
| Top genuine rule | {low_income} => {high_immigrants, high_transit_commute} | lift=1.78, conf=0.59, supp=0.295 |
| Note | Top 6 rules are definitional: underserved := low_income ∧ low_stop_density | |

## Classification (NB 0.6)

| Placeholder | Value | Source |
|---|---|---|
| RF weighted F1 (demographics) | **0.81** | `cross_val_score(rf, X, y, scoring='f1_weighted')` |
| NB F1 | **0.71** | `cross_val_score(nb, X, y, scoring='f1_weighted')` |
| GBT F1 | **0.80** | `cross_val_score(gbt, X, y, scoring='f1_weighted')` |
| Silhouette score (route clustering k=4) | **0.40** | `silhouette_score()` executed NB 0.2 |
| Top 3 features | **population_density, pct_transit, pct_car** | `rf.feature_importances_` |

## Equity (NB 0.7)

| Placeholder | Value | Source |
|---|---|---|
| Q1 median access score | **52.63** | `eq.travel_time_equity_report()` |
| Q2 median access score | **38.28** | same |
| Q3 median access score | **28.83** | same |
| Q4 median access score | **20.63** | same |
| Q5 median access score | **21.78** | same |
| Q1 vs Q5 access pattern | **Inverse: Q1 highest, Q5 lowest** | Inner-city proximity effect |
| Income pattern | **Monotonically decreasing: poorest have most access** | Proximity to transit spine |
| Neighbourhoods reprioritized | **209 / 237 (88.2%)** | `eq.equity_weighted_accessibility()` |
| Top gainer neighbourhood | **Alpine Place** | `cf.nlargest(1, 'rank_change')` |
| Top gainer rank change | **+44 ranks** | same |
| 2nd gainer | **Valhalla (+37 ranks)** | same |
| 3rd gainer | **Pembina Strip (+36 ranks)** | same |

**IMPORTANT NOTE:** The income-access relationship is NON-LINEAR. Q2 (median=35.80) has higher access than Q5 (27.34). This is a middle-income advantage pattern, not a simple Q1-vs-Q5 gradient. The LaTeX narrative should reflect this nuance.

**How to fill:**
```python
from ptn_analysis.context import TransitContext
ctx = TransitContext.from_defaults()
eq = ctx.equity()

# Equity quintiles
report = eq.travel_time_equity_report()
print(report[['income_quintile', 'median_access_score', 'mean_access_score']])

# Counterfactual
cf = eq.equity_weighted_accessibility()
reprioritized = (cf['rank_change'].abs() > 0).sum()
print(f"Reprioritized: {reprioritized} / {len(cf)} ({100*reprioritized/len(cf):.1f}%)")
print(cf.nlargest(3, 'rank_change')[['neighbourhood', 'rank_change']])
```

## Network

| Metric | Value | Source |
|---|---|---|
| Node count | **3,873** | `net.stats()['node_count']` |
| Edge count | **4,663** | `net.stats()['edge_count']` |
| Communities (Louvain) | **58** | `net.detect_communities()` |
| Clustering coefficient | **0.0135** | `net.stats()['clustering_coefficient']` |
| Assortativity | **0.1498** | `net.stats()['assortativity']` |
| Density | **0.0003** | `net.stats()['density']` |
| Avg degree | **2.408** | `net.stats()['avg_degree']` |

## Externally Sourced Claims (no notebook needed)

| Claim | Value | Source |
|---|---|---|
| Ridership drop | 14% (207K -> 178K) | BIZ/Probe Jan 2026 |
| Dissatisfaction | 84% | BIZ/Probe Jan 2026 n=1,395 |
| Fare revenue loss | $8.5M | City budget documents |
| Commute time increase | 29 -> 51 min | BIZ/Probe Jan 2026 |
| Disabled dissatisfied | 77% | BIZ/Probe Jan 2026 |
| Seniors dissatisfied | 74% | BIZ/Probe Jan 2026 |
| Low-income dissatisfied | 70% | BIZ/Probe Jan 2026 |
| Operator hires 2024 | 208 | FIPPA 25-02-0228 |
| Operator hires 2023 | 135 | FIPPA 25-03-0477 |
| Engine rebuild avg cost | $53,409 | FIPPA 24-12-2015 |
| No scheduling methodology | confirmed | FIPPA 25-02-0253 |
| No vacancy tracking | confirmed | FIPPA 25-02-0262 |
| No safety tracking | confirmed | FIPPA 24-12-2017 |
| Headway change | 31.0 -> 31.9 min (+0.9, flat) | AVG(mean_headway) ywg_gtfs_route_stats feed 2024-12-15 vs current |
| Routes | 87 -> 71 (-16) | COUNT(DISTINCT route_id) by feed |
| Stops | 5,138 -> 3,873 (-25%) | COUNT(DISTINCT stop_id) ywg_stops |
| Connections | 6,960 -> 4,663 (-33%) | COUNT(*) ywg_stop_connection_counts |
| Silhouette score (route clustering k=4) | **0.40** | silhouette_score() executed notebook 0.2 |
| Sidewalk remediation cost | $334M | Ped/Cycling Strategies 2014 |
| Phase 3 survey reliable connections | 4.6/5 | WTMP Phase 3 n=1,150 |
| Phase 3 survey frequency | 4.6/5 | WTMP Phase 3 n=1,150 |
| OurWinnipeg specific policies | 24% | Agominab 2023 |
