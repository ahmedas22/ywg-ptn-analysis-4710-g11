# Final Report Expansion Ideas (April 6, 2026)

These ideas should be hinted at in the PR2 methodology section and fully developed for the final report.

## Case Studies

### Kenaston Corridor ($757M Road Widening vs Transit ROI)
- 78 property expropriations in River Heights
- Meanwhile $8M/4yr for entire downtown plan (CentrePlan 2050)
- Our transit travel_seconds data can compute: "what if this $757M went to BRT instead?"

### CentrePlan 2050 Ambition Gap
- $8M/4yr for a 30-year downtown plan vs $757M for ONE road widening
- Investment priorities don't match stated goals

### Rail Divide Spatial Analysis
- CN/CP corridors physically split the city
- Louvain community detection will show this as a graph boundary
- Service communities don't cross rail lines

### Parking Lot Desert Analysis
- 154 downtown surface parking lots
- City HEDI program offers tax increment financing to convert them
- Our stop_density data quantifies walkability damage
- Great-West Life: 825-vehicle lot operating illegally since 2002 (CBC)

### Portage Place Redevelopment
- 1987 suburban-style mall now being demolished
- $650M mixed-use redevelopment (health centre + 15-storey housing)
- True North/SCO, completion 2028
- THE positive redevelopment story

### Polo Park
- 60% of site is parking (5,600 vehicles)
- Master plan now "Landscape First" approach to add residential
- Transit hub serves BLUE, FX4, D15, D16
- Our passup + ontime data shows this hub's service quality

## Analytical Expansions

### 3+ Pre-PTN Feeds
- Symmetric longitudinal analysis with more historical data points

### Ridership per Route-km
- Inner city efficient vs suburban subsidized
- Quantifies cross-subsidization

### TOD Gap Analysis
- High-density residential with low stop density = no transit-oriented development
- Measurable as `population_density - stop_density_per_km2` gap

### HAF Zoning + PTN Synergy
- 4 units per lot within 800m of frequent transit (June 2025)
- Our H3 analysis directly maps which areas qualify

## Narrative Themes

### Public Safety + Transit
- High-frequency transit creates natural surveillance (Jane Jacobs "eyes on the street")
- Abandoned/low-frequency routes create unsafe environments
- Connecting transit investment to safety outcomes

### Ahmed's Urbanism Thesis
- Winnipeg's growth waves: 1890s boom -> 1920s -> 1950s car era -> 1990s sprawl -> NOW: redevelopment era
- PTN is the transit signal of the redevelopment era
- Best neighbourhoods (Wolseley, Osborne Village, Corydon, River Heights) grew organically in streetcar era
- Model future development on these patterns, not car-era sprawl

### Historical Context
- Streetcar peak: 400 cars, 200 km track, 60M riders/year (Bellamy; MTHA)
- Last streetcar: September 19, 1955
- Winnipeg Transit Photo Collection: 6,000+ prints from 1880s (City Archives)

### Equity Narrative
- Third spaces (NAICS 44-45,71,72,81 from CBP) as proxy for transit ridership anchors
- Stroads, slip lanes, parking lots, no sidewalks: last-mile failure
- Winter: -30C, frostbite at exposed stops, median island stops
- The Forks (#1 destination) = 15 min walk even from Blue BRT: stop placement failure
- BIZ survey: 84% dissatisfied, +22min commute, 50% modal shift, 70% reduced downtown visits
- Lucas (2012) double penalty: low mobility x low spatial destinations = destination desert

### Policy Connection Boxes
- Link each finding to specific city decisions
- Brent Bellamy: "91% of growth in car-oriented suburbs"

## Research Sources

### Academic
- Lucas, K. (2012) Transport and Social Exclusion. Transport Policy, 20: 105-113.
- Allen, J. & Farber, S. (2020) Planning transport for social inclusion. Transportation Research Part D.
- Farber, E. et al. (2024) GTFS2STN. arXiv:2405.02760.
- Bao, T. et al. (2020) PubtraVis. ISPRS Int. J. Geo-Inf., 9(4).
- Ho, S. (2024) Transit unreliability clusters: Winnipeg. UManitoba thesis.
- Boeing, G. (2021) Street Network Models. Geographical Analysis.
- Iacono, M. et al. (2010) Measuring Non-Motorized Accessibility. J. Transport Geography, 18: 133-140.
- Sato et al. (2024) city2graph. University of Liverpool.

### Standards
- TCQSM (TRB, 2013) -- 400m pedestrian catchment.
- NACTO Transit Street Design Guide.
- WalkScore Transit Score methodology (2011) -- walkscore.com/transit-score-methodology.shtml.

### Winnipeg-Specific
- Chicken, S. (2024) Why Winnipeg doesn't know who's riding. The Narwhal.
- Downtown Winnipeg BIZ / Probe Research (Dec 2025) -- rider satisfaction n=1,395.
- Bellamy, B. -- WFP urbanism columns; RAIC Advocate for Architecture Award.
- City of Winnipeg WTMP (2024-2025) -- Primary Transit Network.
- City of Winnipeg CentrePlan 2050 -- downtown 30-year vision.
- City of Winnipeg HAF zoning (2025) -- 4 units per lot near transit.
- Cushman & Wakefield (2025) -- 18.6% downtown office vacancy Q4 2025.
- CBC (2025-2026) -- Kenaston/Route 90 $757M widening coverage.

### Policy
- Marohn, C. (2020) The Growth Ponzi Scheme. Strong Towns.

## Data Quality Framework (for methodology section)

1. **Accuracy**: GTFS = official Winnipeg Transit published schedules. Census = Statistics Canada official. Open Data = City of Winnipeg operational records. No third-party transformations.
2. **Completeness**: Pipeline checks `relation_exists()` before every query. Feed regime registry ensures all required feeds are loaded. Missing infrastructure data treated as 0 (flagged as "insufficient data").
3. **Consistency**: `feed_id` column on every table prevents era mixing. `ptn_era` views enforce temporal isolation. City prefix (`ywg_`) ensures namespace separation.
4. **Timeliness**: 4 GTFS feeds spanning Apr-Dec 2025 (6 months of network evolution). Census 2021 (4-year lag -- documented limitation). BIZ survey Dec 2025 (6 months post-launch).
5. **Validity**: SQL schema types enforced. `_validate_identifier()` prevents SQL injection. GTFS >24h times handled with `SPLIT_PART + TRY_CAST`. Negative travel times filtered. NULL propagation via COALESCE.

## Census Data Available (from ywg_census_da)
- commute_total, commute_public_transit, commute_car_truck_van, commute_walked, commute_bicycle
- recent_immigrants_2016_2021, median_household_income_2020

## ywg_census_poverty_2021
- From Winnipeg Open Data `ige9-5jxk` -- "Higher Poverty Areas From the 2021 Census"
- GeoJSON with poverty area boundaries (LIM-AT rates)
- Loaded but NEVER QUERIED in analysis -- opportunity for Sudipta's equity overlay
