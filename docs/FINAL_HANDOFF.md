# Final Report Handoff — Team Tasks

**Deadlines:**
- **Mar 26 (Wed):** Checkpoint — first draft of your sections due
- **Mar 31 (Mon):** Content freeze — no new text after this date
- **Apr 6 (Sun):** Final submission
- **Apr 7-9 (Mon-Wed):** Presentations

---

## Getting Started

```bash
git pull origin main
cd ywg-ptn-analysis-4710-g11
source .venv/bin/activate   # or: uv sync && source .venv/bin/activate
make test                   # should pass 35+ tests
```

The report source is `reports/final/main.tex`. Edit ONLY your assigned sections (marked with `%=== YOUR_NAME WRITES:` comments). Compile with:
```bash
cd reports/final && pdflatex main.tex && pdflatex main.tex
```

---

## Cathy Li — Literature Review + Network Communities

### Task 1: Rewrite Literature Review (~1.5 pages)

**Location:** `reports/final/main.tex`, Section 2 (lines ~184-217)

Rewrite Ahmed's scaffold in your voice. The narrative thread is:

**Leo (theory) → CCPA (warning) → Ho (methodology peer) → Agominab (planning context) → Our Work (empirical test + prescriptive fix)**

- **Leo (2013):** Winnipeg plans aspirationally but develops chaotically
- **CCPA (2018):** Warned income levels must be considered (pages 70-74)
- **Ho (2024):** Used spatial scan statistics on same city — closest peer study
- **Agominab (2023):** Only 24% of OurWinnipeg policies are specific enough to implement
- **Our Work:** First empirical test using 10 DM techniques

**Must cite:** Allen & Farber 2020, Lucas 2012, Conway 2017, Singh 2022, CCPA 2018

**Must add:** Leo 2013 (aspirational planning), Agominab 2023 (24% specific policies), Allen & Farber depth on competitive accessibility, Lucas transport poverty definition

**Key claim:** Prior work predicted the equity gap; our 10 DM techniques validate it.

**DO NOT remove** the "2018 CCPA prediction" paragraph — it's the crown of the lit review.

### Task 2: Network Communities Interpretation (~0.5 page)

**Location:** `reports/final/main.tex`, Section 5.6 (lines ~447-461)

**Key framing:** "Blind mathematical validation" — Louvain knew nothing about rail tracks; it rediscovered the CN/CP divide from graph topology alone (Goods Movement Study 2022 confirms this physically).

Report: N communities, alignment with rail corridors, top hubs by betweenness. Reference figures: `network_communities_pr2.png`, `community_boundary_alignment.png`.

### Reading List:
- Conway 2017 — `.tmp/literature/Conway_Accessibility_TRB_2017.pdf`
- Singh 2022 — `.tmp/literature/Singh_BLUE_BRT_JTLU_2022.pdf`
- Leo 2013 — `.tmp/literature/Leo_Aspirational_Planning_2013.pdf`
- CCPA 2018 — `.tmp/literature/CCPA_Winnipeg_Mobility_2018.pdf` (pages 70-74)
- Agominab 2023 — `.tmp/literature/Agominab_Compact_Cities_2023.pdf`

---

## Sudipta Sarker — Equity Sections

### Task 1: Fill [NB~0.7] Placeholders (~0.5 page each section)

**Location:** `reports/final/main.tex`, Sections 5.7, 5.8, 5.9

**Values:** See `docs/CLAIMS_SHEET.md` — run the Python snippets to get real numbers.

**Replace ALL `[NB~0.7]` placeholders** with actual values from the claims sheet.

### Section 5.7: Equity by Income Quintile
- **Key framing:** Lucas (2012) "Double Penalty" — Q1 faces BOTH reduced transit access (Penalty 1) AND fewer destinations (Penalty 2). PTN exacerbates by forcing transfers.
- **Figure:** `equity_quintile.png`

### Section 5.8: Poverty Overlay
- **Key claim:** Do LIM-AT poverty areas have systematically worse transit access?
- **Figure:** `poverty_correlation.png`

### Section 5.9: Prescriptive Counterfactual
- Report which neighbourhoods gained/lost priority under equity weighting
- **Figure:** `counterfactual_rerank.png`

### Optional: Replace Placeholder Figures
If you have time, open `notebooks/1.4-sudipta-final-equity.ipynb` and replace the placeholder figures with real charts from the data in `data/exports/teammate/`.

### Reading List:
- Allen & Farber 2020 — income-based accessibility gaps
- Lucas 2012 — transport poverty = transport disadvantage + social disadvantage
- CCPA 2018 — "consideration should be given to income levels"
- BIZ/Probe 2026 — 84% dissatisfied (see CLAIMS_SHEET.md)

### API Quick Reference:
```python
from ptn_analysis.context import TransitContext
ctx = TransitContext.from_defaults()
eq = ctx.equity()

eq.travel_time_equity_report()       # → Q1-Q5 comparison
eq.poverty_transit_correlation()     # → scatter data
eq.equity_weighted_accessibility()   # → counterfactual reranking
eq.poverty_overlay()                 # → LIM-AT/MBM overlay
```

Or load pre-computed data (no DB needed):
```python
import pandas as pd
report = pd.read_parquet("data/exports/teammate/equity_report.parquet")
poverty = pd.read_parquet("data/exports/teammate/poverty_correlation.parquet")
counterfactual = pd.read_parquet("data/exports/teammate/counterfactual.parquet")
```

---

## Stephenie Michael — Dashboard Description + Figure QA

### Task 1: Dashboard Description (~0.3 page)

**Location:** `reports/final/main.tex`, Section 4.3 (lines ~351-358)

1. Run: `make dashboard`
2. Take a screenshot → save as `reports/final/figures/dashboard_screenshot.png`
3. Describe the 8 tabs: Overview, Map, Coverage/Equity, Network, Frequency, Live, Equity Deep Dive, Densification Alignment
4. Focus on Equity Deep Dive tab and Densification Alignment tab

### Task 2: Figure QA

See `docs/FIGURE_QA_FINAL.md` for the checklist. Verify all figures in `reports/final/figures/` are:
- Readable at print size
- Consistent colour palette
- Properly labelled axes
- No clipped text

---

## Git Workflow

1. Edit ONLY your LaTeX sections
2. Commit with your name: `git commit -m "feat(report): [Your Name] - [brief description]"`
3. Push to main: `git push origin main`
4. If merge conflict: pull first, resolve, then push

---

## Presentation Assignments

| Speaker | Time | Slides |
|---|---|---|
| Ahmed | 4 min | 1-4, 7-8 (Hook, Thesis, Key Findings, Recommendations) |
| Sudipta | 2.5 min | 5 (Association Rules, Equity, Double Penalty) |
| Cathy | 2.5 min | 6 (Classification smoking gun, Network communities, Rail divide) |
| Stephenie | 1 min | Q&A support, dashboard visual explanation |

**Q&A Defense (top 3):**
1. "Why not ridership data?" → FIPPA confirms it doesn't exist
2. "How generalizable?" → Methodology transferable, findings Winnipeg-specific
3. "What about safety?" → FIPPA 24-12-2017: no safety tracking methodology exists
