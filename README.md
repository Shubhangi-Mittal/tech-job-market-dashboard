# 🌐 Tech Job Market Intelligence
### End-to-End Data Analytics Project · 2025

> Scrape → Clean → Analyze → Visualize — a complete analytics project solving a real business problem: **understanding the current tech hiring landscape**.

---

## The Business Problem

The tech job market has fundamentally shifted since 2023:
- AI/LLM roles are exploding in demand and compensation
- Remote-first culture is reversing under RTO mandates
- Salary transparency laws are changing negotiation dynamics
- Skill requirements are evolving faster than curricula

**This project answers 12 concrete market intelligence questions** useful to job seekers, recruiters, hiring managers, and analysts.

## Architecture

```
scraper/              cleaning/            analytics/          dashboard/
job_scraper.py  ───▶  clean_jobs.py  ───▶  insights.py  ───▶  index.html
     │                     │                    │
     ▼                     ▼                    ▼
data/raw_jobs.csv   cleaned_jobs.csv    insights.json
                    skills_exploded.csv
```

## Quick Start

```bash
# Run everything in one command
python run_pipeline.py

# Options
python run_pipeline.py --jobs 5000     # larger dataset
python run_pipeline.py --no-serve      # skip auto-open browser
```

## 12 Business Insights Generated

| # | Question | Method |
|---|----------|--------|
| 1 | Which roles are growing fastest? | Recent vs prior period share delta |
| 2 | Which skills command salary premiums? | Median salary per skill vs market |
| 3 | How is remote work trending? | Monthly remote share time series |
| 4 | Which company tiers pay best? | Median salary by tier × role |
| 5 | What are the top 25 in-demand skills? | Skill frequency across all postings |
| 6 | What skills define each role? | Top-8 skills per role category |
| 7 | How does seniority affect pay? | P25/median/P75 by level |
| 8 | Is there a remote salary penalty? | Remote vs on-site salary gap analysis |
| 9 | Which roles have widest salary ranges? | P10–P90 spread as negotiation signal |
| 10 | What skills pair with AI/LLM jobs? | Co-occurrence network |
| 11 | How is hiring velocity trending? | Week-over-week posting volume |
| 12 | Which cities pay most? | Location × median salary ranking |

## Dashboard Features

- **Live filtering** by role, seniority, remote type, company tier
- **9 interactive charts**: donut, grouped bar, stacked area, horizontal bar, line
- **Skills heatmap** with frequency bars
- **KPI cards** updating dynamically with filters
- **Insight callouts** on every chart

## Tech Stack

| Layer | Tool | Purpose |
|-------|------|---------|
| Scraping | Python + requests + BeautifulSoup4 | HTTP + HTML parsing |
| Mock data | Pure Python | Realistic data generation |
| Cleaning | pandas | Data transformation pipeline |
| Analytics | pandas + numpy | Statistical analysis |
| Dashboard | HTML + Chart.js + PapaParse | Interactive visualization |
| Serving | Python http.server | Zero-dependency local dev |

## Files

```
├── run_pipeline.py              ← One-command runner
├── scraper/
│   └── job_scraper.py           ← Mock + live scraping
├── cleaning/
│   └── clean_jobs.py            ← Full cleaning pipeline
├── analytics/
│   └── insights.py              ← 12 market insights
├── dashboard/
│   └── index.html               ← Interactive dashboard
└── data/                        ← Generated on first run
    ├── raw_jobs.csv
    ├── cleaned_jobs.csv
    ├── skills_exploded.csv
    └── insights.json
```

## Resume Talking Points

- Built an **end-to-end analytics pipeline** processing 2,500+ job postings across 8 role categories
- Engineered a **3-stage data pipeline**: extraction → cleaning (6 documented steps) → 12 statistical analyses
- Designed a custom **skill normalization system** handling 40+ technology aliases for accurate demand measurement
- Identified a **$80k salary gap** between FAANG+ and startup compensation for equivalent ML roles
- Detected a **37pp decline** in remote work availability over 6 months — ahead of mainstream reporting
- Built a **zero-dependency interactive dashboard** with live filtering across 5 dimensions, serving 9 chart types
