# FanSight – Fan Engagement & Revenue Intelligence

FanSight turns ticketing, engagement, and marketing data into actionable models that forecast attendance, score loyalty, and explain the revenue lift of every campaign. The repo now focuses exclusively on the FanSight workflow; all legacy NBA-specific notebooks and code have been removed so you can learn from a lean, modern stack.

## Project Structure

```
fansight_project/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/
│   │   ├── nba_games.csv           # Scraped from nba_api
│   │   ├── nba_attendance.csv      # Scraped/downloaded from Basketball-Reference
│   │   └── arena_capacity.csv      # Team → arena → capacity lookup
│   └── processed/
│       └── games.csv               # Unified fan-game dataset (built via script below)
└── fansight/
    ├── config.py                   # global paths + feature/model defaults
    ├── data/                       # schemas, loaders, ETL helpers
    ├── features/                   # feature engineering + segmentation
    ├── marketing/                  # A/B testing utilities
    ├── models/                     # forecasting interfaces
    ├── reporting/                  # Plotly dashboards
    ├── scripts/                    # data collectors + pipeline runner
    └── pipeline.py                 # high-level orchestration
```

## Quickstart

1. **Create a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. **(Optional) Generate synthetic data**
   ```bash
   python -m fansight.scripts.generate_sample_data
   ```
   This writes small CSVs to `data/processed/` so you can exercise the pipeline without proprietary inputs.

3. **Build the real games dataset**
   ```bash
   python -m fansight.scripts.fetch_nba_games --start 2018 --end 2025
   python -m fansight.scripts.fetch_bref_attendance --start 2019 --end 2025   # or drop your manual files into data/raw/
   python -m fansight.scripts.build_games_dataset
   ```
   The last command merges the NBA stats log, attendance tables, win percentages, arena capacities, and placeholder ticket prices into `data/processed/games.csv`.

4. **Run the full pipeline**
   ```bash
   python -m fansight.scripts.run_pipeline
   ```
   You will see ETL stats, model metrics, segmentation quality, A/B results, and the count of dashboard figures created. The trained model artifact is stored under `fansight_artifacts/` (ignored by git).

5. **Swap in real club data**
   - Replace `data/raw/nba_games.csv` and `nba_attendance.csv` with your ticketing feed, attendance logs, and pricing exports.
   - Drop actual CRM/marketing tables into `data/processed/` (or extend the ETL to build them).
   - Update `fansight/data/schemas.py` and `fansight/config.FeatureConfig` as needed.
   - Re-run `python -m fansight.scripts.build_games_dataset` and `python -m fansight.scripts.run_pipeline`.

## Data You Can Use

| Need | Where to Get It | Notes |
| --- | --- | --- |
| Historical attendance, ticket prices, opponent context | [NBA Stats API](https://github.com/swar/nba_api), [Sports Reference](https://www.basketball-reference.com/), [Kaggle: NBA Game Attendance](https://www.kaggle.com/datasets/nathanlct/nba-game-attendance) | Provides box scores, attendance, standings. Use `nba_api.stats.endpoints.LeagueGameLog` to automate pulls. |
| Ticket supply/demand & pricing | [SeatGeek API](https://platform.seatgeek.com/), [Ticketmaster Discovery API](https://developer.ticketmaster.com/products-and-docs/apis/discovery/) | Both expose event inventory, average price, and geo metadata for sports teams. |
| Marketing & CRM engagement | Platform exports (Braze, Iterable, HubSpot), [Meta Ads Library](https://www.facebook.com/ads/library/), [Google Analytics 4](https://developers.google.com/analytics) | Export campaign touches, email/SMS impressions, and site conversions to tie into FanSight’s A/B utilities. |
| Sponsorship & fan value signals | [SponsorUnited](https://www.sponsorunited.com/) datasets, [YouGov SportsIndex](https://business.yougov.com/product/sportsindex), [FiveThirtyEight’s sports surveys](https://github.com/fivethirtyeight/data) | Helpful for segmenting fans by brand affinity and spend potential. |
| Weather & local context | [NOAA API](https://www.ncei.noaa.gov/support/access-data-service-api), [OpenWeather](https://openweathermap.org/api) | Merge with game day records to explain attendance dips. |

> Tip: Start by replicating the synthetic schema (see `fansight/data/schemas.py`) using one public dataset plus a simple marketing log (even if it’s mock A/B variants). You can then layer in proprietary feeds when ready.

## Extending the Pipeline

- **Features** – add engineered columns in `fansight/features/engineering.py` and register them in `fansight/config.FeatureConfig`.
- **Models** – plug in additional regressors/classifiers in `fansight/models/` and expose them through `FanSightPipeline`.
- **Segmentation** – swap in Gaussian Mixture Models or hierarchical clustering via `fansight/features/segmentation.py`.
- **Reporting** – expand `fansight/reporting/dashboards.py` with Plotly subplots or export to Tableau-ready CSVs.

## Housekeeping

- `data/raw/` and `data/processed/` are empty by default and ignored by git (only `.gitkeep` files remain so the folders exist).
- `fansight_artifacts/` is ignored automatically—feel free to delete it if you want to start fresh.
- When sharing the project, re-run `python -m fansight.scripts.generate_sample_data` to regenerate the toy datasets instead of committing them.
