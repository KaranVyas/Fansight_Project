"""Fetch NBA attendance from Basketball-Reference schedules and save to data/raw/."""

from __future__ import annotations

import argparse
from io import StringIO
from pathlib import Path
from typing import List

import pandas as pd
import requests
import time

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BASE_URL = "https://www.basketball-reference.com/leagues/NBA_{season}_games-{month}.html"
MONTHS = ["october", "november", "december", "january", "february", "march", "april", "may", "june"]


def fetch_month(season_end_year: int, month: str, session: requests.Session, retries: int = 5) -> pd.DataFrame | None:
    url = BASE_URL.format(season=season_end_year, month=month)
    for attempt in range(retries):
        resp = session.get(url, timeout=30)
        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            wait = 5 * (attempt + 1)
            print(f"Rate limited on {url}. Sleeping {wait}s before retry.")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        break
    else:
        raise RuntimeError(f"Failed to fetch {url} after {retries} attempts.")
    tables = pd.read_html(StringIO(resp.text))
    if not tables:
        return None
    df = tables[0]
    df = df.dropna(subset=["Visitor/Neutral"])
    df["Season"] = f"{season_end_year-1}-{str(season_end_year)[-2:]}"
    df["Month"] = month.title()
    df.rename(
        columns={
            "Visitor/Neutral": "visitor_team",
            "Home/Neutral": "home_team",
            "Start (ET)": "start_et",
            "Attend.": "attendance",
        },
        inplace=True,
    )
    return df[
        [
            "Season",
            "Month",
            "Date",
            "visitor_team",
            "home_team",
            "attendance",
            "start_et",
            "Notes",
        ]
    ]


def fetch_schedule(season_end_year: int, session: requests.Session) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for month in MONTHS:
        data = fetch_month(season_end_year, month, session)
        if data is not None:
            frames.append(data)
            time.sleep(2.0)
    if not frames:
        raise RuntimeError(f"No data fetched for season ending {season_end_year}")
    return pd.concat(frames, ignore_index=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Download attendance by game from Basketball-Reference.")
    parser.add_argument("--start", type=int, default=2019, help="First season end year (e.g., 2019 for 2018-19).")
    parser.add_argument("--end", type=int, default=2025, help="Last season end year (non-inclusive).")
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "nba_attendance.csv",
        help="Where to save the compiled CSV.",
    )
    args = parser.parse_args()

    frames: List[pd.DataFrame] = []
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
            "Referer": "https://www.basketball-reference.com/",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        }
    )
    session.get("https://www.basketball-reference.com/", timeout=30)
    for season in range(args.start, args.end):
        print(f"Fetching season ending {season}…")
        frames.append(fetch_schedule(season, session))

    compiled = pd.concat(frames, ignore_index=True)
    compiled["attendance"] = pd.to_numeric(compiled["attendance"], errors="coerce").astype("Int64")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    compiled.to_csv(args.output, index=False)
    print(f"Saved {len(compiled):,} rows to {args.output}")


if __name__ == "__main__":
    main()
