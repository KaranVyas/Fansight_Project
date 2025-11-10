"""Fetch NBA game logs (with attendance) and save them under data/raw/."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import List

import pandas as pd
from nba_api.stats.endpoints import leaguegamelog

DEFAULT_SLEEP = 1.2  # seconds between API calls to avoid rate limiting
PROJECT_ROOT = Path(__file__).resolve().parents[2]


def season_strings(start: int, end: int) -> List[str]:
    """Return NBA-style season strings (e.g., 2018-19)."""

    if start >= end:
        raise ValueError("`end` season must be greater than `start`.")
    seasons = []
    for year in range(start, end):
        seasons.append(f"{year}-{str(year + 1)[-2:]}")
    return seasons


def fetch_season(season: str, timeout: int = 30) -> pd.DataFrame:
    """Call the NBA API for a single season."""

    endpoint = leaguegamelog.LeagueGameLog(
        season=season,
        timeout=timeout,
        season_type_all_star="Regular Season",
    )
    df = endpoint.get_data_frames()[0]
    df["SEASON"] = season
    return df


def main() -> None:
    parser = argparse.ArgumentParser(description="Download NBA game logs and save them to data/raw/.")
    parser.add_argument("--start", type=int, default=2018, help="First season start year (e.g., 2018 for 2018-19).")
    parser.add_argument("--end", type=int, default=2025, help="Last season start year (non-inclusive).")
    parser.add_argument(
        "--sleep",
        type=float,
        default=DEFAULT_SLEEP,
        help="Seconds to sleep between API calls to stay under rate limits.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "data" / "raw" / "nba_games.csv",
        help="Where to save the compiled CSV.",
    )
    args = parser.parse_args()

    frames = []
    for season in season_strings(args.start, args.end):
        print(f"Fetching {season}…")
        df = fetch_season(season)
        frames.append(df)
        time.sleep(args.sleep)

    compiled = pd.concat(frames, ignore_index=True)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    compiled.to_csv(args.output, index=False)
    print(f"Saved {len(compiled):,} rows to {args.output}")


if __name__ == "__main__":
    main()
