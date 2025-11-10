"""Build processed games dataset by merging NBA stats logs with attendance."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_GAMES = PROJECT_ROOT / "data" / "raw" / "nba_games.csv"
RAW_ATTENDANCE = PROJECT_ROOT / "data" / "raw" / "nba_attendance.csv"
RAW_CAPACITY = PROJECT_ROOT / "data" / "raw" / "arena_capacity.csv"
OUTPUT = PROJECT_ROOT / "data" / "processed" / "games.csv"

# Quick placeholder average ticket prices (USD) per home team.
# Replace with real pricing data when available.
AVERAGE_TICKET_PRICE = {
    "Boston Celtics": 120,
    "Golden State Warriors": 150,
    "Los Angeles Lakers": 140,
    "New York Knicks": 160,
    "Toronto Raptors": 110,
    "Chicago Bulls": 95,
}


def compute_win_pct(games: pd.DataFrame) -> pd.DataFrame:
    games = games.copy()
    games["game_date"] = pd.to_datetime(games["GAME_DATE"])
    games["win_flag"] = (games["WL"] == "W").astype(int)
    games = games.sort_values(["TEAM_ABBREVIATION", "SEASON", "game_date", "GAME_ID"])

    group_cols = ["TEAM_ABBREVIATION", "SEASON"]
    games["games_prior"] = games.groupby(group_cols).cumcount()
    games["wins_prior"] = games.groupby(group_cols)["win_flag"].cumsum() - games["win_flag"]
    games["win_pct_prior"] = games["wins_prior"] / games["games_prior"].replace(0, pd.NA)
    return games[["GAME_ID", "TEAM_ABBREVIATION", "win_pct_prior"]]


def load_games() -> pd.DataFrame:
    games = pd.read_csv(RAW_GAMES)
    abbrev_to_name: Dict[str, str] = (
        games[["TEAM_ABBREVIATION", "TEAM_NAME"]]
        .drop_duplicates()
        .set_index("TEAM_ABBREVIATION")["TEAM_NAME"]
        .to_dict()
    )

    win_pct_lookup = compute_win_pct(games)
    home_win_pct = win_pct_lookup.rename(
        columns={
            "GAME_ID": "game_id",
            "TEAM_ABBREVIATION": "home_abbr",
            "win_pct_prior": "win_pct_home",
        }
    )
    visitor_win_pct = win_pct_lookup.rename(
        columns={
            "GAME_ID": "game_id",
            "TEAM_ABBREVIATION": "visitor_abbr",
            "win_pct_prior": "win_pct_visitor",
        }
    )

    home_games = games[games["MATCHUP"].str.contains("vs.", regex=False)].copy()
    home_games["home_abbr"] = home_games["TEAM_ABBREVIATION"]
    home_games["home_team"] = home_games["TEAM_NAME"]
    home_games["visitor_abbr"] = home_games["MATCHUP"].str.extract(r"vs\. (\w+)")
    home_games["visitor_team"] = home_games["visitor_abbr"].map(abbrev_to_name)

    home_games["game_date"] = pd.to_datetime(home_games["GAME_DATE"])
    home_games["game_id"] = home_games["GAME_ID"]

    home_games = home_games.merge(home_win_pct, on=["game_id", "home_abbr"], how="left")
    home_games = home_games.merge(visitor_win_pct, on=["game_id", "visitor_abbr"], how="left")

    stats_columns = [
        "FGM",
        "FGA",
        "FG_PCT",
        "FG3M",
        "FG3A",
        "FG3_PCT",
        "FTM",
        "FTA",
        "FT_PCT",
        "OREB",
        "DREB",
        "REB",
        "AST",
        "STL",
        "BLK",
        "TOV",
        "PF",
        "PTS",
        "PLUS_MINUS",
    ]

    rename_map = {col: col.lower() for col in stats_columns}
    rename_map.update(
        {
            "WL": "result",
            "PTS": "home_pts",
            "TEAM_ID": "team_id",
            "TEAM_ABBREVIATION": "team_abbreviation",
            "TEAM_NAME": "team_name",
            "SEASON_ID": "season_id",
            "SEASON": "season",
        }
    )

    home_games = home_games.rename(columns=rename_map)

    keep_cols = [
        "game_id",
        "game_date",
        "home_team",
        "visitor_team",
        "season_id",
        "season",
        "team_id",
        "team_abbreviation",
        "team_name",
        "result",
        "fgm",
        "fga",
        "fg_pct",
        "fg3m",
        "fg3a",
        "fg3_pct",
        "ftm",
        "fta",
        "ft_pct",
        "oreb",
        "dreb",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "pf",
        "home_pts",
        "plus_minus",
        "win_pct_home",
        "win_pct_visitor",
    ]

    return home_games[keep_cols]


def load_attendance() -> pd.DataFrame:
    df = pd.read_csv(RAW_ATTENDANCE)
    df = df.rename(
        columns={
            "Date": "game_date",
            "start_et": "start_et",
            "visitor_team": "visitor_team",
            "visitor_pts": "visitor_pts",
            "home_team": "home_team",
            "PTS.1": "home_pts_att",
            "attendance": "attendance",
            "Arena": "arena",
        }
    )
    df["game_date"] = pd.to_datetime(df["game_date"])
    cleaned_attendance = (
        df["attendance"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace("", pd.NA)
    )
    df["attendance"] = (
        pd.to_numeric(cleaned_attendance, errors="coerce")
        .round()
        .astype(pd.Int64Dtype())
    )
    df["visitor_team"] = df["visitor_team"].str.strip()
    df["home_team"] = df["home_team"].str.strip()
    df = df[
        [
            "game_date",
            "visitor_team",
            "home_team",
            "attendance",
            "arena",
            "Notes",
            "Source file",
        ]
    ].rename(columns={"Notes": "notes", "Source file": "source_file"})
    return df


def load_capacity() -> pd.DataFrame:
    if not RAW_CAPACITY.exists():
        return pd.DataFrame(columns=["home_team", "arena_capacity", "arena_name_capacity"])

    df = pd.read_csv(RAW_CAPACITY)
    df["home_team"] = df["home_team"].str.strip()
    df["arena_capacity"] = pd.to_numeric(df["capacity"], errors="coerce").astype("Int64")
    df["arena_name_capacity"] = df["arena"].str.strip()
    return df[["home_team", "arena_capacity", "arena_name_capacity"]]


def build_dataset() -> pd.DataFrame:
    games = load_games()
    attendance = load_attendance()

    merged = games.merge(
        attendance,
        on=["game_date", "home_team", "visitor_team"],
        how="left",
        suffixes=("", "_att"),
    )

    merged = merged.dropna(subset=["attendance"])
    merged = merged.sort_values("game_date").drop_duplicates("game_id", keep="first")

    capacity = load_capacity()
    if not capacity.empty:
        merged = merged.merge(capacity, on="home_team", how="left")
        merged["capacity"] = merged["arena_capacity"]
        merged = merged.drop(columns=["arena_capacity", "arena_name_capacity"], errors="ignore")
    else:
        merged["capacity"] = pd.NA

    merged["ticket_price"] = merged["home_team"].map(AVERAGE_TICKET_PRICE).fillna(100)

    ordered = merged.sort_values("game_date").reset_index(drop=True)
    return ordered


def main() -> None:
    dataset = build_dataset()
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    dataset.to_csv(OUTPUT, index=False)
    print(f"Saved {len(dataset):,} rows to {OUTPUT}")


if __name__ == "__main__":
    main()
