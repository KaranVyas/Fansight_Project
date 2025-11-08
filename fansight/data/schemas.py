"""Schema declarations used to validate FanSight datasets."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List


@dataclass(frozen=True)
class TableSchema:
    """Simple schema object describing required and optional columns."""

    name: str
    required: List[str]
    optional: List[str]

    def validate(self, columns: List[str]) -> Dict[str, List[str]]:
        """Return missing/extra columns for easy diagnostics."""

        missing = sorted(set(self.required) - set(columns))
        extras = sorted(set(columns) - set(self.required) - set(self.optional))
        return {"missing": missing, "extra": extras}


GAME_SCHEMA = TableSchema(
    name="games",
    required=[
        "game_id",
        "game_date",
        "home_team",
        "visitor_team",
        "attendance",
        "capacity",
        "ticket_price",
        "win_pct_home",
        "win_pct_visitor",
    ],
    optional=[
        "promotion_flag",
        "campaign_id",
        "day_of_week",
        "month",
        "is_rivalry",
        "attendance_lag_1",
        "attendance_lag_3",
        "season_id",
        "team_id",
        "team_abbreviation",
        "team_name",
        "wl",
        "min",
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
        "pts",
        "plus_minus",
        "video_available",
        "season",
        "arena",
        "notes",
        "source_file",
        "ticket_price",
        "win_pct_home",
        "win_pct_visitor",
    ],
)

FAN_SCHEMA = TableSchema(
    name="fans",
    required=[
        "fan_id",
        "segment",
        "tenure_days",
        "loyalty_score",
        "avg_spend",
        "lifetime_value",
        "price_sensitivity",
        "engagement_score",
    ],
    optional=[
        "home_team",
        "favorite_player",
        "city",
        "email_opt_in",
    ],
)

CAMPAIGN_SCHEMA = TableSchema(
    name="campaign_touches",
    required=[
        "campaign_id",
        "fan_id",
        "touch_date",
        "campaign_channel",
        "campaign_spend",
        "conversion",
    ],
    optional=[
        "game_id",
        "promotion_flag",
        "creative",
    ],
)
