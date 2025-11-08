"""Generate lightweight sample datasets so the FanSight pipeline can run end-to-end."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    games = pd.DataFrame(
        [
            {
                "game_id": 1,
                "game_date": "2024-01-05",
                "home_team": "Metro Meteors",
                "visitor_team": "Coast Captains",
                "attendance": 15870,
                "capacity": 18000,
                "ticket_price": 68.0,
                "win_pct_home": 0.55,
                "win_pct_visitor": 0.48,
                "promotion_flag": 1,
                "campaign_channel": "email",
                "day_of_week": "Friday",
                "month": "January",
                "is_rivalry": 0,
                "attendance_lag_1": 15010,
                "attendance_lag_3": 14980,
            },
            {
                "game_id": 2,
                "game_date": "2024-01-12",
                "home_team": "Metro Meteors",
                "visitor_team": "Capital Comets",
                "attendance": 16420,
                "capacity": 18000,
                "ticket_price": 72.0,
                "win_pct_home": 0.57,
                "win_pct_visitor": 0.61,
                "promotion_flag": 0,
                "campaign_channel": "sms",
                "day_of_week": "Friday",
                "month": "January",
                "is_rivalry": 1,
                "attendance_lag_1": 15870,
                "attendance_lag_3": 15010,
            },
            {
                "game_id": 3,
                "game_date": "2024-01-19",
                "home_team": "Metro Meteors",
                "visitor_team": "Harbor Hawks",
                "attendance": 17210,
                "capacity": 18000,
                "ticket_price": 75.0,
                "win_pct_home": 0.6,
                "win_pct_visitor": 0.5,
                "promotion_flag": 1,
                "campaign_channel": "social",
                "day_of_week": "Friday",
                "month": "January",
                "is_rivalry": 0,
                "attendance_lag_1": 16420,
                "attendance_lag_3": 15870,
            },
            {
                "game_id": 4,
                "game_date": "2024-02-02",
                "home_team": "Metro Meteors",
                "visitor_team": "Desert Drakes",
                "attendance": 17750,
                "capacity": 18000,
                "ticket_price": 80.0,
                "win_pct_home": 0.62,
                "win_pct_visitor": 0.52,
                "promotion_flag": 1,
                "campaign_channel": "email",
                "day_of_week": "Friday",
                "month": "February",
                "is_rivalry": 0,
                "attendance_lag_1": 17210,
                "attendance_lag_3": 16420,
            },
        ]
    )
    games["game_date"] = pd.to_datetime(games["game_date"])

    fans = pd.DataFrame(
        [
            {
                "fan_id": 100,
                "segment": "Loyal",
                "tenure_days": 1500,
                "loyalty_score": 0.92,
                "avg_spend": 145.0,
                "lifetime_value": 5200.0,
                "price_sensitivity": 0.2,
                "engagement_score": 0.88,
                "home_team": "Metro Meteors",
            },
            {
                "fan_id": 101,
                "segment": "Value",
                "tenure_days": 600,
                "loyalty_score": 0.65,
                "avg_spend": 95.0,
                "lifetime_value": 2100.0,
                "price_sensitivity": 0.55,
                "engagement_score": 0.73,
                "home_team": "Metro Meteors",
            },
            {
                "fan_id": 102,
                "segment": "New",
                "tenure_days": 120,
                "loyalty_score": 0.4,
                "avg_spend": 60.0,
                "lifetime_value": 500.0,
                "price_sensitivity": 0.7,
                "engagement_score": 0.51,
                "home_team": "Metro Meteors",
            },
        ]
    )

    touches = pd.DataFrame(
        [
            {
                "campaign_id": "EML-001",
                "fan_id": 100,
                "game_id": 1,
                "touch_date": "2023-12-31",
                "campaign_channel": "email",
                "campaign_spend": 1.2,
                "conversion": 1,
                "promotion_flag": 1,
                "variant": "treatment",
            },
            {
                "campaign_id": "SMS-015",
                "fan_id": 101,
                "game_id": 2,
                "touch_date": "2024-01-07",
                "campaign_channel": "sms",
                "campaign_spend": 0.9,
                "conversion": 1,
                "promotion_flag": 0,
                "variant": "control",
            },
            {
                "campaign_id": "SOC-020",
                "fan_id": 102,
                "game_id": 3,
                "touch_date": "2024-01-15",
                "campaign_channel": "social",
                "campaign_spend": 1.5,
                "conversion": 0,
                "promotion_flag": 1,
                "variant": "treatment",
            },
            {
                "campaign_id": "EML-002",
                "fan_id": 100,
                "game_id": 4,
                "touch_date": "2024-01-28",
                "campaign_channel": "email",
                "campaign_spend": 1.1,
                "conversion": 1,
                "promotion_flag": 1,
                "variant": "treatment",
            },
            {
                "campaign_id": "SOC-021",
                "fan_id": 101,
                "game_id": 4,
                "touch_date": "2024-01-30",
                "campaign_channel": "social",
                "campaign_spend": 1.3,
                "conversion": 0,
                "promotion_flag": 1,
                "variant": "control",
            },
        ]
    )
    touches["touch_date"] = pd.to_datetime(touches["touch_date"])

    games.to_csv(PROCESSED_DIR / "games.csv", index=False)
    fans.to_csv(PROCESSED_DIR / "fans.csv", index=False)
    touches.to_csv(PROCESSED_DIR / "campaign_touches.csv", index=False)
    print(f"Wrote sample datasets to {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
