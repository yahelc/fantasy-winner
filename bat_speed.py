"""
Pulls 2026 bat speed / bat tracking data from Baseball Savant.
Merges with FanGraphs expected stats to identify breakout candidates.
"""

import io
import requests
import pandas as pd

SAVANT_BAT_SPEED_URL = (
    "https://baseballsavant.mlb.com/leaderboard/bat-tracking"
    "?attackZone=&batSide=&contactType=&count=&dateStart=2026-03-27"
    "&dateEnd=2026-12-31&gameType=R&isHardHit=&minSwings=q"
    "&minGroupSwings=1&pitchType=&positiveAngle=&venue=&csv=true"
)

HEADERS = {"User-Agent": "Mozilla/5.0"}


def get_bat_speed() -> pd.DataFrame:
    """Fetch 2026 bat speed leaderboard from Baseball Savant."""
    print("Fetching 2026 bat speed data from Baseball Savant...")
    r = requests.get(SAVANT_BAT_SPEED_URL, headers=HEADERS, timeout=15)
    r.raise_for_status()

    df = pd.read_csv(io.StringIO(r.text.lstrip("\ufeff")))

    # Normalize column names
    df = df.rename(columns={
        "id":                        "mlbam_id",
        "name":                      "Name_raw",       # "Last, First" format
        "swings_competitive":        "swings",
        "avg_bat_speed":             "bat_speed",
        "hard_swing_rate":           "hard_swing_rate",
        "squared_up_per_bat_contact":"squared_up_contact%",
        "squared_up_per_swing":      "squared_up_swing%",
        "blast_per_bat_contact":     "blast_contact%",
        "blast_per_swing":           "blast_swing%",
        "swing_length":              "swing_length",
        "whiff_per_swing":           "whiff%",
    })

    # Convert "Last, First" → "First Last"
    df["Name"] = df["Name_raw"].apply(
        lambda x: " ".join(reversed([p.strip() for p in x.split(",")])) if pd.notna(x) else x
    )

    numeric_cols = ["bat_speed", "hard_swing_rate", "squared_up_contact%",
                    "squared_up_swing%", "blast_contact%", "blast_swing%",
                    "swing_length", "whiff%", "swings"]
    for c in numeric_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Keep one row per player (highest swing count if dupes)
    df = (df.sort_values("swings", ascending=False)
            .drop_duplicates(subset=["Name"], keep="first")
            .sort_values("bat_speed", ascending=False)
            .reset_index(drop=True))
    print(f"  Got {len(df)} batters with bat speed data.")
    return df


def merge_with_statcast(bat_df: pd.DataFrame, fg_df: pd.DataFrame) -> pd.DataFrame:
    """Merge bat speed with FanGraphs expected stats on player name."""
    # Keep only the highest-PA row per player (handles mid-season trades)
    fg_deduped = (
        fg_df.sort_values("PA", ascending=False)
             .drop_duplicates(subset=["Name"], keep="first")
    )
    merged = bat_df.merge(
        fg_deduped[["Name", "Team", "PA", "PA_total", "xwOBA", "K%", "BB%", "SB",
                    "pts_per_game", "composite_score", "data_season"]],
        on="Name",
        how="left"
    )
    return merged
