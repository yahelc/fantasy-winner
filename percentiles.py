"""
Baseball Savant percentile rankings for your roster.

Usage:
    python percentiles.py                  # next-week roster, 2026
    python percentiles.py --week current   # current ESPN lineup
    python percentiles.py --year 2025      # historical season
"""

import argparse
import io
import unicodedata
import requests
import pandas as pd
from tabulate import tabulate

from config import LEAGUE_ID, SEASON, MY_TEAM_NAME
from fetch_espn import get_league, get_my_team, get_roster_for_day

SAVANT_URL = "https://baseballsavant.mlb.com/leaderboard/percentile-rankings"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ── column definitions ──────────────────────────────────────────────────────
# (savant_col, display_name, higher_is_better, is_pct_stat)
HITTER_COLS = [
    ("xwoba",           "xwOBA",  True,  False),
    ("xba",             "xBA",    True,  False),
    ("xslg",            "xSLG",   True,  False),
    ("exit_velocity",   "EV",     True,  False),
    ("brl_percent",     "Brl%",   True,  True),
    ("hard_hit_percent","HH%",    True,  True),
    ("bat_speed",       "BSpd",   True,  False),
    ("squared_up_rate", "Sq%",    True,  True),
    ("chase_percent",   "Chs%",   False, True),
    ("whiff_percent",   "Whf%",   False, True),
    ("k_percent",       "K%",     False, True),
    ("bb_percent",      "BB%",    True,  True),
    ("sprint_speed",    "Spd",    True,  False),
]

PITCHER_COLS = [
    ("xera",            "xERA",   True,  False),
    ("xwoba",           "xwOBA",  True,  False),
    ("xba",             "xBA",    True,  False),
    ("exit_velocity",   "EV",     True,  False),
    ("brl_percent",     "Brl%",   True,  True),
    ("hard_hit_percent","HH%",    True,  True),
    ("k_percent",       "K%",     True,  True),
    ("bb_percent",      "BB%",    True,  True),
    ("whiff_percent",   "Whf%",   True,  True),
    ("chase_percent",   "Chs%",   True,  True),
    ("fb_velocity",     "FBv",    True,  False),
]


def _fetch_percentiles(player_type: str, year: int) -> pd.DataFrame:
    r = requests.get(
        SAVANT_URL,
        params={"type": player_type, "year": year, "position": "", "team": "", "csv": "true"},
        headers=HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text.lstrip("\ufeff")))
    # Savant names are "Last, First" → convert to "First Last"
    df["Name"] = df["player_name"].apply(
        lambda x: " ".join(reversed([p.strip() for p in x.split(",")])) if pd.notna(x) else x
    )
    return df


def _pct_cell(val, is_pct_stat: bool = False) -> str:
    """Format a percentile value as an integer string, or '—' if missing."""
    if pd.isna(val):
        return "—"
    return str(int(round(val))) + ("%" if is_pct_stat else "")


def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )


def _build_table(roster_names: list[str], savant_df: pd.DataFrame,
                 col_defs: list) -> pd.DataFrame:
    """Match roster players to Savant percentiles and build display table."""
    # Build accent-normalized index for matching
    savant_df = savant_df.copy()
    savant_df["Name_norm"] = savant_df["Name"].apply(_strip_accents)
    # Deduplicate by name so .loc always returns a Series, not a DataFrame
    savant_df = savant_df.drop_duplicates(subset=["Name"])
    savant_norm_df = savant_df.drop_duplicates(subset=["Name_norm"])
    savant_idx = savant_df.set_index("Name")
    savant_norm_idx = savant_norm_df.set_index("Name_norm")

    rows = []
    for name in roster_names:
        row = {"Name": name}
        name_norm = _strip_accents(name)
        if name in savant_idx.index:
            r = savant_idx.loc[name]
        elif name_norm in savant_norm_idx.index:
            r = savant_norm_idx.loc[name_norm]
        else:
            # Try last-name fallback (accent-normalized)
            last_norm = name_norm.split()[-1]
            matches = savant_df[savant_df["Name_norm"].apply(lambda x: x.split()[-1]) == last_norm]
            r = matches.iloc[0] if len(matches) == 1 else None

        for col, header, _, is_pct in col_defs:
            if r is not None and col in savant_df.columns:
                val = r[col] if isinstance(r, pd.Series) else r.get(col)
                row[header] = _pct_cell(val, is_pct)
            else:
                row[header] = "—"
        rows.append(row)

    return pd.DataFrame(rows)


def percentile_report(year: int, use_next_week: bool, league=None):
    print(f"Fetching Savant percentile rankings ({year})...")
    hitter_pct = _fetch_percentiles("batter", year)
    pitcher_pct = _fetch_percentiles("pitcher", year)
    print(f"  {len(hitter_pct)} hitters, {len(pitcher_pct)} pitchers in Savant data.")

    # Get roster
    if league is None:
        from fetch_espn import get_league
        league = get_league()

    if use_next_week:
        print("  Fetching next-week roster (Tuesday)...")
        roster = get_roster_for_day(MY_TEAM_NAME, league=league)
        if not roster:
            my_team = get_my_team(league, MY_TEAM_NAME)
            roster = my_team.roster if my_team else []
    else:
        my_team = get_my_team(league, MY_TEAM_NAME)
        roster = my_team.roster if my_team else []

    if not roster:
        print("Could not fetch roster.")
        return

    # Separate hitters and pitchers by eligible slots
    pitcher_slots = {"SP", "RP", "P"}
    hitter_names = []
    pitcher_names = []
    for p in roster:
        slots = set(p.eligibleSlots or [])
        if slots & pitcher_slots and "C" not in slots and "1B" not in slots:
            pitcher_names.append(p.name)
        else:
            hitter_names.append(p.name)

    week_label = "Next week" if use_next_week else "Current week"
    header_cols = [h for _, h, _, _is_pct in HITTER_COLS]

    # ── Hitters ──────────────────────────────────────────────────────────────
    h_table = _build_table(hitter_names, hitter_pct, HITTER_COLS)
    print(f"\n{'='*80}")
    print(f"  HITTER PERCENTILES — {week_label} roster  ({year})")
    print(f"  Savant percentiles: 99=best in MLB.  Chase%/Whiff%/K% — lower raw stat = higher pct.")
    print(f"{'='*80}")
    print(tabulate(h_table, headers="keys", tablefmt="simple", showindex=False))

    # ── Pitchers ─────────────────────────────────────────────────────────────
    p_table = _build_table(pitcher_names, pitcher_pct, PITCHER_COLS)
    print(f"\n{'='*80}")
    print(f"  PITCHER PERCENTILES — {week_label} roster  ({year})")
    print(f"  All columns: 99=best in MLB.  xwOBA/EV/Brl% = opponent metrics (Savant inverts).")
    print(f"{'='*80}")
    print(tabulate(p_table, headers="keys", tablefmt="simple", showindex=False))


def main():
    parser = argparse.ArgumentParser(description="Savant percentile rankings for your roster")
    parser.add_argument("--year",  type=int, default=SEASON, help="Season year (default: current)")
    parser.add_argument("--week",  choices=["current", "next"], default="next",
                        help="current = today's ESPN lineup, next = Tuesday roster (default)")
    args = parser.parse_args()

    print("Connecting to ESPN...")
    league = get_league()
    print(f"  Connected: {league.settings.name}")

    percentile_report(
        year=args.year,
        use_next_week=(args.week == "next"),
        league=league,
    )


if __name__ == "__main__":
    main()
