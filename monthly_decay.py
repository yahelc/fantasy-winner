"""
Unified time-decay model for batting rate stats.

Pulls monthly data from MLB Stats API across 2025 + 2026, applies
exponential time decay from the most recent month backward. A
compressed off-season gap (OFF_SEASON_MONTHS = 2) sits between
Sep 2025 and Apr 2026 — shorter than the literal 6-month break
because players don't regress as fast during rest as during play.

Effective weight per player-month = PA × decay_factor.
Early-April 2026 data (~5–15 PA) is naturally down-weighted vs
a full month of 2025 data (~100 PA), even though it's the freshest.
As the 2026 season accumulates, 2025 data automatically gets discounted.

Result replaces both the old flat 2025 averages and the 60/40
year blend — everything that feeds pts/G comes from here.
xwOBA is left to the season-level data (it's only a tiebreaker).
"""

import unicodedata
from datetime import date
import requests
import pandas as pd
import numpy as np

MLB_STATS_URL = "https://statsapi.mlb.com/api/v1/stats"


def _strip_accents(s: str) -> str:
    """Normalize accented characters to ASCII equivalents."""
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


# 2025 is a complete season — fixed date ranges.
_MONTHS_2025 = [
    ("Apr 2025", "2025-03-27", "2025-04-30"),
    ("May 2025", "2025-05-01", "2025-05-31"),
    ("Jun 2025", "2025-06-01", "2025-06-30"),
    ("Jul 2025", "2025-07-01", "2025-07-31"),
    ("Aug 2025", "2025-08-01", "2025-08-31"),
    ("Sep 2025", "2025-09-01", "2025-10-01"),
]

# 2026 windows — all possible months for the season.
# At runtime we include only months that have started, capping end at today.
_SEASON_2026_WINDOWS = [
    ("Apr 2026", date(2026, 3, 27), date(2026, 4, 30)),
    ("May 2026", date(2026, 5,  1), date(2026, 5, 31)),
    ("Jun 2026", date(2026, 6,  1), date(2026, 6, 30)),
    ("Jul 2026", date(2026, 7,  1), date(2026, 7, 31)),
    ("Aug 2026", date(2026, 8,  1), date(2026, 8, 31)),
    ("Sep 2026", date(2026, 9,  1), date(2026, 10, 1)),
]

OFF_SEASON_MONTHS = 2   # compressed steps between Sep 2025 and Apr 2026
DECAY = 0.85            # per-month decay factor


def _active_2026_months(today: date = None) -> list:
    """
    Return 2026 month windows that have started as of today,
    capping the end date at today so we don't request future data.
    """
    if today is None:
        today = date.today()
    active = []
    for label, start, end in _SEASON_2026_WINDOWS:
        if start > today:
            break
        actual_end = min(end, today)
        active.append((label, start.strftime("%Y-%m-%d"), actual_end.strftime("%Y-%m-%d")))
    return active


def _build_months(today: date = None) -> list:
    """Full timeline: complete 2025 season + active 2026 months through today."""
    return _MONTHS_2025 + _active_2026_months(today)


def _build_weights(months: list, off_season_months: int, decay: float) -> dict:
    """
    Assign exponential decay weights, step 0 = most recent month.
    Adds OFF_SEASON_MONTHS extra steps when crossing from 2026 to 2025,
    regardless of how many 2026 months exist in the list.
    """
    weights = {}
    step = 0
    prev_year = None
    for label, _, _ in reversed(months):
        year = int(label.split()[-1])
        if prev_year is not None and year < prev_year:
            step += off_season_months   # cross the year boundary
        weights[label] = decay ** step
        step += 1
        prev_year = year
    return weights


# Computed at import time — reflects today's date on each run.
MONTHS = _build_months()
MONTH_WEIGHTS = _build_weights(MONTHS, OFF_SEASON_MONTHS, DECAY)


def _fetch_mlb_batting_month(label: str, start: str, end: str) -> pd.DataFrame:
    """
    Fetch batting stats from MLB Stats API for a date range.
    Returns one row per player with rate stats computed from counting stats.
    """
    season = int(label.split()[-1])
    params = {
        "stats": "byDateRange",
        "group": "hitting",
        "season": season,
        "startDate": start,
        "endDate": end,
        "playerPool": "all",
        "limit": 2000,
    }
    r = requests.get(MLB_STATS_URL, params=params, timeout=20)
    r.raise_for_status()
    splits = r.json().get("stats", [{}])[0].get("splits", [])

    rows = []
    for s in splits:
        stat = s.get("stat", {})
        pa = int(stat.get("plateAppearances", 0) or 0)
        g  = int(stat.get("gamesPlayed", 0) or 0)
        if pa == 0:
            continue
        k  = int(stat.get("strikeOuts", 0) or 0)
        bb = int(stat.get("baseOnBalls", 0) or 0)
        sb = int(stat.get("stolenBases", 0) or 0)
        rows.append({
            "Name": _strip_accents(s.get("player", {}).get("fullName", "")),
            "PA":   pa,
            "G":    g,
            "AB":   int(stat.get("atBats", 0) or 0),
            "K%":   k / pa,
            "BB%":  bb / pa,
            "SB/G": sb / g if g > 0 else float("nan"),
            "OBP":  float(stat.get("obp", 0) or 0),
            "SLG":  float(stat.get("slg", 0) or 0),
        })

    return pd.DataFrame(rows)


def fetch_all_monthly_splits(min_pa_per_month: int = 10) -> pd.DataFrame:
    """
    Fetch one MLB Stats API chunk per month across 2025+2026,
    and return a long-format df with one row per (player, month).
    """
    frames = []
    for label, start, end in MONTHS:
        print(f"  Fetching {label} ({start} -> {end})...")
        try:
            df = _fetch_mlb_batting_month(label, start, end)
        except Exception as e:
            print(f"    Failed: {e}")
            continue

        df = df[df["PA"] >= min_pa_per_month].copy()
        df["month"]        = label
        df["month_weight"] = MONTH_WEIGHTS[label]

        frames.append(df[[
            "Name", "PA", "G", "AB",
            "K%", "BB%", "SB/G", "OBP", "SLG",
            "month", "month_weight",
        ]])

    return pd.concat(frames, ignore_index=True)


def compute_decayed_stats(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    For each player, compute PA-weighted x time-weighted averages
    of K%, BB%, SB/G, OBP, SLG across all months.

    effective_weight = PA x month_weight
    decayed_rate = sum(rate x eff_weight) / sum(eff_weight)

    PA_total and G_total are raw sums (no weighting) for sample-size display.
    """
    rate_cols = ["K%", "BB%", "SB/G", "OBP", "SLG"]
    results = []

    for name, grp in monthly.groupby("Name"):
        grp = grp.copy()
        grp["eff_w"] = grp["PA"] * grp["month_weight"]
        total_w = grp["eff_w"].sum()

        if total_w == 0:
            continue

        row = {
            "Name":     name,
            "PA_total": int(grp["PA"].sum()),
            "G_total":  int(grp["G"].sum()),
        }

        for col in rate_cols:
            valid = grp[grp[col].notna()]
            if valid.empty:
                row[f"{col}_decay"] = np.nan
            else:
                w = valid["eff_w"]
                row[f"{col}_decay"] = (valid[col] * w).sum() / w.sum()

        results.append(row)

    return pd.DataFrame(results)


def get_unified_decayed(min_pa_per_month: int = 10) -> pd.DataFrame:
    """
    Main entry point. Returns one row per player with unified
    PA-weighted, time-decayed rate stats across 2025+2026.
    Columns: Name, PA_total, G_total, K%_decay, BB%_decay,
             SB/G_decay, OBP_decay, SLG_decay.
    """
    print("Fetching unified monthly splits (2025+2026 time decay)...")
    monthly = fetch_all_monthly_splits(min_pa_per_month)
    decayed = compute_decayed_stats(monthly)
    print(f"  Computed decayed stats for {len(decayed)} players.")
    return decayed


# Backward-compat alias
def get_decayed_2025(min_pa_per_month: int = 10) -> pd.DataFrame:
    return get_unified_decayed(min_pa_per_month)


if __name__ == "__main__":
    from tabulate import tabulate

    print(f"\nMonth weights (DECAY={DECAY}, off-season gap={OFF_SEASON_MONTHS} months):")
    for label, w in sorted(MONTH_WEIGHTS.items(), key=lambda x: x[1], reverse=True):
        print(f"  {label}: {w:.4f}")

    decayed = get_unified_decayed()
    display = decayed.sort_values("PA_total", ascending=False).head(25).copy()
    for c in ["K%_decay", "BB%_decay", "SB/G_decay", "OBP_decay", "SLG_decay"]:
        display[c] = display[c].apply(lambda x: f"{x:.3f}" if pd.notna(x) else "-")

    print(f"\nTop 25 players by total PA:")
    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False))
