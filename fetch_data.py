"""
Pulls hitter and pitcher data from FanGraphs via pybaseball.
Includes Statcast-era expected stats (xwOBA, xERA, barrel%, etc.)

Scoring rate stats (K%, BB%, OBP, SLG, SB/G) come from unified
time-decay across 2025+2026 monthly splits — no explicit year blend.
PA-weighting ensures April 2026's tiny sample is naturally discounted
vs a full month of 2025 data.

xwOBA, xSLG, Barrel%, EV, etc. stay at season level (2026 if available,
else 2025) since monthly xwOBA is noisier and those are only tiebreakers.
"""

import pandas as pd
import pybaseball
from pybaseball import batting_stats, pitching_stats
from config import SEASON, MIN_PA, MIN_IP

pybaseball.cache.enable()

# Statcast quality metrics to PA-blend between seasons (early 2026 samples are noisy)
HITTER_BLEND_COLS  = ["xwOBA", "xSLG", "xBA", "xOBP", "EV", "Barrel%", "HardHit%"]
# Pitcher quality metrics to IP-blend between seasons
PITCHER_BLEND_COLS = ["xERA", "xFIP", "K%", "BB%", "Barrel%", "HardHit%"]


def get_hitters(season: int = SEASON, min_pa: int = MIN_PA,
                time_decay: bool = True) -> pd.DataFrame:
    """
    Pull FanGraphs batting stats and apply unified time-decay across
    2025+2026 monthly splits for scoring rate stats.

    time_decay=True (default): K%, BB%, OBP, SLG, SB/G come from
        PA-weighted exponential decay — Apr 2026 > Sep 2025 > ... > Apr 2025.
    time_decay=False: use 2026 season-level rates only (2025 supplement
        for players not yet in 2026 data).
    """
    print(f"Fetching hitter data (unified time-decay, season {season})...")

    raw = {}
    try:
        df26 = batting_stats(season, qual=1)
        df26 = df26[df26["PA"] >= min_pa]
        df26["data_season"] = season
        raw[season] = _normalize_hitter_cols(df26)
        print(f"  {season}: {len(raw[season])} hitters (≥{min_pa} PA)")
    except Exception as e:
        print(f"  {season} unavailable: {e}")

    try:
        df25 = batting_stats(season - 1, qual=100)
        df25["data_season"] = season - 1
        raw[season - 1] = _normalize_hitter_cols(df25)
        print(f"  {season-1}: {len(raw[season-1])} hitters")
    except Exception as e:
        print(f"  {season-1} unavailable: {e}")

    if time_decay:
        from monthly_decay import get_unified_decayed
        decayed = get_unified_decayed()
        df = _apply_unified_decay(raw, decayed, season)
    else:
        df = _build_base(raw, season)

    print(f"  Total: {len(df)} hitters")
    return df


def _build_base(raw: dict, season: int) -> pd.DataFrame:
    """
    Build base frame without decay: 2026 players first,
    supplement with 2025-only players.
    """
    if season in raw and (season - 1) in raw:
        cur = raw[season].copy()
        prior = raw[season - 1].copy()
        prior_only = prior[~prior["Name"].isin(set(cur["Name"]))]
        df = pd.concat([cur, prior_only], ignore_index=True)
    elif season in raw:
        df = raw[season].copy()
    else:
        df = raw[season - 1].copy()

    return df.sort_values("PA", ascending=False).drop_duplicates(subset=["Name"]).reset_index(drop=True)


def _blend_by_weight(base: pd.DataFrame, prior: pd.DataFrame,
                     blend_cols: list, weight_col: str) -> pd.DataFrame:
    """
    For players appearing in both base and prior, PA/IP-weight blend
    the given quality metric columns. Players only in one dataset
    are left unchanged.

    weight_col: the column used as the blend weight (e.g. "PA" or "IP").
    """
    prior_dedup = (prior.sort_values(weight_col, ascending=False)
                        .drop_duplicates("Name")
                        .set_index("Name"))
    base = base.copy()

    for col in blend_cols:
        if col not in base.columns or col not in prior_dedup.columns:
            continue
        for idx, row in base.iterrows():
            name = row["Name"]
            if name not in prior_dedup.index:
                continue
            cv = row.get(col)
            pv = prior_dedup.loc[name, col]
            w_cur  = row.get(weight_col, 0) or 0
            w_pri  = prior_dedup.loc[name, weight_col] if weight_col in prior_dedup.columns else 0
            w_pri  = w_pri or 0
            if pd.isna(cv) or pd.isna(pv) or (w_cur + w_pri) == 0:
                continue
            base.at[idx, col] = (cv * w_cur + pv * w_pri) / (w_cur + w_pri)

    return base


def _apply_unified_decay(raw: dict, decayed: pd.DataFrame, season: int) -> pd.DataFrame:
    """
    Build final hitter DataFrame using unified time-decay rates.

    Base player records come from season-level data (2026 if available,
    else 2025). Scoring rate stats — K%, BB%, OBP, SLG, SB/G — are
    replaced by unified decay values. Statcast quality metrics (xwOBA,
    xSLG, etc.) are PA-blended between 2025 and 2026 to prevent tiny
    early-season samples from distorting the quality tiebreaker.
    """
    base = _build_base(raw, season)

    # PA-blend Statcast quality metrics for players in both seasons
    if season in raw and (season - 1) in raw:
        base = _blend_by_weight(base, raw[season - 1], HITTER_BLEND_COLS, "PA")

    # Merge decayed rates (Name is the join key)
    base = base.merge(decayed, on="Name", how="left")

    # Replace scoring rate stats with decayed versions where available
    replacements = {
        "K%":  "K%_decay",
        "BB%": "BB%_decay",
        "OBP": "OBP_decay",
        "SLG": "SLG_decay",
    }
    for col, decay_col in replacements.items():
        if decay_col in base.columns:
            mask = base[decay_col].notna()
            base.loc[mask, col] = base.loc[mask, decay_col]

    # SB/G from decay — scorer picks this up via SB_per_g_blend column
    if "SB/G_decay" in base.columns:
        base["SB_per_g_blend"] = base["SB/G_decay"]

    # Drop intermediate decay columns (keep PA_total and G_total for display)
    drop = [c for c in base.columns
            if c.endswith("_decay") and c not in ("PA_total", "G_total")]
    base = base.drop(columns=drop, errors="ignore")

    return base.reset_index(drop=True)


_SEASON_2025_WEEKS = 26.5   # Mar 27 – Sep 28, 2025 ≈ 185 days / 7
_OPENING_DAY_2026  = "2026-03-27"


def _add_pitcher_g_total(current: pd.DataFrame, prior: pd.DataFrame,
                         season: int) -> pd.DataFrame:
    """
    Add G_total (appearances across both seasons) and weeks_sampled to
    each pitcher so score_players can compute actual apps/week.

    weeks_sampled = full 2025 season (if pitcher has 2025 data)
                  + weeks elapsed in 2026 season (if pitcher has 2026 data)
    """
    from datetime import date
    opening_day = date(*[int(x) for x in _OPENING_DAY_2026.split("-")])
    weeks_2026 = max(1.0, (date.today() - opening_day).days / 7)

    prior_dedup = (prior.sort_values("G", ascending=False)
                        .drop_duplicates("Name")
                        .set_index("Name"))

    current = current.copy()
    for idx, row in current.iterrows():
        name = row["Name"]
        g_cur = float(row.get("G") or 0)
        has_prior = name in prior_dedup.index
        g_pri = float(prior_dedup.loc[name, "G"]) if has_prior else 0.0
        current.at[idx, "G_total"] = g_cur + g_pri
        current.at[idx, "weeks_sampled"] = (
            (_SEASON_2025_WEEKS if has_prior else 0.0) + weeks_2026
        )

    return current


def _blend_ip_per_gs(current: pd.DataFrame, prior: pd.DataFrame) -> pd.DataFrame:
    """
    For pitchers in both seasons, compute IP/GS using combined raw counts:
        (IP_2026 + IP_2025) / (GS_2026 + GS_2025)

    Stores result as IP_per_GS_blended so scorer can use it instead of
    the current-season-only IP/GS, which is meaningless after 2 starts.
    """
    current = current.copy()
    if "GS" not in current.columns or "GS" not in prior.columns:
        return current

    prior_dedup = (prior.sort_values("IP", ascending=False)
                        .drop_duplicates("Name")
                        .set_index("Name"))

    for idx, row in current.iterrows():
        name = row["Name"]
        if name not in prior_dedup.index:
            continue
        gs_cur = float(row.get("GS") or 0)
        ip_cur = float(row.get("IP") or 0)
        gs_pri = float(prior_dedup.loc[name].get("GS") or 0)
        ip_pri = float(prior_dedup.loc[name].get("IP") or 0)
        total_gs = gs_cur + gs_pri
        if total_gs == 0:
            continue
        current.at[idx, "IP_per_GS_blended"] = (ip_cur + ip_pri) / total_gs

    return current


def get_pitchers(season: int = SEASON, min_ip: int = MIN_IP) -> pd.DataFrame:
    """
    Pull FanGraphs pitching stats. Tries current season first, falls back to prior.
    """
    print(f"Fetching pitcher data for {season}...")

    current, prior = None, None

    try:
        current = pitching_stats(season, qual=min_ip)
        current["data_season"] = season
        print(f"  {season}: {len(current)} pitchers")
    except Exception as e:
        print(f"  {season} unavailable: {e}")

    try:
        prior = pitching_stats(season - 1, qual=30)
        prior["data_season"] = season - 1
        print(f"  {season-1}: {len(prior)} pitchers (prior season supplement)")
    except Exception as e:
        print(f"  {season-1} unavailable: {e}")

    if current is not None and prior is not None:
        current = _normalize_pitcher_cols(current)
        prior   = _normalize_pitcher_cols(prior)
        # IP-blend quality metrics so tiny 2026 samples don't dominate
        current = _blend_by_weight(current, prior, PITCHER_BLEND_COLS, "IP")
        # Blend IP/GS rate using combined start counts across both seasons
        current = _blend_ip_per_gs(current, prior)
        # Actual appearances across both seasons for apps/week calculation
        current = _add_pitcher_g_total(current, prior, season)
        current_names = set(current["Name"])
        prior_supplement = prior[~prior["Name"].isin(current_names)].copy()
        # 2025-only pitchers: G_total = G, weeks_sampled = full 2025 season
        prior_supplement["G_total"] = prior_supplement["G"]
        prior_supplement["weeks_sampled"] = _SEASON_2025_WEEKS
        df = pd.concat([current, prior_supplement], ignore_index=True)
    elif current is not None:
        df = _normalize_pitcher_cols(current)
    elif prior is not None:
        df = _normalize_pitcher_cols(prior)
    else:
        raise RuntimeError("Could not fetch pitcher data for any season")

    print(f"  Total: {len(df)} pitchers")
    return df


def _normalize_hitter_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "Name": "Name", "Team": "Team", "G": "G", "PA": "PA", "AB": "AB",
        "R": "R", "H": "H", "2B": "2B", "3B": "3B", "HR": "HR",
        "RBI": "RBI", "BB": "BB", "SO": "K", "SB": "SB",
        "xBA": "xBA", "xOBP": "xOBP", "xSLG": "xSLG", "xwOBA": "xwOBA",
        "EV": "EV", "Barrel%": "Barrel%", "HardHit%": "HardHit%",
        "K%": "K%", "BB%": "BB%", "OBP": "OBP", "SLG": "SLG",
        "Sprint Speed": "Sprint Speed",
    }
    existing = {k: v for k, v in rename.items() if k in df.columns}
    df = df.rename(columns=existing)

    if "TB" not in df.columns and all(c in df.columns for c in ["H", "2B", "3B", "HR"]):
        df["TB"] = df["H"] + df["2B"] + 2 * df["3B"] + 3 * df["HR"]

    for pct_col in ["K%", "BB%", "Barrel%", "HardHit%"]:
        if pct_col in df.columns and df[pct_col].dtype == object:
            df[pct_col] = df[pct_col].str.replace("%", "").astype(float) / 100

    return df


def _normalize_pitcher_cols(df: pd.DataFrame) -> pd.DataFrame:
    rename = {
        "Name": "Name", "Team": "Team", "G": "G", "GS": "GS", "IP": "IP",
        "W": "W", "L": "L", "SV": "SV", "SO": "K", "BB": "BB",
        "H": "H", "ER": "ER", "xERA": "xERA", "xFIP": "xFIP",
        "K%": "K%", "BB%": "BB%", "Barrel%": "Barrel%",
        "HardHit%": "HardHit%", "BABIP": "BABIP", "QS": "QS",
    }
    existing = {k: v for k, v in rename.items() if k in df.columns}
    df = df.rename(columns=existing)

    for pct_col in ["K%", "BB%", "Barrel%", "HardHit%"]:
        if pct_col in df.columns and df[pct_col].dtype == object:
            df[pct_col] = df[pct_col].str.replace("%", "").astype(float) / 100

    return df


if __name__ == "__main__":
    hitters  = get_hitters()
    pitchers = get_pitchers()
    print("\nSample hitters:")
    cols = ["Name", "Team", "data_season", "PA", "PA_total", "xwOBA", "K%", "BB%", "SB"]
    print(hitters[[c for c in cols if c in hitters.columns]].head(10).to_string())
    print("\nSample pitchers:")
    cols = ["Name", "Team", "data_season", "IP", "xERA", "xFIP", "K%", "BB%"]
    print(pitchers[[c for c in cols if c in pitchers.columns]].head(10).to_string())
