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

import io
import unicodedata
import requests
import pandas as pd
import pybaseball
from pybaseball import batting_stats, pitching_stats
from config import SEASON, MIN_PA, MIN_IP

pybaseball.cache.enable()

_MLB_STATS_URL   = "https://statsapi.mlb.com/api/v1/stats"
_SAVANT_EXPECTED = "https://baseballsavant.mlb.com/leaderboard/expected_statistics"
_SAVANT_HEADERS  = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}


def _strip_accents(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _fetch_mlb_batting_season(season: int, min_pa: int = 1) -> pd.DataFrame:
    """Season batting stats from MLB Stats API."""
    r = requests.get(_MLB_STATS_URL, params={
        "stats": "season", "group": "hitting", "season": season,
        "playerPool": "all", "limit": 2000,
    }, timeout=20)
    r.raise_for_status()
    splits = r.json().get("stats", [{}])[0].get("splits", [])
    rows = []
    for s in splits:
        stat = s.get("stat", {})
        pa = int(stat.get("plateAppearances", 0) or 0)
        if pa < min_pa:
            continue
        k  = int(stat.get("strikeOuts", 0) or 0)
        bb = int(stat.get("baseOnBalls", 0) or 0)
        rows.append({
            "Name": s.get("player", {}).get("fullName", ""),
            "Team": s.get("team", {}).get("abbreviation", ""),
            "G":    int(stat.get("gamesPlayed", 0) or 0),
            "PA":   pa,
            "AB":   int(stat.get("atBats", 0) or 0),
            "H":    int(stat.get("hits", 0) or 0),
            "HR":   int(stat.get("homeRuns", 0) or 0),
            "R":    int(stat.get("runs", 0) or 0),
            "RBI":  int(stat.get("rbi", 0) or 0),
            "SB":   int(stat.get("stolenBases", 0) or 0),
            "BB":   bb,
            "K":    k,
            "K%":   k / pa if pa > 0 else float("nan"),
            "BB%":  bb / pa if pa > 0 else float("nan"),
            "OBP":  float(stat.get("obp", 0) or 0),
            "SLG":  float(stat.get("slg", 0) or 0),
            "TB":   int(stat.get("totalBases", 0) or 0),
            "data_season": season,
        })
    return pd.DataFrame(rows)


def _fetch_savant_expected_batters(year: int) -> pd.DataFrame:
    """xwOBA, xSLG, xBA from Baseball Savant."""
    r = requests.get(_SAVANT_EXPECTED, params={
        "type": "batter", "year": year, "position": "", "team": "", "min": 1, "csv": "true",
    }, headers=_SAVANT_HEADERS, timeout=15)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df["Name"] = df["last_name, first_name"].apply(
        lambda x: " ".join(reversed([n.strip() for n in str(x).split(",")])) if pd.notna(x) else ""
    )
    return df.rename(columns={"est_woba": "xwOBA", "est_slg": "xSLG", "est_ba": "xBA"}
                     )[["Name", "xwOBA", "xSLG", "xBA"]].dropna(subset=["Name"])


def _fetch_mlb_pitching_season(season: int, min_ip: float = 1.0) -> pd.DataFrame:
    """Season pitching stats from MLB Stats API."""
    r = requests.get(_MLB_STATS_URL, params={
        "stats": "season", "group": "pitching", "season": season,
        "playerPool": "all", "limit": 2000,
    }, timeout=20)
    r.raise_for_status()
    splits = r.json().get("stats", [{}])[0].get("splits", [])
    rows = []
    for s in splits:
        stat = s.get("stat", {})
        ip = float(stat.get("inningsPitched", 0) or 0)
        if ip < min_ip:
            continue
        bf = int(stat.get("battersFaced", 0) or 0)
        k  = int(stat.get("strikeOuts", 0) or 0)
        bb = int(stat.get("baseOnBalls", 0) or 0)
        rows.append({
            "Name": s.get("player", {}).get("fullName", ""),
            "Team": s.get("team", {}).get("abbreviation", ""),
            "G":    int(stat.get("gamesPlayed", 0) or 0),
            "GS":   int(stat.get("gamesStarted", 0) or 0),
            "IP":   ip,
            "K":    k,
            "BB":   bb,
            "H":    int(stat.get("hits", 0) or 0),
            "ER":   int(stat.get("earnedRuns", 0) or 0),
            "SV":   int(stat.get("saves", 0) or 0),
            "K%":   k / bf if bf > 0 else float("nan"),
            "BB%":  bb / bf if bf > 0 else float("nan"),
            "ERA":  float(stat.get("era", 4.00) or 4.00),
            "data_season": season,
        })
    return pd.DataFrame(rows)


def _fetch_savant_expected_pitchers(year: int) -> pd.DataFrame:
    """xERA from Baseball Savant."""
    r = requests.get(_SAVANT_EXPECTED, params={
        "type": "pitcher", "year": year, "position": "", "team": "", "min": 1, "csv": "true",
    }, headers=_SAVANT_HEADERS, timeout=15)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    df["Name"] = df["last_name, first_name"].apply(
        lambda x: " ".join(reversed([n.strip() for n in str(x).split(",")])) if pd.notna(x) else ""
    )
    return df.rename(columns={"xera": "xERA"})[["Name", "xERA"]].dropna(subset=["Name"])

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

    # Fall back to MLB Stats API + Baseball Savant when FanGraphs is unavailable
    if season not in raw:
        try:
            df26 = _fetch_mlb_batting_season(season, min_pa=min_pa)
            df26["data_season"] = season
            raw[season] = df26
            print(f"  {season} (MLB API): {len(df26)} hitters")
        except Exception as e:
            print(f"  {season} MLB API unavailable: {e}")

    if (season - 1) not in raw:
        try:
            df25 = _fetch_mlb_batting_season(season - 1, min_pa=100)
            df25["data_season"] = season - 1
            raw[season - 1] = df25
            print(f"  {season-1} (MLB API): {len(df25)} hitters")
        except Exception as e:
            print(f"  {season-1} MLB API unavailable: {e}")

    if time_decay:
        from monthly_decay import get_unified_decayed
        decayed = get_unified_decayed()
        df = _apply_unified_decay(raw, decayed, season)
    else:
        df = _build_base(raw, season)

    # Merge Savant expected stats (xwOBA, xSLG, xBA) if not already present
    if "xwOBA" not in df.columns or df["xwOBA"].isna().all():
        for yr in (season, season - 1):
            try:
                savant = _fetch_savant_expected_batters(yr)
                # Savant names are "First Last"; strip accents to match
                savant["Name"] = savant["Name"].apply(_strip_accents)
                df = df.merge(savant, on="Name", how="left", suffixes=("", f"_sav{yr}"))
                print(f"  Savant expected stats merged ({yr}): {savant['xwOBA'].notna().sum()} players")
                break
            except Exception as e:
                print(f"  Savant {yr} unavailable: {e}")

    print(f"  Total: {len(df)} hitters")
    return df


def _build_base(raw: dict, season: int) -> pd.DataFrame:
    """
    Build base frame without decay: 2026 players first,
    supplement with 2025-only players.
    Returns empty DataFrame if neither season is available.
    """
    if season in raw and (season - 1) in raw:
        cur = raw[season].copy()
        prior = raw[season - 1].copy()
        prior_only = prior[~prior["Name"].isin(set(cur["Name"]))]
        df = pd.concat([cur, prior_only], ignore_index=True)
    elif season in raw:
        df = raw[season].copy()
    elif (season - 1) in raw:
        df = raw[season - 1].copy()
    else:
        return pd.DataFrame()

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

    # If season-level API was unavailable (e.g. 403), fall back to decayed data as base.
    # We lose Team, xwOBA, HR etc., but scoring rate stats are intact.
    if base.empty:
        print("  WARNING: season-level data unavailable; using monthly splits as base.")
        base = decayed.rename(columns={"PA_total": "PA", "G_total": "G"}).copy()
        for col, decay_col in {
            "K%": "K%_decay", "BB%": "BB%_decay",
            "OBP": "OBP_decay", "SLG": "SLG_decay",
        }.items():
            if decay_col in base.columns:
                base[col] = base[decay_col]
        if "SB/G_decay" in base.columns:
            base["SB_per_g_blend"] = base["SB/G_decay"]
        drop = [c for c in base.columns if c.endswith("_decay")]
        return base.drop(columns=drop, errors="ignore").reset_index(drop=True)

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


def _fetch_pitching_range(start: str, end: str, min_ip: float = 0) -> pd.DataFrame:
    """
    Fetch pitching stats via batting_stats_range-equivalent for pitchers.
    Computes K% and BB% from counting stats (xFIP/xERA unavailable here).
    """
    from pybaseball import pitching_stats_range
    df = pitching_stats_range(start, end)
    df = df.rename(columns={"Tm": "Team", "SO": "K_raw", "BF": "BF"})
    if "BF" in df.columns:
        df["K%"] = df["K_raw"] / df["BF"].replace(0, float("nan"))
        df["BB%"] = df["BB"] / df["BF"].replace(0, float("nan"))
    if min_ip:
        df = df[df["IP"] >= min_ip]
    return df.reset_index(drop=True)


def get_pitchers(season: int = SEASON, min_ip: int = MIN_IP) -> pd.DataFrame:
    """
    Pull FanGraphs pitching stats. Tries current season first, falls back to prior.
    If the legacy season endpoint returns 403, falls back to pitching_stats_range.
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
        # Legacy endpoint unavailable — fall back to MLB Stats API
        print("  WARNING: season-level pitcher data unavailable; falling back to MLB Stats API.")
        cur_r, pri_r = None, None
        try:
            cur_r = _fetch_mlb_pitching_season(season, min_ip=min_ip)
            cur_r["data_season"] = season
            print(f"  {season} (MLB API): {len(cur_r)} pitchers")
        except Exception as e:
            print(f"  {season} MLB API unavailable: {e}")
        try:
            pri_r = _fetch_mlb_pitching_season(season - 1, min_ip=30)
            pri_r["data_season"] = season - 1
            print(f"  {season-1} (MLB API): {len(pri_r)} pitchers")
        except Exception as e:
            print(f"  {season-1} MLB API unavailable: {e}")

        if cur_r is not None and pri_r is not None:
            cur_r = _blend_ip_per_gs(cur_r, pri_r)
            cur_r = _add_pitcher_g_total(cur_r, pri_r, season)
            cur_names = set(cur_r["Name"])
            pri_supp  = pri_r[~pri_r["Name"].isin(cur_names)].copy()
            pri_supp["G_total"] = pri_supp["G"]
            pri_supp["weeks_sampled"] = _SEASON_2025_WEEKS
            df = pd.concat([cur_r, pri_supp], ignore_index=True)
        elif cur_r is not None:
            df = cur_r
        elif pri_r is not None:
            df = pri_r
        else:
            raise RuntimeError("Could not fetch pitcher data for any season via any method")

    # Merge Savant xERA if not already present
    if "xERA" not in df.columns or df["xERA"].isna().all():
        for yr in (season, season - 1):
            try:
                savant_p = _fetch_savant_expected_pitchers(yr)
                savant_p["Name"] = savant_p["Name"].apply(_strip_accents)
                df = df.merge(savant_p, on="Name", how="left", suffixes=("", f"_sav{yr}"))
                print(f"  Savant pitcher xERA merged ({yr}): {savant_p['xERA'].notna().sum()} pitchers")
                break
            except Exception as e:
                print(f"  Savant pitcher {yr} unavailable: {e}")

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
