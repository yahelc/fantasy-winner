"""
Data-shaping layer for the web app.
Extracts logic from analyze.py / percentiles.py without any print/tabulate calls.
Returns DataFrames and structured dicts.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import numpy as np
import pandas as pd
from datetime import date

from config import MY_TEAM_NAME, SEASON, MIN_PA_RELIABLE, MIN_IP_RELIABLE
from fetch_espn import get_my_team, get_free_agents, get_roster_for_day
from analyze import (
    _BENCH_SLOTS,
    _primary_slots,
    _add_notes,
    DISPLAY_HITTER_COLS,
    DISPLAY_PITCHER_COLS,
    HITTER_DEBUG_COLS,
    PITCHER_DEBUG_COLS,
    HITTER_POSITIONS,
    PITCHER_POSITIONS,
    _sample_note,
)

# FanGraphs → Schedule API team code map (copied from schedule_report)
_FG_TO_SCHED = {
    "SDP": "SD",
    "SFG": "SF",
    "TBR": "TB",
    "KCR": "KC",
    "WSN": "WSH",
    "CHW": "CWS",
    "ARI": "ARI",
}


# ---------------------------------------------------------------------------
# 1. Roster report
# ---------------------------------------------------------------------------

def get_roster_data(league, hitters_scored, pitchers_scored) -> dict:
    """Returns {"hitters": df, "pitchers": df, "team_name": str, "unmatched": list}"""
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if my_team is None:
        return {"hitters": pd.DataFrame(), "pitchers": pd.DataFrame(),
                "team_name": "Unknown", "unmatched": []}

    roster_names = {p.name for p in my_team.roster}
    my_hitters  = hitters_scored[hitters_scored["Name"].isin(roster_names)]
    my_pitchers = pitchers_scored[pitchers_scored["Name"].isin(roster_names)]

    unmatched = sorted(
        roster_names - set(my_hitters["Name"]) - set(my_pitchers["Name"])
    )

    my_hitters  = _add_notes(my_hitters.copy())
    my_pitchers = _add_notes(my_pitchers.copy(), pa_col="IP")

    h_cols = [c for c in DISPLAY_HITTER_COLS if c in my_hitters.columns]
    p_cols = [c for c in DISPLAY_PITCHER_COLS if c in my_pitchers.columns]

    return {
        "hitters":   my_hitters[h_cols].reset_index(drop=True),
        "pitchers":  my_pitchers[p_cols].reset_index(drop=True),
        "team_name": my_team.team_name,
        "unmatched": unmatched,
    }


# ---------------------------------------------------------------------------
# 2. Lineup report
# ---------------------------------------------------------------------------

def get_lineup_data(league, hitters_scored, pitchers_scored, week: str = "next") -> dict:
    """
    Returns {"h_active": df, "h_bench": df, "p_active": df, "p_bench": df,
             "team_name": str, "roster_date": str}

    week: "next" fetches Tuesday roster; "current" uses current roster.
    """
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if my_team is None:
        empty = pd.DataFrame()
        return {"h_active": empty, "h_bench": empty, "p_active": empty,
                "p_bench": empty, "team_name": "Unknown", "roster_date": ""}

    use_next = (week != "current")

    if use_next:
        roster = get_roster_for_day(MY_TEAM_NAME, league=league)
        if not roster:
            roster = my_team.roster
    else:
        roster = my_team.roster

    roster_date = "next Tuesday" if use_next else "today"

    slot_map     = {p.name: getattr(p, "lineupSlot", "BE") for p in roster}
    eligible_map = {p.name: _primary_slots(p.eligibleSlots) for p in roster}
    roster_names = set(slot_map.keys())

    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)
    my_players = all_scored[all_scored["Name"].isin(roster_names)].copy()
    my_players["Slot"] = my_players["Name"].map(slot_map)

    is_bench = my_players["Slot"].isin(_BENCH_SLOTS)
    active = my_players[~is_bench].sort_values("composite_score", ascending=False)
    bench  = my_players[is_bench].sort_values("composite_score", ascending=False)

    # Flag bench players who score higher than their weakest same-position active player
    active_by_pos: dict = {}
    for _, row in active.iterrows():
        for pos in eligible_map.get(row["Name"], set()):
            prev = active_by_pos.get(pos, float("inf"))
            active_by_pos[pos] = min(prev, row["composite_score"])

    def _bench_flag(row) -> str:
        shared = eligible_map.get(row["Name"], set())
        upgradeable = [
            pos for pos in shared
            if row["composite_score"] > active_by_pos.get(pos, float("inf"))
        ]
        return "START?" if upgradeable else ""

    bench = bench.copy()
    bench["flag"] = bench.apply(_bench_flag, axis=1)

    pitcher_names = set(pitchers_scored["Name"])
    is_pitcher_active = active["Name"].isin(pitcher_names)
    h_active = active[~is_pitcher_active].copy()
    p_active = active[is_pitcher_active].copy()
    is_pitcher_bench = bench["Name"].isin(pitcher_names)
    h_bench = bench[~is_pitcher_bench].copy()
    p_bench = bench[is_pitcher_bench].copy()

    h_cols = ["Slot", "Name", "Team", "Yr", "PA", "PA_total", "xwOBA",
              "K%", "BB%", "SB", "pts_per_game", "composite_score", "note"]
    p_cols = ["Slot", "Name", "Team", "Yr", "G", "G_total", "GS", "IP",
              "xERA", "xFIP", "K%", "pts_per_game", "pts_per_week", "composite_score", "note"]
    bench_h_cols = h_cols + ["flag"]
    bench_p_cols = p_cols + ["flag"]

    def _prepare(df, cols):
        df = _add_notes(df.copy())
        available = [c for c in cols if c in df.columns]
        return df[available].reset_index(drop=True)

    return {
        "h_active":    _prepare(h_active, h_cols),
        "h_bench":     _prepare(h_bench, bench_h_cols),
        "p_active":    _prepare(p_active, p_cols),
        "p_bench":     _prepare(p_bench, bench_p_cols),
        "team_name":   my_team.team_name,
        "roster_date": roster_date,
    }


# ---------------------------------------------------------------------------
# 3. Free agents
# ---------------------------------------------------------------------------

def get_fa_data(league, hitters_scored, pitchers_scored,
                position: str = None, n: int = 25) -> dict:
    """Returns {"hitters": df or None, "pitchers": df or None, "position": str}"""
    fas = get_free_agents(league, size=200)
    fa_names = {p.name for p in fas}
    pos_label = (position or "ALL").upper()

    if position and position.upper() in PITCHER_POSITIONS:
        pool = pitchers_scored[pitchers_scored["Name"].isin(fa_names)]
        pool = _add_notes(pool.copy(), pa_col="IP")
        p_cols = [c for c in DISPLAY_PITCHER_COLS if c in pool.columns]
        return {
            "hitters":  None,
            "pitchers": pool[p_cols].head(n).reset_index(drop=True),
            "position": pos_label,
        }
    elif position and position.upper() in HITTER_POSITIONS:
        pool = hitters_scored[hitters_scored["Name"].isin(fa_names)]
        pool = _add_notes(pool.copy())
        h_cols = [c for c in DISPLAY_HITTER_COLS if c in pool.columns]
        return {
            "hitters":  pool[h_cols].head(n).reset_index(drop=True),
            "pitchers": None,
            "position": pos_label,
        }
    else:
        pool_h = hitters_scored[hitters_scored["Name"].isin(fa_names)]
        pool_p = pitchers_scored[pitchers_scored["Name"].isin(fa_names)]
        pool_h = _add_notes(pool_h.copy())
        pool_p = _add_notes(pool_p.copy(), pa_col="IP")
        h_cols = [c for c in DISPLAY_HITTER_COLS if c in pool_h.columns]
        p_cols = [c for c in DISPLAY_PITCHER_COLS if c in pool_p.columns]
        return {
            "hitters":  pool_h[h_cols].head(n).reset_index(drop=True),
            "pitchers": pool_p[p_cols].head(n).reset_index(drop=True),
            "position": pos_label,
        }


# ---------------------------------------------------------------------------
# 4. Upgrade report
# ---------------------------------------------------------------------------

def get_upgrade_data(league, hitters_scored, pitchers_scored, n: int = 15) -> dict:
    """Returns {"upgrades": df}"""
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        return {"upgrades": pd.DataFrame()}

    fas = get_free_agents(league, size=300)
    fa_positions = {p.name: _primary_slots(p.eligibleSlots) for p in fas}
    roster_positions = {p.name: _primary_slots(p.eligibleSlots) for p in my_team.roster}

    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)

    upgrades = []
    for player_name, my_primary in roster_positions.items():
        if not my_primary:
            continue
        my_row = all_scored[all_scored["Name"] == player_name]
        if my_row.empty:
            continue
        my_score = my_row.iloc[0]["composite_score"]

        matching_fas = [
            fa for fa, fa_primary in fa_positions.items()
            if fa_primary & my_primary
        ]
        if not matching_fas:
            continue

        fa_pool = all_scored[all_scored["Name"].isin(matching_fas)]
        if fa_pool.empty:
            continue

        best_fa = fa_pool.iloc[0]
        delta = best_fa["composite_score"] - my_score
        if delta > 0:
            shared = my_primary & fa_positions[best_fa["Name"]]
            upgrades.append({
                "Drop":       player_name,
                "My Score":   round(my_score, 3),
                "Add":        best_fa["Name"],
                "FA Score":   round(best_fa["composite_score"], 3),
                "Delta":      round(delta, 3),
                "Shared Pos": "/".join(sorted(shared)),
                "Note":       _sample_note(best_fa),
            })

    upgrades_df = pd.DataFrame(upgrades)
    if not upgrades_df.empty:
        upgrades_df = upgrades_df.sort_values("Delta", ascending=False).head(n).reset_index(drop=True)

    return {"upgrades": upgrades_df}


# ---------------------------------------------------------------------------
# 5. Upgrade by position
# ---------------------------------------------------------------------------

def get_upgrade_pos_data(league, hitters_scored, pitchers_scored,
                         position: str, n: int = 20) -> dict:
    """Returns {"combined": df, "position": str, "is_pitcher": bool}"""
    pos = position.upper()
    if pos == "UTIL":
        pos = "UTL"

    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        return {"combined": pd.DataFrame(), "position": pos, "is_pitcher": False}

    fas = get_free_agents(league, size=300)
    is_pitcher = pos in PITCHER_POSITIONS
    scored = pitchers_scored if is_pitcher else hitters_scored

    if pos == "UTL":
        my_names = {p.name for p in my_team.roster}
        fa_names  = {p.name for p in fas}
    else:
        pos_set = {pos}
        my_names = {p.name for p in my_team.roster if pos_set & _primary_slots(p.eligibleSlots)}
        fa_names  = {p.name for p in fas          if pos_set & _primary_slots(p.eligibleSlots)}

    my_rows = scored[scored["Name"].isin(my_names)].copy()
    my_rows["Source"] = "MINE"
    fa_rows = scored[scored["Name"].isin(fa_names)].copy()
    fa_rows["Source"] = "FA"

    combined = (pd.concat([my_rows, fa_rows], ignore_index=True)
                  .sort_values("composite_score", ascending=False))

    base_cols = DISPLAY_PITCHER_COLS if is_pitcher else DISPLAY_HITTER_COLS
    display_cols = ["Source"] + [c for c in base_cols if c != "note"] + ["note"]

    combined = _add_notes(combined, pa_col="IP" if is_pitcher else "PA")
    available_cols = [c for c in display_cols if c in combined.columns]
    combined = combined[available_cols].head(n).reset_index(drop=True)

    return {"combined": combined, "position": pos, "is_pitcher": is_pitcher}


# ---------------------------------------------------------------------------
# 6. Compare players
# ---------------------------------------------------------------------------

def get_compare_data(names_str: str, hitters_scored, pitchers_scored,
                     debug: bool = False) -> dict:
    """
    names_str is comma-separated. Returns:
    {"h_rows": df, "p_rows": df, "debug": bool, "not_found": list}
    """
    names = [n.strip() for n in names_str.split(",") if n.strip()]
    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)

    rows = []
    not_found = []
    pitcher_set = set(pitchers_scored["Name"])

    for name in names:
        match = all_scored[all_scored["Name"].str.contains(name, case=False, na=False)]
        if match.empty:
            not_found.append(name)
        else:
            rows.append(match.iloc[0])

    if not rows:
        return {
            "h_rows": pd.DataFrame(),
            "p_rows": pd.DataFrame(),
            "debug": debug,
            "not_found": not_found,
        }

    result = pd.DataFrame(rows)

    if debug:
        h_rows = result[~result["Name"].isin(pitcher_set)]
        p_rows = result[result["Name"].isin(pitcher_set)]

        if not h_rows.empty:
            h_cols = [c for c in HITTER_DEBUG_COLS if c in h_rows.columns]
            h_rows = _add_notes(h_rows.copy())
            h_rows = h_rows[h_cols].reset_index(drop=True)
        else:
            h_rows = pd.DataFrame()

        if not p_rows.empty:
            p_cols = [c for c in PITCHER_DEBUG_COLS if c in p_rows.columns]
            p_rows = _add_notes(p_rows.copy(), pa_col="IP")
            p_rows = p_rows[p_cols].reset_index(drop=True)
        else:
            p_rows = pd.DataFrame()
    else:
        cols = [c for c in DISPLAY_HITTER_COLS + DISPLAY_PITCHER_COLS if c in result.columns]
        cols = list(dict.fromkeys(cols))

        result = _add_notes(result.copy())
        result = result[[c for c in cols if c in result.columns]].reset_index(drop=True)

        # Split for display even in non-debug mode
        h_rows = result[~result["Name"].isin(pitcher_set)].reset_index(drop=True)
        p_rows = result[result["Name"].isin(pitcher_set)].reset_index(drop=True)

    return {
        "h_rows":    h_rows,
        "p_rows":    p_rows,
        "debug":     debug,
        "not_found": not_found,
    }


# ---------------------------------------------------------------------------
# 7. Week projection
# ---------------------------------------------------------------------------

def get_week_data(league, hitters_scored, pitchers_scored) -> dict:
    """Returns {"hitters": df, "pitchers": df, "week_start": date,
                "week_end": date, "total_h": float, "total_p": float}"""
    from fetch_schedule import get_team_games_next_week

    team_games, week_start, week_end = get_team_games_next_week()

    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        empty = pd.DataFrame()
        return {"hitters": empty, "pitchers": empty,
                "week_start": week_start, "week_end": week_end,
                "total_h": 0.0, "total_p": 0.0}

    # Use next-Tuesday roster to reflect pending adds/drops
    roster = get_roster_for_day(MY_TEAM_NAME, league=league) or my_team.roster
    roster_names = {p.name for p in roster}

    # FanGraphs uses "- - -" for recently traded players; fall back to ESPN proTeam
    espn_team = {p.name: getattr(p, "proTeam", "") for p in roster}

    def _resolve_team(name, fg_team):
        if fg_team == "- - -" or not fg_team or pd.isna(fg_team):
            return espn_team.get(name, "")
        return fg_team

    def _gms(df):
        teams = df.apply(lambda r: _resolve_team(r["Name"], r["Team"]), axis=1)
        return teams.map(lambda t: team_games.get(_FG_TO_SCHED.get(t, t), 0)).astype(int)

    def _fix_team(df):
        df["Team"] = df.apply(lambda r: _resolve_team(r["Name"], r["Team"]), axis=1)
        return df

    # Hitters
    h = _fix_team(hitters_scored[hitters_scored["Name"].isin(roster_names)].copy())
    h["gms"] = _gms(h)
    h["proj_week_pts"] = (h["pts_per_game"] * h["gms"]).round(2)
    h = h.sort_values("proj_week_pts", ascending=False)
    h = _add_notes(h)

    h_cols = ["Name", "Team", "Yr", "gms", "pts_per_game", "proj_week_pts", "note"]
    h_available = [c for c in h_cols if c in h.columns]
    h_out = h[h_available].reset_index(drop=True)

    # Pitchers
    p = _fix_team(pitchers_scored[pitchers_scored["Name"].isin(roster_names)].copy())
    p["gms"] = _gms(p)

    gs = p.get("GS", pd.Series(0, index=p.index)).fillna(0)
    is_starter = gs > (p["G"] * 0.5)

    fallback = pd.Series(np.where(is_starter, 1.4, 2.5), index=p.index)
    if "G_total" in p.columns and "weeks_sampled" in p.columns:
        apps_per_week = (p["G_total"] / p["weeks_sampled"].replace(0, np.nan)).fillna(fallback)
    else:
        apps_per_week = fallback

    proj = pd.Series(index=p.index, dtype=float)
    proj[is_starter]  = p.loc[is_starter,  "pts_per_game"] * (p.loc[is_starter,  "gms"] / 5)
    proj[~is_starter] = (p.loc[~is_starter, "pts_per_game"]
                         * apps_per_week[~is_starter]
                         * (p.loc[~is_starter, "gms"] / 7))
    p["proj_week_pts"] = proj.round(2)
    p = p.sort_values("proj_week_pts", ascending=False)
    p = _add_notes(p, pa_col="IP")

    p_cols = ["Name", "Team", "Yr", "gms", "GS", "pts_per_game", "pts_per_week",
              "proj_week_pts", "note"]
    p_available = [c for c in p_cols if c in p.columns]
    p_out = p[p_available].reset_index(drop=True)

    total_h = float(h["proj_week_pts"].sum())
    total_p = float(p["proj_week_pts"].sum())

    return {
        "hitters":    h_out,
        "pitchers":   p_out,
        "week_start": week_start,
        "week_end":   week_end,
        "total_h":    round(total_h, 1),
        "total_p":    round(total_p, 1),
    }


# ---------------------------------------------------------------------------
# 8. Schedule report
# ---------------------------------------------------------------------------

def _enrich_hitters(df: pd.DataFrame, schedule: dict) -> pd.DataFrame:
    df = df.copy()
    sched_team = df["Team"].map(lambda t: _FG_TO_SCHED.get(t, t))
    df["gms"]  = sched_team.map(lambda t: schedule.get(t, {}).get("games", 0))
    df["days"] = sched_team.map(lambda t: " ".join(schedule.get(t, {}).get("days", [])))
    return df


def _enrich_pitchers(df: pd.DataFrame, schedule: dict, start_proj: dict) -> pd.DataFrame:
    df = df.copy()
    sched_team = df["Team"].map(lambda t: _FG_TO_SCHED.get(t, t))
    df["gms"]  = sched_team.map(lambda t: schedule.get(t, {}).get("games", 0))
    df["days"] = sched_team.map(lambda t: " ".join(schedule.get(t, {}).get("days", [])))

    gs = df.get("GS", pd.Series(0, index=df.index)).fillna(0)
    is_starter = gs > (df["G"] * 0.5)

    def _starts_col(row):
        if not is_starter[row.name]:
            return ""
        proj = start_proj.get(row["Name"], [])
        if not proj:
            return "?"
        dates = " ".join(d.strftime("%a%-d") for d in proj)
        label = f"{len(proj)}x ({dates})"
        if len(proj) >= 2:
            label += " ?"
        return label

    df["starts"] = df.apply(_starts_col, axis=1)
    df["_n_starts"] = df["Name"].map(
        lambda n: len(start_proj.get(n, [])) if (
            n in df["Name"].values and is_starter[df[df["Name"] == n].index[0]]
        ) else 0
    )
    return df


def get_schedule_data(league, hitters_scored, pitchers_scored, n: int = 20) -> dict:
    """Returns {"my_h": df, "my_p": df, "fa_h": df, "fa_p": df,
                "week_start": date, "week_end": date}"""
    from fetch_schedule import get_schedule, project_starts_in_week

    schedule, week_start, week_end = get_schedule()

    roster = get_roster_for_day(MY_TEAM_NAME, league=league)
    if not roster:
        my_team = get_my_team(league, team_name=MY_TEAM_NAME)
        roster = my_team.roster if my_team else []
    roster_names = {p.name for p in roster}

    fas = get_free_agents(league, size=300)
    fa_names = {p.name for p in fas}

    # Gather starters for start projection
    my_p_df = pitchers_scored[pitchers_scored["Name"].isin(roster_names)].copy()
    gs = my_p_df.get("GS", pd.Series(0, index=my_p_df.index)).fillna(0)
    my_starters = my_p_df[gs > (my_p_df["G"] * 0.5)]["Name"].tolist()

    fa_p_df = pitchers_scored[pitchers_scored["Name"].isin(fa_names)].copy()
    fa_gs = fa_p_df.get("GS", pd.Series(0, index=fa_p_df.index)).fillna(0)
    fa_starters = (fa_p_df[fa_gs > (fa_p_df["G"] * 0.5)]
                   .sort_values("composite_score", ascending=False)
                   .head(40)["Name"].tolist())

    all_starters = list(set(my_starters + fa_starters))
    start_proj = project_starts_in_week(all_starters, week_start, week_end)

    my_h = _enrich_hitters(hitters_scored[hitters_scored["Name"].isin(roster_names)], schedule)
    my_p = _enrich_pitchers(my_p_df, schedule, start_proj)
    fa_h = _enrich_hitters(hitters_scored[hitters_scored["Name"].isin(fa_names)], schedule)
    fa_p = _enrich_pitchers(fa_p_df, schedule, start_proj)

    h_cols  = ["Name", "Team", "gms", "days", "pts_per_game", "composite_score"]
    sp_cols = ["Name", "Team", "gms", "starts", "pts_per_game", "pts_per_week", "composite_score"]

    def _prep_h(df, extra_cols=None):
        cols = h_cols + (extra_cols or [])
        df = _add_notes(df.copy())
        if "note" not in cols:
            cols = cols + ["note"]
        available = [c for c in cols if c in df.columns]
        return df[available].reset_index(drop=True)

    def _prep_p(df, extra_cols=None):
        cols = sp_cols + (extra_cols or [])
        df = _add_notes(df.copy(), pa_col="IP")
        if "note" not in cols:
            cols = cols + ["note"]
        available = [c for c in cols if c in df.columns]
        return df[available].reset_index(drop=True)

    my_h_out = _prep_h(my_h.sort_values("gms", ascending=False))
    my_p_out = _prep_p(
        my_p.sort_values(["_n_starts", "gms", "composite_score"], ascending=False)
    )
    fa_h_out = _prep_h(
        fa_h.sort_values(["gms", "composite_score"], ascending=False).head(n)
    )
    fa_p_out = _prep_p(
        fa_p.sort_values(["_n_starts", "composite_score"], ascending=False).head(n)
    )

    return {
        "my_h":       my_h_out,
        "my_p":       my_p_out,
        "fa_h":       fa_h_out,
        "fa_p":       fa_p_out,
        "week_start": week_start,
        "week_end":   week_end,
    }


# ---------------------------------------------------------------------------
# 9. Percentiles
# ---------------------------------------------------------------------------

def get_percentiles_data(league, week: str = "next", year: int = 2026) -> dict:
    """Returns {"hitters": df, "pitchers": df, "year": int, "week": str}"""
    from percentiles import _fetch_percentiles, _build_table, HITTER_COLS, PITCHER_COLS

    hitter_pct  = _fetch_percentiles("batter",  year)
    pitcher_pct = _fetch_percentiles("pitcher", year)

    use_next = (week == "next")

    if use_next:
        roster = get_roster_for_day(MY_TEAM_NAME, league=league)
        if not roster:
            my_team = get_my_team(league, MY_TEAM_NAME)
            roster = my_team.roster if my_team else []
    else:
        my_team = get_my_team(league, MY_TEAM_NAME)
        roster = my_team.roster if my_team else []

    pitcher_slots = {"SP", "RP", "P"}
    hitter_names  = []
    pitcher_names = []
    for p in roster:
        slots = set(p.eligibleSlots or [])
        if slots & pitcher_slots and "C" not in slots and "1B" not in slots:
            pitcher_names.append(p.name)
        else:
            hitter_names.append(p.name)

    h_table = _build_table(hitter_names, hitter_pct, HITTER_COLS)
    p_table = _build_table(pitcher_names, pitcher_pct, PITCHER_COLS)

    return {
        "hitters": h_table,
        "pitchers": p_table,
        "year":    year,
        "week":    week,
    }


# ---------------------------------------------------------------------------
# 10. Simulate: "what would week X lineup score in week Y?"
# ---------------------------------------------------------------------------

# Season starts on Opening Day; each scoring period is one day.
# Week N starts on WEEK1_START + (N-1)*7 days.
_WEEK1_START = date(2026, 3, 27)


def _week_start_date(week_num: int) -> date:
    from datetime import timedelta
    return _WEEK1_START + timedelta(days=(week_num - 1) * 7)


def _collect_actual_points(league, scoring_week: int) -> tuple[dict[str, float], date, date]:
    """
    Return (pts_map, week_start, week_end) where pts_map is
    {player_name: actual_fantasy_pts} for all ROSTERED players.

    Uses rosterForMatchupPeriod to get full-week totals.
    Free agents are not tracked by ESPN and will not appear in pts_map.
    """
    import requests as _requests
    import json as _json
    from datetime import timedelta

    endpoint = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{SEASON}/segments/0/leagues/{league.league_id}"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    filt = {"schedule": {"filterMatchupPeriodIds": {"value": [scoring_week]}}}

    r = _requests.get(
        endpoint,
        params={"view": ["mMatchup", "mMatchupScore"]},
        headers={**headers, "x-fantasy-filter": _json.dumps(filt)},
        timeout=20,
    )
    r.raise_for_status()
    data = r.json()

    # Derive week date range from pointsByScoringPeriod
    today = date.today()
    current_sp = league.scoringPeriodId
    all_sps: set[int] = set()
    pts: dict[str, float] = {}

    for matchup in data.get("schedule", []):
        for side_key in ("home", "away"):
            side = matchup.get(side_key, {})
            # Collect scoring period IDs for date range
            pbsp = side.get("pointsByScoringPeriod", {})
            all_sps.update(int(k) for k in pbsp.keys())

            entries = side.get("rosterForMatchupPeriod", {}).get("entries", [])
            for entry in entries:
                player = entry.get("playerPoolEntry", {}).get("player", {})
                name = player.get("fullName")
                if not name or name in pts:
                    continue
                stats_list = player.get("stats", [])
                week_pts = next(
                    (s.get("appliedTotal", 0.0)
                     for s in stats_list
                     if s.get("scoringPeriodId") == 0 and s.get("statSourceId") == 0),
                    None,
                )
                pts[name] = float(week_pts) if week_pts is not None else 0.0

    if all_sps:
        week_start = today - timedelta(days=current_sp - min(all_sps))
        week_end   = today - timedelta(days=current_sp - max(all_sps))
    else:
        week_start = _week_start_date(scoring_week)
        week_end   = week_start + timedelta(days=6)

    return pts, week_start, week_end


def _compute_fa_points(fa_names: list[str], week_start: date, week_end: date) -> dict[str, float]:
    """
    Compute actual fantasy points for free agents using FanGraphs game stats.
    Uses pybaseball batting_stats_range / pitching_stats_range.
    QS (quality starts) is not available from FanGraphs date-range API and is excluded.
    """
    import pybaseball
    from config import BATTING_WEIGHTS, PITCHING_WEIGHTS

    start_str = week_start.strftime("%Y-%m-%d")
    end_str   = week_end.strftime("%Y-%m-%d")

    result: dict[str, float] = {}

    try:
        bat = pybaseball.batting_stats_range(start_str, end_str)
        bat = bat[bat["Lev"].str.startswith("Maj", na=False)].copy()
        bat["TB"] = bat["H"] + bat["2B"] + 2 * bat["3B"] + 3 * bat["HR"]
        bat["K"]  = bat["SO"]
        bat_index = {n.lower(): n for n in bat["Name"]}
    except Exception:
        bat = None
        bat_index = {}

    try:
        pit = pybaseball.pitching_stats_range(start_str, end_str)
        pit = pit[pit["Lev"].str.startswith("Maj", na=False)].copy()
        pit["K"] = pit["SO"]
        pit_index = {n.lower(): n for n in pit["Name"]}
    except Exception:
        pit = None
        pit_index = {}

    for name in fa_names:
        name_lower = name.lower()

        # Try exact match, then first-last-name partial
        def _find(index, df, col="Name"):
            if name_lower in index:
                rows = df[df[col] == index[name_lower]]
                return rows.iloc[0] if not rows.empty else None
            # Partial: last name match (handles "Jr.", accents, etc.)
            last = name_lower.split()[-1].rstrip(".")
            candidates = [k for k in index if last in k]
            if len(candidates) == 1:
                rows = df[df[col] == index[candidates[0]]]
                return rows.iloc[0] if not rows.empty else None
            return None

        # Try batting first
        if bat is not None:
            row = _find(bat_index, bat)
            if row is not None:
                pts = 0.0
                for stat, wt in BATTING_WEIGHTS.items():
                    pts += wt * float(row.get(stat, 0) or 0)
                result[name] = round(pts, 1)
                continue

        # Try pitching
        if pit is not None:
            row = _find(pit_index, pit)
            if row is not None:
                pts = 0.0
                for stat, wt in PITCHING_WEIGHTS.items():
                    if stat in ("QS", "NH", "PG"):
                        continue  # not available in date-range API
                    pts += wt * float(row.get(stat, 0) or 0)
                result[name] = round(pts, 1)
                continue

    return result


def get_simulate_data(league, lineup_week: int, scoring_week: int) -> dict:
    """
    Returns {
        "rows": df with Name/Slot/ActualPts,
        "total": float,
        "lineup_week": int, "scoring_week": int,
        "lineup_date": date, "not_found": list[str],
    }
    """
    from datetime import timedelta

    # Roster from Tuesday of lineup_week
    roster_date = _week_start_date(lineup_week)
    # Move to Tuesday of that week
    days_to_tuesday = (1 - roster_date.weekday()) % 7
    if days_to_tuesday == 0:
        days_to_tuesday = 7
    roster_date = roster_date + timedelta(days=days_to_tuesday)
    roster = get_roster_for_day(MY_TEAM_NAME, league=league, target_date=roster_date)

    if not roster:
        my_team = get_my_team(league, MY_TEAM_NAME)
        roster = my_team.roster if my_team else []

    # Collect actual points from scoring_week across all teams
    actual_pts, week_start, week_end = _collect_actual_points(league, scoring_week)

    # First pass: find which players need FanGraphs fallback
    _BENCH = {"BE", "BN", "IL", "IL+", "NA"}
    fa_names = [p.name for p in roster if p.name not in actual_pts]

    # Compute FA points via FanGraphs stats for the scoring week date range
    fa_pts = _compute_fa_points(fa_names, week_start, week_end)

    # Build per-player rows
    rows = []
    not_found = []
    for p in roster:
        slot = getattr(p, "lineupSlot", "BE") or "BE"
        pts = actual_pts.get(p.name)
        if pts is None:
            pts = fa_pts.get(p.name)
        if pts is None:
            not_found.append(p.name)
            pts = float("nan")
        rows.append({
            "Name": p.name,
            "Slot": slot,
            "Pts": pts,
            "active": slot not in _BENCH,
        })

    df = pd.DataFrame(rows)
    _base = {
        "lineup_week": lineup_week,
        "scoring_week": scoring_week,
        "lineup_date": roster_date,
        "week_start": week_start,
        "week_end": week_end,
        "not_found": not_found,
    }

    if df.empty:
        return {**_base, "rows": df, "total_active": 0.0, "total_all": 0.0}

    df = df.sort_values(["active", "Pts"], ascending=[False, False]).reset_index(drop=True)
    total_active = float(df.loc[df["active"], "Pts"].fillna(0).sum())
    total_all = float(df["Pts"].fillna(0).sum())

    display_df = df[["Name", "Slot", "Pts"]].copy()
    display_df["Pts"] = display_df["Pts"].fillna(0.0)

    return {
        **_base,
        "rows": display_df,
        "total_active": round(total_active, 1),
        "total_all": round(total_all, 1),
    }
