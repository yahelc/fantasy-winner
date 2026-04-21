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
    "ARI": "AZ",   # MLB Stats API uses AZ
    "OAK": "ATH",  # Athletics relocated; MLB Stats API uses ATH
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
    espn_team_map = {p.name: getattr(p, "proTeam", "") for p in roster}
    roster_names = set(slot_map.keys())

    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)
    my_players = all_scored[all_scored["Name"].isin(roster_names)].copy()
    my_players["Slot"] = my_players["Name"].map(slot_map)

    # Resolve "- - -" (FanGraphs placeholder for recently traded players) via ESPN
    def _resolve_team(name, team):
        if team == "- - -" or not team or (isinstance(team, float) and pd.isna(team)):
            return espn_team_map.get(name, "")
        return team

    if "Team" in my_players.columns:
        my_players["Team"] = my_players.apply(
            lambda r: _resolve_team(r["Name"], r["Team"]), axis=1
        )

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
        pos_set = {"LF", "CF", "RF"} if pos == "OF" else {pos}
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

def get_percentiles_data(league, week: str = "next", year: int = 2026,
                         source: str = "roster", pos: str = "", team: str = "") -> dict:
    """Returns {"hitters": df, "pitchers": df, "year": int, "week": str, "source": str, "pos": str, "team": str}"""
    from percentiles import _fetch_percentiles, _build_table, HITTER_COLS, PITCHER_COLS

    hitter_pct  = _fetch_percentiles("batter",  year)
    pitcher_pct = _fetch_percentiles("pitcher", year)

    pitcher_slots = {"SP", "RP", "P"}

    if source == "fa":
        fas = get_free_agents(league, size=300)
        if pos:
            pos_set = {"LF", "CF", "RF"} if pos == "OF" else {pos}
            players = [p for p in fas if pos_set & _primary_slots(p.eligibleSlots)]
        else:
            players = fas
        hitter_names  = [p.name for p in players
                         if not (pitcher_slots & _primary_slots(p.eligibleSlots))]
        pitcher_names = [p.name for p in players
                         if pitcher_slots & _primary_slots(p.eligibleSlots)]
    elif source == "team" and team:
        roster = get_roster_for_day(team, league=league)
        if not roster:
            t = next((t for t in league.teams if team.lower() in t.team_name.lower()), None)
            roster = t.roster if t else []
        bench_slots = {"BE", "BN", "IL", "IL+", "NA"}
        hitter_names  = []
        pitcher_names = []
        bench_hitter_names  = []
        bench_pitcher_names = []
        for p in roster:
            slots = set(p.eligibleSlots or [])
            slot  = getattr(p, "lineupSlot", "BE") or "BE"
            is_bench = slot in bench_slots
            if slots & pitcher_slots and "C" not in slots and "1B" not in slots:
                (bench_pitcher_names if is_bench else pitcher_names).append(p.name)
            else:
                (bench_hitter_names if is_bench else hitter_names).append(p.name)
    else:
        use_next = (week == "next")
        if use_next:
            roster = get_roster_for_day(MY_TEAM_NAME, league=league)
            if not roster:
                my_team = get_my_team(league, MY_TEAM_NAME)
                roster = my_team.roster if my_team else []
        else:
            my_team = get_my_team(league, MY_TEAM_NAME)
            roster = my_team.roster if my_team else []

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

    # Bench tables only populated for other-team view
    if source == "team" and team:
        h_bench_table = _build_table(bench_hitter_names, hitter_pct, HITTER_COLS)
        p_bench_table = _build_table(bench_pitcher_names, pitcher_pct, PITCHER_COLS)
    else:
        h_bench_table = None
        p_bench_table = None

    return {
        "hitters":        h_table,
        "pitchers":       p_table,
        "h_bench":        h_bench_table,
        "p_bench":        p_bench_table,
        "year":           year,
        "week":           week,
        "source":         source,
        "pos":            pos,
        "team":           team,
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

    from config import ESPN_S2, ESPN_SWID

    endpoint = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{SEASON}/segments/0/leagues/{league.league_id}"
    )
    headers = {"User-Agent": "Mozilla/5.0"}
    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 and ESPN_SWID else {}
    filt = {"schedule": {"filterMatchupPeriodIds": {"value": [scoring_week]}}}

    r = _requests.get(
        endpoint,
        params={"view": ["mMatchup", "mMatchupScore"]},
        headers={**headers, "x-fantasy-filter": _json.dumps(filt)},
        cookies=cookies,
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
                # IL/IL+ slot IDs — ESPN returns projected stats for these
                # players even in completed weeks, so force to 0.
                if entry.get("lineupSlotId") in (16, 17):
                    pts[name] = 0.0
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


# ---------------------------------------------------------------------------
# 11. Matchup SP view — "can my pitching catch up?"
# ---------------------------------------------------------------------------
import unicodedata as _unicodedata
import math as _math

_MLB_SCHED_URL    = "https://statsapi.mlb.com/api/v1/schedule"
_MLB_BOXSCORE_URL = "https://statsapi.mlb.com/api/v1/game/{}/boxscore"

_FINAL_STATES = frozenset({"F", "FR", "FT", "FO", "UR", "UF", "O"})
_LIVE_STATES  = frozenset({"I", "IO", "IR", "MA"})
_DAY_NAMES    = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


_team_wrc_cache: dict = {"data": {}, "ts": 0.0}

def _fetch_team_wrc_plus(scored_hitters=None) -> dict[str, int]:
    """Returns {MLB_Stats_API_abbr: wRC+} from already-fetched hitter data (no HTTP call)."""
    import time as _time
    now = _time.monotonic()
    if _team_wrc_cache["data"] and (now - _team_wrc_cache["ts"]) < 1800:
        return _team_wrc_cache["data"]
    if scored_hitters is None or scored_hitters.empty:
        return _team_wrc_cache["data"]
    try:
        df = scored_hitters
        if "wRC+" not in df.columns or "Team" not in df.columns or "PA" not in df.columns:
            return _team_wrc_cache["data"]
        df = df[df["PA"] > 0].dropna(subset=["wRC+", "PA", "Team"])
        result: dict[str, int] = {}
        for fg_team, grp in df.groupby("Team"):
            total_pa = grp["PA"].sum()
            if total_pa == 0:
                continue
            wrc = (grp["wRC+"] * grp["PA"]).sum() / total_pa
            mlb = _FG_TO_SCHED.get(str(fg_team), str(fg_team))
            result[mlb.upper()] = int(round(float(wrc)))
        if result:
            _team_wrc_cache["data"] = result
            _team_wrc_cache["ts"] = now
        return result
    except Exception:
        return _team_wrc_cache["data"]


def _norm_name(name: str) -> str:
    s = _unicodedata.normalize("NFKD", name)
    s = "".join(c for c in s if not _unicodedata.combining(c))
    s = s.lower()
    for suf in (" jr.", " jr", " sr.", " sr", " ii", " iii", " iv", " v"):
        if s.endswith(suf):
            s = s[: -len(suf)]
    return s.strip()


def _fetch_week_schedule_mp(week_start: date, week_end: date) -> list[dict]:
    import requests as _req
    r = _req.get(
        _MLB_SCHED_URL,
        params={
            "sportId":   1,
            "startDate": week_start.isoformat(),
            "endDate":   week_end.isoformat(),
            "hydrate":   "probablePitcher,linescore,team",
            "gameType":  "R",
        },
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20,
    )
    r.raise_for_status()
    games = []
    for date_entry in r.json().get("dates", []):
        gdate = date.fromisoformat(date_entry["date"])
        for game in date_entry.get("games", []):
            coded = game.get("status", {}).get("codedGameState", "S")
            t = game.get("teams", {})
            home = t.get("home", {})
            away = t.get("away", {})
            hp = home.get("probablePitcher") or {}
            ap = away.get("probablePitcher") or {}
            games.append({
                "game_pk":         game["gamePk"],
                "date":            gdate,
                "status":          coded,
                "home_abbr":       home.get("team", {}).get("abbreviation", ""),
                "away_abbr":       away.get("team", {}).get("abbreviation", ""),
                "home_pitcher":    hp.get("fullName"),
                "home_pitcher_id": hp.get("id"),
                "away_pitcher":    ap.get("fullName"),
                "away_pitcher_id": ap.get("id"),
            })
    return games


def _fetch_boxscore_stats_mp(game_pk: int, pitcher_id: int, cache: dict) -> dict | None:
    import requests as _req
    if game_pk not in cache:
        try:
            r = _req.get(
                _MLB_BOXSCORE_URL.format(game_pk),
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=5,
            )
            cache[game_pk] = r.json() if r.ok else None
        except Exception:
            cache[game_pk] = None

    data = cache[game_pk]
    if not data:
        return None

    teams = data.get("teams", {})
    for side in ("home", "away"):
        player = teams.get(side, {}).get("players", {}).get(f"ID{pitcher_id}")
        if not player:
            continue
        pit = player.get("stats", {}).get("pitching", {})
        if not pit:
            continue

        try:
            ip = float(pit.get("inningsPitched", 0) or 0)
        except (ValueError, TypeError):
            ip = 0.0

        er   = int(pit.get("earnedRuns",  0) or 0)
        hits = int(pit.get("hits",        0) or 0)
        bb   = int(pit.get("baseOnBalls", 0) or 0)
        k    = int(pit.get("strikeOuts",  0) or 0)

        dec   = data.get("decisions") or {}
        is_w  = (dec.get("winner") or {}).get("id") == pitcher_id
        is_l  = (dec.get("loser")  or {}).get("id") == pitcher_id
        is_sv = (dec.get("save")   or {}).get("id") == pitcher_id
        qs    = 1 if ip >= 6.0 and er <= 3 else 0

        opp_side = "away" if side == "home" else "home"
        opp_bat  = teams.get(opp_side, {}).get("teamStats", {}).get("batting", {})
        nh = 1 if int(opp_bat.get("hits", 1) or 1) == 0 else 0
        pg = 1 if (nh
                   and int(opp_bat.get("baseOnBalls", 1) or 1) == 0
                   and int(opp_bat.get("hitBatsmen",  1) or 1) == 0) else 0

        return {
            "IP": ip, "H": hits, "ER": er, "BB": bb, "K": k,
            "W": int(is_w), "L": int(is_l), "SV": int(is_sv),
            "QS": qs, "NH": nh, "PG": pg,
        }
    return None


def _stats_to_pts_mp(stats: dict) -> float:
    from config import PITCHING_WEIGHTS
    return sum(PITCHING_WEIGHTS.get(s, 0) * float(v or 0) for s, v in stats.items())


def _sp_avg_pts(name: str, scored_pitchers) -> float:
    if scored_pitchers is None or scored_pitchers.empty:
        return float("nan")
    norm = _norm_name(name)
    for _, row in scored_pitchers.iterrows():
        if _norm_name(str(row.get("Name", ""))) == norm:
            return float(row.get("pts_per_game", float("nan")))
    last = norm.split()[-1]
    matches = [row for _, row in scored_pitchers.iterrows()
               if _norm_name(str(row.get("Name", ""))).split()[-1] == last]
    if len(matches) == 1:
        return float(matches[0].get("pts_per_game", float("nan")))
    return float("nan")


def _get_matchup_list_mp(league) -> list[dict]:
    import requests as _req, json as _json
    current_mp = getattr(league, "currentMatchupPeriod", 1)
    endpoint = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{SEASON}/segments/0/leagues/{league.league_id}"
    )
    filt = {"schedule": {"filterMatchupPeriodIds": {"value": [current_mp]}}}
    r = _req.get(
        endpoint,
        params={"view": "mMatchup"},
        headers={"User-Agent": "Mozilla/5.0", "x-fantasy-filter": _json.dumps(filt)},
        timeout=15,
    )
    r.raise_for_status()
    team_map = {t.team_id: t.team_name for t in league.teams}
    matchups = []
    for i, m in enumerate(r.json().get("schedule", [])):
        home_id = (m.get("home") or {}).get("teamId")
        away_id = (m.get("away") or {}).get("teamId")
        if home_id is None or away_id is None:
            continue
        matchups.append({
            "id":           i,
            "home_team":    team_map.get(home_id, f"Team {home_id}"),
            "away_team":    team_map.get(away_id, f"Team {away_id}"),
            "home_team_id": home_id,
            "away_team_id": away_id,
        })
    return matchups


def _build_team_starts_mp(
    roster, pitcher_game_map: dict, scored_pitchers,
    cap: int, boxscore_cache: dict,
    team_wrc: dict | None = None,
    last_start_map: dict | None = None,
    team_week_games: dict | None = None,
    week_end: "date | None" = None,
) -> dict:
    from collections import defaultdict
    from datetime import timedelta

    _ACTIVE_SP_SLOTS = {"SP", "P"}
    sps = [
        p for p in roster
        if "SP" in (getattr(p, "eligibleSlots", None) or [])
        and getattr(p, "lineupSlot", "") in _ACTIVE_SP_SLOTS
    ]

    # Build name → MLB Stats API team abbreviation from scored_pitchers
    # (FanGraphs team data is accurate; convert via existing _FG_TO_SCHED map)
    scored_team_map: dict[str, str] = {}
    if scored_pitchers is not None and not scored_pitchers.empty:
        for _, row in scored_pitchers.iterrows():
            n = _norm_name(str(row.get("Name", "")))
            fg_team = str(row.get("Team", ""))
            mlb_team = _FG_TO_SCHED.get(fg_team, fg_team)
            if n and mlb_team:
                scored_team_map[n] = mlb_team.upper()

    all_starts = []
    for p in sps:
        norm     = _norm_name(p.name)
        mlb_team = scored_team_map.get(norm, "")
        games    = pitcher_game_map.get(norm, [])
        if not games:
            last  = norm.split()[-1]
            cands = [k for k in pitcher_game_map if k.split()[-1] == last]
            if len(cands) == 1:
                games = pitcher_game_map[cands[0]]

        # Filter to games on the player's actual MLB team.
        # Falls back to unfiltered only if team unknown (player not in scored data).
        if mlb_team and games:
            team_games = [g for g in games
                          if g.get("pitcher_team", "").upper() == mlb_team]
            if team_games:
                games = team_games

        # Rotation projection for SPs not yet listed as probable pitchers.
        # MLB only announces probables ~2 days ahead; project remaining starts
        # using last_start + 5-day rotation and the team's week schedule.
        if not games and mlb_team and last_start_map and team_week_games and week_end:
            last = last_start_map.get(norm)
            if last is not None:
                team_sched = sorted(
                    team_week_games.get(mlb_team.upper(), []),
                    key=lambda g: g["date"],
                )
                projected: list = []
                base = last
                for _ in range(2):   # at most 2 starts per week
                    for rest in (5, 4, 6, 7):
                        proj_date = base + timedelta(days=rest)
                        if proj_date > week_end:
                            break
                        upcoming = [g for g in team_sched if g["date"] >= proj_date]
                        if upcoming and upcoming[0]["date"] <= week_end:
                            tg = upcoming[0]
                            projected.append({
                                "game_pk":      None,
                                "date":         tg["date"],
                                "status":       tg["status"],
                                "opponent":     tg["opponent"],
                                "is_home":      tg["is_home"],
                                "pitcher_id":   None,
                                "pitcher_team": mlb_team,
                            })
                            base = tg["date"]
                            break
                    else:
                        break   # rest loop exhausted without finding a game
                games = projected

        # A pitcher cannot start twice on the same date — deduplicate.
        seen_dates: set = set()
        deduped = []
        for g in games:
            if g["date"] not in seen_dates:
                seen_dates.add(g["date"])
                deduped.append(g)
        games = deduped

        # Project a 2nd start if the pitcher's only game is early enough in the week
        # for another rotation turn (e.g. Monday start → Saturday start).
        if (len(games) == 1 and mlb_team and team_week_games and week_end):
            latest = games[0]["date"]
            team_sched = sorted(
                team_week_games.get(mlb_team.upper(), []),
                key=lambda g: g["date"],
            )
            for rest in (5, 4, 6, 7):
                proj2 = latest + timedelta(days=rest)
                if proj2 > week_end:
                    break
                upcoming = [g for g in team_sched if g["date"] >= proj2 and g["date"] <= week_end]
                if upcoming and upcoming[0]["date"] not in seen_dates:
                    tg = upcoming[0]
                    games.append({
                        "game_pk":      None,
                        "date":         tg["date"],
                        "status":       tg["status"],
                        "opponent":     tg["opponent"],
                        "is_home":      tg["is_home"],
                        "pitcher_id":   None,
                        "pitcher_team": mlb_team,
                    })
                    break

        avg = _sp_avg_pts(p.name, scored_pitchers)

        for g in games:
            status    = g["status"]
            is_final  = status in _FINAL_STATES
            is_live   = status in _LIVE_STATES
            is_future = not is_final and not is_live
            pid       = g["pitcher_id"]
            pts       = float("nan")

            if (is_final or is_live) and pid:
                raw = _fetch_boxscore_stats_mp(g["game_pk"], pid, boxscore_cache)
                if raw:
                    pts = round(_stats_to_pts_mp(raw), 1)
                elif is_final:
                    pts = 0.0

            if _math.isnan(pts):
                pts      = avg
                is_live  = False
                is_final = False
                is_future = True

            all_starts.append({
                "name":     p.name,
                "date":     g["date"],
                "day":      _DAY_NAMES[g["date"].weekday()],
                "opponent": g["opponent"],
                "is_home":  g.get("is_home", True),
                "opp_wrc":  (team_wrc or {}).get(g["opponent"].upper()),
                "pts":      pts,
                "is_avg":   is_future,
                "is_live":  is_live and not is_future,
                "counts":   True,
                "days_rest": None,
            })

    all_starts.sort(key=lambda x: (x["date"], x["name"]))

    # Compute days rest: days between this start and the pitcher's previous start
    recent: dict[str, date] = dict(last_start_map or {})
    for s in all_starts:
        norm = _norm_name(s["name"])
        prev = recent.get(norm)
        s["days_rest"] = (s["date"] - prev).days - 1 if prev is not None else None
        recent[norm] = s["date"]

    # Cap logic: find breach day, mark subsequent days red
    cum        = 0
    breach_day = None
    by_day: dict = defaultdict(list)
    for s in all_starts:
        by_day[s["date"]].append(s)

    for d in sorted(by_day):
        day_starts = by_day[d]
        if breach_day is not None and d > breach_day:
            for s in day_starts:
                s["counts"] = False
        else:
            cum += len(day_starts)
            if cum >= cap and breach_day is None:
                breach_day = d

    pts_done      = sum(s["pts"] for s in all_starts
                        if not s["is_avg"] and not s["is_live"]
                        and s["counts"] and not _math.isnan(s["pts"]))
    pts_live      = sum(s["pts"] for s in all_starts
                        if s["is_live"] and s["counts"]
                        and not _math.isnan(s["pts"]))
    pts_remaining = sum(s["pts"] for s in all_starts
                        if s["is_avg"] and s["counts"]
                        and not _math.isnan(s["pts"]))

    return {
        "starts":        all_starts,
        "start_count":   sum(1 for s in all_starts if s["counts"]),
        "cap":           cap,
        "breach_day":    breach_day,
        "pts_done":      round(pts_done, 1),
        "pts_live":      round(pts_live, 1),
        "pts_remaining": round(pts_remaining, 1),
        "total":         round(pts_done + pts_live + pts_remaining, 1),
    }


def get_matchup_data(league, matchup_id: int | None, scored_pitchers, scored_hitters=None) -> dict:
    from config import SP_STARTS_CAP
    from datetime import timedelta

    # ESPN matchup weeks run Mon–Sun; week 1 = Mar 30 (first Mon of season)
    _MATCHUP_WEEK1_MON = date(2026, 3, 30)
    current_mp = getattr(league, "currentMatchupPeriod", 1)
    week_start = _MATCHUP_WEEK1_MON + timedelta(days=(current_mp - 1) * 7)
    week_end   = week_start + timedelta(days=6)

    matchup_list = _get_matchup_list_mp(league)

    if matchup_id is None:
        matchup_id = 0
        for m in matchup_list:
            if (MY_TEAM_NAME.lower() in m["home_team"].lower() or
                    MY_TEAM_NAME.lower() in m["away_team"].lower()):
                matchup_id = m["id"]
                break

    sel = next((m for m in matchup_list if m["id"] == matchup_id), None)
    if sel is None and matchup_list:
        sel        = matchup_list[0]
        matchup_id = 0
    if sel is None:
        return {"matchup_list": [], "error": "No matchups found"}

    team_roster = {t.team_id: t.roster for t in league.teams}
    home_roster = team_roster.get(sel["home_team_id"], [])
    away_roster = team_roster.get(sel["away_team_id"], [])

    schedule = _fetch_week_schedule_mp(week_start, week_end)

    pitcher_game_map: dict[str, list] = {}
    for g in schedule:
        for role in ("home_pitcher", "away_pitcher"):
            pname = g.get(role)
            if not pname:
                continue
            pid          = g.get(role + "_id")
            pitcher_team = g["home_abbr"] if role == "home_pitcher" else g["away_abbr"]
            opponent     = g["away_abbr"] if role == "home_pitcher" else g["home_abbr"]
            norm         = _norm_name(pname)
            pitcher_game_map.setdefault(norm, []).append({
                "game_pk":      g["game_pk"],
                "date":         g["date"],
                "status":       g["status"],
                "opponent":     opponent,
                "is_home":      role == "home_pitcher",
                "pitcher_id":   pid,
                "pitcher_team": pitcher_team,
            })

    # Lookback schedule: find each pitcher's last start before this week
    lookback = _fetch_week_schedule_mp(week_start - timedelta(days=8), week_start - timedelta(days=1))
    last_start_map: dict[str, date] = {}
    for g in lookback:
        for role in ("home_pitcher", "away_pitcher"):
            pname = g.get(role)
            if not pname:
                continue
            norm = _norm_name(pname)
            d = g["date"]
            if norm not in last_start_map or d > last_start_map[norm]:
                last_start_map[norm] = d

    # Team → all week games map for rotation projection (all games, not just announced probables)
    team_week_games: dict[str, list] = {}
    for g in schedule:
        for abbr_key, opp_key, is_home in [
            ("home_abbr", "away_abbr", True),
            ("away_abbr", "home_abbr", False),
        ]:
            team_week_games.setdefault(g[abbr_key].upper(), []).append({
                "date":     g["date"],
                "opponent": g[opp_key],
                "is_home":  is_home,
                "status":   g["status"],
            })

    team_wrc = _fetch_team_wrc_plus(scored_hitters)
    boxscore_cache: dict = {}

    home_data = _build_team_starts_mp(
        home_roster, pitcher_game_map, scored_pitchers, SP_STARTS_CAP, boxscore_cache,
        team_wrc=team_wrc, last_start_map=last_start_map,
        team_week_games=team_week_games, week_end=week_end,
    )
    home_data["team_name"] = sel["home_team"]

    away_data = _build_team_starts_mp(
        away_roster, pitcher_game_map, scored_pitchers, SP_STARTS_CAP, boxscore_cache,
        team_wrc=team_wrc, last_start_map=last_start_map,
        team_week_games=team_week_games, week_end=week_end,
    )
    away_data["team_name"] = sel["away_team"]

    delta = round(home_data["total"] - away_data["total"], 1)

    return {
        "matchup_list": matchup_list,
        "matchup_id":   matchup_id,
        "week_start":   week_start,
        "week_end":     week_end,
        "current_mp":   current_mp,
        "cap":          SP_STARTS_CAP,
        "home":         home_data,
        "away":         away_data,
        "delta":        delta,
    }


# ---------------------------------------------------------------------------
# 12. Decisions: evaluate past add/drop transactions
# ---------------------------------------------------------------------------

_OPENING_DAY = date(2026, 3, 27)


def _sp_to_matchup(sp: int) -> int:
    """Scoring period → matchup period (1-indexed, 7 days each)."""
    return (sp - 1) // 7 + 1


def _matchup_date_range(mp: int) -> tuple:
    from datetime import timedelta
    start = _OPENING_DAY + timedelta(days=(mp - 1) * 7)
    end   = start + timedelta(days=6)
    return start, end


def _mlb_week_pts(week_start, week_end) -> dict:
    """
    Fetch actual batting + pitching stats from MLB Stats API for a date range,
    compute fantasy points, and return {player_name: pts}.
    Names are accent-stripped to match ESPN transaction names.
    QS/NH/PG are unavailable from this API and are excluded.
    """
    import unicodedata
    import requests as _req
    from config import BATTING_WEIGHTS, PITCHING_WEIGHTS

    def _strip(s):
        return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")

    _MLB = "https://statsapi.mlb.com/api/v1/stats"
    start_str = week_start.strftime("%Y-%m-%d")
    end_str   = week_end.strftime("%Y-%m-%d")
    season    = week_start.year
    pts: dict[str, float] = {}

    # Batting
    try:
        r = _req.get(_MLB, params={
            "stats": "byDateRange", "group": "hitting", "season": season,
            "startDate": start_str, "endDate": end_str, "playerPool": "all", "limit": 2000,
        }, timeout=20)
        r.raise_for_status()
        for s in r.json().get("stats", [{}])[0].get("splits", []):
            stat = s.get("stat", {})
            name = _strip(s.get("player", {}).get("fullName", ""))
            if not name:
                continue
            h   = int(stat.get("hits", 0) or 0)
            bb2 = int(stat.get("doubles", 0) or 0)
            b3  = int(stat.get("triples", 0) or 0)
            hr  = int(stat.get("homeRuns", 0) or 0)
            tb  = h + bb2 + 2 * b3 + 3 * hr
            p = (
                tb                                      * BATTING_WEIGHTS.get("TB", 0) +
                int(stat.get("runs", 0) or 0)           * BATTING_WEIGHTS.get("R",  0) +
                int(stat.get("rbi", 0) or 0)            * BATTING_WEIGHTS.get("RBI",0) +
                int(stat.get("baseOnBalls", 0) or 0)    * BATTING_WEIGHTS.get("BB", 0) +
                int(stat.get("strikeOuts", 0) or 0)     * BATTING_WEIGHTS.get("K",  0) +
                int(stat.get("stolenBases", 0) or 0)    * BATTING_WEIGHTS.get("SB", 0)
            )
            if name in pts:
                pts[name] = max(pts[name], round(p, 1))  # keep higher if duped
            else:
                pts[name] = round(p, 1)
    except Exception:
        pass

    # Pitching
    try:
        r = _req.get(_MLB, params={
            "stats": "byDateRange", "group": "pitching", "season": season,
            "startDate": start_str, "endDate": end_str, "playerPool": "all", "limit": 2000,
        }, timeout=20)
        r.raise_for_status()
        for s in r.json().get("stats", [{}])[0].get("splits", []):
            stat = s.get("stat", {})
            name = _strip(s.get("player", {}).get("fullName", ""))
            if not name or name in pts:  # skip if already counted as batter
                continue
            ip  = float(stat.get("inningsPitched", 0) or 0)
            p = (
                ip                                          * PITCHING_WEIGHTS.get("IP", 0) +
                int(stat.get("strikeOuts", 0) or 0)         * PITCHING_WEIGHTS.get("K",  0) +
                int(stat.get("hits", 0) or 0)               * PITCHING_WEIGHTS.get("H",  0) +
                int(stat.get("earnedRuns", 0) or 0)         * PITCHING_WEIGHTS.get("ER", 0) +
                int(stat.get("baseOnBalls", 0) or 0)        * PITCHING_WEIGHTS.get("BB", 0) +
                int(stat.get("wins", 0) or 0)               * PITCHING_WEIGHTS.get("W",  0) +
                int(stat.get("losses", 0) or 0)             * PITCHING_WEIGHTS.get("L",  0) +
                int(stat.get("saves", 0) or 0)              * PITCHING_WEIGHTS.get("SV", 0)
            )
            pts[name] = round(p, 1)
    except Exception:
        pass

    return pts


def get_decisions_data(league) -> dict:
    """
    Returns {"decisions": list, "current_mp": int} where each decision is:
      {
        "added": str, "dropped": str|None,
        "date": date, "txn_sp": int, "effective_mp": int,
        "weeks": [{"mp": int, "start": date, "end": date,
                   "added_pts": float|None, "dropped_pts": float|None,
                   "delta": float|None}],
        "net": float,
      }

    Uses FanGraphs date-range stats for all player lookups (works for any
    historical week; QS/NH/PG are excluded as noted in _compute_fa_points).
    """
    from config import MY_TEAM_NAME, ESPN_S2, ESPN_SWID, LEAGUE_ID
    from datetime import datetime, timedelta

    if not ESPN_S2 or not ESPN_SWID:
        return {"decisions": [], "current_mp": 1, "error": "ESPN credentials not configured."}

    # Create an authenticated League for transaction history (requires cookies).
    # All other routes use the unauthenticated league object.
    from espn_api.baseball import League as _League
    auth_league = _League(league_id=LEAGUE_ID, year=SEASON, espn_s2=ESPN_S2, swid=ESPN_SWID)

    acts = auth_league.recent_activity(size=500)
    current_mp = getattr(auth_league, "currentMatchupPeriod", 1)

    # Parse my add/drop pairs
    raw_decisions = []
    for a in acts:
        teams_in_act = {str(item[0]) for item in a.actions}
        if not any(MY_TEAM_NAME.lower() in t.lower() for t in teams_in_act):
            continue
        adds  = [item[2] for item in a.actions
                 if "ADDED" in item[1] and MY_TEAM_NAME.lower() in str(item[0]).lower()]
        drops = [item[2] for item in a.actions
                 if "DROPPED" in item[1] and MY_TEAM_NAME.lower() in str(item[0]).lower()]
        if not adds:
            continue

        txn_date = datetime.fromtimestamp(a.date / 1000).date()
        sp = max(1, (txn_date - _OPENING_DAY).days + 1)

        for added in adds:
            dropped = drops[0] if drops else None
            raw_decisions.append({"added": added, "dropped": dropped, "date": txn_date, "txn_sp": sp})

    # Determine effective matchup period for each decision by checking
    # whether the added player appeared on the Tuesday roster that week.
    decisions = []
    for d in raw_decisions:
        mp_txn = _sp_to_matchup(d["txn_sp"])

        effective_mp = mp_txn + 1  # default: takes effect next week
        for mp_candidate in (mp_txn, mp_txn + 1):
            if mp_candidate > current_mp:
                break
            start, _ = _matchup_date_range(mp_candidate)
            days_to_tue = (1 - start.weekday()) % 7
            tuesday = start + timedelta(days=days_to_tue)
            roster = get_roster_for_day(MY_TEAM_NAME, league=league, target_date=tuesday)
            if d["added"] in {p.name for p in roster}:
                effective_mp = mp_candidate
                break

        # Collect per-week points for completed weeks using MLB Stats API.
        # ESPN's rosterForMatchupPeriod is only available for active/recent matchups.
        weeks = []
        mlb_cache: dict = {}  # mp -> pts_map

        for mp in range(effective_mp, current_mp):
            if mp not in mlb_cache:
                ws, we = _matchup_date_range(mp)
                try:
                    mlb_cache[mp] = {"pts": _mlb_week_pts(ws, we), "start": ws, "end": we}
                except Exception:
                    mlb_cache[mp] = {"pts": {}, "start": ws, "end": we}

            c = mlb_cache[mp]
            ws, we = c["start"], c["end"]

            all_pts     = c["pts"]
            added_pts   = all_pts.get(d["added"])
            dropped_pts = all_pts.get(d["dropped"]) if d["dropped"] else None

            delta = (round(added_pts - dropped_pts, 1)
                     if added_pts is not None and dropped_pts is not None else None)

            weeks.append({
                "mp":          mp,
                "start":       ws,
                "end":         we,
                "added_pts":   round(added_pts,   1) if added_pts   is not None else None,
                "dropped_pts": round(dropped_pts, 1) if dropped_pts is not None else None,
                "delta":       delta,
            })

        net          = round(sum(w["delta"]       for w in weeks if w["delta"]       is not None), 1)
        total_added  = round(sum(w["added_pts"]   for w in weeks if w["added_pts"]   is not None), 1)
        total_dropped= round(sum(w["dropped_pts"] for w in weeks if w["dropped_pts"] is not None), 1)
        decisions.append({
            **d,
            "effective_mp":  effective_mp,
            "weeks":         weeks,
            "net":           net,
            "total_added":   total_added,
            "total_dropped": total_dropped,
        })

    return {"decisions": decisions, "current_mp": current_mp}
