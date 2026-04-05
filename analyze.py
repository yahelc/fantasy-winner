"""
Main analysis script.

Usage:
    python analyze.py                    # full report
    python analyze.py --roster           # just your roster scores
    python analyze.py --fa --pos OF      # free agent hitters at OF
    python analyze.py --fa --pos SP      # free agent starters
    python analyze.py --compare "Name1" "Name2"  # head-to-head
"""

import argparse
from datetime import datetime
from pathlib import Path
import pandas as pd
from tabulate import tabulate

from config import MY_TEAM_NAME, SEASON, MIN_PA_RELIABLE, MIN_IP_RELIABLE
from fetch_data import get_hitters, get_pitchers
from fetch_espn import get_league, get_my_team, get_free_agents, get_roster_for_day, player_summary
from score_players import score_hitters, score_pitchers

CACHE_DIR = Path(__file__).parent / "cache"
HITTERS_CACHE  = CACHE_DIR / "hitters.parquet"
PITCHERS_CACHE = CACHE_DIR / "pitchers.parquet"


def _load_or_fetch(cache_path: Path, fetch_fn, fresh: bool, label: str) -> pd.DataFrame:
    if not fresh and cache_path.exists():
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")
        print(f"  Using cached {label} (saved {mtime}) — pass --fresh to re-fetch")
        return pd.read_parquet(cache_path)
    df = fetch_fn()
    CACHE_DIR.mkdir(exist_ok=True)
    df.to_parquet(cache_path, index=False)
    print(f"  Saved {label} to cache.")
    return df


HITTER_POSITIONS = {"C", "1B", "2B", "3B", "SS", "LF", "CF", "RF", "OF", "DH", "UTL"}
PITCHER_POSITIONS = {"SP", "RP", "P"}

DISPLAY_HITTER_COLS = ["Name", "Team", "Yr", "G", "PA", "PA_total", "xwOBA", "K%", "BB%", "SB",
                       "pts_per_game", "composite_score", "note"]
DISPLAY_PITCHER_COLS = ["Name", "Team", "Yr", "G", "G_total", "GS", "IP", "xERA", "xFIP", "K%",
                        "pts_per_game", "pts_per_week", "composite_score", "note"]


def _add_notes(df: pd.DataFrame, pa_col: str = "PA", ip_col: str = "IP") -> pd.DataFrame:
    """Add a note column flagging small sample or prior-season data."""
    df = df.copy()
    notes = []
    for _, row in df.iterrows():
        n = []
        if "data_season" in df.columns and row.get("data_season") == SEASON - 1:
            n.append(f"{SEASON-1} data")
        if pa_col in df.columns and pd.notna(row.get(pa_col)) and row[pa_col] < MIN_PA_RELIABLE:
            n.append("small sample")
        if ip_col in df.columns and pa_col not in df.columns and pd.notna(row.get(ip_col)) and row[ip_col] < MIN_IP_RELIABLE:
            n.append("small sample")
        notes.append(", ".join(n) if n else "")
    df["note"] = notes
    if "data_season" in df.columns:
        df["Yr"] = df["data_season"].astype(int)
    return df


def _pct_fmt(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    """Format rate columns as percentages for display."""
    df = df.copy()
    for c in cols:
        if c in df.columns:
            df[c] = df[c].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "—")
    return df


def print_table(df: pd.DataFrame, cols: list, title: str, n: int = 30):
    df = _add_notes(df)
    available = [c for c in cols if c in df.columns]
    display = df[available].head(n).copy()
    display = _pct_fmt(display, ["K%", "BB%"])
    for int_col in ["PA_total", "G_total"]:
        if int_col in display.columns:
            display[int_col] = display[int_col].apply(
                lambda x: str(int(x)) if pd.notna(x) else "—"
            )
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")
    print(tabulate(display, headers="keys", tablefmt="simple", showindex=False,
                   floatfmt=".3f"))


_BENCH_SLOTS = {"BE", "BN", "IL", "IL+", "NA"}


def roster_report(league, hitters_scored, pitchers_scored):
    """Score your current roster."""
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if my_team is None:
        print("Could not find your team. Check MY_TEAM_NAME in config.py")
        return

    owners = ", ".join(o.get("firstName", "") + " " + o.get("lastName", "") for o in (my_team.owners or []))
    print(f"\nTeam: {my_team.team_name} | Owner: {owners}")
    roster_names = {p.name for p in my_team.roster}

    my_hitters  = hitters_scored[hitters_scored["Name"].isin(roster_names)]
    my_pitchers = pitchers_scored[pitchers_scored["Name"].isin(roster_names)]

    unmatched = roster_names - set(my_hitters["Name"]) - set(my_pitchers["Name"])
    if unmatched:
        print(f"\n  (No Statcast data found for: {', '.join(sorted(unmatched))})")

    print_table(my_hitters, DISPLAY_HITTER_COLS, "YOUR HITTERS — ranked by composite score")
    print_table(my_pitchers, DISPLAY_PITCHER_COLS, "YOUR PITCHERS — ranked by composite score")


def lineup_report(league, hitters_scored, pitchers_scored):
    """
    Show full roster with ESPN lineup slot (active vs bench), sorted by
    composite score within each group. Flags bench players who score
    higher than the weakest active player at a shared position.
    """
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if my_team is None:
        print("Could not find your team. Check MY_TEAM_NAME in config.py")
        return

    owners = ", ".join(o.get("firstName", "") + " " + o.get("lastName", "") for o in (my_team.owners or []))
    print(f"\nTeam: {my_team.team_name} | Owner: {owners}")

    # Fetch roster as of next Tuesday to reflect cleared transactions
    print("  Fetching next-week roster (Tuesday)...")
    roster = get_roster_for_day(MY_TEAM_NAME, league=league)
    if not roster:
        print("  Falling back to current roster.")
        roster = my_team.roster

    # Build slot map from ESPN: name → lineupSlot
    slot_map = {p.name: getattr(p, "lineupSlot", "BE") for p in roster}
    eligible_map = {p.name: _primary_slots(p.eligibleSlots) for p in roster}
    roster_names = set(slot_map.keys())

    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)
    my_players = all_scored[all_scored["Name"].isin(roster_names)].copy()
    my_players["Slot"] = my_players["Name"].map(slot_map)

    is_bench = my_players["Slot"].isin(_BENCH_SLOTS)
    active = my_players[~is_bench].sort_values("composite_score", ascending=False)
    bench  = my_players[is_bench].sort_values("composite_score", ascending=False)

    # Flag bench players who score higher than their weakest same-position active player
    active_by_pos: dict[str, float] = {}
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

    is_pitcher_active = active["Name"].isin(set(pitchers_scored["Name"]))
    h_active = active[~is_pitcher_active]
    p_active = active[is_pitcher_active]
    is_pitcher_bench = bench["Name"].isin(set(pitchers_scored["Name"]))
    h_bench = bench[~is_pitcher_bench]
    p_bench = bench[is_pitcher_bench]

    h_cols = ["Slot", "Name", "Team", "Yr", "PA", "PA_total", "xwOBA",
              "K%", "BB%", "SB", "pts_per_game", "composite_score", "note"]
    p_cols = ["Slot", "Name", "Team", "Yr", "G", "G_total", "GS", "IP",
              "xERA", "xFIP", "K%", "pts_per_game", "pts_per_week", "composite_score", "note"]

    print_table(h_active, h_cols, "ACTIVE HITTERS", n=30)
    if not h_bench.empty:
        bench_h_cols = h_cols + ["flag"]
        print_table(h_bench, bench_h_cols, "BENCH HITTERS", n=15)

    print_table(p_active, p_cols, "ACTIVE PITCHERS", n=20)
    if not p_bench.empty:
        bench_p_cols = p_cols + ["flag"]
        print_table(p_bench, bench_p_cols, "BENCH PITCHERS", n=10)


def fa_report(league, hitters_scored, pitchers_scored, position: str = None, n: int = 25):
    """Show top available free agents, optionally filtered by position."""
    print(f"\nFetching free agents (position filter: {position or 'ALL'})...")
    fas = get_free_agents(league, size=200)
    fa_names = {p.name for p in fas}

    if position and position.upper() in PITCHER_POSITIONS:
        pool = pitchers_scored[pitchers_scored["Name"].isin(fa_names)]
        print_table(pool, DISPLAY_PITCHER_COLS,
                    f"TOP FREE AGENT PITCHERS ({position.upper()})", n=n)
    elif position and position.upper() in HITTER_POSITIONS:
        pool = hitters_scored[hitters_scored["Name"].isin(fa_names)]
        print_table(pool, DISPLAY_HITTER_COLS,
                    f"TOP FREE AGENT HITTERS ({position.upper()})", n=n)
    else:
        pool_h = hitters_scored[hitters_scored["Name"].isin(fa_names)]
        pool_p = pitchers_scored[pitchers_scored["Name"].isin(fa_names)]
        print_table(pool_h, DISPLAY_HITTER_COLS, "TOP FREE AGENT HITTERS", n=n)
        print_table(pool_p, DISPLAY_PITCHER_COLS, "TOP FREE AGENT PITCHERS", n=n)



def _sample_note(row) -> str:
    notes = []
    if row.get("data_season") == SEASON - 1:
        notes.append(f"{SEASON-1} data")
    pa = row.get("PA", None)
    ip = row.get("IP", None)
    if pa is not None and pa < MIN_PA_RELIABLE:
        notes.append(f"only {int(pa)} PA")
    elif ip is not None and pa is None and ip < MIN_IP_RELIABLE:
        notes.append(f"only {ip:.0f} IP")
    return ", ".join(notes)


# Slots that are too generic to use for position matching —
# a UTIL-eligible catcher should NOT match a 2B slot.
_GENERIC_SLOTS = {"UTIL", "BE", "BN", "IL", "IL+", "NA", "IF", "OF", "DH", "P", "RP"}

def _primary_slots(slots: list) -> set:
    """Return only specific position slots, stripping generic ones."""
    return {s for s in (slots or []) if s not in _GENERIC_SLOTS}


def upgrade_report(league, hitters_scored, pitchers_scored, n: int = 15):
    """
    For each roster position slot, find the best available FA who can fill
    that same specific slot and scores higher than the current occupant.

    Matching is done on PRIMARY positions only (C, 1B, 2B, 3B, SS, LF, CF, RF,
    SP, RP) — generic slots like UTIL, BE, IF, OF are excluded from matching
    so that a catcher doesn't show up as an upgrade at shortstop.
    """
    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        return

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

        # FAs who share at least one primary position slot
        matching_fas = [
            fa for fa, fa_primary in fa_positions.items()
            if fa_primary & my_primary  # non-empty intersection of specific slots
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
                "Drop": player_name,
                "My Score": round(my_score, 3),
                "Add": best_fa["Name"],
                "FA Score": round(best_fa["composite_score"], 3),
                "Delta": round(delta, 3),
                "Shared Pos": "/".join(sorted(shared)),
                "Note": _sample_note(best_fa),
            })

    upgrades_df = pd.DataFrame(upgrades).sort_values("Delta", ascending=False)

    print(f"\n{'='*70}")
    print("  UPGRADE OPPORTUNITIES  (matched on specific position slots only)")
    print(f"{'='*70}")
    if upgrades_df.empty:
        print("  No upgrades found — you're locked in!")
    else:
        print(tabulate(upgrades_df.head(n), headers="keys", tablefmt="simple",
                       showindex=False, floatfmt=".3f"))


def upgrade_pos_report(league, hitters_scored, pitchers_scored, position: str, n: int = 20):
    """
    Show my current player(s) at a position alongside top FA options in one table.
    My players are marked "MINE"; free agents marked "FA".
    Sorted by composite_score so you can see exactly where the upgrade threshold is.

    For UTIL: shows all my hitters (any hitter is UTIL-eligible) + top FA hitters.
    For specific slots (SS, 3B, etc.): filters to players with that primary position.
    """
    pos = position.upper()
    if pos == "UTIL":
        pos = "UTL"

    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        return

    fas = get_free_agents(league, size=300)
    is_pitcher = pos in PITCHER_POSITIONS
    scored = pitchers_scored if is_pitcher else hitters_scored

    if pos == "UTL":
        # Any hitter qualifies for UTIL
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

    print_table(combined, display_cols,
                f"{'PITCHERS' if is_pitcher else 'HITTERS'} — {pos}: MINE vs FA", n=n)


HITTER_DEBUG_COLS  = ["Name", "PA", "PA_total", "K%", "BB%", "xwOBA", "xSLG",
                      "proj_TB_g", "proj_R_g", "proj_RBI_g", "proj_BB_g",
                      "proj_K_g", "proj_SB_g", "pts_per_game", "xwOBA_zscore", "composite_score"]
PITCHER_DEBUG_COLS = ["Name", "IP", "K%", "xERA", "xFIP",
                      "proj_IP_g", "proj_K_g", "proj_H_g", "proj_ER_g", "proj_BB_g",
                      "proj_QS_g", "proj_W_g", "proj_L_g", "pts_per_game", "xFIP_zscore", "composite_score"]


def compare_players(names: list, hitters_scored, pitchers_scored, debug: bool = False):
    """Side-by-side comparison of named players."""
    all_scored = pd.concat([hitters_scored, pitchers_scored], ignore_index=True)
    rows = []
    is_pitcher = []
    for name in names:
        match = all_scored[all_scored["Name"].str.contains(name, case=False, na=False)]
        if match.empty:
            print(f"  No data found for: {name}")
        else:
            rows.append(match.iloc[0])
            is_pitcher.append(match.iloc[0]["Name"] in set(pitchers_scored["Name"]))
    if not rows:
        return
    result = pd.DataFrame(rows)
    if debug:
        # Separate hitters and pitchers for cleaner debug output
        h_rows = result[~result["Name"].isin(pitchers_scored["Name"])]
        p_rows = result[result["Name"].isin(pitchers_scored["Name"])]
        if not h_rows.empty:
            cols = [c for c in HITTER_DEBUG_COLS if c in h_rows.columns]
            print_table(h_rows, cols, "HITTER COMPONENT BREAKDOWN")
        if not p_rows.empty:
            cols = [c for c in PITCHER_DEBUG_COLS if c in p_rows.columns]
            print_table(p_rows, cols, "PITCHER COMPONENT BREAKDOWN")
    else:
        cols = [c for c in DISPLAY_HITTER_COLS + DISPLAY_PITCHER_COLS if c in result.columns]
        cols = list(dict.fromkeys(cols))  # deduplicate preserving order
        print_table(result, cols, "PLAYER COMPARISON")


def week_report(league, hitters_scored, pitchers_scored):
    """
    Project next week's fantasy points for your roster based on the
    actual team schedule (games per team from MLB Stats API).

    Hitters:  pts/game × games_next_week
    Starters: pts/game × games_next_week / 5  (rotation slot)
    Relievers: pts/game × apps_per_week × (games_next_week / 7)
    """
    from fetch_schedule import get_team_games_next_week
    import numpy as np

    team_games, week_start, week_end = get_team_games_next_week()
    print(f"  Schedule: {week_start} – {week_end}")
    if not team_games:
        print("  No games found for that week (off-season or API error).")
        return

    my_team = get_my_team(league, team_name=MY_TEAM_NAME)
    if not my_team:
        return

    roster_names = {p.name for p in my_team.roster}

    # --- Hitters ---
    h = hitters_scored[hitters_scored["Name"].isin(roster_names)].copy()
    h["gms"] = h["Team"].map(team_games).fillna(0).astype(int)
    h["proj_week_pts"] = (h["pts_per_game"] * h["gms"]).round(2)
    h = h.sort_values("proj_week_pts", ascending=False)

    h_cols = ["Name", "Team", "Yr", "gms", "pts_per_game", "proj_week_pts", "note"]

    # --- Pitchers ---
    p = pitchers_scored[pitchers_scored["Name"].isin(roster_names)].copy()
    p["gms"] = p["Team"].map(team_games).fillna(0).astype(int)

    gs = p.get("GS", pd.Series(0, index=p.index)).fillna(0)
    is_starter = gs > (p["G"] * 0.5)

    # apps/week from actual usage rate (G_total / weeks_sampled)
    if "G_total" in p.columns and "weeks_sampled" in p.columns:
        apps_per_week = (p["G_total"] / p["weeks_sampled"].replace(0, np.nan)).fillna(
            np.where(is_starter, 1.4, 2.5)
        )
    else:
        apps_per_week = pd.Series(np.where(is_starter, 1.4, 2.5), index=p.index)

    proj = pd.Series(index=p.index, dtype=float)
    proj[is_starter]  = p.loc[is_starter,  "pts_per_game"] * (p.loc[is_starter,  "gms"] / 5)
    proj[~is_starter] = (p.loc[~is_starter, "pts_per_game"]
                         * apps_per_week[~is_starter]
                         * (p.loc[~is_starter, "gms"] / 7))
    p["proj_week_pts"] = proj.round(2)
    p = p.sort_values("proj_week_pts", ascending=False)

    p_cols = ["Name", "Team", "Yr", "gms", "GS", "pts_per_game", "pts_per_week",
              "proj_week_pts", "note"]

    total_h = h["proj_week_pts"].sum()
    total_p = p["proj_week_pts"].sum()

    print_table(h, h_cols, f"HITTERS — projected pts, week of {week_start}", n=30)
    print_table(p, p_cols, f"PITCHERS — projected pts, week of {week_start}", n=20)
    print(f"\n  Roster total: {total_h:.1f} (bat) + {total_p:.1f} (pitch) = "
          f"{total_h + total_p:.1f} pts")


def schedule_report(league, hitters_scored, pitchers_scored, n: int = 20):
    """
    For the upcoming week, show:
    1. Your roster with team game counts and day-by-day schedule
    2. Rotation-inferred start projections for starters (last start + 5-day steps)
    3. Top FA hitters and starters ranked by games / start projections
    """
    from fetch_schedule import get_schedule, project_starts_in_week
    import numpy as np

    # FanGraphs uses some 3-letter codes the MLB schedule API doesn't
    _FG_TO_SCHED = {"SDP": "SD", "SFG": "SF", "TBR": "TB", "KCR": "KC",
                    "WSN": "WSH", "CHW": "CWS", "ARI": "ARI"}

    schedule, week_start, week_end = get_schedule()
    print(f"  Week: {week_start} – {week_end}\n")

    if not schedule:
        print("  No schedule data found.")
        return

    # --- Roster ---
    roster = get_roster_for_day(MY_TEAM_NAME, league=league)
    if not roster:
        my_team = get_my_team(league, team_name=MY_TEAM_NAME)
        roster = my_team.roster if my_team else []
    roster_names = {p.name for p in roster}

    fas = get_free_agents(league, size=300)
    fa_names = {p.name for p in fas}

    def _enrich_hitters(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        sched_team = df["Team"].map(lambda t: _FG_TO_SCHED.get(t, t))
        df["gms"]  = sched_team.map(lambda t: schedule.get(t, {}).get("games", 0))
        df["days"] = sched_team.map(lambda t: " ".join(schedule.get(t, {}).get("days", [])))
        return df

    def _enrich_pitchers(df: pd.DataFrame, start_proj: dict) -> pd.DataFrame:
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
                return "?"           # starter but no last-start data
            dates = " ".join(d.strftime("%a%-d") for d in proj)
            label = f"{len(proj)}× ({dates})"
            if len(proj) >= 2:
                label += " ?"   # ~30% precision in backtesting
            return label

        df["starts"] = df.apply(_starts_col, axis=1)
        df["_n_starts"] = df["Name"].map(
            lambda n: len(start_proj.get(n, [])) if is_starter[df[df["Name"] == n].index[0]] else 0
            if n in df["Name"].values else 0
        )
        return df

    # My roster pitchers — look up all starters
    my_p_df = pitchers_scored[pitchers_scored["Name"].isin(roster_names)].copy()
    gs = my_p_df.get("GS", pd.Series(0, index=my_p_df.index)).fillna(0)
    my_starters = my_p_df[gs > (my_p_df["G"] * 0.5)]["Name"].tolist()

    # FA pitchers — look up starters among top candidates by score
    fa_p_df = pitchers_scored[pitchers_scored["Name"].isin(fa_names)].copy()
    fa_gs = fa_p_df.get("GS", pd.Series(0, index=fa_p_df.index)).fillna(0)
    fa_starters = (fa_p_df[fa_gs > (fa_p_df["G"] * 0.5)]
                   .sort_values("composite_score", ascending=False)
                   .head(40)["Name"].tolist())

    all_starters = list(set(my_starters + fa_starters))
    print(f"  Looking up last start dates for {len(all_starters)} starters...")
    start_proj = project_starts_in_week(all_starters, week_start, week_end)

    my_h = _enrich_hitters(hitters_scored[hitters_scored["Name"].isin(roster_names)])
    my_p = _enrich_pitchers(my_p_df, start_proj)
    fa_h = _enrich_hitters(hitters_scored[hitters_scored["Name"].isin(fa_names)])
    fa_p = _enrich_pitchers(fa_p_df, start_proj)

    h_cols  = ["Name", "Team", "gms", "days", "pts_per_game", "composite_score"]
    sp_cols = ["Name", "Team", "gms", "starts", "pts_per_game", "pts_per_week", "composite_score"]

    print_table(my_h.sort_values("gms", ascending=False),
                h_cols, "MY HITTERS — games next week", n=25)
    print_table(my_p.sort_values(["_n_starts", "gms", "composite_score"], ascending=False),
                sp_cols, "MY PITCHERS — projected starts next week  (2× ? = possible, ~30% hist. precision)", n=15)

    print_table(
        fa_h.sort_values(["gms", "composite_score"], ascending=False),
        h_cols + ["note"], "TOP FA HITTERS — by games next week", n=n,
    )
    print_table(
        fa_p.sort_values(["_n_starts", "composite_score"], ascending=False),
        sp_cols + ["note"], "TOP FA STARTERS — 2-start candidates first  (? = possible, not confirmed)", n=n,
    )


def main():
    parser = argparse.ArgumentParser(description="Fantasy baseball analytics")
    parser.add_argument("--roster",      action="store_true", help="Show your roster scores")
    parser.add_argument("--lineup",      action="store_true", help="Show full roster with active/bench slots and START? flags")
    parser.add_argument("--fa",          action="store_true", help="Show top free agents")
    parser.add_argument("--upgrade",     action="store_true", help="Show upgrade opportunities")
    parser.add_argument("--upgrade-pos", type=str, metavar="POS",
                        help="My players vs top FAs at a position (e.g. SS, UTIL, SP)")
    parser.add_argument("--compare",     nargs="+",           help="Compare players by name")
    parser.add_argument("--debug",       action="store_true", help="Show scoring component breakdown (use with --compare)")
    parser.add_argument("--pos",         type=str,            help="Filter by position (e.g. SP, OF)")
    parser.add_argument("--n",           type=int, default=25, help="Number of rows to show")
    parser.add_argument("--week",        action="store_true",
                        help="Project next week's roster pts based on actual schedule")
    parser.add_argument("--schedule",    action="store_true",
                        help="Show next week's game schedule per player + 2-start flags + top FAs by games")
    parser.add_argument("--fresh",       action="store_true",
                        help="Re-fetch all data from FanGraphs, ignoring cache")
    args = parser.parse_args()

    # Default: run everything
    run_all = not any([args.roster, args.fa, args.upgrade, args.upgrade_pos,
                       args.compare, args.week, args.lineup, args.schedule])

    print(f"Loading data for {SEASON} season...")
    hitters  = _load_or_fetch(HITTERS_CACHE,  get_hitters,  args.fresh, "hitters")
    pitchers = _load_or_fetch(PITCHERS_CACHE, get_pitchers, args.fresh, "pitchers")

    hitters_scored  = score_hitters(hitters)
    pitchers_scored = score_pitchers(pitchers)

    print("Connecting to ESPN...")
    league = get_league()
    print(f"  Connected: {league.settings.name}")

    if args.compare:
        compare_players(args.compare, hitters_scored, pitchers_scored, debug=args.debug)
        return

    if args.upgrade_pos:
        upgrade_pos_report(league, hitters_scored, pitchers_scored,
                           args.upgrade_pos, n=args.n)
        return

    if args.lineup:
        lineup_report(league, hitters_scored, pitchers_scored)
        return

    if run_all or args.roster:
        roster_report(league, hitters_scored, pitchers_scored)

    if run_all or args.fa:
        fa_report(league, hitters_scored, pitchers_scored, position=args.pos, n=args.n)

    if run_all or args.upgrade:
        upgrade_report(league, hitters_scored, pitchers_scored)

    if args.week:
        print("\nFetching next week's MLB schedule...")
        week_report(league, hitters_scored, pitchers_scored)

    if args.schedule:
        print("\nFetching next week's MLB schedule...")
        schedule_report(league, hitters_scored, pitchers_scored, n=args.n)


if __name__ == "__main__":
    main()
