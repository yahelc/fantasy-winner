"""
Targeted analyses:

1. Who should replace Bo Bichette? (2B/SS eligible, ranked by 2026 bat speed)
2. Caballero vs Cruz: does the SB rate justify the K penalty in this scoring system?

Run:
    python questions.py
"""

import pandas as pd
import numpy as np
from tabulate import tabulate

from fetch_data import get_hitters
from fetch_espn import get_league, get_free_agents
from score_players import score_hitters
from bat_speed import get_bat_speed, merge_with_statcast
from config import SEASON, BATTING_WEIGHTS


# ── 1. Bichette replacement ────────────────────────────────────────────────

def bichette_replacement():
    print("\n" + "="*70)
    print("  BICHETTE REPLACEMENT: 2026 bat speed × FA eligibility")
    print("  Logic: 2026 early bat speed as leading indicator of true talent")
    print("="*70)

    # Pull all data
    bat_df    = get_bat_speed()
    fg_df     = score_hitters(get_hitters())
    league    = get_league()
    fas       = get_free_agents(league, size=300)

    fa_names     = {p.name for p in fas}
    fa_slots     = {p.name: set(p.eligibleSlots or []) for p in fas}

    # Bichette is 2B/SS — find FAs with either slot
    TARGET_SLOTS = {"2B", "SS"}
    eligible_fas = {
        name for name, slots in fa_slots.items()
        if slots & TARGET_SLOTS
    }

    # Merge bat speed with FanGraphs scores
    merged = merge_with_statcast(bat_df, fg_df)

    # Filter to FA-eligible players at 2B/SS
    fa_pool = merged[merged["Name"].isin(eligible_fas)].copy()

    # Add a "bat speed rank" among all players in the leaderboard
    merged_sorted = merged.sort_values("bat_speed", ascending=False).reset_index(drop=True)
    merged_sorted["bs_rank"] = merged_sorted.index + 1
    fa_pool = fa_pool.merge(merged_sorted[["Name", "bs_rank"]], on="Name", how="left")

    fa_pool = fa_pool.sort_values("bat_speed", ascending=False)

    # Show Bichette's profile first
    bichette = fg_df[fg_df["Name"] == "Bo Bichette"]
    bichette_bs = bat_df[bat_df["Name"] == "Bo Bichette"]
    print(f"\n  Bo Bichette (current):")
    if not bichette.empty:
        b = bichette.iloc[0]
        print(f"    xwOBA: {b.get('xwOBA', '—'):.3f}  K%: {b.get('K%', 0):.1%}  "
              f"BB%: {b.get('BB%', 0):.1%}  SB: {b.get('SB', 0):.0f}  "
              f"pts/G: {b.get('pts_per_game', 0):.3f}  score: {b.get('composite_score', 0):.3f}")
    if not bichette_bs.empty:
        bs = bichette_bs.iloc[0]
        print(f"    2026 bat speed: {bs.get('bat_speed', '—')} mph  "
              f"hard swing: {bs.get('hard_swing_rate', 0):.1%}  "
              f"whiff%: {bs.get('whiff%', 0):.1%}  swings: {bs.get('swings', 0):.0f}")
    else:
        print("    (No 2026 bat speed data yet for Bichette)")

    print(f"\n  Available FA upgrades at 2B/SS — ranked by 2026 bat speed:")
    print(f"  (higher bat speed = harder contact potential; check swings for sample size)\n")

    display_cols = ["Name", "swings", "bat_speed", "hard_swing_rate",
                    "whiff%", "xwOBA", "K%", "BB%", "SB",
                    "pts_per_game", "composite_score", "data_season"]
    available = [c for c in display_cols if c in fa_pool.columns]
    display = fa_pool[available].head(20).copy()

    # Format
    for pct_col in ["K%", "BB%", "hard_swing_rate", "whiff%"]:
        if pct_col in display.columns:
            display[pct_col] = display[pct_col].apply(
                lambda x: f"{x:.1%}" if pd.notna(x) and isinstance(x, float) else "—"
            )
    display["bat_speed"] = display["bat_speed"].apply(
        lambda x: f"{x:.1f}" if pd.notna(x) else "—"
    )

    print(tabulate(display, headers="keys", tablefmt="simple",
                   showindex=False, floatfmt=".3f"))

    # Highlight best option
    valid = fa_pool.dropna(subset=["bat_speed", "composite_score"])
    valid = valid[valid["swings"] >= 20]  # at least 20 competitive swings
    if not valid.empty:
        top = valid.sort_values("bat_speed", ascending=False).iloc[0]
        print(f"\n  >> Top bat-speed FA at 2B/SS with ≥20 swings: {top['Name']} "
              f"({top['bat_speed']:.1f} mph)")


# ── 2. Caballero vs Cruz: SB vs K math ────────────────────────────────────

def sb_vs_k_analysis():
    print("\n" + "="*70)
    print("  CABALLERO vs CRUZ: stolen base premium vs strikeout penalty")
    print(f"  Scoring: SB = +{BATTING_WEIGHTS['SB']} pts,  K = {BATTING_WEIGHTS['K']} pt")
    print("="*70)

    fg_df = score_hitters(get_hitters())

    players = {
        "Jose Caballero": fg_df[fg_df["Name"] == "Jose Caballero"],
        "Oneil Cruz":     fg_df[fg_df["Name"] == "Oneil Cruz"],
    }

    # Also pull a "baseline" comparable — average 2B/SS/OF type
    # Use Bo Bichette as baseline since he's the player being considered for replacement
    players["Bo Bichette (baseline)"] = fg_df[fg_df["Name"] == "Bo Bichette"]

    results = []
    component_details = {}

    for label, df in players.items():
        if df.empty:
            print(f"  No data for {label}")
            continue
        row = df.iloc[0]

        g   = row.get("G", 1)
        pa  = row.get("PA", 0)
        ab  = row.get("AB", pa * 0.88)

        pa_g = pa / g
        ab_g = ab / g

        xslg  = row.get("xSLG", row.get("SLG", 0.400))
        xobp  = row.get("xOBP", row.get("OBP", 0.320))
        k_pct = row.get("K%", 0.22)
        bb_pct= row.get("BB%", 0.08)
        sb    = row.get("SB", 0)
        sb_g  = sb / g
        k_g   = k_pct * pa_g
        bb_g  = bb_pct * pa_g
        tb_g  = xslg * ab_g
        r_g   = xobp * pa_g * 0.42
        rbi_g = xslg * ab_g * 0.28

        components = {
            "TB/G":  (tb_g,  BATTING_WEIGHTS["TB"]),
            "R/G":   (r_g,   BATTING_WEIGHTS["R"]),
            "RBI/G": (rbi_g, BATTING_WEIGHTS["RBI"]),
            "BB/G":  (bb_g,  BATTING_WEIGHTS["BB"]),
            "K/G":   (k_g,   BATTING_WEIGHTS["K"]),
            "SB/G":  (sb_g,  BATTING_WEIGHTS["SB"]),
        }

        pts_g = sum(rate * weight for rate, weight in components.values())
        sb_pts_g  = sb_g  * BATTING_WEIGHTS["SB"]
        k_pts_g   = k_g   * BATTING_WEIGHTS["K"]
        net_sb_k  = sb_pts_g + k_pts_g  # SB gain minus K drag

        # Per 162 games projection
        scale = 162 / g

        results.append({
            "Player":          label,
            "G":               int(g),
            "PA/G":            round(pa_g, 2),
            "SB/G":            round(sb_g, 3),
            "K/G":             round(k_g, 3),
            "SB pts/G":        round(sb_pts_g, 3),
            "K pts/G":         round(k_pts_g, 3),
            "Net SB-K/G":      round(net_sb_k, 3),
            "Other pts/G":     round(pts_g - net_sb_k, 3),
            "Total pts/G":     round(pts_g, 3),
            "Proj pts/162G":   round(pts_g * 162, 1),
            "xwOBA":           round(row.get("xwOBA", float("nan")), 3),
        })
        component_details[label] = components

    results_df = pd.DataFrame(results)
    print(tabulate(results_df, headers="keys", tablefmt="simple",
                   showindex=False, floatfmt=".3f"))

    # ── Component breakdown ─────────────────────────────────────────────
    print(f"\n  Per-game point breakdown by category:\n")
    header = f"  {'Category':<12}" + "".join(f"  {k:>22}" for k in component_details)
    print(header)
    print("  " + "-" * (12 + 24 * len(component_details)))

    all_categories = list(next(iter(component_details.values())).keys())
    for cat in all_categories:
        row_str = f"  {cat:<12}"
        for label, comps in component_details.items():
            rate, weight = comps[cat]
            pts = rate * weight
            row_str += f"  {rate:>8.3f} × {weight:>+2d} = {pts:>+6.3f}"
        print(row_str)

    # ── The key question ────────────────────────────────────────────────
    print(f"\n  VERDICT:")
    if len(results) >= 2:
        cab  = next(r for r in results if "Caballero" in r["Player"])
        cruz = next(r for r in results if "Cruz" in r["Player"])
        base = next((r for r in results if "Bichette" in r["Player"]), None)

        print(f"\n  Caballero net SB−K per game: {cab['Net SB-K/G']:+.3f} pts")
        print(f"  Cruz net SB−K per game:      {cruz['Net SB-K/G']:+.3f} pts")
        print(f"  Cruz's 32% K rate costs him  {(cruz['K/G'] - cab['K/G']):.2f} extra K/G "
              f"= {(cruz['K/G'] - cab['K/G']) * abs(BATTING_WEIGHTS['K']):.2f} extra penalty pts/G")
        print(f"  Cruz offsets with power — "
              f"higher TB/G from .{int(fg_df[fg_df['Name']=='Oneil Cruz'].iloc[0].get('xSLG', 0)*1000)}xSLG")

        print(f"\n  Caballero total pts/G:  {cab['Total pts/G']:.3f}")
        print(f"  Cruz total pts/G:       {cruz['Total pts/G']:.3f}")
        if base:
            print(f"  Bichette total pts/G:   {base['Total pts/G']:.3f}  ← baseline")

        print(f"\n  Over a full 162G season:")
        print(f"    Caballero projected: {cab['Proj pts/162G']:.0f} pts")
        print(f"    Cruz projected:      {cruz['Proj pts/162G']:.0f} pts")
        if base:
            print(f"    Bichette projected:  {base['Proj pts/162G']:.0f} pts")

        delta_cab  = cab['Total pts/G']  - (base['Total pts/G'] if base else 2.5)
        delta_cruz = cruz['Total pts/G'] - (base['Total pts/G'] if base else 2.5)

        print(f"\n  vs Bichette baseline:")
        print(f"    Caballero: {delta_cab:+.3f} pts/G  ({delta_cab*162:+.0f} projected pts/season)")
        print(f"    Cruz:      {delta_cruz:+.3f} pts/G  ({delta_cruz*162:+.0f} projected pts/season)")


if __name__ == "__main__":
    bichette_replacement()
    sb_vs_k_analysis()
