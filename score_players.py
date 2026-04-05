"""
Converts Statcast/FanGraphs metrics into projected fantasy points per game.

Philosophy:
- We use *expected* stats (xwOBA, xSLG, xERA) rather than raw results
  to capture true skill, not luck.
- Per-game rates normalize for playing time differences.
- Final score = projected fantasy pts/game × reliability factor
"""

import numpy as np
import pandas as pd
from config import BATTING_WEIGHTS, PITCHING_WEIGHTS


# ---------------------------------------------------------------------------
# Hitter scoring
# ---------------------------------------------------------------------------

def score_hitters(df: pd.DataFrame) -> pd.DataFrame:
    """
    Project fantasy points per game for hitters.

    Methodology:
    - TB/game: from xSLG × (AB/G). xSLG captures power quality.
    - R/game: xOBP × (PA/G) × 0.45 (rough run-scoring rate for OBP)
    - RBI/game: (xSLG - xOBP × 0.3) proxy for extra base production
    - BB/game: BB% × (PA/G)
    - K/game: K% × (PA/G)
    - SB/game: actual SB/G (sprint speed as quality signal, not direct projection)
    """
    df = df.copy()

    pa_per_g = df["PA"] / df["G"].replace(0, np.nan)
    ab_per_g = df["AB"] / df["G"].replace(0, np.nan)

    # Use expected stats where available, fall back to actual
    xslg = df.get("xSLG", df.get("SLG", 0.400))
    xobp = df.get("xOBP", df.get("OBP", 0.320))
    k_pct = df.get("K%", 0.22)
    bb_pct = df.get("BB%", 0.08)
    # Priority: blended decay rate > blended rate > raw SB/G
    if "SB_per_g_blend" in df.columns and "SB_per_g_decay" in df.columns:
        sb_per_g = df["SB_per_g_decay"].fillna(df["SB_per_g_blend"])
    elif "SB_per_g_blend" in df.columns:
        sb_per_g = df["SB_per_g_blend"]
    else:
        sb_per_g = df.get("SB", 0) / df["G"].replace(0, np.nan)

    # Per-player run/RBI conversion rates, blended with league average.
    # Empirical rate = what fraction of "times on base" became R, and
    # what fraction of "total bases" became RBI, based on their actual history.
    # Blend 50/50 with league average to regress small samples toward the mean.
    LEAGUE_R_RATE   = 0.42
    LEAGUE_RBI_RATE = 0.28

    obp_actual = df.get("OBP", xobp)
    slg_actual = df.get("SLG", xslg)

    emp_r_rate   = (df.get("R",   0) / (obp_actual * df["PA"].replace(0, np.nan))).clip(0.20, 0.65)
    emp_rbi_rate = (df.get("RBI", 0) / (slg_actual * df["AB"].replace(0, np.nan))).clip(0.10, 0.55)

    r_rate   = 0.50 * emp_r_rate.fillna(LEAGUE_R_RATE)   + 0.50 * LEAGUE_R_RATE
    rbi_rate = 0.50 * emp_rbi_rate.fillna(LEAGUE_RBI_RATE) + 0.50 * LEAGUE_RBI_RATE

    proj = pd.DataFrame(index=df.index)
    proj["proj_TB_g"]  = xslg * ab_per_g
    proj["proj_R_g"]   = xobp * pa_per_g * r_rate
    proj["proj_RBI_g"] = xslg * ab_per_g * rbi_rate
    proj["proj_BB_g"]  = bb_pct * pa_per_g
    proj["proj_K_g"]   = k_pct * pa_per_g
    proj["proj_SB_g"]  = sb_per_g.fillna(0)

    # Apply league weights
    pts = (
        proj["proj_TB_g"]  * BATTING_WEIGHTS["TB"]  +
        proj["proj_R_g"]   * BATTING_WEIGHTS["R"]   +
        proj["proj_RBI_g"] * BATTING_WEIGHTS["RBI"] +
        proj["proj_BB_g"]  * BATTING_WEIGHTS["BB"]  +
        proj["proj_K_g"]   * BATTING_WEIGHTS["K"]   +  # negative weight
        proj["proj_SB_g"]  * BATTING_WEIGHTS["SB"]
    )

    df["pts_per_game"] = pts.round(3)

    # Attach components for debug inspection (--debug flag in analyze.py)
    for col in proj.columns:
        df[col] = proj[col].round(3)

    # Quality signal: boost players with elite Statcast metrics
    # xwOBA z-score as a confidence modifier (keeps rankings stable)
    if "xwOBA" in df.columns:
        xwoba = df["xwOBA"].fillna(df["xwOBA"].median())
        league_mean = xwoba.mean()
        league_std  = xwoba.std()
        df["xwOBA_zscore"] = ((xwoba - league_mean) / league_std).round(3)
    else:
        df["xwOBA_zscore"] = 0.0

    # Composite score: pts/game weighted by xwOBA quality signal
    # Weight: 80% projected pts, 20% xwOBA quality
    if "xwOBA_zscore" in df.columns:
        df["composite_score"] = (0.80 * df["pts_per_game"] + 0.20 * df["xwOBA_zscore"]).round(3)
    else:
        df["composite_score"] = df["pts_per_game"]

    return df.sort_values("composite_score", ascending=False)


# ---------------------------------------------------------------------------
# Pitcher scoring
# ---------------------------------------------------------------------------

def score_pitchers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Project fantasy points per game started (or per appearance for relievers).

    Methodology:
    - IP/G: innings per appearance
    - K/G: from K% × (BF/G estimate)
    - H, ER, BB per game: from xERA/xFIP components
    - QS probability: based on IP/GS and xERA
    - W probability: roughly 0.5 × QS rate for starters
    - SV: actual SV/G for relievers
    """
    df = df.copy()

    ip_per_g = df["IP"] / df["G"].replace(0, np.nan)
    gs = df.get("GS", 0).fillna(0)
    is_starter = gs > (df["G"] * 0.5)  # majority of appearances are starts

    # Batters faced ≈ IP × 3.3 (accounts for baserunners)
    bf_per_g = ip_per_g * 3.3

    k_pct  = df.get("K%", 0.22).fillna(0.22)
    bb_pct = df.get("BB%", 0.08).fillna(0.08)

    # Use xERA to estimate ER/9, then per game
    xera = df.get("xERA", df.get("ERA", 4.00)).fillna(4.00)
    er_per_9  = xera
    er_per_g  = (er_per_9 / 9) * ip_per_g

    # H allowed: approximate from ERA and BB (H ≈ WHIP×IP - BB)
    # Use xFIP-based WHIP approximation
    xfip = df.get("xFIP", xera).fillna(xera)
    whip_est  = 0.95 + (xfip - 4.0) * 0.12  # rough linear fit
    h_per_g   = (whip_est * ip_per_g) - (bb_pct * bf_per_g)
    h_per_g   = h_per_g.clip(lower=0)

    bb_per_g  = bb_pct * bf_per_g
    k_per_g   = k_pct  * bf_per_g

    # QS probability: use blended IP/GS if available, fall back to calculated
    if "IP_per_GS_blended" in df.columns:
        ip_per_gs = df["IP_per_GS_blended"].fillna(df["IP"] / gs.replace(0, np.nan))
    else:
        ip_per_gs = df["IP"] / gs.replace(0, np.nan)
    qs_prob = np.where(
        is_starter,
        np.clip((ip_per_gs - 4.0) / 3.0, 0, 1) * np.clip((5.5 - xera) / 2.5, 0, 1),
        0
    )

    # Win probability for starters: ~55% of QS become wins historically
    w_prob = np.where(is_starter, qs_prob * 0.55, 0)

    # Save probability for closers
    sv_per_g = df.get("SV", 0).fillna(0) / df["G"].replace(0, np.nan)

    proj = pd.DataFrame(index=df.index)
    proj["proj_IP_g"]  = ip_per_g
    proj["proj_K_g"]   = k_per_g
    proj["proj_H_g"]   = h_per_g
    proj["proj_ER_g"]  = er_per_g
    proj["proj_BB_g"]  = bb_per_g
    proj["proj_QS_g"]  = pd.Series(qs_prob, index=df.index)
    proj["proj_W_g"]   = pd.Series(w_prob, index=df.index)
    proj["proj_SV_g"]  = sv_per_g.fillna(0)
    # L probability: starters who don't get QS or W
    proj["proj_L_g"]   = np.where(is_starter, (1 - w_prob) * 0.20, 0)

    pts = (
        proj["proj_IP_g"]  * PITCHING_WEIGHTS["IP"]  +
        proj["proj_K_g"]   * PITCHING_WEIGHTS["K"]   +
        proj["proj_H_g"]   * PITCHING_WEIGHTS["H"]   +
        proj["proj_ER_g"]  * PITCHING_WEIGHTS["ER"]  +
        proj["proj_BB_g"]  * PITCHING_WEIGHTS["BB"]  +
        proj["proj_QS_g"]  * PITCHING_WEIGHTS["QS"]  +
        proj["proj_W_g"]   * PITCHING_WEIGHTS["W"]   +
        proj["proj_L_g"]   * PITCHING_WEIGHTS["L"]   +
        proj["proj_SV_g"]  * PITCHING_WEIGHTS["SV"]
    )

    df["pts_per_game"] = pts.round(3)

    # Attach components for debug inspection (--debug flag in analyze.py)
    for col in proj.columns:
        df[col] = proj[col].round(3)

    # Quality signal: xFIP z-score (lower xFIP = better)
    if "xFIP" in df.columns:
        xfip_vals = df["xFIP"].fillna(df["xFIP"].median())
        league_mean = xfip_vals.mean()
        league_std  = xfip_vals.std()
        df["xFIP_zscore"] = (-(xfip_vals - league_mean) / league_std).round(3)  # inverted
    else:
        df["xFIP_zscore"] = 0.0

    df["composite_score"] = (0.80 * df["pts_per_game"] + 0.20 * df["xFIP_zscore"]).round(3)

    # pts_per_week: derive actual apps/week from combined G across both seasons.
    # Fall back to positional defaults (starters 1.4/week, relievers 2.5/week)
    # if G_total/weeks_sampled are unavailable.
    if "G_total" in df.columns and "weeks_sampled" in df.columns:
        apps_per_week = (
            df["G_total"] / df["weeks_sampled"].replace(0, np.nan)
        ).fillna(pd.Series(np.where(is_starter, 1.4, 2.5), index=df.index))
    else:
        apps_per_week = pd.Series(np.where(is_starter, 1.4, 2.5), index=df.index)
    df["pts_per_week"] = (df["pts_per_game"] * apps_per_week).round(2)

    return df.sort_values("composite_score", ascending=False)
