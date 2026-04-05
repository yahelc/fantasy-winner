"""
backtest_schedule.py

Validates the 5-day rotation prediction methodology against actual 2025 MLB data.

Methodology:
  For a given fantasy week (Mon-Sun), given a pitcher's last start date before the week:
    1. Compute next_start = last_start + 5 days
    2. Walk forward in 5-day steps until next_start >= week_start
    3. Count how many 5-day steps fall within [week_start, week_end]
    4. predicted_starts = count of those dates

Filters applied (v2):
  Filter 1 — Off-rotation proxy (injury/skip):
    If (projected_first_start - last_actual_start).days > 8, the pitcher has already
    missed a turn. Predict 0 starts (or 1 if the first projected date falls in the week).

  Filter 2 — Team schedule check:
    Both projected start dates must fall on days the pitcher's team actually plays.
    Fetches full 2025 MLB schedule from statsapi.mlb.com.

Data sources:
  - pybaseball.pitching_stats(2025, qual=1) for list of 2025 starters (GS >= 5)
  - MLB Stats API for per-pitcher game logs
  - MLB Stats API schedule endpoint for team game days
"""

import time
import requests
from datetime import date, timedelta
from collections import defaultdict

import pybaseball

# ── Constants ─────────────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "Mozilla/5.0"}
MLB_PEOPLE_SEARCH_URL = "https://statsapi.mlb.com/api/v1/people/search"
MLB_STATS_URL = "https://statsapi.mlb.com/api/v1/people/{}/stats"
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

SEASON = 2025
MIN_GS = 5  # minimum games started to include in backtest

# 2025 season: Opening Day Mar 27, season ended ~Sep 28
SEASON_START = date(2025, 3, 27)
SEASON_END = date(2025, 9, 28)

API_SLEEP = 0.3  # seconds between MLB API calls

# MLB team abbreviation normalization (same as used elsewhere in this project)
TEAM_ABBR_MAP = {
    "ATH": "OAK",
    "AZ":  "ARI",
    "TB":  "TBR",
    "KC":  "KCR",
    "SF":  "SFG",
    "SD":  "SDP",
    "WSH": "WSN",
    "CWS": "CHW",
}


def normalize_team(abbr: str) -> str:
    return TEAM_ABBR_MAP.get(abbr, abbr)


# ── Fantasy week generation ────────────────────────────────────────────────────

def generate_fantasy_weeks(season_start: date, season_end: date) -> list[tuple[date, date]]:
    """
    Generate all Mon-Sun fantasy weeks covering the 2025 season.
    First week starts on the first Monday on or after season_start.
    """
    days_ahead = (7 - season_start.weekday()) % 7  # 0 = already Monday
    first_monday = season_start + timedelta(days=days_ahead)

    weeks = []
    wk_start = first_monday
    while wk_start <= season_end:
        wk_end = wk_start + timedelta(days=6)
        weeks.append((wk_start, wk_end))
        wk_start += timedelta(days=7)
    return weeks


# ── MLB API helpers ────────────────────────────────────────────────────────────

def lookup_mlb_id(name: str) -> int | None:
    """Search MLB Stats API for a player's numeric ID by full name."""
    try:
        r = requests.get(
            MLB_PEOPLE_SEARCH_URL,
            params={"names": name, "sportId": 1},
            headers=HEADERS,
            timeout=10,
        )
        r.raise_for_status()
        people = r.json().get("people", [])
        return people[0]["id"] if people else None
    except Exception as e:
        print(f"  [WARN] lookup_mlb_id({name!r}): {e}")
        return None


def fetch_game_log(mlb_id: int, season: int) -> list[dict]:
    """
    Fetch full game log for a pitcher in the given season.
    Returns list of split dicts with keys: date, gamesStarted, etc.
    Filters to starts only (gamesStarted > 0).
    """
    try:
        r = requests.get(
            MLB_STATS_URL.format(mlb_id),
            params={"stats": "gameLog", "group": "pitching", "season": season},
            headers=HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        stats = r.json().get("stats", [])
        if not stats or not stats[0].get("splits"):
            return []
        splits = stats[0]["splits"]
        starts = [s for s in splits if s["stat"].get("gamesStarted", 0) > 0]
        return starts
    except Exception as e:
        print(f"  [WARN] fetch_game_log({mlb_id}): {e}")
        return []


def fetch_team_schedule(season_start: date, season_end: date) -> tuple[set[tuple[int, date]], dict[int, str]]:
    """
    Fetch the full 2025 MLB regular season schedule.
    Returns:
      - game_days: set of (team_id, game_date) pairs
      - team_id_to_abbr: dict mapping MLB team id -> normalized abbreviation
    """
    print("Fetching 2025 MLB schedule from statsapi.mlb.com...")
    game_days: set[tuple[int, date]] = set()
    team_id_to_abbr: dict[int, str] = {}

    try:
        r = requests.get(
            MLB_SCHEDULE_URL,
            params={
                "sportId": 1,
                "startDate": season_start.isoformat(),
                "endDate": season_end.isoformat(),
                "gameType": "R",
                "hydrate": "team",
            },
            headers=HEADERS,
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [WARN] fetch_team_schedule: {e}")
        return game_days, team_id_to_abbr

    for day_entry in data.get("dates", []):
        game_date = date.fromisoformat(day_entry["date"])
        for game in day_entry.get("games", []):
            for side in ("away", "home"):
                team_info = game.get("teams", {}).get(side, {}).get("team", {})
                team_id = team_info.get("id")
                abbr = team_info.get("abbreviation", "")
                if team_id and abbr:
                    team_id_to_abbr[team_id] = normalize_team(abbr)
                    game_days.add((team_id, game_date))

    print(f"  Schedule loaded: {len(game_days):,d} (team_id, date) pairs across "
          f"{len(data.get('dates', []))} game days, {len(team_id_to_abbr)} teams")
    return game_days, team_id_to_abbr


# ── Prediction logic ───────────────────────────────────────────────────────────

def predict_starts_in_week(last_start: date, week_start: date, week_end: date) -> int:
    """
    Given the pitcher's last start date before the week, apply the 5-day rotation
    methodology and return the count of projected starts in [week_start, week_end].
    """
    nxt = last_start + timedelta(days=5)
    while nxt < week_start:
        nxt += timedelta(days=5)
    count = 0
    cur = nxt
    while cur <= week_end:
        count += 1
        cur += timedelta(days=5)
    return count


def predict_starts_with_filters(
    last_start: date,
    week_start: date,
    week_end: date,
    pitcher_team_id: int,
    team_schedule: set[tuple[int, date]],
) -> tuple[int, str]:
    """
    Apply 5-day rotation logic plus Filter 1 (off-rotation proxy) and
    Filter 2 (team schedule check).

    Returns (predicted_starts, filter_applied) where filter_applied is one of:
      'none', 'filter1_offrotation', 'filter2_schedule'
    """
    # ── Filter 1: off-rotation proxy ──────────────────────────────────────────
    # The pitcher's very next expected start is last_start + 5.
    # If that gap is > 8 days it means the pitcher has already missed a turn
    # (i.e., last_start + 5 was in a prior week and they did not actually start then).
    # We detect this by checking: does the rotation cadence from last_start place a
    # start inside [last_start+1 .. week_start-1] that was "missed"?
    # Equivalent: the first start in the rotation BEFORE week_start would be the one
    # that is > 8 days from last_start, meaning a turn was skipped.
    #
    # Concretely: walk the rotation forward from last_start in 5-day steps.
    # Find the last projected date before week_start. That is the "missed" candidate.
    # If (missed_candidate - last_start).days > 8, the pitcher skipped a turn.
    # (A 5-day gap is normal; 6-7 is scheduling variance; >8 means a turn was missed.)
    missed_candidate = last_start + timedelta(days=5)
    # Walk to the last step that is still before week_start
    while missed_candidate + timedelta(days=5) < week_start:
        missed_candidate += timedelta(days=5)
    # If missed_candidate is before week_start and the gap is > 8, a turn was skipped
    if missed_candidate < week_start:
        gap = (missed_candidate - last_start).days
        if gap > 8:
            return 0, "filter1_offrotation"

    # Step 2: project the candidate start dates in the week (raw rotation logic)
    nxt = last_start + timedelta(days=5)
    while nxt < week_start:
        nxt += timedelta(days=5)

    # Collect all projected starts in the week
    projected = []
    cur = nxt
    while cur <= week_end:
        projected.append(cur)
        cur += timedelta(days=5)

    if not projected:
        return 0, "none"

    # ── Filter 2: team schedule check ─────────────────────────────────────────
    # Only count projected starts that fall on actual game days for the pitcher's team.
    schedule_filtered = [d for d in projected if (pitcher_team_id, d) in team_schedule]

    filter_applied = "none"
    if len(schedule_filtered) < len(projected):
        filter_applied = "filter2_schedule"

    return len(schedule_filtered), filter_applied


# ── Main backtest ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("BACKTEST: 5-day rotation methodology vs actual 2025 MLB data")
    print("VERSION: v2 — with off-rotation proxy + team schedule filters")
    print("=" * 70)
    print()

    # Step 1: Get list of 2025 starters from pybaseball
    print("Fetching 2025 pitching stats from FanGraphs via pybaseball...")
    pybaseball.cache.enable()
    try:
        stats_df = pybaseball.pitching_stats(2025, qual=1)
    except Exception as e:
        print(f"ERROR fetching pitching stats: {e}")
        return

    # Filter to pitchers with >= MIN_GS starts
    starters = stats_df[stats_df["GS"] >= MIN_GS].copy()
    print(f"  Found {len(starters)} pitchers with GS >= {MIN_GS}")

    pitcher_names = starters["Name"].tolist()
    print(f"  Processing {len(pitcher_names)} starters")
    print()

    # Step 2: Fetch game logs for each pitcher
    print(f"Fetching game logs from MLB Stats API (sleep={API_SLEEP}s between calls)...")
    # name -> (sorted list of (start_date, team_id) tuples, last_team_id)
    pitcher_game_logs: dict[str, tuple[list[tuple[date, int]], int]] = {}

    for i, name in enumerate(pitcher_names, 1):
        mlb_id = lookup_mlb_id(name)
        time.sleep(API_SLEEP)
        if mlb_id is None:
            print(f"  [{i}/{len(pitcher_names)}] {name}: MLB ID not found, skipping")
            continue

        splits = fetch_game_log(mlb_id, SEASON)
        time.sleep(API_SLEEP)

        if not splits:
            print(f"  [{i}/{len(pitcher_names)}] {name}: no game log data, skipping")
            continue

        # Each split has team.id — extract per-start team_id to handle mid-season trades
        start_entries = sorted(
            (date.fromisoformat(s["date"]), s.get("team", {}).get("id", 0))
            for s in splits
        )

        last_split = max(splits, key=lambda s: s["date"])
        last_team_id = last_split.get("team", {}).get("id", 0)

        pitcher_game_logs[name] = (start_entries, last_team_id)

        if i % 10 == 0 or i == len(pitcher_names):
            print(f"  [{i}/{len(pitcher_names)}] Progress — {len(pitcher_game_logs)} pitchers with data so far")

    print(f"\nGame logs fetched for {len(pitcher_game_logs)} pitchers.")
    print()

    # Step 3: Fetch team schedule (for Filter 2)
    team_schedule, team_id_to_abbr = fetch_team_schedule(SEASON_START, SEASON_END)
    print()

    # Step 4: Generate fantasy weeks
    weeks = generate_fantasy_weeks(SEASON_START, SEASON_END)
    print(f"Generated {len(weeks)} fantasy weeks ({weeks[0][0]} to {weeks[-1][1]})")
    print()

    # Step 5: For each (pitcher, week) pair, compute predicted vs actual starts
    # using BOTH the baseline logic and the filtered logic
    print("Computing predictions vs actuals for all (pitcher, week) pairs...")

    records = []  # list of dicts

    filter1_changed = 0  # predictions changed by filter 1 alone
    filter2_changed = 0  # predictions changed by filter 2 alone (filter1 did not fire)

    for pitcher, (start_entries_all, last_team_id) in pitcher_game_logs.items():
        # start_entries_all: sorted list of (date, team_id)
        start_dates_all = [d for d, _tid in start_entries_all]

        for week_start, week_end in weeks:
            prior_entries = [(d, tid) for d, tid in start_entries_all if d < week_start]
            if not prior_entries:
                continue

            last_start, last_prior_team_id = max(prior_entries, key=lambda x: x[0])

            # Actual starts this week
            actual = sum(1 for d in start_dates_all if week_start <= d <= week_end)

            # Baseline prediction (no filters)
            predicted_baseline = predict_starts_in_week(last_start, week_start, week_end)

            # Filtered prediction — use the team from the most recent prior start
            # (handles mid-season trades correctly)
            predicted_filtered, filter_applied = predict_starts_with_filters(
                last_start, week_start, week_end, last_prior_team_id, team_schedule
            )

            # Track individual filter contributions
            if filter_applied == "filter1_offrotation" and predicted_baseline != predicted_filtered:
                filter1_changed += 1
            elif filter_applied == "filter2_schedule" and predicted_baseline != predicted_filtered:
                filter2_changed += 1

            records.append({
                "pitcher": pitcher,
                "week_start": week_start,
                "week_end": week_end,
                "last_start_before_week": last_start,
                "team_id": last_prior_team_id,
                "predicted_baseline": predicted_baseline,
                "predicted_filtered": predicted_filtered,
                "filter_applied": filter_applied,
                "actual": actual,
            })

    print(f"Total (pitcher, week) pairs evaluated: {len(records)}")
    print()

    # ── Step 6: Compute metrics for BOTH baseline and filtered ────────────────

    def compute_metrics(records, pred_key):
        tp = fp = tn = fn = 0
        for r in records:
            pred2 = r[pred_key] >= 2
            act2  = r["actual"] >= 2
            if pred2 and act2:
                tp += 1
            elif pred2 and not act2:
                fp += 1
            elif not pred2 and act2:
                fn += 1
            else:
                tn += 1
        total = len(records)
        precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
        recall    = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
        f1        = (2 * precision * recall / (precision + recall)
                     if (precision + recall) > 0 else float("nan"))
        accuracy  = (tp + tn) / total if total > 0 else float("nan")
        base_rate = sum(1 for r in records if r["actual"] >= 2) / total if total > 0 else float("nan")
        return dict(tp=tp, fp=fp, tn=tn, fn=fn, total=total,
                    precision=precision, recall=recall, f1=f1,
                    accuracy=accuracy, base_rate=base_rate)

    m_base     = compute_metrics(records, "predicted_baseline")
    m_filtered = compute_metrics(records, "predicted_filtered")

    total = len(records)

    # Distribution table for filtered predictions
    dist_filtered: dict[tuple[int, int], int] = defaultdict(int)
    for r in records:
        dist_filtered[(r["predicted_filtered"], r["actual"])] += 1

    # FP breakdown for filtered predictions
    fp_zero  = sum(1 for r in records if r["predicted_filtered"] >= 2 and r["actual"] == 0)
    fp_one   = sum(1 for r in records if r["predicted_filtered"] >= 2 and r["actual"] == 1)
    fp_total = m_filtered["fp"]

    fn_pred_zero = sum(1 for r in records if r["actual"] >= 2 and r["predicted_filtered"] == 0)
    fn_pred_one  = sum(1 for r in records if r["actual"] >= 2 and r["predicted_filtered"] == 1)

    # ── Print results ──────────────────────────────────────────────────────────

    print("=" * 70)
    print("RESULTS (Filtered v2)")
    print("=" * 70)
    print()

    mf = m_filtered
    print("── Confusion Matrix (binary: predicted 2+ starts vs actual 2+ starts) ──")
    print("   [FILTERED v2]")
    print()
    print(f"{'':20s}  {'Actual 0-1':>12s}  {'Actual 2+':>12s}")
    print(f"{'Predicted 0-1':20s}  {mf['tn']:>12,d}  {mf['fn']:>12,d}  (TN, FN)")
    print(f"{'Predicted 2+':20s}  {mf['fp']:>12,d}  {mf['tp']:>12,d}  (FP, TP)")
    print()

    print("── Binary Classification Metrics ──────────────────────────────────────")
    print()
    print(f"  Total pitcher-weeks evaluated : {total:,d}")
    print(f"  Base rate (actual 2+ starts)  : {mf['base_rate']:.1%}")
    print()
    print(f"  Precision  (TP / (TP+FP))     : {mf['precision']:.3f}  ({mf['precision']:.1%})")
    print(f"  Recall     (TP / (TP+FN))     : {mf['recall']:.3f}  ({mf['recall']:.1%})")
    print(f"  F1 score                       : {mf['f1']:.3f}")
    print(f"  Accuracy   ((TP+TN) / total)  : {mf['accuracy']:.3f}  ({mf['accuracy']:.1%})")
    print()

    print("── Distribution Table (predicted_starts, actual_starts) — FILTERED ────")
    print()
    all_predicted = sorted(set(k[0] for k in dist_filtered))
    all_actual    = sorted(set(k[1] for k in dist_filtered))

    header = f"  {'pred \\ actual':>14s}" + "".join(f"  {a:>8d}" for a in all_actual) + "     TOTAL"
    print(header)
    print("  " + "-" * (len(header) - 2))
    for p in all_predicted:
        row_vals = [dist_filtered.get((p, a), 0) for a in all_actual]
        row_total = sum(row_vals)
        row_str = "".join(f"  {v:>8,d}" for v in row_vals)
        print(f"  {p:>14d}{row_str}  {row_total:>8,d}")
    print("  " + "-" * (len(header) - 2))
    col_totals = [sum(dist_filtered.get((p, a), 0) for p in all_predicted) for a in all_actual]
    grand_total = sum(col_totals)
    totals_str = "".join(f"  {v:>8,d}" for v in col_totals)
    print(f"  {'TOTAL':>14s}{totals_str}  {grand_total:>8,d}")
    print()

    print("── False Positive (Predicted 2+, Actual <2) Breakdown — FILTERED ───────")
    print()
    print(f"  Total FP (predicted 2+, actual <2) : {fp_total:,d}")
    if fp_total > 0:
        print(f"    Actual = 0 starts (likely injured/skipped): {fp_zero:,d}  ({fp_zero/fp_total:.1%} of FP)")
        print(f"    Actual = 1 start  (rotation off by ~1 day): {fp_one:,d}  ({fp_one/fp_total:.1%} of FP)")
    print()

    print("── False Negative (Actual 2+, Predicted <2) Breakdown — FILTERED ───────")
    print()
    print(f"  Total FN (actual 2+, predicted <2) : {mf['fn']:,d}")
    if mf["fn"] > 0:
        print(f"    Predicted = 0 starts : {fn_pred_zero:,d}  ({fn_pred_zero/mf['fn']:.1%} of FN)")
        print(f"    Predicted = 1 start  : {fn_pred_one:,d}  ({fn_pred_one/mf['fn']:.1%} of FN)")
    print()

    # ── Comparison table ──────────────────────────────────────────────────────

    print("=" * 70)
    print("BEFORE vs AFTER FILTERS COMPARISON")
    print("=" * 70)
    print()
    mb = m_base
    fmt_row = lambda label, bv, fv: f"  {label:<30s}  {bv:>10}  {fv:>10}"
    print(f"  {'Metric':<30s}  {'Baseline':>10s}  {'Filtered':>10s}")
    print("  " + "-" * 54)
    print(fmt_row("TP", f"{mb['tp']:,d}", f"{mf['tp']:,d}"))
    print(fmt_row("FP", f"{mb['fp']:,d}", f"{mf['fp']:,d}"))
    print(fmt_row("TN", f"{mb['tn']:,d}", f"{mf['tn']:,d}"))
    print(fmt_row("FN", f"{mb['fn']:,d}", f"{mf['fn']:,d}"))
    print(fmt_row("Precision", f"{mb['precision']:.1%}", f"{mf['precision']:.1%}"))
    print(fmt_row("Recall",    f"{mb['recall']:.1%}",    f"{mf['recall']:.1%}"))
    print(fmt_row("F1",        f"{mb['f1']:.3f}",        f"{mf['f1']:.3f}"))
    print(fmt_row("Accuracy",  f"{mb['accuracy']:.1%}",  f"{mf['accuracy']:.1%}"))
    print()

    # ── Filter contribution breakdown ─────────────────────────────────────────

    print("── Filter Contribution (predictions changed) ───────────────────────────")
    print()

    # Among records where baseline predicted 2+
    baseline_2plus = [r for r in records if r["predicted_baseline"] >= 2]
    f1_eliminated = sum(1 for r in baseline_2plus if r["filter_applied"] == "filter1_offrotation" and r["predicted_filtered"] < 2)
    f2_eliminated = sum(1 for r in baseline_2plus if r["filter_applied"] == "filter2_schedule" and r["predicted_filtered"] < 2)
    f2_reduced_1  = sum(1 for r in baseline_2plus if r["filter_applied"] == "filter2_schedule" and r["predicted_filtered"] == 1)

    # How many of those eliminations were true FPs (actual < 2) — i.e., correct eliminations
    f1_correct = sum(1 for r in baseline_2plus
                     if r["filter_applied"] == "filter1_offrotation"
                     and r["predicted_filtered"] < 2
                     and r["actual"] < 2)
    f2_correct = sum(1 for r in baseline_2plus
                     if r["filter_applied"] == "filter2_schedule"
                     and r["predicted_filtered"] < 2
                     and r["actual"] < 2)
    f1_wrong  = f1_eliminated - f1_correct
    f2_wrong  = f2_eliminated - f2_correct

    print(f"  Filter 1 (off-rotation proxy, gap > 8 days):")
    print(f"    Predictions eliminated (baseline 2+, now 0) : {f1_eliminated:,d}")
    print(f"      Correct eliminations (actual was <2)      : {f1_correct:,d}  ({f1_correct/f1_eliminated:.1%} precision)" if f1_eliminated else "    (none)")
    print(f"      Wrong eliminations   (actual was 2+)      : {f1_wrong:,d}")
    print()
    print(f"  Filter 2 (team schedule check):")
    print(f"    Predictions eliminated (baseline 2+, now <2): {f2_eliminated:,d}")
    print(f"      Correct eliminations (actual was <2)      : {f2_correct:,d}  ({f2_correct/f2_eliminated:.1%} precision)" if f2_eliminated else "    (none)")
    print(f"      Wrong eliminations   (actual was 2+)      : {f2_wrong:,d}")
    print(f"    Of which reduced to 1 (not fully eliminated): {f2_reduced_1:,d}")
    print()

    print(f"  Net change in FP  : {mb['fp']} → {mf['fp']}  (Δ {mf['fp'] - mb['fp']:+d})")
    print(f"  Net change in FN  : {mb['fn']} → {mf['fn']}  (Δ {mf['fn'] - mb['fn']:+d})")
    print()

    # ── Remaining FP patterns ─────────────────────────────────────────────────

    print("── Remaining False Positive Patterns ───────────────────────────────────")
    print()

    remaining_fps = [r for r in records if r["predicted_filtered"] >= 2 and r["actual"] < 2]
    if remaining_fps:
        # Group by gap size (first_projected - last_start)
        gap_dist: dict[int, int] = defaultdict(int)
        for r in remaining_fps:
            first_proj = r["last_start_before_week"] + timedelta(days=5)
            while first_proj < r["week_start"]:
                first_proj += timedelta(days=5)
            gap = (first_proj - r["last_start_before_week"]).days
            gap_dist[gap] += 1

        print(f"  Remaining FP count: {len(remaining_fps):,d}")
        print()
        print("  Gap distribution (first_projected - last_actual start, in days):")
        for g in sorted(gap_dist):
            pct = gap_dist[g] / len(remaining_fps)
            bar = "#" * int(pct * 30)
            print(f"    gap={g:2d}d : {gap_dist[g]:5,d}  ({pct:.1%})  {bar}")
        print()

        # By actual starts (0 vs 1)
        fp_act0 = sum(1 for r in remaining_fps if r["actual"] == 0)
        fp_act1 = sum(1 for r in remaining_fps if r["actual"] == 1)
        print(f"  Actual = 0 starts (injured/skipped/missed): {fp_act0:,d}  ({fp_act0/len(remaining_fps):.1%})")
        print(f"  Actual = 1 start  (rotation slip ~1 day) : {fp_act1:,d}  ({fp_act1/len(remaining_fps):.1%})")
        print()
        print("  Interpretation:")
        if fp_act0 > fp_act1:
            print("    Majority are actual-0 cases — injury/IL moves not captured by the gap proxy.")
            print("    Real-time ESPN injuryStatus check would address these.")
        else:
            print("    Majority are rotation-slip cases (off by ~1 day from a 5-day window boundary).")
            print("    A ±1-day tolerance on the 5-day cadence might reduce these.")
    else:
        print("  No remaining false positives!")
    print()

    # ── Final summary ──────────────────────────────────────────────────────────
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print(f"  Pitchers evaluated          : {len(pitcher_game_logs):,d}")
    print(f"  Fantasy weeks in 2025       : {len(weeks):,d}")
    print(f"  Pitcher-week pairs          : {total:,d}")
    print(f"  Base rate (actual 2 starts) : {mf['base_rate']:.1%}")
    print()
    print(f"  Precision   : {mf['precision']:.1%}  (when we predict 2 starts, we are right {mf['precision']:.1%} of the time)")
    print(f"  Recall      : {mf['recall']:.1%}  (we catch {mf['recall']:.1%} of all true 2-start weeks)")
    print(f"  F1          : {mf['f1']:.3f}")
    print(f"  Accuracy    : {mf['accuracy']:.1%}")
    print()

    if mf["precision"] >= 0.70 and mf["recall"] >= 0.60:
        verdict = "STRONG — methodology is reliable for identifying 2-start weeks"
    elif mf["precision"] >= 0.60 or mf["recall"] >= 0.60:
        verdict = "MODERATE — methodology has signal but misses are common"
    else:
        verdict = "WEAK — 5-day rotation model has limited predictive power"
    print(f"  Verdict: {verdict}")
    print()
    if fp_total > 0:
        print(f"  Key failure mode: {fp_zero:,d} predicted-2 weeks had ZERO actual starts")
        print(f"    ({fp_zero/fp_total:.1%} of FP) — likely injuries, skips, or call-ups after season start")
    print()


if __name__ == "__main__":
    main()
