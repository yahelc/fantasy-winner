"""
backtest_four_configs.py

Runs backtest in four configurations and prints a comparison table:
  1. No filters (baseline)
  2. Filter 1 only (gap > 8 days → predict 0)
  3. Filter 2 only (team schedule check)
  4. Both filters combined
"""

import time
import requests
from datetime import date, timedelta
from collections import defaultdict

import pybaseball

HEADERS = {"User-Agent": "Mozilla/5.0"}
MLB_PEOPLE_SEARCH_URL = "https://statsapi.mlb.com/api/v1/people/search"
MLB_STATS_URL = "https://statsapi.mlb.com/api/v1/people/{}/stats"
MLB_SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"

SEASON = 2025
MIN_GS = 5
SEASON_START = date(2025, 3, 27)
SEASON_END = date(2025, 9, 28)
API_SLEEP = 0.3

TEAM_ABBR_MAP = {
    "ATH": "OAK", "AZ": "ARI", "TB": "TBR", "KC": "KCR",
    "SF": "SFG", "SD": "SDP", "WSH": "WSN", "CWS": "CHW",
}


def normalize_team(abbr):
    return TEAM_ABBR_MAP.get(abbr, abbr)


def generate_fantasy_weeks(season_start, season_end):
    days_ahead = (7 - season_start.weekday()) % 7
    first_monday = season_start + timedelta(days=days_ahead)
    weeks = []
    wk_start = first_monday
    while wk_start <= season_end:
        weeks.append((wk_start, wk_start + timedelta(days=6)))
        wk_start += timedelta(days=7)
    return weeks


def lookup_mlb_id(name):
    try:
        r = requests.get(MLB_PEOPLE_SEARCH_URL, params={"names": name, "sportId": 1},
                         headers=HEADERS, timeout=10)
        r.raise_for_status()
        people = r.json().get("people", [])
        return people[0]["id"] if people else None
    except Exception as e:
        print(f"  [WARN] lookup_mlb_id({name!r}): {e}")
        return None


def fetch_game_log(mlb_id, season):
    try:
        r = requests.get(MLB_STATS_URL.format(mlb_id),
                         params={"stats": "gameLog", "group": "pitching", "season": season},
                         headers=HEADERS, timeout=15)
        r.raise_for_status()
        stats = r.json().get("stats", [])
        if not stats or not stats[0].get("splits"):
            return []
        splits = stats[0]["splits"]
        return [s for s in splits if s["stat"].get("gamesStarted", 0) > 0]
    except Exception as e:
        print(f"  [WARN] fetch_game_log({mlb_id}): {e}")
        return []


def fetch_team_schedule(season_start, season_end):
    print("Fetching 2025 MLB schedule from statsapi.mlb.com...")
    game_days = set()
    team_id_to_abbr = {}
    try:
        r = requests.get(MLB_SCHEDULE_URL,
                         params={"sportId": 1, "startDate": season_start.isoformat(),
                                 "endDate": season_end.isoformat(), "gameType": "R", "hydrate": "team"},
                         headers=HEADERS, timeout=30)
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
    print(f"  Schedule loaded: {len(game_days):,d} (team_id, date) pairs, {len(team_id_to_abbr)} teams")
    return game_days, team_id_to_abbr


def predict_no_filters(last_start, week_start, week_end):
    """Baseline: pure 5-day rotation, no filters."""
    nxt = last_start + timedelta(days=5)
    while nxt < week_start:
        nxt += timedelta(days=5)
    count = 0
    cur = nxt
    while cur <= week_end:
        count += 1
        cur += timedelta(days=5)
    return count


def apply_filter1(last_start, week_start):
    """Return True if Filter 1 fires (off-rotation proxy, gap > 8 days)."""
    missed_candidate = last_start + timedelta(days=5)
    while missed_candidate + timedelta(days=5) < week_start:
        missed_candidate += timedelta(days=5)
    if missed_candidate < week_start:
        gap = (missed_candidate - last_start).days
        if gap > 8:
            return True
    return False


def project_starts_in_week(last_start, week_start, week_end):
    """Return list of projected start dates in the week."""
    nxt = last_start + timedelta(days=5)
    while nxt < week_start:
        nxt += timedelta(days=5)
    projected = []
    cur = nxt
    while cur <= week_end:
        projected.append(cur)
        cur += timedelta(days=5)
    return projected


def predict_filter1_only(last_start, week_start, week_end):
    """Filter 1 only: if off-rotation proxy fires, return 0. Otherwise raw count."""
    if apply_filter1(last_start, week_start):
        return 0
    return predict_no_filters(last_start, week_start, week_end)


def predict_filter2_only(last_start, week_start, week_end, team_id, team_schedule):
    """Filter 2 only: apply schedule check, ignore off-rotation proxy."""
    projected = project_starts_in_week(last_start, week_start, week_end)
    schedule_filtered = [d for d in projected if (team_id, d) in team_schedule]
    return len(schedule_filtered)


def predict_both_filters(last_start, week_start, week_end, team_id, team_schedule):
    """Both filters: Filter 1 first, then Filter 2."""
    if apply_filter1(last_start, week_start):
        return 0
    projected = project_starts_in_week(last_start, week_start, week_end)
    schedule_filtered = [d for d in projected if (team_id, d) in team_schedule]
    return len(schedule_filtered)


def compute_metrics(records, pred_key):
    tp = fp = tn = fn = 0
    for r in records:
        pred2 = r[pred_key] >= 2
        act2 = r["actual"] >= 2
        if pred2 and act2:
            tp += 1
        elif pred2 and not act2:
            fp += 1
        elif not pred2 and act2:
            fn += 1
        else:
            tn += 1
    precision = tp / (tp + fp) if (tp + fp) > 0 else float("nan")
    recall = tp / (tp + fn) if (tp + fn) > 0 else float("nan")
    f1 = (2 * precision * recall / (precision + recall)
          if (precision + recall) > 0 else float("nan"))
    return dict(tp=tp, fp=fp, tn=tn, fn=fn, precision=precision, recall=recall, f1=f1)


def main():
    print("Fetching 2025 pitching stats from FanGraphs via pybaseball...")
    pybaseball.cache.enable()
    try:
        stats_df = pybaseball.pitching_stats(2025, qual=1)
    except Exception as e:
        print(f"ERROR fetching pitching stats: {e}")
        return

    starters = stats_df[stats_df["GS"] >= MIN_GS].copy()
    pitcher_names = starters["Name"].tolist()
    print(f"  Found {len(pitcher_names)} pitchers with GS >= {MIN_GS}")

    print(f"\nFetching game logs from MLB Stats API...")
    pitcher_game_logs = {}

    for i, name in enumerate(pitcher_names, 1):
        mlb_id = lookup_mlb_id(name)
        time.sleep(API_SLEEP)
        if mlb_id is None:
            continue
        splits = fetch_game_log(mlb_id, SEASON)
        time.sleep(API_SLEEP)
        if not splits:
            continue
        start_entries = sorted(
            (date.fromisoformat(s["date"]), s.get("team", {}).get("id", 0))
            for s in splits
        )
        pitcher_game_logs[name] = start_entries
        if i % 20 == 0 or i == len(pitcher_names):
            print(f"  [{i}/{len(pitcher_names)}] {len(pitcher_game_logs)} pitchers with data")

    print(f"\nGame logs fetched for {len(pitcher_game_logs)} pitchers.")

    team_schedule, _ = fetch_team_schedule(SEASON_START, SEASON_END)

    weeks = generate_fantasy_weeks(SEASON_START, SEASON_END)
    print(f"Generated {len(weeks)} fantasy weeks\n")

    print("Computing predictions for all four configurations...")
    records = []

    for pitcher, start_entries_all in pitcher_game_logs.items():
        start_dates_all = [d for d, _tid in start_entries_all]
        for week_start, week_end in weeks:
            prior_entries = [(d, tid) for d, tid in start_entries_all if d < week_start]
            if not prior_entries:
                continue
            last_start, last_prior_team_id = max(prior_entries, key=lambda x: x[0])
            actual = sum(1 for d in start_dates_all if week_start <= d <= week_end)

            p_none = predict_no_filters(last_start, week_start, week_end)
            p_f1 = predict_filter1_only(last_start, week_start, week_end)
            p_f2 = predict_filter2_only(last_start, week_start, week_end, last_prior_team_id, team_schedule)
            p_both = predict_both_filters(last_start, week_start, week_end, last_prior_team_id, team_schedule)

            records.append({
                "actual": actual,
                "pred_none": p_none,
                "pred_f1": p_f1,
                "pred_f2": p_f2,
                "pred_both": p_both,
            })

    print(f"Total pitcher-week pairs: {len(records)}\n")

    configs = [
        ("No filters",     "pred_none"),
        ("Filter 1 only",  "pred_f1"),
        ("Filter 2 only",  "pred_f2"),
        ("Both filters",   "pred_both"),
    ]

    print(f"{'Config':<16}  {'Precision':>9}  {'Recall':>6}  {'F1':>6}  {'TP':>6}  {'FP':>6}  {'FN':>6}")
    print("-" * 70)
    for label, key in configs:
        m = compute_metrics(records, key)
        print(f"{label:<16}  {m['precision']:>9.3f}  {m['recall']:>6.3f}  {m['f1']:>6.3f}"
              f"  {m['tp']:>6,d}  {m['fp']:>6,d}  {m['fn']:>6,d}")


if __name__ == "__main__":
    main()
