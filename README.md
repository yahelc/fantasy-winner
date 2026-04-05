# fantasy-winner

Analytics tooling for ESPN fantasy baseball — Ferryhawk Tuah (league 1056949816, H2H points).

Pulls data from FanGraphs (via pybaseball) and Baseball Savant, scores players against
your league's exact H2H point weights, and surfaces actionable roster decisions.

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Always use `.venv/bin/python` (or activate the venv first). Python 3.14.

---

## Scoring model

**Batting:** R+1, TB+1, RBI+1, BB+1, K−1, SB+2

**Pitching:** IP+3, H−1, ER−2, BB−1, K+1, QS+5, W+5, L−2, SV+5, NH+10, PG+20

**Rate stats** (K%, BB%, OBP, SLG, SB/G) come from a unified time-decay model across
2025 + 2026 monthly splits. Exponential decay from most recent: Apr 2026 = 1.00,
Sep 2025 = 0.72, Aug 2025 = 0.61 ... Apr 2025 = 0.32. PA-weighting means that
3 days of April 2026 data (~5–15 PA) naturally count less than a full 2025 month.

**xwOBA, xSLG, Barrel%** come from season-level data (2026 if available, else 2025)
and act as a quality tiebreaker — 20% weight in composite score.

---

## Scripts

### `analyze.py` — main analysis CLI

The primary tool. Connects to ESPN, fetches and scores all players, and runs reports.

```bash
# Full report: your roster + top FAs + upgrade opportunities
python analyze.py

# Just your roster scores
python analyze.py --roster

# Top free agents (all positions)
python analyze.py --fa

# Top free agents at a specific position
python analyze.py --fa --pos SS
python analyze.py --fa --pos OF
python analyze.py --fa --pos SP
python analyze.py --fa --pos RP

# Upgrade opportunities — compares each roster slot to best available FA
python analyze.py --upgrade

# Head-to-head player comparison (partial name match works)
python analyze.py --compare "Correa" "Bichette" "Wilson"
python analyze.py --compare "Skenes" "Peralta"

# Control how many rows to show (default 25)
python analyze.py --fa --pos OF --n 15
```

**Output columns:**
- `Yr` — which season the data is tagged to (2026 = has current season data)
- `PA` — season-level PA from FanGraphs (current season)
- `xwOBA` — expected weighted on-base average (Statcast quality signal)
- `K%`, `BB%` — time-decayed rate stats
- `pts_per_game` — projected fantasy points per game (direct scoring model output)
- `composite_score` — 80% pts/G + 20% xwOBA z-score (final ranking metric)
- `note` — flags "small sample" (<150 PA) or "2025 data" (no 2026 stats yet)

---

### `questions.py` — targeted one-off analyses

Two specific analyses: Bichette replacement (2B/SS FAs ranked by 2026 bat speed)
and Caballero vs Cruz (SB premium vs K penalty math).

```bash
python questions.py
```

**What it shows:**
1. Bichette replacement: all FA-eligible 2B/SS players sorted by 2026 bat speed,
   merged with scoring model — find players with elite bat speed before the market catches on.
2. Caballero vs Cruz: per-game point breakdown showing exactly how SB+2 and K−1
   interact, with a per-component table and 162-game projection.

---

### `monthly_decay.py` — inspect the time-decay model

Run standalone to see the decay weights and per-player decayed rate stats.

```bash
python monthly_decay.py
```

**Output:**
- Month weights table (shows the 0.85/month decay rate across the full timeline)
- Top 25 players by total PA with their decayed K%, BB%, OBP, SLG, SB/G

Useful for sanity-checking that a player's decay stats look right, or to see
how many total plate appearances are feeding a player's profile.

---

### `fetch_data.py` — test data fetching

Run to verify the data pipeline is working and preview what gets fetched.

```bash
python fetch_data.py
```

Shows a sample of hitters and pitchers with their data source season and key stats.
Useful after updating `config.py` or if you suspect stale cache issues.

---

### `bat_speed.py` — 2026 bat speed data

Run to preview the Baseball Savant bat tracking leaderboard.

```bash
python bat_speed.py
```

Shows top 25 players by bat speed with hard swing rate, whiff%, and swing count.
Bat speed is always 2026-only (not blended) — it's used as a leading indicator
of hitting quality for players the market may not have priced in yet.

---

## Key files

| File | Purpose |
|------|---------|
| `config.py` | League ID, scoring weights, thresholds — edit this to change league settings |
| `fetch_data.py` | FanGraphs data fetching + unified time-decay application |
| `monthly_decay.py` | PA-weighted exponential decay across 2025+2026 monthly splits |
| `fetch_espn.py` | ESPN API connection — roster, free agents, league data |
| `score_players.py` | Converts rate stats → projected pts/G and composite score |
| `bat_speed.py` | Baseball Savant bat speed leaderboard (2026-only) |
| `analyze.py` | Main CLI — all the analysis modes |
| `questions.py` | One-off targeted analyses |

---

## Adjusting the model

**Change the off-season gap or decay rate** — edit `monthly_decay.py`:
```python
OFF_SEASON_MONTHS = 2   # compressed months between Sep 2025 and Apr 2026
DECAY = 0.85            # per-month decay factor
```

**Disable time decay** (use season-level rates only):
```python
from fetch_data import get_hitters
hitters = get_hitters(time_decay=False)
```

**The 2026 month list updates automatically.** `monthly_decay.py` computes the active
months at runtime based on today's date — no manual edits needed as the season progresses.
On May 5th it will include Apr 2026 (full month) and May 2026 (through May 5th), and the
weights will automatically re-anchor to May 2026 as step 0. New months appear as they start.

**Clear stale cache:**
```bash
rm -rf $(python -c "import pybaseball; print(pybaseball.cache.get_cache_dir())")
```

---

## Caveats

- **Small 2026 samples**: Through early April, most players have 15–40 PA. The model
  handles this via PA-weighting, but treat any player flagged "small sample" as noisy.
- **Name matching**: ESPN and FanGraphs names don't always match exactly. If a player
  shows "No data found," try a shorter partial name in `--compare`.
- **Defense is worthless**: This scoring system awards zero points for defensive metrics.
  Sprint speed matters only insofar as it predicts stolen bases.
- **Pitching**: The model doesn't apply time decay to pitchers (too few monthly IP splits
  to be reliable). Pitcher scores use 2026 season-level stats with 2025 as supplement.
