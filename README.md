# fantasy-winner

Analytics tooling for ESPN fantasy baseball — Ferryhawk Tuah (league 1056949816, H2H points).

Pulls data from FanGraphs (via pybaseball) and Baseball Savant, scores players against
your league's exact H2H point weights, and surfaces actionable roster decisions.
Available as a **local web app** (primary) and a set of **CLI scripts**.

---

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Always use `.venv/bin/python` (or activate the venv first). Python 3.14.

---

## Web app

The web app is the primary interface. It exposes all analysis views in a browser-accessible
format with color-coded tables, sortable columns, and HTMX async loading.

```bash
.venv/bin/uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```

Access at `http://localhost:8000`. Use `--host 0.0.0.0` to reach it from your phone or
tablet on the same network.

### Views

**Lineup** (`/lineup`)
Your next-week roster split into active and bench, scored by the model. Bench players
who outscore a weaker active player at the same position are flagged "START?".
Use `?week=current` to see the current ESPN lineup instead of the Tuesday projection.

**Free Agents** (`/fa`)
Top available free agents ranked by composite score. Filter by position with `?pos=SS`,
`?pos=OF`, `?pos=SP`, etc.

**Upgrades** (`/upgrade`)
Two modes depending on whether you pass a position:
- No position: table of "Drop X → Add Y" showing the delta in composite score for each
  roster slot where a better FA exists.
- With position (`?pos=OF`, `?pos=SP`, etc.): combined table of your players and top FAs
  at that position, sorted by composite score. Your roster rows are highlighted in blue.

**Week Projection** (`/week`)
Projected fantasy points for next week: `pts/game × games your team plays`. Starters use
rotation cadence (games÷5); relievers use historical appearances/week × team game share.

**Schedule** (`/schedule`)
Full schedule breakdown: your players' team game counts for next week, projected start
dates for your starters, plus the same view for top available free agents.

**Percentiles** (`/percentiles`)
Baseball Savant percentile heatmap for your roster — xwOBA, xBA, xSLG, EV, Brl%, HH%,
bat speed, K%, BB%, sprint speed for hitters; xERA, FBv, Whf%, K%, BB%, Brl% for pitchers.
Toggle `?year=2025` or `?year=2026`. Cells colored red→yellow→green on 0–99 scale.

**Compare** (`/compare`)
Head-to-head comparison of any players. Pass comma-separated names as `?names=Judge,Stanton`.
Partial name matching works. Add `?debug=1` for the full scoring model breakdown.

**Simulate** (`/simulate`)
"If I had Week X's lineup, how many points would it score in Week Y?"

Uses the ESPN Tuesday roster projection for the lineup week. For scoring:
- Players who were rostered anywhere in the league during the scoring week use ESPN actual
  fantasy points (full-week matchup total, not just one day).
- Players who were free agents use FanGraphs game stats for the scoring week date range
  with your league's scoring weights applied directly. Quality starts, no-hitters, and
  perfect games are not available from the date-range API and are excluded (pitcher totals
  may be slightly understated as a result).

### Cache and refresh

Scored data (FanGraphs + scoring model) is cached to `cache/hitters.parquet` and
`cache/pitchers.parquet` and reused across restarts. ESPN roster/FA data has a 5-minute
in-memory cache.

Add `?fresh=1` to any URL to bust the cache, re-fetch from FanGraphs and ESPN, and
write new parquet files. Data age is shown in the navbar.

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

**composite_score** = 80% pts/game + 20% xwOBA z-score (hitters) or xFIP z-score (pitchers).

---

## CLI scripts

### `analyze.py` — main analysis CLI

```bash
# Full report: your roster + top FAs + upgrade opportunities
.venv/bin/python analyze.py

# Just your roster scores
.venv/bin/python analyze.py --roster

# Top free agents (all positions)
.venv/bin/python analyze.py --fa

# Top free agents at a specific position
.venv/bin/python analyze.py --fa --pos SS
.venv/bin/python analyze.py --fa --pos SP

# Upgrade opportunities
.venv/bin/python analyze.py --upgrade

# Head-to-head player comparison (partial name match works)
.venv/bin/python analyze.py --compare "Correa" "Bichette"
.venv/bin/python analyze.py --compare "Skenes" "Peralta"

# Control row count (default 25)
.venv/bin/python analyze.py --fa --pos OF --n 15
```

**Output columns:**
- `Yr` — which season the data is from (2026 = has current season data)
- `PA` / `IP` — current season volume
- `xwOBA` / `xERA` — Statcast quality signal (tiebreaker)
- `K%`, `BB%` — time-decayed rate stats
- `pts_per_game` — projected fantasy points per game
- `composite_score` — final ranking metric (80% pts/G + 20% quality)
- `note` — "small sample" (<150 PA / <30 IP) or "2025 data"

---

### `percentiles.py` — Baseball Savant percentile rankings (CLI)

```bash
.venv/bin/python percentiles.py                  # next-week roster, 2026
.venv/bin/python percentiles.py --week current   # current ESPN lineup
.venv/bin/python percentiles.py --year 2025      # historical season
```

Shows Savant percentile rankings for every player on your roster. All percentiles are
on the 0–99 scale where 99 = best in MLB (inverted where necessary, e.g. low K% = 99).

---

### `monthly_decay.py` — inspect the time-decay model

```bash
.venv/bin/python monthly_decay.py
```

Prints the decay weight table and top 25 players by total PA with their decayed K%, BB%,
OBP, SLG, SB/G. Useful for sanity-checking player profiles or verifying the model.

---

### `bat_speed.py` — 2026 bat speed leaderboard

```bash
.venv/bin/python bat_speed.py
```

Shows top 25 players by bat speed (mph) with hard swing rate, whiff%, and swing count.
Bat speed is always 2026-only — used as a leading indicator for players the market
may not have priced in yet.

---

### `fetch_data.py` — verify data pipeline

```bash
.venv/bin/python fetch_data.py
```

Fetches and previews hitter/pitcher data. Run this to confirm the pipeline works after
changing `config.py` or if you suspect cache issues.

---

### `questions.py` — one-off analyses

```bash
.venv/bin/python questions.py
```

Targeted analyses that don't fit the general model (Bichette replacement rankings,
Caballero vs Cruz SB/K math, etc.).

---

## Key files

| File | Purpose |
|------|---------|
| `config.py` | League ID, scoring weights, team name, thresholds — edit to change league settings |
| `fetch_data.py` | FanGraphs data fetching + unified time-decay application |
| `monthly_decay.py` | PA-weighted exponential decay across 2025+2026 monthly splits |
| `fetch_espn.py` | ESPN API: roster, free agents, historical roster state by date |
| `fetch_schedule.py` | MLB Stats API: weekly game counts and projected start dates |
| `score_players.py` | Rate stats → projected pts/G and composite score |
| `bat_speed.py` | Baseball Savant bat speed leaderboard (2026-only) |
| `percentiles.py` | Savant percentile rankings; also imported by the web app |
| `analyze.py` | Main CLI — all analysis modes |
| `questions.py` | One-off targeted analyses |
| `web/app.py` | FastAPI routes |
| `web/reports.py` | Data-shaping layer — all views return DataFrames, no printing |
| `web/render.py` | DataFrame → color-coded Bootstrap HTML table |
| `web/data.py` | Two-level cache (in-memory TTL + parquet disk) |

---

## Adjusting the model

**Change decay rate or off-season gap** — edit `monthly_decay.py`:
```python
OFF_SEASON_MONTHS = 2   # compressed months between Sep 2025 and Apr 2026
DECAY = 0.85            # per-month decay factor
```

**Disable time decay** (use season-level rates only):
```python
from fetch_data import get_hitters
hitters = get_hitters(time_decay=False)
```

**The 2026 month list updates automatically.** `monthly_decay.py` computes active months
at runtime — no manual edits needed as the season progresses.

**Clear parquet cache:**
```bash
rm cache/hitters.parquet cache/pitchers.parquet
```

**Clear pybaseball HTTP cache:**
```bash
rm -rf $(python -c "import pybaseball; print(pybaseball.cache.get_cache_dir())")
```

---

## Caveats

- **Small 2026 samples**: Through early April, most players have 15–40 PA. Treat anyone
  flagged "small sample" as noisy. The model handles this via PA-weighting, not filtering.
- **Name matching**: ESPN and FanGraphs names don't always match exactly. If a player
  shows "No data found," try a shorter partial name.
- **Recently traded players**: FanGraphs shows `"- - -"` as team. The web app falls back
  to the ESPN `proTeam` attribute for schedule lookups.
- **Defense is worthless**: This scoring system awards zero points for defensive metrics.
  Sprint speed matters only as a stolen base predictor.
- **Pitcher time decay**: Not applied to pitchers (too few monthly IP splits to be
  reliable). Pitcher scores use 2026 season-level stats with 2025 as supplement.
- **Simulate QS/NH/PG**: FanGraphs date-range stats don't include quality starts,
  no-hitters, or perfect games. Pitcher totals in the simulator may be slightly low.
