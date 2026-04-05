# fantasy-winner

Analytics tooling for ESPN fantasy baseball (Ferryhawk Tuah, league 1056949816, H2H points).
Runs as both a **web app** (primary interface) and a set of **CLI scripts**.

## Stack
- Python 3.14, virtualenv at `.venv/` — always use `.venv/bin/python`
- `pybaseball` for FanGraphs + Statcast data
- `espn-api` for roster/FA/league data
- `requests` for direct Baseball Savant CSV endpoints and ESPN API calls
- **Web**: FastAPI + Jinja2 + HTMX + Bootstrap 5.3

## Running the web app
```bash
.venv/bin/uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload
```
Access at `http://localhost:8000` (or LAN IP for phone/tablet access).

## Key files

### Root — data pipeline
- `config.py` — league ID, scoring weights, team name, season, thresholds
- `fetch_data.py` — pulls hitter/pitcher stats from FanGraphs; applies unified time decay
- `fetch_espn.py` — ESPN league, roster, free agent access; `get_roster_for_day()` for any date
- `fetch_schedule.py` — MLB Stats API for weekly game counts and projected starts
- `score_players.py` — converts stats to fantasy pts/G and composite score
- `monthly_decay.py` — PA-weighted exponential decay across 2025+2026 monthly splits
- `bat_speed.py` — 2026 bat speed from Baseball Savant
- `percentiles.py` — Baseball Savant percentile rankings (CLI); also imported by web app
- `analyze.py` — CLI entrypoint: --roster, --fa, --upgrade, --compare

### Web app — `web/`
- `web/app.py` — FastAPI routes; all routes use HTMX shell+data pattern
- `web/reports.py` — data-shaping layer (no printing); returns DataFrames/dicts for all views
- `web/render.py` — `df_to_html()`: DataFrame → Bootstrap HTML table with color coding
- `web/data.py` — two-level cache: 5-min TTL for ESPN league, 30-min TTL + parquet disk for scored data
- `web/templates/` — Jinja2 templates (base.html + per-route shell/data pairs)
- `web/static/tables.js` — client-side sort, row expansion, column tooltips
- `web/static/style.css` — table styling, sticky columns/headers, mine-row highlight

## Web app routes

| Route | Description |
|-------|-------------|
| `/lineup` | Next-week roster (from ESPN Tuesday projection), split active/bench. Flags bench players who should be started over a weaker active player at the same position. |
| `/fa` | Free agents ranked by composite score. Filter by position (`?pos=SS`). |
| `/upgrade` | No pos param: table of "Drop X, Add Y" with delta scores. With pos param (e.g. `?pos=OF`): combined view of my players + FAs at that position, MINE rows highlighted. |
| `/week` | Projects fantasy points for next week using schedule game counts × pts/game. Pitchers use rotation cadence (starters: gms/5 starts) or historical appearance rate (relievers). |
| `/schedule` | Full schedule breakdown: my players' team game counts + projected starts, plus top available FAs with same info. |
| `/percentiles` | Baseball Savant percentile heatmap for my roster. Toggle 2025/2026 season. Percentile columns color-coded red→yellow→green (0–99). |
| `/compare` | Head-to-head player comparison. Comma-separated names, partial match works. |
| `/simulate` | "If I had Week X's lineup, how many points would it score in Week Y?" Rostered players use ESPN actual points (full matchup total from `rosterForMatchupPeriod`). Free agents use FanGraphs `batting_stats_range`/`pitching_stats_range` with league scoring weights applied. QS/NH/PG not available from date-range API — pitcher points may be slightly understated. |

All routes support `?fresh=1` to invalidate cache and re-fetch.

## HTMX shell+data pattern
Every route has two handlers:
- `GET /route` → serves the shell template immediately (spinner shown)
- `GET /route/data` → does the slow work; HTMX swaps it in on load

After every HTMX swap, `tables.js` re-initializes sort handlers, row controls, and tooltips via `htmx:afterSettle`.

## Caching
- **ESPN league** (`get_league_cached`): 5-min in-memory TTL
- **Scored data** (`get_scored_data`): checks `cache/hitters.parquet` + `cache/pitchers.parquet` on disk; 30-min in-memory TTL on top
- `?fresh=1` clears the in-memory cache and redirects; next request re-fetches from FanGraphs/ESPN and writes new parquet files
- Data age (parquet mtime) is shown in the navbar

## Scoring model
- **pts/G**: built from K%, BB%, SB/G, OBP, SLG (rate stats × league weights)
- **composite_score**: 80% pts/G + 20% xwOBA z-score (hitters) or xFIP z-score (pitchers)
- **Time decay**: unified PA-weighted exponential decay across 2025+2026 monthly splits
  - Apr 2026 = weight 1.00; Sep 2025 = 0.72 (2-month compressed off-season); Apr 2025 = 0.32
  - PA-weighting means early April 2026 (~5 PA) is naturally down-weighted vs full months
  - No explicit year blend — the PA×decay math handles relative importance automatically
- **xwOBA/Statcast**: season-level (2026 if available, else 2025) — only a tiebreaker signal

## ESPN API notes
- `league.box_scores(matchup_period=N)` returns `rosterForCurrentScoringPeriod` — **today's stats only**, not the week total. For full-week per-player points, fetch `mMatchup + mMatchupScore` views directly and read `rosterForMatchupPeriod → player.stats[scoringPeriodId=0].appliedTotal`.
- `get_roster_for_day(team_name, league, target_date)` uses `scoringPeriodId` offset from `league.scoringPeriodId` to fetch any past or future roster state.
- FanGraphs team codes differ from MLB Stats API: SDP→SD, TBR→TB, KCR→KC, WSN→WSH, CHW→CWS, SFG→SF. Applied via `_FG_TO_SCHED` map.
- FanGraphs shows `"- - -"` for recently traded players. Fall back to ESPN `player.proTeam`.

## Conventions
- Always show tables before interpretation
- Lead with verdicts, not methodology
- Note data season on every table — early 2026 samples are small (20-30 PA)
- Bat speed stays 2026-only; scoring stats use unified time decay
- Defense has zero value in this scoring system — never factor it in
