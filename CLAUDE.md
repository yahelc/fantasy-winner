# fantasy-winner

Analytics tooling for ESPN fantasy baseball (Ferryhawk Tuah, league 1056949816).

## Stack
- Python 3.14, virtualenv at `.venv/` — always use `.venv/bin/python`
- `pybaseball` for FanGraphs + Statcast data
- `espn-api` for roster/FA/league data
- `requests` for direct Baseball Savant CSV endpoints

## Key files
- `config.py` — league weights, team name, season, blend weights
- `fetch_data.py` — pulls hitter/pitcher data; applies unified time decay by default
- `fetch_espn.py` — ESPN league, roster, free agent access
- `score_players.py` — converts stats to fantasy pts/G and composite score
- `monthly_decay.py` — unified PA-weighted time decay across 2025+2026 monthly splits
- `bat_speed.py` — 2026 bat speed from Baseball Savant
- `analyze.py` — main CLI: --roster, --fa, --upgrade, --compare
- `questions.py` — one-off targeted analyses

## Scoring model
- **pts/G**: built from K%, BB%, SB/G, OBP, SLG (rate stats × league weights)
- **composite_score**: 80% pts/G + 20% xwOBA z-score (quality tiebreaker)
- **Time decay**: unified PA-weighted exponential decay across 2025+2026 monthly splits (default ON)
  - Apr 2026 = weight 1.00; Sep 2025 = 0.72 (2-month compressed off-season); Apr 2025 = 0.32
  - PA-weighting means early April 2026 (~5 PA) is naturally down-weighted vs full months
  - No explicit year blend — the PA×decay math handles relative importance automatically
- **xwOBA/Statcast**: season-level (2026 if available, else 2025) — only a tiebreaker signal

## Conventions
- Always show tables before interpretation
- Lead with verdicts, not methodology
- Note data_season on every table — early 2026 samples are small (20-30 PA)
- Bat speed stays 2026-only; scoring stats use unified time decay
- Defense has zero value in this scoring system — never factor it in
