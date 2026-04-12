# pip install fastapi uvicorn[standard] jinja2
import time
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

import pandas as pd
from fetch_data import get_hitters, get_pitchers
from fetch_espn import get_league as _get_league
from score_players import score_hitters, score_pitchers

CACHE_DIR = Path(__file__).parent.parent / "cache"
HITTERS_CACHE  = CACHE_DIR / "hitters.parquet"
PITCHERS_CACHE = CACHE_DIR / "pitchers.parquet"

_mem: dict = {}   # in-memory cache: key -> (value, timestamp)
_LEAGUE_TTL = 300
_DATA_TTL   = 1800


def invalidate():
    _mem.clear()
    for f in (HITTERS_CACHE, PITCHERS_CACHE):
        f.unlink(missing_ok=True)


def get_cache_info() -> dict:
    info = {"data_age": None, "league_fresh": "league" in _mem}
    if HITTERS_CACHE.exists():
        mtime = datetime.fromtimestamp(HITTERS_CACHE.stat().st_mtime)
        info["data_age"] = mtime.strftime("%b %d %H:%M")
    return info


def get_league_cached(fresh: bool = False):
    key = "league"
    now = time.monotonic()
    if not fresh and key in _mem:
        val, ts = _mem[key]
        if now - ts < _LEAGUE_TTL:
            return val
    league = _get_league()
    _mem[key] = (league, now)
    return league


def get_scored_data(fresh: bool = False):
    key = "scored"
    now = time.monotonic()
    if not fresh and key in _mem:
        val, ts = _mem[key]
        if now - ts < _DATA_TTL:
            return val
    if not fresh and HITTERS_CACHE.exists() and PITCHERS_CACHE.exists():
        raw_h = pd.read_parquet(HITTERS_CACHE)
        raw_p = pd.read_parquet(PITCHERS_CACHE)
    else:
        raw_h = get_hitters()
        raw_p = get_pitchers()
        CACHE_DIR.mkdir(exist_ok=True)
        raw_h.to_parquet(HITTERS_CACHE, index=False)
        raw_p.to_parquet(PITCHERS_CACHE, index=False)
    result = (score_hitters(raw_h), score_pitchers(raw_p))
    _mem[key] = (result, now)
    return result
