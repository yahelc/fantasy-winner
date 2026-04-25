# pip install fastapi uvicorn[standard] jinja2
# Run: .venv/bin/uvicorn web.app:app --host 0.0.0.0 --port 8000 --reload

import subprocess
import sys
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from web import data as data_module
from web import reports
from web.render import df_to_html

app = FastAPI()
BASE = Path(__file__).parent
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")
templates = Jinja2Templates(directory=BASE / "templates")

try:
    _GIT_DEPLOY_TIME = subprocess.check_output(
        ["git", "log", "-1", "--format=%cd", "--date=format:%b %-d %-I:%M%p"],
        stderr=subprocess.DEVNULL,
    ).decode().strip().lower()
except Exception:
    _GIT_DEPLOY_TIME = "unknown"
templates.env.globals["git_deploy_time"] = _GIT_DEPLOY_TIME


def _error_fragment(msg: str) -> HTMLResponse:
    return HTMLResponse(
        f'<div class="alert alert-danger"><strong>Error:</strong> {msg}</div>',
        status_code=200,
    )


# ---------------------------------------------------------------------------
# Root redirect
# ---------------------------------------------------------------------------

@app.get("/", response_class=RedirectResponse)
async def root():
    return RedirectResponse(url="/lineup")


# ---------------------------------------------------------------------------
# Lineup
# ---------------------------------------------------------------------------

@app.get("/lineup", response_class=HTMLResponse)
async def lineup_shell(request: Request, week: str = "next", fresh: int = 0):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(url=f"/lineup?week={week}", status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "lineup.html", {"week": week, "cache_info": cache_info})


@app.get("/lineup/data", response_class=HTMLResponse)
async def lineup_data(request: Request, week: str = "next"):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_lineup_data(league, hitters, pitchers, week=week)

        score_cols = ["pts_per_game", "composite_score"]
        pct_fmt_cols = ["K%", "BB%"]
        float_cols = ["xwOBA", "xERA", "xFIP", "pts_per_week"]
        int_cols = ["PA", "PA_total", "G", "G_total", "GS", "SB", "Yr"]

        ctx["h_active_html"] = df_to_html(
            ctx["h_active"], score_cols=score_cols,
            pct_fmt_cols=pct_fmt_cols, float_cols=float_cols, int_cols=int_cols,
        )
        ctx["h_bench_html"] = df_to_html(
            ctx["h_bench"], score_cols=score_cols,
            pct_fmt_cols=pct_fmt_cols, float_cols=float_cols, int_cols=int_cols,
        )
        ctx["p_active_html"] = df_to_html(
            ctx["p_active"], score_cols=score_cols,
            pct_fmt_cols=pct_fmt_cols, float_cols=float_cols, int_cols=int_cols,
        )
        ctx["p_bench_html"] = df_to_html(
            ctx["p_bench"], score_cols=score_cols,
            pct_fmt_cols=pct_fmt_cols, float_cols=float_cols, int_cols=int_cols,
        )
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "lineup_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Free Agents
# ---------------------------------------------------------------------------

@app.get("/fa", response_class=HTMLResponse)
async def fa_shell(request: Request, pos: str = "", n: int = 25, fresh: int = 0):
    if fresh:
        data_module.invalidate()
        url = f"/fa?pos={pos}&n={n}"
        return RedirectResponse(url=url, status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "fa.html", {"pos": pos, "n": n, "cache_info": cache_info})


@app.get("/fa/data", response_class=HTMLResponse)
async def fa_data(request: Request, pos: str = "", n: int = 200):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_fa_data(league, hitters, pitchers, position=pos or None, n=n)

        score_cols = ["pts_per_game", "composite_score"]
        pct_fmt_cols = ["K%", "BB%"]
        float_cols = ["xwOBA", "xERA", "xFIP", "pts_per_week"]
        int_cols = ["PA", "PA_total", "G", "G_total", "GS", "SB", "Yr"]

        ctx["hitters_html"] = df_to_html(
            ctx["hitters"], score_cols=score_cols, pct_fmt_cols=pct_fmt_cols,
            float_cols=float_cols, int_cols=int_cols,
        ) if ctx["hitters"] is not None else None

        ctx["pitchers_html"] = df_to_html(
            ctx["pitchers"], score_cols=score_cols, pct_fmt_cols=pct_fmt_cols,
            float_cols=float_cols, int_cols=int_cols,
        ) if ctx["pitchers"] is not None else None
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "fa_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Upgrades
# ---------------------------------------------------------------------------

@app.get("/upgrade", response_class=HTMLResponse)
async def upgrade_shell(request: Request, pos: str = "", fresh: int = 0):
    if fresh:
        data_module.invalidate()
        url = f"/upgrade?pos={pos}"
        return RedirectResponse(url=url, status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "upgrade.html", {"pos": pos, "cache_info": cache_info})


@app.get("/upgrade/data", response_class=HTMLResponse)
async def upgrade_data(request: Request, pos: str = "", n: int = 200):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()

        if pos:
            ctx = reports.get_upgrade_pos_data(league, hitters, pitchers, position=pos, n=n)
            score_cols = ["pts_per_game", "composite_score"]
            pct_fmt_cols = ["K%", "BB%"]
            float_cols = ["xwOBA", "xERA", "xFIP", "pts_per_week"]
            int_cols = ["PA", "PA_total", "G", "G_total", "GS", "SB", "Yr"]
            ctx["combined_html"] = df_to_html(
                ctx["combined"], score_cols=score_cols, pct_fmt_cols=pct_fmt_cols,
                float_cols=float_cols, int_cols=int_cols,
            )
            ctx["mode"] = "pos"
        else:
            ctx = reports.get_upgrade_data(league, hitters, pitchers, n=n)
            ctx["upgrades_html"] = df_to_html(
                ctx["upgrades"],
                score_cols=["My Score", "FA Score", "Delta"],
            )
            ctx["mode"] = "all"
        ctx["pos"] = pos
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "upgrade_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Week projection
# ---------------------------------------------------------------------------

@app.get("/week", response_class=HTMLResponse)
async def week_shell(request: Request, fresh: int = 0):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(url="/week", status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "week.html", {"cache_info": cache_info})


@app.get("/week/data", response_class=HTMLResponse)
async def week_data(request: Request):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_week_data(league, hitters, pitchers)

        score_cols = ["pts_per_game", "proj_week_pts", "pts_per_week"]
        int_cols = ["gms", "GS", "Yr"]
        float_cols = []

        ctx["hitters_html"] = df_to_html(
            ctx["hitters"], score_cols=score_cols, int_cols=int_cols,
        )
        ctx["pitchers_html"] = df_to_html(
            ctx["pitchers"], score_cols=score_cols, int_cols=int_cols,
        )
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "week_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------

@app.get("/schedule", response_class=HTMLResponse)
async def schedule_shell(request: Request, fresh: int = 0):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(url="/schedule", status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "schedule.html", {"cache_info": cache_info})


@app.get("/schedule/data", response_class=HTMLResponse)
async def schedule_data(request: Request, n: int = 100):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_schedule_data(league, hitters, pitchers, n=n)

        score_cols = ["pts_per_game", "composite_score", "pts_per_week"]
        int_cols = ["gms"]

        ctx["my_h_html"] = df_to_html(ctx["my_h"], score_cols=score_cols, int_cols=int_cols)
        ctx["my_p_html"] = df_to_html(ctx["my_p"], score_cols=score_cols, int_cols=int_cols)
        ctx["fa_h_html"] = df_to_html(ctx["fa_h"], score_cols=score_cols, int_cols=int_cols)
        ctx["fa_p_html"] = df_to_html(ctx["fa_p"], score_cols=score_cols, int_cols=int_cols)
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "schedule_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Percentiles
# ---------------------------------------------------------------------------

@app.get("/percentiles", response_class=HTMLResponse)
async def percentiles_shell(
    request: Request, year: int = 2026, week: str = "next",
    source: str = "roster", pos: str = "", team: str = "", fresh: int = 0,
):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(
            url=f"/percentiles?year={year}&week={week}&source={source}&pos={pos}&team={team}",
            status_code=302,
        )
    league = data_module.get_league_cached()
    teams = [t.team_name for t in league.teams]
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "percentiles.html", {
        "year": year, "week": week, "source": source, "pos": pos,
        "team": team, "teams": teams, "cache_info": cache_info,
    })


@app.get("/percentiles/data", response_class=HTMLResponse)
async def percentiles_data(
    request: Request, year: int = 2026, week: str = "next",
    source: str = "roster", pos: str = "", team: str = "",
):
    try:
        league = data_module.get_league_cached()
        ctx = reports.get_percentiles_data(
            league, week=week, year=year, source=source, pos=pos, team=team,
        )

        from percentiles import HITTER_COLS, PITCHER_COLS

        # Build list of display column names for percentile coloring
        h_display_cols = [h for _, h, _, _ in HITTER_COLS]
        p_display_cols = [h for _, h, _, _ in PITCHER_COLS]

        ctx["hitters_html"] = df_to_html(ctx["hitters"], pct_cols=h_display_cols)
        ctx["pitchers_html"] = df_to_html(ctx["pitchers"], pct_cols=p_display_cols)
        ctx["h_bench_html"] = (
            df_to_html(ctx["h_bench"], pct_cols=h_display_cols)
            if ctx.get("h_bench") is not None else None
        )
        ctx["p_bench_html"] = (
            df_to_html(ctx["p_bench"], pct_cols=p_display_cols)
            if ctx.get("p_bench") is not None else None
        )
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "percentiles_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------

@app.get("/compare/suggest")
async def compare_suggest(q: str = ""):
    if not q or len(q) < 1:
        return []
    q_lower = q.lower()
    try:
        hitters, pitchers = data_module.get_scored_data()
        import unicodedata
        def _strip(s):
            return "".join(c for c in unicodedata.normalize("NFD", str(s)) if unicodedata.category(c) != "Mn")
        names = set()
        for df in (hitters, pitchers):
            if "Name" in df.columns:
                for name in df["Name"]:
                    if q_lower in _strip(str(name)).lower() or q_lower in str(name).lower():
                        names.add(str(name))
        return sorted(names)[:15]
    except Exception:
        return []


@app.get("/compare", response_class=HTMLResponse)
async def compare_shell(request: Request, names: str = "", debug: int = 0, fresh: int = 0):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(url=f"/compare?names={names}&debug={debug}", status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "compare.html", {"names": names, "debug": debug, "cache_info": cache_info})


@app.get("/compare/data", response_class=HTMLResponse)
async def compare_data(request: Request, names: str = "", debug: int = 0):
    try:
        if not names.strip():
            return HTMLResponse("<p class='text-muted'>Enter player names above to compare.</p>")

        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_compare_data(names, hitters, pitchers, debug=bool(debug))

        score_cols = ["pts_per_game", "composite_score", "xwOBA_zscore", "xFIP_zscore"]
        pct_fmt_cols = ["K%", "BB%"]
        float_cols = ["xwOBA", "xERA", "xFIP", "pts_per_week"]
        int_cols = ["PA", "PA_total", "G", "G_total", "GS", "SB", "Yr"]

        ctx["h_html"] = df_to_html(
            ctx["h_rows"], score_cols=score_cols, pct_fmt_cols=pct_fmt_cols,
            float_cols=float_cols, int_cols=int_cols,
        ) if not ctx["h_rows"].empty else None

        ctx["p_html"] = df_to_html(
            ctx["p_rows"], score_cols=score_cols, pct_fmt_cols=pct_fmt_cols,
            float_cols=float_cols, int_cols=int_cols,
        ) if not ctx["p_rows"].empty else None

        # Savant percentiles for compared players
        from percentiles import _fetch_percentiles, _build_table, HITTER_COLS, PITCHER_COLS
        from config import SEASON as _SEASON
        h_display_cols = [h for _, h, _, _ in HITTER_COLS]
        p_display_cols = [h for _, h, _, _ in PITCHER_COLS]
        try:
            h_names = list(ctx["h_rows"]["Name"]) if not ctx["h_rows"].empty else []
            p_names = list(ctx["p_rows"]["Name"]) if not ctx["p_rows"].empty else []
            if h_names:
                h_pct = _fetch_percentiles("batter", _SEASON)
                ctx["h_pct_html"] = df_to_html(_build_table(h_names, h_pct, HITTER_COLS), pct_cols=h_display_cols)
            else:
                ctx["h_pct_html"] = None
            if p_names:
                p_pct = _fetch_percentiles("pitcher", _SEASON)
                ctx["p_pct_html"] = df_to_html(_build_table(p_names, p_pct, PITCHER_COLS), pct_cols=p_display_cols)
            else:
                ctx["p_pct_html"] = None
        except Exception:
            ctx["h_pct_html"] = None
            ctx["p_pct_html"] = None

        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "compare_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------

@app.get("/decisions", response_class=HTMLResponse)
async def decisions_shell(request: Request, fresh: int = 0):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(url="/decisions", status_code=302)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "decisions.html", {"cache_info": cache_info})


@app.get("/decisions/data", response_class=HTMLResponse)
async def decisions_data(request: Request):
    try:
        league = data_module.get_league_cached()
        ctx = reports.get_decisions_data(league)
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "decisions_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Top Performances leaderboard
# ---------------------------------------------------------------------------

@app.get("/top", response_class=HTMLResponse)
async def top_shell(
    request: Request,
    type: str = "all",
    grouping: str = "week",
    team: str = "",
    week: int = 0,
    n: int = 50,
    fresh: int = 0,
):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(
            url=f"/top?type={type}&grouping={grouping}&team={team}&week={week}&n={n}",
            status_code=302,
        )
    league = data_module.get_league_cached()
    current_week = getattr(league, "currentMatchupPeriod", 1)
    teams = sorted(t.team_name for t in league.teams)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "top.html", {
        "type": type,
        "grouping": grouping,
        "team": team,
        "week": week,
        "n": n,
        "teams": teams,
        "current_week": current_week,
        "cache_info": cache_info,
    })


@app.get("/top/debug")
async def top_debug(week: int = 0):
    """Diagnostic: dump raw ESPN API response structure for top-performances parsing."""
    import requests as _req, json as _json
    from config import ESPN_S2, ESPN_SWID, SEASON

    league = data_module.get_league_cached()
    current_week = getattr(league, "currentMatchupPeriod", 1)
    w = week or current_week

    endpoint = (
        f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
        f"/seasons/{SEASON}/segments/0/leagues/{league.league_id}"
    )
    cookies = {"espn_s2": ESPN_S2, "SWID": ESPN_SWID} if ESPN_S2 and ESPN_SWID else {}
    filt = {"schedule": {"filterMatchupPeriodIds": {"value": [w]}}}

    try:
        r = _req.get(
            endpoint,
            params={"view": ["mMatchup", "mMatchupScore"]},
            headers={"User-Agent": "Mozilla/5.0", "x-fantasy-filter": _json.dumps(filt)},
            cookies=cookies, timeout=20,
        )
        status = r.status_code
        data = r.json()
    except Exception as e:
        return {"error": str(e)}

    schedule = data.get("schedule", [])
    out = {"http_status": status, "week": w, "schedule_count": len(schedule), "matchups": []}

    for m in schedule[:4]:  # inspect up to 4 matchups
        info = {"matchupPeriodId": m.get("matchupPeriodId")}
        for sk in ("home", "away"):
            side = m.get(sk, {})
            entries = side.get("rosterForMatchupPeriod", {}).get("entries", [])
            sample_stats = []
            if entries:
                pl = entries[0].get("playerPoolEntry", {}).get("player", {}) or entries[0].get("player", {})
                for s in pl.get("stats", [])[:6]:
                    sample_stats.append({
                        "scoringPeriodId": s.get("scoringPeriodId"),
                        "statSourceId": s.get("statSourceId"),
                        "statSplitTypeId": s.get("statSplitTypeId"),
                        "seasonId": s.get("seasonId"),
                        "appliedTotal": s.get("appliedTotal"),
                    })
            info[sk] = {
                "teamId": side.get("teamId"),
                "rosterForMatchupPeriod_entries": len(entries),
                "pointsByScoringPeriod_keys": list(side.get("pointsByScoringPeriod", {}).keys())[:5],
                "sample_player": entries[0].get("playerPoolEntry", {}).get("player", {}).get("fullName") if entries else None,
                "sample_player_stats": sample_stats,
            }
        out["matchups"].append(info)

    return out



async def top_data(
    request: Request,
    type: str = "all",
    grouping: str = "week",
    team: str = "",
    week: int = 0,
    n: int = 50,
):
    try:
        league = data_module.get_league_cached()
        ctx = reports.get_top_performances_data(
            league,
            grouping=grouping,
            type_filter=type,
            team_filter=team,
            week=week,
            n=n,
        )
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "top_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Simulate
# ---------------------------------------------------------------------------

@app.get("/simulate", response_class=HTMLResponse)
async def simulate_shell(
    request: Request,
    lineup_week: int = 0,
    scoring_week: int = 0,
    fresh: int = 0,
):
    if fresh:
        data_module.invalidate()
        return RedirectResponse(
            url=f"/simulate?lineup_week={lineup_week}&scoring_week={scoring_week}",
            status_code=302,
        )
    league = data_module.get_league_cached()
    current_week = getattr(league, "currentMatchupPeriod", 1)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "simulate.html", {
        "lineup_week":   lineup_week or current_week,
        "scoring_week":  scoring_week or max(1, current_week - 1),
        "current_week":  current_week,
        "cache_info":    cache_info,
    })


@app.get("/simulate/data", response_class=HTMLResponse)
async def simulate_data(
    request: Request,
    lineup_week: int = 1,
    scoring_week: int = 1,
):
    try:
        league = data_module.get_league_cached()
        ctx = reports.get_simulate_data(league, lineup_week, scoring_week)
        ctx["rows_html"] = df_to_html(ctx["rows"], score_cols=["Pts"])
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "simulate_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))


# ---------------------------------------------------------------------------
# Matchup SP view
# ---------------------------------------------------------------------------

@app.get("/matchup", response_class=HTMLResponse)
async def matchup_shell(
    request: Request,
    matchup_id: int = Query(default=None),
    fresh: int = 0,
):
    if fresh:
        data_module.invalidate()
        qs = f"?matchup_id={matchup_id}" if matchup_id is not None else ""
        return RedirectResponse(url=f"/matchup{qs}", status_code=302)
    league = data_module.get_league_cached()
    current_mp = getattr(league, "currentMatchupPeriod", 1)
    cache_info = data_module.get_cache_info()
    return templates.TemplateResponse(request, "matchup.html", {
        "matchup_id": matchup_id,
        "current_mp": current_mp,
        "cache_info": cache_info,
    })


@app.get("/matchup/data", response_class=HTMLResponse)
async def matchup_data(
    request: Request,
    matchup_id: int = Query(default=None),
):
    try:
        league = data_module.get_league_cached()
        hitters, pitchers = data_module.get_scored_data()
        ctx = reports.get_matchup_data(league, matchup_id, pitchers, hitters)
        ctx["cache_info"] = data_module.get_cache_info()
        return templates.TemplateResponse(request, "matchup_data.html", ctx)
    except Exception as e:
        traceback.print_exc()
        return _error_fragment(str(e))
