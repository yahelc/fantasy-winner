"""
Pulls roster and free agent data from ESPN Fantasy API.
Public league — no auth required.
"""

import requests
from datetime import date
from espn_api.baseball import League
from espn_api.baseball.player import Player
from espn_api.baseball.constant import POSITION_MAP
from config import LEAGUE_ID, SEASON

_ESPN_ENDPOINT = (
    f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/flb"
    f"/seasons/{SEASON}/segments/0/leagues/{LEAGUE_ID}"
)
_HEADERS = {"User-Agent": "Mozilla/5.0"}
_SEASON_START = date(SEASON, 3, 27)   # Opening Day


def get_league() -> League:
    return League(league_id=LEAGUE_ID, year=SEASON)


def get_my_team(league: League, team_name: str = None, team_id: int = None):
    """
    Return your team object. Match by name substring or team ID.
    If neither provided, prints all teams and returns None.
    """
    if team_id is not None:
        for team in league.teams:
            if team.team_id == team_id:
                return team

    if team_name is not None:
        for team in league.teams:
            if team_name.lower() in team.team_name.lower():
                return team

    print("Teams in league:")
    for t in league.teams:
        owners = ", ".join(o.get("firstName", "") + " " + o.get("lastName", "") for o in (t.owners or []))
        print(f"  [{t.team_id}] {t.team_name} — owner: {owners}")
    return None


def get_roster_for_day(team_name: str, league: "League" = None,
                       target_date: date = None) -> list:
    """
    Fetch roster as ESPN will show it on target_date (defaults to next Tuesday).
    Returns a list of Player-like objects with .name, .lineupSlot, .eligibleSlots.

    Uses the league's current scoringPeriodId as an anchor so the day offset
    is always correct regardless of when in the season this is called.
    """
    from datetime import timedelta

    today = date.today()
    if target_date is None:
        days_until_tuesday = (1 - today.weekday()) % 7  # Tuesday = weekday 1
        if days_until_tuesday == 0:
            days_until_tuesday = 7
        target_date = today + timedelta(days=days_until_tuesday)

    # Anchor off ESPN's known current scoring period rather than a hardcoded date
    if league is not None:
        current_period = league.scoringPeriodId
        delta = (target_date - today).days
        scoring_period = current_period + delta
    else:
        scoring_period = (target_date - _SEASON_START).days + 1

    r = requests.get(
        _ESPN_ENDPOINT,
        params={"view": ["mTeam", "mRoster"], "scoringPeriodId": scoring_period},
        headers=_HEADERS,
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()

    for team in data["teams"]:
        name = (team.get("name") or
                team.get("location", "") + " " + team.get("nickname", "")).strip()
        if team_name.lower() in name.lower():
            players = []
            for entry in team["roster"]["entries"]:
                p = Player(entry, SEASON)
                players.append(p)
            print(f"  Roster as of {target_date} (scoring period {scoring_period}): "
                  f"{len(players)} players")
            return players

    return []


def get_roster_names(team) -> list[str]:
    """Return list of player names on a roster."""
    return [p.name for p in team.roster]


def get_free_agents(league: League, position: str = None, size: int = 100) -> list:
    """
    Fetch available free agents, optionally filtered by position.
    position: 'SP', 'RP', 'C', '1B', '2B', '3B', 'SS', 'OF', 'DH', etc.
    """
    fas = league.free_agents(size=size)
    if position:
        fas = [p for p in fas if position in (p.eligibleSlots or [])]
    return fas


def player_summary(player) -> dict:
    """Extract key fields from an ESPN player object."""
    return {
        "name": player.name,
        "position": player.position,
        "eligible_slots": player.eligibleSlots,
        "pro_team": getattr(player, "proTeam", "?"),
        "injured": getattr(player, "injured", False),
        "injury_status": getattr(player, "injuryStatus", "ACTIVE"),
        "avg_draft_position": getattr(player, "avg_draft_position", None),
    }


if __name__ == "__main__":
    league = get_league()
    print(f"League: {league.settings.name}")
    print(f"Teams: {len(league.teams)}\n")

    my_team = get_my_team(league)  # will print all teams so you can find yours

    print("\nTop 20 available free agents:")
    fas = get_free_agents(league, size=20)
    for p in fas:
        s = player_summary(p)
        print(f"  {s['name']} ({s['position']}) — {s['pro_team']} — injured: {s['injured']}")
