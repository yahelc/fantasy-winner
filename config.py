"""
League configuration for Ferryhawk Tuah (ESPN League ID: 1056949816)
H2H Points league — 2026 season
"""

LEAGUE_ID = 1056949816
SEASON = 2026
MY_TEAM_NAME = "Onderdonk"  # partial match is fine

# Fantasy points per stat event
BATTING_WEIGHTS = {
    "R":  1,
    "TB": 1,
    "RBI": 1,
    "BB": 1,
    "K":  -1,  # strikeouts hurt
    "SB": 2,   # premium
}

PITCHING_WEIGHTS = {
    "IP":  3,
    "H":  -1,
    "ER": -2,
    "BB": -1,
    "K":   1,
    "QS":  5,
    "W":   5,
    "L":  -2,
    "SV":  5,
    "NH": 10,
    "PG": 20,
}

# Statcast / FanGraphs columns we care about for hitters
# These are used to *project* fantasy scoring, not as direct inputs
HITTER_STATCAST_COLS = [
    "Name", "Team",
    "PA", "AB", "G",
    "xBA", "xOBP", "xSLG", "xwOBA",
    "EV",          # avg exit velocity
    "Barrel%",     # barrel rate
    "HardHit%",    # hard hit rate
    "K%",          # strikeout rate (want LOW)
    "BB%",         # walk rate (want HIGH)
    "Sprint Speed", # stolen base proxy
    "SB",          # actual SBs this season
    "HR", "R", "RBI", "TB",  # counting stats for context
]

# Statcast / FanGraphs columns for pitchers
PITCHER_STATCAST_COLS = [
    "Name", "Team",
    "G", "GS", "IP",
    "xERA", "xFIP",
    "K%",          # strikeout rate (want HIGH)
    "BB%",         # walk rate (want LOW)
    "Barrel%",     # barrels allowed (want LOW)
    "HardHit%",    # hard contact allowed
    "BABIP",
    "K", "BB", "W", "L", "SV", "QS", "ER", "H",
]

# Minimum PA/IP thresholds to include in analysis
MIN_PA = 20          # low for early season; flag small samples via MIN_PA_RELIABLE
MIN_IP = 5          # low threshold — early season has limited IP
MIN_PA_RELIABLE = 150  # flag players below this as small-sample
MIN_IP_RELIABLE = 30

SP_STARTS_CAP = 9  # max SP starts credited per team per week
