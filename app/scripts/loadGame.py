from nba_api.stats.static import teams
import psycopg2
from nba_api.stats.library.parameters import Season
from nba_api.stats.library.parameters import SeasonType
from time import sleep
from nba_api.stats.endpoints import leaguegamefinder
from datetime import datetime

team_abbreviations = [
    # current teams
    "ATL", "BOS", "BRK", "CHI", "CHA", "CLE", "DAL", "DEN",
    "DET", "GSW", "HOU", "IND", "LAC", "LAL", "MEM", "MIA",
    "MIL", "MIN", "NOP", "NYK", "OKC", "ORL", "PHI", "PHX",
    "POR", "SAC", "SAS", "TOR", "UTA", "WAS",

    # historical teams in play-by-play era
    "NJN",  # New Jersey Nets (pre-Brooklyn)
    "CHB",  # Original Charlotte Hornets before relocation
    "VAN",  # Vancouver Grizzlies
    "SEA"   # Seattle SuperSonics
]

# connect to postgres db
conn = psycopg2.connect(
    database="streamd",
    user="docker",
    password="docker",
    port=5431
)
cur = conn.cursor()

nba_teams = teams.get_teams()

team_ids = [team['id'] for team in nba_teams]

id_map = {}
for team in nba_teams:
    id_map[team['abbreviation']] = team['id']
id_map['NJN'] = 1
id_map['CHB'] = 2
id_map['VAN'] = 3
id_map['SEA'] = 4

for id in team_ids:
    print(f'LOADING GAMES FOR TEAM {id}')
    gamefinder_regular = leaguegamefinder.LeagueGameFinder(team_id_nullable=id, season_type_nullable="Regular Season")    
    games = gamefinder_regular.get_data_frames()[0]
    for row, game in games.iterrows():
        # if the game includes a non play-by-play era team
        team_1 = game['TEAM_ABBREVIATION']
        team_2 = game['MATCHUP'][-3:]
        if team_1 == 'CHH':
            team_1 = 'CHA'
        if team_2 == 'CHH':
            team_2 = 'CHA'
        if team_1 not in team_abbreviations or team_2 not in team_abbreviations:
            continue
        game_id = game['GAME_ID']
        season_id = game['SEASON_ID']
        home_team_id = -1
        away_team_id = -1
        if game['MATCHUP'][4] == '@': # home team away
            away_team_id = game['TEAM_ID']
            home_team_id = id_map[team_2]
        else:
            home_team_id = game['TEAM_ID']
            away_team_id = id_map[team_2]
        game_date = datetime.strptime(game['GAME_DATE'], "%Y-%m-%d").date()
        season_type = "regular"
        cur.execute("INSERT INTO GAME (game_id, season_id, home_team_id, away_team_id, game_date, season_type) " \
        "Values (%s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;", (game_id, season_id, home_team_id, away_team_id, game_date, season_type))

    gamefinder_playoff = leaguegamefinder.LeagueGameFinder(team_id_nullable=id, season_type_nullable="Playoffs")
    games = gamefinder_playoff.get_data_frames()[0]
    for row, game in games.iterrows():
        team_1 = game['TEAM_ABBREVIATION']
        team_2 = game['MATCHUP'][-3:]
        if team_1 == 'CHH':
            team_1 = 'CHA'
        if team_2 == 'CHH':
            team_2 = 'CHA'
        if team_1 not in team_abbreviations or team_2 not in team_abbreviations:
            continue
        game_id = game['GAME_ID']
        season_id = game['SEASON_ID']
        home_team_id = -1
        away_team_id = -1
        if game['MATCHUP'][4] == '@': # home team away
            away_team_id = game['TEAM_ID']
            home_team_id = id_map[team_2]
        else:
            home_team_id = game['TEAM_ID']
            away_team_id = id_map[team_2]
        game_date = datetime.strptime(game['GAME_DATE'], "%Y-%m-%d").date()
        season_type = "playoff"
        cur.execute("INSERT INTO GAME (game_id, season_id, home_team_id, away_team_id, game_date, season_type) " \
        "Values (%s, %s, %s, %s, %s, %s) ON CONFLICT (game_id) DO NOTHING;", (game_id, season_id, home_team_id, away_team_id, game_date, season_type))

conn.commit()